// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dispatcher

import (
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"sync"
	"time"
)

const targetChanSize = 1024

type checker struct {
	pchannel string
	lagChan  chan *msgstream.MsgPosition

	// TODO: maybe need mutex
	primeDispatcher *dispatcher
	soloDispatchers map[string]*dispatcher

	factory   msgstream.Factory
	closeChan chan struct{}
	closeOnce sync.Once
}

func newChecker(pchannel string, factory msgstream.Factory) *checker {
	return &checker{
		pchannel:        pchannel,
		lagChan:         make(chan *msgstream.MsgPosition, 10),
		soloDispatchers: make(map[string]*dispatcher),
		factory:         factory,
		closeChan:       make(chan struct{}),
	}
}

func (c *checker) addDispatcher(vchannel string, pos *internalpb.MsgPosition) (<-chan *msgstream.MsgPack, error) {
	target := make(chan *msgstream.MsgPack, targetChanSize)
	d, err := newDispatcher(c.factory, c.pchannel, pos, c.lagChan)
	if err != nil {
		return nil, err
	}
	d.addTarget(vchannel, target)
	if c.primeDispatcher == nil {
		c.primeDispatcher = d
	} else {
		c.soloDispatchers[vchannel] = d
	}
	return target, nil
}

func (c *checker) removeDispatcher(vchannel string) {
	if c.primeDispatcher != nil {
		c.primeDispatcher.removeTarget(vchannel)
	}
	if _, ok := c.soloDispatchers[vchannel]; ok {
		c.soloDispatchers[vchannel].removeTarget(vchannel)
	}
}

func (c *checker) isEmpty() bool {
	return c.primeDispatcher.targetNum() == 0 && len(c.soloDispatchers) == 0
}

func (c *checker) closeAll() {
	c.closeOnce.Do(func() {
		c.closeChan <- struct{}{}
	})
	if c.primeDispatcher != nil {
		c.primeDispatcher.handle(terminate)
	}
	for _, d := range c.soloDispatchers {
		d.handle(terminate)
	}
}

func (c *checker) check() {
	timer := time.NewTimer(1 * time.Second)
	for {
		select {
		case <-c.closeChan:
			return
		case <-timer.C:
			primePos := c.primeDispatcher.getCurPosition()
			for vchannel, sd := range c.soloDispatchers {
				if sd.getCurPosition().GetTimestamp() == primePos.GetTimestamp() {
					c.merge(vchannel)
				}
			}
		case pos := <-c.lagChan:
			c.separate(pos.ChannelName, pos)
		}
	}
}

func (c *checker) merge(vchannel string) {
	c.primeDispatcher.handle(pause)
	c.soloDispatchers[vchannel].handle(pause)
	c.primeDispatcher.addTarget(c.soloDispatchers[vchannel].getTarget())
	c.soloDispatchers[vchannel].handle(terminate)
	delete(c.soloDispatchers, vchannel)
	c.soloDispatchers[vchannel].handle(resume)
}

func (c *checker) separate(vchannel string, pos *internalpb.MsgPosition) {
	newSolo, err := newDispatcher(c.factory, c.pchannel, pos, c.lagChan)
	if err != nil {
		panic(err)
	}
	c.soloDispatchers[vchannel] = newSolo
	newSolo.handle(start)
}
