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

package msgdispatcher

import (
	"context"
	"fmt"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/retry"
	"go.uber.org/zap"
	"sync"
	"time"
)

const targetChanSize = 1024

type checker struct {
	subPrefix string
	pchannel  string
	lagChan   chan *msgstream.MsgPosition

	dispatchersMu   sync.RWMutex
	mainDispatcher  *dispatcher
	soloDispatchers map[string]*dispatcher

	factory   msgstream.Factory
	closeChan chan struct{}
	closeOnce sync.Once
}

func newChecker(pchannel string, subPrefix string, factory msgstream.Factory) *checker {
	log.Info("create new checker", zap.String("pchannel", pchannel))
	return &checker{
		subPrefix:       subPrefix,
		pchannel:        pchannel,
		lagChan:         make(chan *msgstream.MsgPosition, 10),
		soloDispatchers: make(map[string]*dispatcher),
		factory:         factory,
		closeChan:       make(chan struct{}),
	}
}

func (c *checker) addDispatcher(vchannel string, pos *internalpb.MsgPosition, subPos mqwrapper.SubscriptionInitialPosition) (<-chan *msgstream.MsgPack, error) {
	target := make(chan *msgstream.MsgPack, targetChanSize)
	d, err := newDispatcher(c.factory, c.pchannel, pos, fmt.Sprintf("%s-%s", c.subPrefix, vchannel), subPos, c.lagChan)
	if err != nil {
		return nil, err
	}
	d.addTarget(vchannel, target)
	c.dispatchersMu.Lock()
	if c.mainDispatcher == nil {
		c.mainDispatcher = d
		log.Info("checker addDispatcher as mainDispatcher", zap.String("vchannel", vchannel))
	} else {
		c.soloDispatchers[vchannel] = d
		log.Info("checker addDispatcher as a new soloDispatcher", zap.String("vchannel", vchannel))
	}
	c.dispatchersMu.Unlock()
	d.handle(start)
	return target, nil
}

func (c *checker) removeDispatcher(vchannel string) {
	c.dispatchersMu.Lock()
	defer c.dispatchersMu.Unlock()
	if c.mainDispatcher != nil {
		c.mainDispatcher.removeTarget(vchannel)
		c.mainDispatcher.closeTarget(vchannel)
		log.Info("checker remove target from mainDispatcher done", zap.String("vchannel", vchannel))
	}
	if _, ok := c.soloDispatchers[vchannel]; ok {
		c.soloDispatchers[vchannel].handle(terminate)
		c.soloDispatchers[vchannel].removeTarget(vchannel)
		c.soloDispatchers[vchannel].closeTarget(vchannel)
		delete(c.soloDispatchers, vchannel)
		log.Info("checker remove soloDispatcher done", zap.String("vchannel", vchannel))
	}
}

func (c *checker) isEmpty() bool {
	c.dispatchersMu.RUnlock()
	defer c.dispatchersMu.RUnlock()
	return c.mainDispatcher.targetNum() == 0 && len(c.soloDispatchers) == 0
}

func (c *checker) close() {
	c.closeOnce.Do(func() {
		c.closeChan <- struct{}{}
	})
	c.dispatchersMu.RLock()
	defer c.dispatchersMu.RUnlock()
	if c.mainDispatcher != nil {
		c.mainDispatcher.handle(terminate)
	}
	log.Info("checker closed", zap.String("pchannel", c.pchannel))
}

func (c *checker) check() {
	log.Info("checker start", zap.String("pchannel", c.pchannel))
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-c.closeChan:
			log.Info("checker exited", zap.String("pchannel", c.pchannel))
			return
		case <-ticker.C:
			candidates := make(map[string]struct{})
			c.dispatchersMu.RUnlock()
			mainPos := c.mainDispatcher.getCurPosition()
			for vchannel, sd := range c.soloDispatchers {
				if sd.getCurPosition().GetTimestamp() == mainPos.GetTimestamp() {
					candidates[vchannel] = struct{}{}
				}
			}
			c.dispatchersMu.RUnlock()
			c.merge(candidates)
		case pos := <-c.lagChan:
			c.split(pos.ChannelName, pos)
		}
	}
}

func (c *checker) merge(vchannels map[string]struct{}) {
	log.Info("checker is merging soloDispatchers to mainDispatcher...", zap.Any("vchannels", vchannels))
	c.dispatchersMu.Lock()
	defer c.dispatchersMu.Unlock()
	c.mainDispatcher.handle(pause)
	for vchannel := range vchannels {
		c.soloDispatchers[vchannel].handle(pause)
		// after pause, check time alignment again, if not, evict it and try to merge next time
		if c.mainDispatcher.getCurPosition().GetTimestamp() != c.soloDispatchers[vchannel].getCurPosition().GetTimestamp() {
			c.soloDispatchers[vchannel].handle(resume)
			delete(vchannels, vchannel)
		}
	}
	for vchannel := range vchannels {
		ch, err := c.soloDispatchers[vchannel].getTarget(vchannel)
		if err != nil {
			log.Warn("get invalid target from soloDispatcher, ignore it because it has been removed", zap.Error(err))
		} else {
			c.mainDispatcher.addTarget(vchannel, ch)
		}
		c.soloDispatchers[vchannel].handle(terminate)
		delete(c.soloDispatchers, vchannel)
	}
	c.mainDispatcher.handle(resume)
	log.Info("checker merges soloDispatchers to mainDispatcher done", zap.Any("vchannels", vchannels))
}

func (c *checker) split(vchannel string, pos *internalpb.MsgPosition) {
	log.Info("checker is splitting soloDispatcher from mainDispatcher", zap.String("vchannel", vchannel))

	var newSolo *dispatcher
	err := retry.Do(context.Background(), func() error {
		var err error
		newSolo, err = newDispatcher(c.factory, c.pchannel, pos, fmt.Sprintf("%s-%s", c.subPrefix, vchannel), mqwrapper.SubscriptionPositionUnknown, c.lagChan)
		return err
	}, retry.Attempts(10))
	if err != nil {
		log.Error("checker split soloDispatcher from mainDispatcher failed", zap.String("vchannel", vchannel), zap.Error(err))
		panic(err)
	}

	c.dispatchersMu.Lock()
	defer c.dispatchersMu.Unlock()
	c.soloDispatchers[vchannel] = newSolo
	newSolo.handle(start)
	log.Info("checker split soloDispatcher from mainDispatcher done", zap.String("vchannel", vchannel))
}
