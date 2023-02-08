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
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/retry"
)

type lagInfo struct {
	vchannel string
	pos      *msgstream.MsgPosition
	target   chan<- *msgstream.MsgPack
}

type checker struct {
	role     string
	nodeID   int64
	pchannel string
	lagChan  chan *lagInfo

	checkPeriod    time.Duration
	targetChanSize int

	dispatchersMu   sync.RWMutex
	mainDispatcher  *dispatcher
	soloDispatchers map[string]*dispatcher

	factory   msgstream.Factory
	closeChan chan struct{}
	closeOnce sync.Once
}

func newChecker(pchannel string, role string, nodeID int64, factory msgstream.Factory) *checker {
	const defaultTargetChanSize = 1024
	log.Info("create new checker", zap.String("role", role),
		zap.Int64("nodeID", nodeID), zap.String("pchannel", pchannel))
	return &checker{
		role:            role,
		nodeID:          nodeID,
		pchannel:        pchannel,
		lagChan:         make(chan *lagInfo, 100), // TODO: move to config
		checkPeriod:     1 * time.Second,          // TODO: move to config
		targetChanSize:  defaultTargetChanSize,
		soloDispatchers: make(map[string]*dispatcher),
		factory:         factory,
		closeChan:       make(chan struct{}),
	}
}

func (c *checker) addDispatcher(vchannel string, pos *internalpb.MsgPosition, subPos mqwrapper.SubscriptionInitialPosition) (<-chan *msgstream.MsgPack, error) {
	log := log.With(zap.String("role", c.role), zap.Int64("nodeID", c.nodeID), zap.String("vchannel", vchannel))
	target := make(chan *msgstream.MsgPack, c.targetChanSize)
	subName := fmt.Sprintf("%s-%d-%s", c.role, c.nodeID, vchannel) // TODO: dyh, maybe should not use vchannel in subName

	c.dispatchersMu.Lock()
	defer c.dispatchersMu.Unlock()
	isMain := c.mainDispatcher == nil
	d, err := newDispatcher(c.factory, isMain, c.pchannel, pos, subName, subPos, c.lagChan)
	if err != nil {
		return nil, err
	}
	d.addTarget(vchannel, target)
	if isMain {
		c.mainDispatcher = d
		log.Info("addDispatcher as mainDispatcher")
	} else {
		c.soloDispatchers[vchannel] = d
		log.Info("addDispatcher as a new soloDispatcher")
	}
	d.handle(start)
	return target, nil
}

func (c *checker) removeDispatcher(vchannel string) {
	log := log.With(zap.String("role", c.role), zap.Int64("nodeID", c.nodeID), zap.String("vchannel", vchannel))
	c.dispatchersMu.Lock()
	defer c.dispatchersMu.Unlock()
	if c.mainDispatcher != nil {
		c.mainDispatcher.closeTarget(vchannel)
		log.Info("close target from mainDispatcher done")
		if c.mainDispatcher.targetNum() == 0 {
			c.mainDispatcher.handle(terminate)
			c.mainDispatcher = nil
			log.Info("remove mainDispatcher done")
		}
	}
	if _, ok := c.soloDispatchers[vchannel]; ok {
		c.soloDispatchers[vchannel].closeTarget(vchannel)
		c.soloDispatchers[vchannel].handle(terminate)
		delete(c.soloDispatchers, vchannel)
		log.Info("remove soloDispatcher done")
	}
}

func (c *checker) dispatcherNum() int {
	c.dispatchersMu.RLock()
	defer c.dispatchersMu.RUnlock()
	var res int
	if c.mainDispatcher != nil {
		res++
	}
	return res + len(c.soloDispatchers)
}

func (c *checker) close() {
	c.closeOnce.Do(func() {
		c.closeChan <- struct{}{}
	})
	log.Info("checker closed", zap.String("role", c.role),
		zap.Int64("nodeID", c.nodeID), zap.String("pchannel", c.pchannel))
}

func (c *checker) run() {
	log := log.With(zap.String("role", c.role), zap.Int64("nodeID", c.nodeID), zap.String("pchannel", c.pchannel))
	log.Info("checker is running...")
	ticker := time.NewTicker(c.checkPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-c.closeChan:
			log.Info("checker exited")
			return
		case <-ticker.C:
			c.dispatchersMu.RLock()
			if c.mainDispatcher == nil {
				c.dispatchersMu.RUnlock()
				continue
			}
			candidates := make(map[string]struct{})
			mainPos := c.mainDispatcher.getCurTs()
			for vchannel, sd := range c.soloDispatchers {
				if sd.getCurTs() == mainPos {
					candidates[vchannel] = struct{}{}
				}
			}
			c.dispatchersMu.RUnlock()
			if len(candidates) > 0 {
				c.merge(candidates)
			}
		case info := <-c.lagChan:
			c.split(info)
		}
	}
}

func (c *checker) merge(vchannels map[string]struct{}) {
	log := log.With(zap.String("role", c.role), zap.Int64("nodeID", c.nodeID))
	log.Info("merging soloDispatchers to mainDispatcher...", zap.Any("vchannel", vchannels))
	c.dispatchersMu.Lock()
	defer c.dispatchersMu.Unlock()
	c.mainDispatcher.handle(pause)
	for vchannel := range vchannels {
		c.soloDispatchers[vchannel].handle(pause)
		// after pause, check time alignment again, if not, evict it and try to merge next time
		if c.mainDispatcher.getCurTs() != c.soloDispatchers[vchannel].getCurTs() {
			c.soloDispatchers[vchannel].handle(resume)
			delete(vchannels, vchannel)
		}
	}
	for vchannel := range vchannels {
		ch, err := c.soloDispatchers[vchannel].getTarget(vchannel)
		if err != nil {
			log.Warn("get invalid target, ignore it because it has been removed", zap.Error(err))
		} else {
			c.mainDispatcher.addTarget(vchannel, ch)
		}
		c.soloDispatchers[vchannel].handle(terminate)
		delete(c.soloDispatchers, vchannel)
	}
	c.mainDispatcher.handle(resume)
	log.Info("merge soloDispatchers to mainDispatcher done",
		zap.Int("vchannelNum", len(vchannels)), zap.Any("vchannel", vchannels))
}

func (c *checker) split(info *lagInfo) {
	log := log.With(zap.String("role", c.role), zap.Int64("nodeID", c.nodeID), zap.String("vchannel", info.vchannel))
	log.Info("splitting soloDispatcher from mainDispatcher...")

	c.dispatchersMu.Lock()
	if _, ok := c.soloDispatchers[info.vchannel]; ok {
		// remove stale soloDispatcher if it existed
		c.soloDispatchers[info.vchannel].handle(terminate)
		delete(c.soloDispatchers, info.vchannel)
	}
	c.dispatchersMu.Unlock()

	var newSolo *dispatcher
	err := retry.Do(context.Background(), func() error {
		var err error
		subName := fmt.Sprintf("%s-%d-%s", c.role, c.nodeID, info.vchannel) // TODO: dyh, maybe should not use vchannel in subName
		newSolo, err = newDispatcher(c.factory, false, c.pchannel, info.pos, subName, mqwrapper.SubscriptionPositionUnknown, c.lagChan)
		return err
	}, retry.Attempts(10))
	if err != nil {
		log.Error("split soloDispatcher from mainDispatcher failed", zap.Error(err))
		panic(err)
	}
	newSolo.addTarget(info.vchannel, info.target)

	c.dispatchersMu.Lock()
	defer c.dispatchersMu.Unlock()
	c.soloDispatchers[info.vchannel] = newSolo
	newSolo.handle(start)
	log.Info("split soloDispatcher from mainDispatcher done")
}
