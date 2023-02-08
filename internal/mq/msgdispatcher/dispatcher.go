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

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type signal int32

const (
	start     signal = 0
	pause     signal = 1
	resume    signal = 2
	terminate signal = 3
)

var signalString = map[int32]string{
	0: "start",
	1: "pause",
	2: "resume",
	3: "terminate",
}

func (s signal) String() string {
	return signalString[int32(s)]
}

const MaxTolerantLag = 3 * time.Second // TODO: move to config

type dispatcher struct {
	done chan struct{}
	wg   sync.WaitGroup
	once sync.Once

	isMain   bool // indicates if it's a main dispatcher
	pchannel string
	lagChan  chan *lagInfo
	curTs    atomic.Uint64

	targetsMu sync.RWMutex
	targets   map[string]chan<- *msgstream.MsgPack

	stream msgstream.MsgStream
}

func newDispatcher(factory msgstream.Factory, isMain bool, pchannel string, position *internalpb.MsgPosition, subName string, subPos mqwrapper.SubscriptionInitialPosition, lagChan chan *lagInfo) (*dispatcher, error) {
	log.With(zap.String("pchannel", pchannel),
		zap.String("subName", subName),
		zap.Bool("isMainDispatcher", isMain))
	stream, err := factory.NewTtMsgStream(context.Background())
	if err != nil {
		return nil, err
	}
	if position != nil && len(position.MsgID) != 0 {
		position.ChannelName = funcutil.ToPhysicalChannel(position.ChannelName)
		stream.AsConsumer([]string{pchannel}, subName, mqwrapper.SubscriptionPositionUnknown)
		err = stream.Seek([]*internalpb.MsgPosition{position})
		if err != nil {
			log.Error("dispatcher seek failed", zap.Error(err))
			return nil, err
		}
		log.Info("dispatcher seek successfully", zap.Time("posTime", tsoutil.PhysicalTime(position.GetTimestamp())),
			zap.Duration("tsLag", time.Since(tsoutil.PhysicalTime(position.GetTimestamp()))))
	} else {
		stream.AsConsumer([]string{pchannel}, subName, subPos)
		log.Info("dispatcher asConsumer successfully")
	}

	d := &dispatcher{
		isMain:   isMain,
		pchannel: pchannel,
		done:     make(chan struct{}, 1),
		lagChan:  lagChan,
		targets:  make(map[string]chan<- *msgstream.MsgPack),
		stream:   stream,
	}
	return d, nil
}

func (d *dispatcher) handle(signal signal) {
	log.With(zap.String("pchannel", d.pchannel),
		zap.String("signal", signal.String()), zap.Bool("isMainDispatcher", d.isMain))
	log.Info("dispatcher get signal")
	switch signal {
	case start:
		d.wg.Add(1)
		go d.work()
	case pause:
		d.done <- struct{}{}
		d.wg.Wait()
	case resume:
		d.wg.Add(1)
		go d.work()
	case terminate:
		d.done <- struct{}{}
		d.wg.Wait()
		d.once.Do(func() {
			d.stream.Close()
		})
	default:
		err := fmt.Errorf("invalid signal in dispatcher handler, pchannel = %s", d.pchannel)
		log.Error(err.Error())
		panic(err)
	}
	log.Info("dispatcher handled signal done")
}

func (d *dispatcher) work() {
	log.With(zap.String("pchannel", d.pchannel), zap.Bool("isMainDispatcher", d.isMain))
	log.Info("dispatcher begin to work")
	defer d.wg.Done()
	for {
		select {
		case <-d.done:
			log.Info("dispatcher stopped working")
			return
		case pack := <-d.stream.Chan():
			if pack == nil || len(pack.EndPositions) != 1 {
				log.Error("dispatcher consumed invalid msgPack")
				continue
			}
			d.curTs.Store(pack.EndPositions[0].GetTimestamp())

			// init packs for all target vchannels, even though there's no msg in pack,
			// but we still need to dispatch time ticks to the targets.
			d.targetsMu.RLock()
			packs := make(map[string]*msgstream.MsgPack, len(d.targets))
			for vchannel := range d.targets {
				packs[vchannel] = &msgstream.MsgPack{
					BeginTs:        pack.BeginTs,
					EndTs:          pack.EndTs,
					Msgs:           make([]msgstream.TsMsg, 0),
					StartPositions: pack.StartPositions,
					EndPositions:   pack.EndPositions,
				}
			}
			d.targetsMu.RUnlock()

			// group messages by vchannels
			for _, msg := range pack.Msgs {
				if msg.VChannel() == "" {
					// for non-dml msg, such as CreateCollection, DropCollection, ...
					// we need to dispatch it to all the vchannels.
					for k := range packs {
						packs[k].Msgs = append(packs[k].Msgs, msg)
					}
					continue
				}
				if _, ok := packs[msg.VChannel()]; !ok {
					continue
				}
				packs[msg.VChannel()].Msgs = append(packs[msg.VChannel()].Msgs, msg)
			}

			// splits lag channels
			d.targetsMu.RLock()
			for vchannel, p := range packs {
				select {
				case <-time.After(MaxTolerantLag):
					d.lagChan <- &lagInfo{
						vchannel: vchannel,
						pos:      pack.StartPositions[0],
						target:   d.targets[vchannel],
					}
					d.removeTarget(vchannel)
					log.Warn("time lag is too long for vchannel, sent lagInfo",
						zap.String("vchannel", vchannel), zap.Duration("lag", MaxTolerantLag))
				case d.targets[vchannel] <- p:
				}
			}
			d.targetsMu.RUnlock()
		}
	}
}

func (d *dispatcher) getCurTs() typeutil.Timestamp {
	return d.curTs.Load()
}

func (d *dispatcher) addTarget(vchannel string, output chan<- *msgstream.MsgPack) {
	d.targetsMu.Lock()
	defer d.targetsMu.Unlock()
	d.targets[vchannel] = output
	log.Info("dispatcher add new target", zap.String("vchannel", vchannel), zap.Bool("isMainDispatcher", d.isMain))
}

func (d *dispatcher) getTarget(vchannel string) (chan<- *msgstream.MsgPack, error) {
	d.targetsMu.RLock()
	defer d.targetsMu.RUnlock()
	if ch, ok := d.targets[vchannel]; ok {
		return ch, nil
	}
	return nil, fmt.Errorf("cannot find target in dispatcher, vchannel = %s, isMainDispatcher = %t", vchannel, d.isMain)
}

func (d *dispatcher) targetNum() int {
	d.targetsMu.RLock()
	defer d.targetsMu.RUnlock()
	return len(d.targets)
}

func (d *dispatcher) removeTarget(vchannel string) {
	log.With(zap.String("vchannel", vchannel), zap.Bool("isMainDispatcher", d.isMain))
	d.targetsMu.Lock()
	defer d.targetsMu.Unlock()
	if _, ok := d.targets[vchannel]; ok {
		delete(d.targets, vchannel)
		log.Info("dispatcher removed target")
	} else {
		log.Warn("target not exist when removeTarget")
	}
}

func (d *dispatcher) closeTarget(vchannel string) {
	log.With(zap.String("vchannel", vchannel), zap.Bool("isMainDispatcher", d.isMain))
	d.targetsMu.Lock()
	defer d.targetsMu.Unlock()
	if ch, ok := d.targets[vchannel]; ok {
		close(ch)
		delete(d.targets, vchannel)
		log.Info("dispatcher closed target")
	} else {
		log.Warn("target not exist when closeTarget")
	}
}
