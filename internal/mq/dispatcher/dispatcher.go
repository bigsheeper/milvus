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
	"context"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"go.uber.org/zap"
	"sync"
	"time"
)

type signal int32

const (
	start     signal = 0
	pause     signal = 1
	resume    signal = 2
	terminate signal = 3
)

const MaxTolerantLag = 3 * time.Second

type dispatcher struct {
	pchannel string

	done    chan struct{}
	wg      sync.WaitGroup
	lagChan chan *msgstream.MsgPosition

	vchannelsMu sync.RWMutex
	vchannels   map[string]chan<- *msgstream.MsgPack

	curPosMu sync.RWMutex
	curPos   *internalpb.MsgPosition

	stream msgstream.MsgStream
}

func newDispatcher(factory msgstream.Factory, pchannel string, position *internalpb.MsgPosition, subPos mqwrapper.SubscriptionInitialPosition, lagChan chan *msgstream.MsgPosition) (*dispatcher, error) {
	stream, err := factory.NewTtMsgStream(context.Background())
	if err != nil {
		return nil, err
	}
	if position != nil {
		position.ChannelName = funcutil.ToPhysicalChannel(position.ChannelName)
		stream.AsConsumer([]string{pchannel}, "aaa", mqwrapper.SubscriptionPositionUnknown) // TODO: sub-name
		err = stream.Seek([]*internalpb.MsgPosition{position})
		if err != nil {
			return nil, err
		}
	} else {
		stream.AsConsumer([]string{pchannel}, "aaa", subPos) // TODO: sub-name
	}

	d := &dispatcher{
		pchannel:  pchannel,
		done:      make(chan struct{}),
		lagChan:   lagChan,
		vchannels: make(map[string]chan<- *msgstream.MsgPack),
		stream:    stream,
	}
	return d, nil
}

func (d *dispatcher) handle(signal signal) {
	switch signal {
	case start:
		d.wg.Add(1)
		go d.work()
		log.Info("dispatcher started", zap.String("pchannel", d.pchannel))
	case pause:
		d.done <- struct{}{}
		d.wg.Wait()
		log.Info("dispatcher paused", zap.String("pchannel", d.pchannel))
	case resume:
		d.wg.Add(1)
		go d.work()
		log.Info("dispatcher resumed", zap.String("pchannel", d.pchannel))
	case terminate:
		d.done <- struct{}{}
		d.stream.Close()
		// TODO: unsub
		d.wg.Wait()
		log.Info("dispatcher stopped", zap.String("pchannel", d.pchannel))
	default:
		panic("invalid signal in dispatcher handler")
	}
}

func (d *dispatcher) work() {
	defer d.wg.Done()
	for {
		select {
		case <-d.done:
			return
		case pack := <-d.stream.Chan(): // TODO: check ok
			d.curPosMu.Lock()
			d.curPos = pack.EndPositions[0]
			d.curPosMu.Unlock()

			// group by vchannel
			d.vchannelsMu.RLock()
			packs := make(map[string]*msgstream.MsgPack, len(d.vchannels))
			for vchannel := range d.vchannels {
				packs[vchannel] = &msgstream.MsgPack{
					BeginTs:        pack.BeginTs,
					EndTs:          pack.EndTs,
					Msgs:           make([]msgstream.TsMsg, 0),
					StartPositions: pack.StartPositions,
					EndPositions:   pack.EndPositions,
				}
			}
			d.vchannelsMu.RUnlock()
			for _, msg := range pack.Msgs {
				if msg.Type() == commonpb.MsgType_CreateCollection {
					continue // TODO: optimize it
				}
				if msg.VChannel() == "" {
					panic(fmt.Errorf("msg's vchannel should not be null, msgType:%s", msg.Type().String()))
				}
				if _, ok := packs[msg.VChannel()]; !ok {
					continue
				}
				packs[msg.VChannel()].Msgs = append(packs[msg.VChannel()].Msgs, msg)
				fmt.Println("=======", tsoutil.PhysicalTime(pack.EndTs), ", msg.VChannel():", msg.VChannel())
			}
			// dispatch
			lagChannels := make([]string, 0)
			d.vchannelsMu.RLock()
			for vchannel, p := range packs {
				select {
				case <-time.After(MaxTolerantLag):
					lagChannels = append(lagChannels, vchannel)
					log.Warn("vchannel delayed too long", zap.String("vchannel", vchannel), zap.Duration("lag", MaxTolerantLag))
				case d.vchannels[vchannel] <- p:
				}
			}
			d.vchannelsMu.RUnlock()

			// separate lag channels
			for _, vchannel := range lagChannels {
				pos := proto.Clone(pack.StartPositions[0]).(*internalpb.MsgPosition)
				pos.ChannelName = vchannel
				d.lagChan <- pos
				d.removeTarget(vchannel)
			}
		}
	}
}

func (d *dispatcher) getCurPosition() *internalpb.MsgPosition {
	d.curPosMu.RLock()
	defer d.curPosMu.RUnlock()
	return proto.Clone(d.curPos).(*internalpb.MsgPosition)
}

func (d *dispatcher) getTarget() (string, chan<- *msgstream.MsgPack) {
	d.vchannelsMu.RLock()
	defer d.vchannelsMu.RUnlock()
	for vch, ch := range d.vchannels {
		return vch, ch
	}
	panic("should not get here")
}

func (d *dispatcher) addTarget(vchannel string, output chan<- *msgstream.MsgPack) {
	d.vchannelsMu.Lock()
	defer d.vchannelsMu.Unlock()
	d.vchannels[vchannel] = output
	log.Info("dispatcher add target", zap.String("vchannel", vchannel))
}

func (d *dispatcher) removeTarget(vchannel string) {
	d.vchannelsMu.Lock()
	defer d.vchannelsMu.Unlock()
	delete(d.vchannels, vchannel)
	log.Info("dispatcher remove target", zap.String("vchannel", vchannel))
}

func (d *dispatcher) targetNum() int {
	d.vchannelsMu.Lock()
	defer d.vchannelsMu.Unlock()
	return len(d.vchannels)
}
