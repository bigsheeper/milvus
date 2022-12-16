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

package querynode

import (
	"context"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"sync"
)

// just for test
type state int

const (
	start  state = 0
	pause  state = 1
	resume state = 2
	stop   state = 3
)

type dispatcher struct {
	mu sync.Mutex

	pchannel  string
	vchannels map[string]chan interface{}
	curPos    *internalpb.MsgPosition

	signaller chan state
	wg        sync.WaitGroup

	stream  msgstream.MsgStream // TODO: init
	factory msgstream.Factory
}

func (d *dispatcher) work(done <-chan struct{}) {
	for {
		select {
		case <-done:
			d.wg.Done()
			return
		case pack := <-d.stream.Chan(): // TODO: check ok
			d.mu.Lock()
			d.curPos = pack.EndPositions[0]
			for _, msg := range pack.Msgs {
				msg.ID() // TODO: check msg.VChannel/collection, send to flowgraph
				d.vchannels["vchannel-x"] <- msg
			}
			d.mu.Unlock()
		}
	}
}

func (d *dispatcher) handler(signaller chan state) {
	done := make(chan struct{})
	for {
		signal := <-signaller
		switch signal {
		case start:
			d.wg.Add(1)
			go d.work(done)
		case pause:
			done <- struct{}{}
			d.wg.Wait()
		case resume:
			d.wg.Add(1)
			go d.work(done)
		case stop:
			done <- struct{}{}
			d.wg.Wait()
			return
		}
	}
}

func (d *dispatcher) start(ctx context.Context, position *internalpb.MsgPosition) error {
	stream, err := d.factory.NewTtMsgStream(ctx)
	if err != nil {
		return err
	}
	stream.AsConsumer([]string{d.pchannel}, "aaa", mqwrapper.SubscriptionPositionUnknown)
	err = stream.Seek([]*internalpb.MsgPosition{position})
	if err != nil {
		return err
	}
	d.stream = stream
	d.signaller = make(chan state)
	go d.handler(d.signaller)
	d.signaller <- start
	return nil
}

func (d *dispatcher) register(ctx context.Context, vchannel string, position *internalpb.MsgPosition, input chan interface{}) error {
	d.mu.Lock()
	if position.GetTimestamp() > d.curPos.GetTimestamp() {
		d.vchannels[vchannel] = input
		d.mu.Unlock()
		return nil
	}

	// pull back
	stream, err := d.factory.NewTtMsgStream(ctx)
	if err != nil {
		return err // TODO: roll back
	}
	stream.AsConsumer([]string{d.pchannel}, "aaa", mqwrapper.SubscriptionPositionUnknown)
	err = stream.Seek([]*internalpb.MsgPosition{position})
	if err != nil {
		return err
	}
	hasMore := true
	for hasMore {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case pack := <-stream.Chan():
			d.mu.Lock()
			if pack.EndPositions[0].GetTimestamp() == d.curPos.GetTimestamp() {
				d.vchannels[vchannel] = input
				d.mu.Unlock()
				hasMore = false
				break
			}
			d.mu.Unlock()
			input <- pack // TODO: check
		}
	}

	return nil
}
