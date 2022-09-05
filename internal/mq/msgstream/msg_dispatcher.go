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

package msgstream

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
)

const (
	chaseTimeout        = 20 * time.Second
	consumerChannelSize = 65536
)

type chaseStrategy int32

const (
	block chaseStrategy = iota
	exception
)

type TtMsgDispatcher struct {
	nodeID       UniqueID
	roleName     string
	pchannelName pchannel

	consumersMu sync.Mutex
	consumers   map[vchannel]chan *MsgPack

	currentTick  atomic.Uint64
	dispatchFunc DispatchFunc
	stream       MsgStream
	factory      Factory

	closeOnce sync.Once
	closeChan chan struct{}
}

func newTtMsgDispatcher(nodeID UniqueID, roleName string, pchannelName pchannel, dispatchFunc DispatchFunc, factory Factory) *TtMsgDispatcher {
	return &TtMsgDispatcher{
		nodeID:       nodeID,
		roleName:     roleName,
		pchannelName: pchannelName,
		consumers:    make(map[vchannel]chan *MsgPack),
		dispatchFunc: dispatchFunc,
		factory:      factory,
		closeChan:    make(chan struct{}),
	}
}

func (t *TtMsgDispatcher) run() error {
	var err error
	t.stream, err = t.factory.NewTtMsgStream(context.Background())
	if err != nil {
		return err
	}
	subName := fmt.Sprintf("%s-%s-%d", t.roleName, t.pchannelName, t.nodeID)
	t.stream.AsConsumer([]pchannel{t.pchannelName}, subName)
	defer t.stream.Close()
	for {
		select {
		case <-t.closeChan:
			log.Info("close TtMsgDispatcher", zap.String("role", t.roleName), zap.String("pchannel", t.pchannelName))
			return nil
		case pack, ok := <-t.stream.Chan():
			if !ok {
				return fmt.Errorf("stream closed, role = %s, pchannel = %s", t.roleName, t.pchannelName)
			}
			dispatchResults := t.dispatchFunc(pack)
			t.consumersMu.Lock()
			for k, v := range dispatchResults {
				if _, ok = t.consumers[k]; ok {
					// TODO: select?
					t.consumers[k] <- v
				}
			}
			t.consumersMu.Unlock()
			t.currentTick.Store(pack.EndTs)
			fmt.Println("currentTick:", t.currentTick.Load())
		}
	}
}

func (t *TtMsgDispatcher) close() {
	t.closeOnce.Do(func() {
		close(t.closeChan)
	})
}

// chaseToCurrent would consume messages from given position to current consumed position.
func (t *TtMsgDispatcher) chaseToCurrent(ctx context.Context, vchannel vchannel, position *MsgPosition) error {
	readStream, err := t.factory.NewTtMsgStream(ctx)
	if err != nil {
		return err
	}
	defer readStream.Close()
	subName := fmt.Sprintf("%s-%d-%s", t.roleName, t.nodeID, vchannel)
	readStream.AsConsumer([]string{position.GetChannelName()}, subName)

	err = readStream.Seek([]*internalpb.MsgPosition{position})
	if err != nil {
		return err
	}
	hasMore := true
	for hasMore {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context done, role = %s, vchannel = %s", t.roleName, vchannel)
		case msgPack, ok := <-readStream.Chan():
			if !ok {
				return fmt.Errorf("readStream chan has been closed, role = %s, vchannel = %s", t.roleName, vchannel)
			}
			if msgPack == nil {
				continue
			}

			dispatchResults := t.dispatchFunc(msgPack)
			t.consumersMu.Lock()
			for k, v := range dispatchResults {
				if _, ok = t.consumers[k]; ok {
					// TODO: select?
					t.consumers[k] <- v
				}
			}
			t.consumersMu.Unlock()

			if msgPack.EndTs >= t.currentTick.Load() {
				hasMore = false
			}
		}
	}
	return nil
}

func (t *TtMsgDispatcher) addConsumer(vchannel vchannel) error {
	t.consumersMu.Lock()
	defer t.consumersMu.Unlock()
	if _, ok := t.consumers[vchannel]; ok {
		return fmt.Errorf("addConsumer failed, vchannel already existed, vchannel = %s", vchannel)
	}
	t.consumers[vchannel] = make(chan *MsgPack, consumerChannelSize)
	return nil
}

func (t *TtMsgDispatcher) register(vchannel vchannel, position *MsgPosition) (<-chan *MsgPack, error) {
	ctx, cancel := context.WithTimeout(context.Background(), chaseTimeout)
	defer cancel()

	err := t.addConsumer(vchannel)
	if err != nil {
		return nil, err
	}

	if position.GetTimestamp() >= t.currentTick.Load() {
		log.Info("position is latter than current, no need to chase",
			zap.String("role", t.roleName), zap.String("vchannel", vchannel))
		t.consumersMu.Lock()
		defer t.consumersMu.Unlock()
		return t.consumers[vchannel], nil
	}

	err = t.chaseToCurrent(ctx, vchannel, position)
	if err != nil {
		return nil, err
	}

	t.consumersMu.Lock()
	defer t.consumersMu.Unlock()
	return t.consumers[vchannel], nil
}

func (t *TtMsgDispatcher) deregister(vchannel vchannel) {
	t.consumersMu.Lock()
	defer t.consumersMu.Unlock()
	close(t.consumers[vchannel])
	delete(t.consumers, vchannel)
}

func (t *TtMsgDispatcher) getConsumerNum() int {
	t.consumersMu.Lock()
	defer t.consumersMu.Unlock()
	return len(t.consumers)
}
