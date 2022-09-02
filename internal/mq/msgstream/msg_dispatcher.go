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

type (
	pchannel = string
	vchannel = string
)

type DispatchFunc func(msgPack *MsgPack) map[vchannel]*MsgPack

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

func NewTtMsgDispatcher(nodeID UniqueID, roleName string, pchannelName pchannel, dispatchFunc DispatchFunc, factory Factory) *TtMsgDispatcher {
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

func (m *TtMsgDispatcher) Run() error {
	var err error
	m.stream, err = m.factory.NewTtMsgStream(context.Background())
	if err != nil {
		return err
	}
	subName := fmt.Sprintf("%s-%s-%d", m.roleName, m.pchannelName, m.nodeID)
	m.stream.AsConsumer([]pchannel{m.pchannelName}, subName)
	defer m.stream.Close()
	for {
		select {
		case <-m.closeChan:
			log.Info("close TtMsgDispatcher", zap.String("role", m.roleName), zap.String("pchannel", m.pchannelName))
			return nil
		case pack, ok := <-m.stream.Chan():
			if !ok {
				return fmt.Errorf("stream closed, role = %s, pchannel = %s", m.roleName, m.pchannelName)
			}
			dispatchResults := m.dispatchFunc(pack)
			m.consumersMu.Lock()
			for k, v := range dispatchResults {
				if _, ok = m.consumers[k]; ok {
					// TODO: select?
					m.consumers[k] <- v
				}
			}
			m.consumersMu.Unlock()
			m.currentTick.Store(pack.EndTs)
		}
	}
}

func (m *TtMsgDispatcher) Close() {
	m.closeOnce.Do(func() {
		close(m.closeChan)
	})
}

// chaseToCurrent would consume messages from given position to current consumed position.
func (m *TtMsgDispatcher) chaseToCurrent(ctx context.Context, vchannel vchannel, position *MsgPosition) error {
	readStream, err := m.factory.NewTtMsgStream(ctx)
	if err != nil {
		return err
	}
	defer readStream.Close()
	subName := fmt.Sprintf("%s-%d-%s", m.roleName, m.nodeID, vchannel)
	readStream.AsConsumer([]string{position.GetChannelName()}, subName)

	err = readStream.Seek([]*internalpb.MsgPosition{position})
	if err != nil {
		return err
	}
	hasMore := true
	for hasMore {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context done, role = %s, vchannel = %s", m.roleName, vchannel)
		case msgPack, ok := <-readStream.Chan():
			if !ok {
				return fmt.Errorf("readStream chan has been closed, role = %s, vchannel = %s", m.roleName, vchannel)
			}
			if msgPack == nil {
				continue
			}

			dispatchResults := m.dispatchFunc(msgPack)
			m.consumersMu.Lock()
			for k, v := range dispatchResults {
				if _, ok = m.consumers[k]; ok {
					// TODO: select?
					m.consumers[k] <- v
				}
			}
			m.consumersMu.Unlock()

			if msgPack.EndTs >= m.currentTick.Load() {
				hasMore = false
			}
		}
	}
	return nil
}

func (m *TtMsgDispatcher) addConsumer(vchannel vchannel) error {
	m.consumersMu.Lock()
	defer m.consumersMu.Unlock()
	if _, ok := m.consumers[vchannel]; ok {
		return fmt.Errorf("addConsumer failed, vchannel already existed, vchannel = %s", vchannel)
	}
	m.consumers[vchannel] = make(chan *MsgPack, consumerChannelSize)
	return nil
}

func (m *TtMsgDispatcher) Register(vchannel vchannel, position *MsgPosition) (<-chan *MsgPack, error) {
	ctx, cancel := context.WithTimeout(context.Background(), chaseTimeout)
	defer cancel()

	err := m.addConsumer(vchannel)
	if err != nil {
		return nil, err
	}

	if position.GetTimestamp() >= m.currentTick.Load() {
		log.Info("position is latter than current, no need to chase",
			zap.String("role", m.roleName), zap.String("vchannel", vchannel))
		m.consumersMu.Lock()
		defer m.consumersMu.Unlock()
		return m.consumers[vchannel], nil
	}

	err = m.chaseToCurrent(ctx, vchannel, position)
	if err != nil {
		return nil, err
	}

	m.consumersMu.Lock()
	defer m.consumersMu.Unlock()
	return m.consumers[vchannel], nil
}
