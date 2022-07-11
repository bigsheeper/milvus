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
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"sync"
)

// DispatchFunc 由 DataNode QueryNode 传入的分发逻辑，判断消息类型，然后映射到不同的 vchannel
type DispatchFunc func(msgPack *MsgPack) map[string]*MsgPack

type MsgDispatcher struct {
	ctx    context.Context
	cancel context.CancelFunc

	// TODO: add DispatcherConfig
	channelBufferSize uint32
	nodeID            int64

	currentMsgID MessageID
	stream       MsgStream

	pchannel    string
	vchannelsMu sync.Mutex // guards vchannels
	vchannels   map[string]chan *MsgPack

	dispatchFunc DispatchFunc
	factory      Factory
}

func NewMsgDispatcher(ctx context.Context, nodeID int64, pchannel string, df DispatchFunc, factory Factory) *MsgDispatcher {
	ctx1, cancel := context.WithCancel(ctx)
	return &MsgDispatcher{
		ctx:    ctx1,
		cancel: cancel,

		channelBufferSize: 65535, // TODO: config or param
		nodeID:            nodeID,

		pchannel:  pchannel,
		vchannels: make(map[string]chan *MsgPack),

		dispatchFunc: df,
		factory:      factory,
	}
}

func (m *MsgDispatcher) constructSubName(channel string) string {
	return fmt.Sprintf("%d-%s", m.nodeID, channel)
}

// Run 分发服务，将消费到的 msgs 通过 dispatchFunc 进行分组，发送到各个 channel 中
func (m *MsgDispatcher) Run() error {
	var err error
	m.stream, err = m.factory.NewTtMsgStream(m.ctx)
	if err != nil {
		return err
	}
	defer m.stream.Close()

	m.stream.AsConsumer([]string{m.pchannel}, m.constructSubName(m.pchannel))
	log.Info("AsConsumer done")

	for {
		select {
		case <-m.ctx.Done():
			log.Info("done")
			return nil
		case msgPack := <-m.stream.Chan():
			for ch, pack := range m.dispatchFunc(msgPack) {
				// send msg
				m.vchannelsMu.Lock()
				if _, ok := m.vchannels[ch]; ok {
					m.vchannels[ch] <- pack // TODO: what if block here
				}
				m.vchannelsMu.Unlock()
			}
		}
	}
}

// Close 结束分发服务，关闭和清理 channel
func (m *MsgDispatcher) Close() error {
	m.cancel()
	return nil
}

func (m *MsgDispatcher) addVChannel(vchannel string, channel chan *MsgPack) {
	m.vchannelsMu.Lock()
	defer m.vchannelsMu.Unlock()
	m.vchannels[vchannel] = channel
}

func (m *MsgDispatcher) hasVChannel(vchannel string) bool {
	m.vchannelsMu.Lock()
	defer m.vchannelsMu.Unlock()
	_, ok := m.vchannels[vchannel]
	return ok
}

func (m *MsgDispatcher) removeVChannel(vchannel string) {
	m.vchannelsMu.Lock()
	defer m.vchannelsMu.Unlock()
	delete(m.vchannels, vchannel)
}

// Register 注册 vchannel，从 position 开始进行 seek，追回较早的数据
func (m *MsgDispatcher) Register(vchannel string, position *internalpb.MsgPosition) (chan *MsgPack, error) {
	if m.hasVChannel(vchannel) {
		return nil, fmt.Errorf("already exists")
	}

	log.Info("start Register")
	// check pchannel
	pchannel := funcutil.ToPhysicalChannel(vchannel)
	if m.pchannel != pchannel {
		return nil, fmt.Errorf("wrong vchannel when Register")
	}

	// init channel
	channel := make(chan *MsgPack, m.channelBufferSize)

	// check position
	lastMsgID, err := m.stream.GetLatestMsgID(pchannel)
	if err != nil {
		return nil, err
	}
	noMoreMsg, err := lastMsgID.LessOrEqualThan(position.GetMsgID())
	if err != nil {
		return nil, err
	}
	if noMoreMsg {
		log.Info("no more message need to seek")
		m.vchannelsMu.Lock()
		m.vchannels[vchannel] = channel
		m.vchannelsMu.Unlock()
		return channel, nil
	}

	// begin to seek
	registerStream, err := m.factory.NewTtMsgStream(m.ctx)
	if err != nil {
		return nil, err
	}
	defer registerStream.Close()
	registerStream.AsConsumer([]string{pchannel}, m.constructSubName(vchannel))
	err = registerStream.Seek([]*internalpb.MsgPosition{position})
	if err != nil {
		return nil, err
	}
	log.Info("start read msg from seek position to last position")
	for !noMoreMsg {
		select {
		case <-m.ctx.Done():
			log.Warn("ctx done")
			break
		case msgPack, ok := <-registerStream.Chan():
			if !ok {
				return nil, fmt.Errorf("fail to read msg")
			}
			if msgPack == nil {
				continue
			}
			for _, tsMsg := range msgPack.Msgs {
				lastMsgID, err = m.stream.GetLatestMsgID(pchannel)
				if err != nil {
					return nil, err
				}
				noMoreMsg, err = lastMsgID.LessOrEqualThan(tsMsg.Position().MsgID)
				if err != nil {
					return nil, err
				}
				if noMoreMsg {
					break
				}
			}
			for ch, pack := range m.dispatchFunc(msgPack) {
				if ch == vchannel {
					channel <- pack // TODO: what if block here
				}
			}
		}
	}

	m.vchannelsMu.Lock()
	m.vchannels[vchannel] = channel
	m.vchannelsMu.Unlock()
	return channel, nil
}

// Unregister 注销 vchannel，清理 flowgraph 前调用
func (m *MsgDispatcher) Unregister(vchannel string) {
	m.removeVChannel(vchannel)
	log.Info("remove done")
}
