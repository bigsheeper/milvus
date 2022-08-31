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
	"strconv"
	"sync"
)

type (
	pchannel = string
	vchannel = string
)

type DispatchFunc func(msgPack *MsgPack) map[vchannel]*MsgPack

type MsgDispatcher struct {
	nodeID       UniqueID
	pchannelName pchannel
	consumers    map[string]chan *MsgPack

	currentMsgID *MsgPosition

	dispatchFunc DispatchFunc
	stream       MsgStream

	closeOnce sync.Once
	closeChan chan struct{}
}

func (m *MsgDispatcher) Run() error {
	subName := m.pchannelName + "-" + strconv.FormatInt(m.nodeID, 10)
	m.stream.AsConsumer([]pchannel{m.pchannelName}, subName)
	for {
		select {
		case <-m.closeChan:
			return nil
		case pack := <-m.stream.Chan():
			pack.EndPositions
			dispatchResults := m.dispatchFunc(pack)
			for k, v := range dispatchResults {
				if _, ok := m.consumers[k]; ok {
					m.consumers[k] <- v
				}
			}
		}
	}
}

func (m *MsgDispatcher) Register() {

}
