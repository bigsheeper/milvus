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
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"sync"
)

type (
	pchannel = string
	vchannel = string
)

type DispatchFunc func(msgPack *MsgPack) map[vchannel]*MsgPack

type TtMsgDispatcherManager struct {
	nodeID       UniqueID
	roleName     string
	dispatchFunc DispatchFunc
	factory      Factory

	dispatchersMu sync.Mutex
	dispatchers   map[pchannel]*TtMsgDispatcher
}

func (t *TtMsgDispatcherManager) getOrCreateDispatcher(pchannel pchannel) (*TtMsgDispatcher, error) {
	t.dispatchersMu.Lock()
	defer t.dispatchersMu.Unlock()
	if _, ok := t.dispatchers[pchannel]; !ok {
		t.dispatchers[pchannel] = newTtMsgDispatcher(t.nodeID, t.roleName, pchannel, t.dispatchFunc, t.factory)
		err := t.dispatchers[pchannel].run()
		if err != nil {
			return nil, err
		}
	}
	return t.dispatchers[pchannel], nil
}

func (t *TtMsgDispatcherManager) Register(vchannel vchannel, position *MsgPosition) (<-chan *MsgPack, error) {
	pchannel := funcutil.ToPhysicalChannel(vchannel)
	dispatcher, err := t.getOrCreateDispatcher(pchannel)
	if err != nil {
		return nil, err
	}
	output, err := dispatcher.register(vchannel, position)
	if err != nil {
		return nil, err
	}
	return output, nil
}

func (t *TtMsgDispatcherManager) Deregister(vchannel vchannel) { // TODO: parse pchannel by vchannel?
	t.dispatchersMu.Lock()
	defer t.dispatchersMu.Unlock()
	pchannel := funcutil.ToPhysicalChannel(vchannel)
	if _, ok := t.dispatchers[pchannel]; !ok {
		return // TODO: if to throw error
	}
	t.dispatchers[pchannel].deregister(vchannel)
	if t.dispatchers[pchannel].getConsumerNum() == 0 {
		t.dispatchers[pchannel].close()
		delete(t.dispatchers, pchannel)
	}
}
