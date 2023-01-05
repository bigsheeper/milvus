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

//import (
//	"github.com/milvus-io/milvus/internal/mq/msgstream"
//	"github.com/milvus-io/milvus/internal/proto/internalpb"
//	"time"
//)
//
//type dispatcherManager struct {
//	pchannel string
//	lagChan  chan *msgstream.MsgPosition
//
//	primeDispatcher *dispatcher
//	soloDispatchers map[string]*dispatcher
//
//	factory msgstream.Factory
//}
//
//func NewDispatcherManager(pchannel string, factory msgstream.Factory) *dispatcherManager {
//	return &dispatcherManager{
//		pchannel:        pchannel,
//		lagChan:         make(chan *msgstream.MsgPosition, 10),
//		soloDispatchers: make(map[string]*dispatcher),
//		factory:         factory,
//	}
//}
//
//func (m *dispatcherManager) work() {
//	timer := time.NewTimer(1 * time.Second)
//	for {
//		select {
//		case <-timer.C:
//			primePos := m.primeDispatcher.getCurPosition()
//			for vchannel, sd := range m.soloDispatchers {
//				if sd.getCurPosition().GetTimestamp() == primePos.GetTimestamp() {
//					m.merge(vchannel)
//				}
//			}
//		case pos := <-m.lagChan:
//			m.separate(pos.ChannelName, pos)
//		}
//	}
//}
//
//func (m *dispatcherManager) merge(vchannel string) {
//	m.primeDispatcher.handle(pause)
//	m.soloDispatchers[vchannel].handle(pause)
//	m.primeDispatcher.addTarget(m.soloDispatchers[vchannel].getTarget())
//	m.soloDispatchers[vchannel].handle(terminate)
//	delete(m.soloDispatchers, vchannel)
//	m.soloDispatchers[vchannel].handle(resume)
//}
//
//func (m *dispatcherManager) separate(vchannel string, pos *internalpb.MsgPosition) {
//	newSolo, err := NewDispatcher(m.factory, m.pchannel, pos, m.lagChan)
//	if err != nil {
//		panic(err)
//	}
//	m.soloDispatchers[vchannel] = newSolo
//	newSolo.handle(start)
//}
