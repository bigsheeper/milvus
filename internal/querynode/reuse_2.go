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
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"time"
)

const MaxTolerantLag = 3 * time.Second

type dispatcherManager struct {
	pchannel string

	primeDispatcher *dispatcher
	soloDispatchers map[string]*dispatcher

	factory msgstream.Factory
}

func (m *dispatcherManager) work() {
	timer := time.NewTimer(1 * time.Second)
	for {
		select {
		case <-timer.C:
			primePos := m.primeDispatcher.getCurPosition()
			for vchannel, sd := range m.soloDispatchers {
				// merge
				if sd.getCurPosition().GetTimestamp() == primePos.GetTimestamp() {
					m.merge(vchannel)
				}
				// separate
				sdTime := tsoutil.PhysicalTime(sd.getCurPosition().GetTimestamp())
				primeTime := tsoutil.PhysicalTime(primePos.GetTimestamp())
				if primeTime.Sub(sdTime) >= MaxTolerantLag {
					m.separate(vchannel)
				}
			}
		}
	}
}

func (m *dispatcherManager) merge(vchannel string) {
	m.primeDispatcher.handle(pause)
	m.soloDispatchers[vchannel].handle(pause)
	m.primeDispatcher.addTarget(m.soloDispatchers[vchannel].getTarget())
	m.soloDispatchers[vchannel].handle(terminate)
	delete(m.soloDispatchers, vchannel)
	m.soloDispatchers[vchannel].handle(resume)
}

func (m *dispatcherManager) separate(vchannel string) {
	m.primeDispatcher.handle(pause)
	m.primeDispatcher.removeTarget(vchannel)
	newSolo, err := NewDispatcher(m.factory, m.pchannel, nil)
	if err != nil {
		panic(err)
	}
	m.soloDispatchers[vchannel] = newSolo
	newSolo.handle(start)
	m.primeDispatcher.handle(resume)
}
