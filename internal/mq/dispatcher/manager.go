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
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"sync"
)

type Manager interface {
	Register(vchannel string, pos *internalpb.MsgPosition, subPos mqwrapper.SubscriptionInitialPosition) (<-chan *msgstream.MsgPack, error)
	Deregister(vchannel string)
}

type GlobalManager struct {
	checkersMu sync.RWMutex
	checkers   map[string]*checker
	factory    msgstream.Factory
}

func NewManager(factory msgstream.Factory) Manager {
	return &GlobalManager{
		checkers: make(map[string]*checker),
		factory:  factory,
	}
}

func (g *GlobalManager) Register(vchannel string, pos *internalpb.MsgPosition, subPos mqwrapper.SubscriptionInitialPosition) (<-chan *msgstream.MsgPack, error) {
	pchannel := funcutil.ToPhysicalChannel(vchannel)
	g.checkersMu.Lock()
	defer g.checkersMu.Unlock()
	if _, ok := g.checkers[pchannel]; !ok {
		g.checkers[pchannel] = newChecker(pchannel, g.factory)
	}
	return g.checkers[pchannel].addDispatcher(vchannel, pos, subPos)
}

func (g *GlobalManager) Deregister(vchannel string) {
	pchannel := funcutil.ToPhysicalChannel(vchannel)
	g.checkersMu.Lock()
	defer g.checkersMu.Unlock()
	if _, ok := g.checkers[pchannel]; ok {
		g.checkers[pchannel].removeDispatcher(vchannel)
		if g.checkers[pchannel].isEmpty() {
			g.checkers[pchannel].closeAll()
		}
	}
}
