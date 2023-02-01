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

package msgdispatcher

import (
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
)

type Manager interface {
	Register(vchannel string, pos *internalpb.MsgPosition, subPos mqwrapper.SubscriptionInitialPosition) (<-chan *msgstream.MsgPack, error)
	Deregister(vchannel string)
}

type managerImpl struct {
	role       string
	nodeID     int64
	checkersMu sync.RWMutex
	checkers   map[string]*checker // pchannel->checker
	factory    msgstream.Factory
}

func NewManager(factory msgstream.Factory, role string, nodeID int64) Manager {
	return &managerImpl{
		role:     role,
		nodeID:   nodeID,
		checkers: make(map[string]*checker),
		factory:  factory,
	}
}

func (g *managerImpl) Register(vchannel string, pos *internalpb.MsgPosition, subPos mqwrapper.SubscriptionInitialPosition) (<-chan *msgstream.MsgPack, error) {
	log := log.With(zap.String("role", g.role), zap.Int64("nodeID", g.nodeID), zap.String("vchannel", vchannel))
	log.Info("manager start to register...")
	pchannel := funcutil.ToPhysicalChannel(vchannel)
	g.checkersMu.Lock()
	defer g.checkersMu.Unlock()
	if _, ok := g.checkers[pchannel]; !ok {
		g.checkers[pchannel] = newChecker(pchannel, g.role, g.nodeID, g.factory)
		go g.checkers[pchannel].run()
	}
	ch, err := g.checkers[pchannel].addDispatcher(vchannel, pos, subPos)
	if err != nil {
		log.Error("manager register failed", zap.Error(err))
		return nil, err
	}
	log.Info("manager register done")
	return ch, nil
}

func (g *managerImpl) Deregister(vchannel string) {
	pchannel := funcutil.ToPhysicalChannel(vchannel)
	g.checkersMu.Lock()
	defer g.checkersMu.Unlock()
	if _, ok := g.checkers[pchannel]; ok {
		g.checkers[pchannel].removeDispatcher(vchannel)
		if g.checkers[pchannel].dispatcherNum() == 0 {
			g.checkers[pchannel].close()
			delete(g.checkers, pchannel)
		}
	}
	log.Info("manager deregister done", zap.String("role", g.role),
		zap.Int64("nodeID", g.nodeID), zap.String("vchannel", vchannel))
}
