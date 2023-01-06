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

package meta

import (
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus/internal/log"
	. "github.com/milvus-io/milvus/internal/util/typeutil"
)

const LoadStatusExpireTime = 24 * time.Hour

var GlobalLoadCache *LoadStatusCache

type LoadStatus struct {
	s *commonpb.Status
	t time.Time
}

type LoadStatusCache struct {
	statusMu sync.RWMutex
	status   map[UniqueID]*LoadStatus
}

func NewLoadStatusCache() *LoadStatusCache {
	return &LoadStatusCache{
		status: make(map[UniqueID]*LoadStatus),
	}
}

func (l *LoadStatusCache) Get(collectionID UniqueID) *commonpb.Status {
	l.statusMu.RLock()
	defer l.statusMu.RUnlock()
	if _, ok := l.status[collectionID]; ok {
		log.Warn("loadStatusCache hits failed load status",
			zap.Int64("collectionID", collectionID),
			zap.String("errCode", l.status[collectionID].s.GetErrorCode().String()),
			zap.String("reason", l.status[collectionID].s.GetReason()))
		return proto.Clone(l.status[collectionID].s).(*commonpb.Status)
	}
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
}

func (l *LoadStatusCache) Put(collectionID UniqueID, status *commonpb.Status) {
	l.statusMu.Lock()
	defer l.statusMu.Unlock()
	if status.ErrorCode == commonpb.ErrorCode_Success {
		return
	}
	l.status[collectionID] = &LoadStatus{
		s: status,
		t: time.Now(),
	}
	log.Warn("loadStatusCache put failed load status", zap.Int64("collectionID", collectionID),
		zap.String("errCode", status.GetErrorCode().String()),
		zap.String("reason", status.GetReason()))
}

func (l *LoadStatusCache) Remove(collectionID UniqueID) {
	l.statusMu.Lock()
	defer l.statusMu.Unlock()
	delete(l.status, collectionID)
	log.Info("loadStatusCache removes load status", zap.Int64("collectionID", collectionID))
}

func (l *LoadStatusCache) TryExpire() {
	l.statusMu.Lock()
	defer l.statusMu.Unlock()
	for k, v := range l.status {
		if time.Since(v.t) > LoadStatusExpireTime {
			log.Info("LoadStatusCache expires cache", zap.Int64("collectionID", k))
			delete(l.status, k)
		}
	}
}
