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
	"sync"

	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type rateCollector struct {
	searchNQInQueue atomic.Int64
	queriesInQueue  atomic.Int64
	tSafesMu        sync.Mutex
	tSafes          map[Channel]Timestamp
}

func newRateCollector() *rateCollector {
	return &rateCollector{
		tSafes: make(map[Channel]Timestamp),
	}
}

func (r *rateCollector) addNQ(nq int64) {
	r.searchNQInQueue.Add(nq)
}

func (r *rateCollector) subNQ(nq int64) {
	r.searchNQInQueue.Sub(nq)
}

func (r *rateCollector) getSearchNQInQueue() int64 {
	return r.searchNQInQueue.Load()
}

func (r *rateCollector) incQuery() {
	r.queriesInQueue.Inc()
}

func (r *rateCollector) decQuery() {
	r.queriesInQueue.Dec()
}

func (r *rateCollector) getQueriesInQueue() int64 {
	return r.queriesInQueue.Load()
}

func (r *rateCollector) updateTSafe(c Channel, t Timestamp) {
	r.tSafesMu.Lock()
	defer r.tSafesMu.Unlock()
	r.tSafes[c] = t
}

func (r *rateCollector) getMinTSafe() Timestamp {
	r.tSafesMu.Lock()
	defer r.tSafesMu.Unlock()
	min := typeutil.MaxTimestamp
	for _, t := range r.tSafes {
		if min > t {
			min = t
		}
	}
	return min
}
