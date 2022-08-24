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

	"github.com/milvus-io/milvus/internal/util/typeutil"
)

// rateCollector helps to collect and calculate values (like rate, timeTick and etc...),
// It implements a sliding window with custom size and granularity to store values.
type rateCollector struct {
	rtCounter *readTaskCounter

	tSafesMu sync.Mutex
	tSafes   map[Channel]Timestamp
}

// newRateCollector returns a new rateCollector with given window and granularity.
func newRateCollector() *rateCollector {
	rc := &rateCollector{
		rtCounter: newReadTaskCounter(),
		tSafes:    make(map[Channel]Timestamp),
	}
	return rc
}

func (r *rateCollector) updateTSafe(c Channel, t Timestamp) {
	r.tSafesMu.Lock()
	defer r.tSafesMu.Unlock()
	r.tSafes[c] = t
}

func (r *rateCollector) getMinTSafe() Timestamp {
	r.tSafesMu.Lock()
	defer r.tSafesMu.Unlock()
	minTt := typeutil.MaxTimestamp
	for _, t := range r.tSafes {
		if minTt > t {
			minTt = t
		}
	}
	return minTt
}
