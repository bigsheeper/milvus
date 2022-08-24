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

	"github.com/milvus-io/milvus/internal/util/metricsinfo"
)

type readTaskQueueType int32

const (
	unsolvedQueueType readTaskQueueType = iota
	readyQueueType
	receiveQueueType
	executeQueueType
)

type readTaskCounter struct {
	sync.Mutex
	searchNQCounter   map[readTaskQueueType]int64
	queryTasksCounter map[readTaskQueueType]int64
}

func newReadTaskCounter() *readTaskCounter {
	return &readTaskCounter{
		searchNQCounter:   make(map[readTaskQueueType]int64),
		queryTasksCounter: make(map[readTaskQueueType]int64),
	}
}

func (r *readTaskCounter) add(t readTask, rtQueueType readTaskQueueType) {
	r.Lock()
	defer r.Unlock()
	if st, ok := t.(*searchTask); ok {
		r.searchNQCounter[rtQueueType] += st.NQ
	} else {
		r.queryTasksCounter[rtQueueType]++
	}
}

func (r *readTaskCounter) sub(t readTask, rtQueueType readTaskQueueType) {
	r.Lock()
	defer r.Unlock()
	if st, ok := t.(*searchTask); ok {
		r.searchNQCounter[rtQueueType] -= st.NQ
	} else {
		r.queryTasksCounter[rtQueueType]--
	}
}

func (r *readTaskCounter) getSearchNQInQueue() metricsinfo.ReadInfoInQueue {
	r.Lock()
	defer r.Unlock()
	return metricsinfo.ReadInfoInQueue{
		UnsolvedQueue: r.searchNQCounter[unsolvedQueueType],
		ReadyQueue:    r.searchNQCounter[readyQueueType],
		ReceiveChan:   r.searchNQCounter[receiveQueueType],
		ExecuteChan:   r.searchNQCounter[executeQueueType],
	}
}

func (r *readTaskCounter) getQueryTasksInQueue() metricsinfo.ReadInfoInQueue {
	r.Lock()
	defer r.Unlock()
	return metricsinfo.ReadInfoInQueue{
		UnsolvedQueue: r.queryTasksCounter[unsolvedQueueType],
		ReadyQueue:    r.queryTasksCounter[readyQueueType],
		ReceiveChan:   r.queryTasksCounter[receiveQueueType],
		ExecuteChan:   r.queryTasksCounter[executeQueueType],
	}
}
