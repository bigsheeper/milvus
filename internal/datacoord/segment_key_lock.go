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

package datacoord

import (
	"sort"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus/pkg/v2/util/lock"
)

type SegmentKeyLock interface {
	Lock(segmentIDs ...int64)
	Unlock(segmentIDs ...int64)

	RLock(segmentIDs ...int64)
	RUnlock(segmentIDs ...int64)
}

type segmentKeyLock struct {
	keyLock *lock.KeyLock[int64]
}

func (s *segmentKeyLock) getKeys(segmentIDs []int64) []int64 {
	segmentIDs = lo.Uniq(segmentIDs)
	// Sort to ensure a globally consistent order for acquiring locks.
	sort.Slice(segmentIDs, func(i, j int) bool {
		return segmentIDs[i] < segmentIDs[j]
	})
	return segmentIDs
}

func (s *segmentKeyLock) Lock(segmentIDs ...int64) {
	if len(segmentIDs) == 1 {
		s.keyLock.Lock(segmentIDs[0])
		return
	}

	keys := s.getKeys(segmentIDs)
	for _, key := range keys {
		s.keyLock.Lock(key)
	}
}

func (s *segmentKeyLock) Unlock(segmentIDs ...int64) {
	if len(segmentIDs) == 1 {
		s.keyLock.Unlock(segmentIDs[0])
		return
	}

	keys := s.getKeys(segmentIDs)
	for _, key := range keys {
		s.keyLock.Unlock(key)
	}
}

func (s *segmentKeyLock) RLock(segmentIDs ...int64) {
	if len(segmentIDs) == 1 {
		s.keyLock.RLock(segmentIDs[0])
		return
	}

	keys := s.getKeys(segmentIDs)
	for _, key := range keys {
		s.keyLock.RLock(key)
	}
}

func (s *segmentKeyLock) RUnlock(segmentIDs ...int64) {
	if len(segmentIDs) == 1 {
		s.keyLock.RUnlock(segmentIDs[0])
		return
	}

	keys := s.getKeys(segmentIDs)
	for _, key := range keys {
		s.keyLock.RUnlock(key)
	}
}

func NewSegmentKeyLock() SegmentKeyLock {
	return &segmentKeyLock{
		keyLock: lock.NewKeyLock[int64](),
	}
}
