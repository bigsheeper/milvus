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
	"context"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/timerecord"
)

const (
	maxTopKMergeRatio = 10.0
	maxNQ             = 10000
)

var _ sqTask = (*searchTask)(nil)

type searchTask struct {
	sqBaseTask

	MetricType            string
	PlaceholderGroup      []byte
	OrigPlaceHolderGroups [][]byte
	NQ                    int64
	OrigNQs               []int64
	TopK                  int64
	OrigTopKs             []int64
	Ret *internalpb.SearchResults
}

func (s *searchTask) PreExecute(ctx context.Context) error {
	panic("not implemented")
}

func (s *searchTask) Execute(ctx context.Context) error {
	panic("not implemented")
}

func (s *searchTask) PostExecute(ctx context.Context) error {
	panic("not implemented")
}

func (s *searchTask) CanMergeWith(t *searchTask) bool {
	if task1.DbID != task2.DbID {
		return false
	}

	if task1.CollectionID != task2.CollectionID {
		return false
	}

	if task1.DslType != task2.DslType {
		return false
	}

	if task1.MetricType != task2.MetricType {
		return false
	}

	if !funcutil.SortedSliceEqual(task1.PartitionIDs, task2.PartitionIDs) {
		return false
	}

	if !bytes.Equal(task1.SerializedExprPlan, task2.SerializedExprPlan) {
		return false
	}

	if task1.TravelTimestamp != task2.TravelTimestamp {
		return false
	}

	pre := task1.NQ * task1.TopK * 1.0
	newTopK := task1.TopK
	if newTopK < task2.TopK {
		newTopK = task2.TopK
	}
	after := (task1.NQ + task2.NQ) * newTopK * 1.0

	if pre == 0 {
		return false
	}
	if after/pre > maxTopKMergeRatio {
		return false
	}
	if task1.NQ+task2.NQ > maxNQ {
		return false
	}
	return true
}

func (s *searchTask) Merge(t *searchTask) *mergeSearchTask {
	target, _ := t.(*searchMsg)
	src, _ := s.(*searchMsg)

	newTopK := target.TopK
	if newTopK < src.TopK {
		newTopK = src.TopK
	}

	target.TopK = newTopK
	target.ReqIDs = append(target.ReqIDs, src.ReqIDs...)
	target.OrigTopKs = append(target.OrigTopKs, src.OrigTopKs...)
	target.OrigNQs = append(target.OrigNQs, src.OrigNQs...)
	target.SourceIDs = append(target.SourceIDs, src.SourceIDs...)
	target.OrigPlaceHolderGroups = append(target.OrigPlaceHolderGroups, src.OrigPlaceHolderGroups...)
	target.NQ += src.NQ
	return target
}

type mergeSearchTask struct {
	searchTask
	originTasks []*searchTask
}

// combinePlaceHolderGroups combine all the placeholder groups.
func (m *mergeSearchTask) combinePlaceHolderGroups() {
	if len(s.OrigPlaceHolderGroups) > 1 {
		ret := &milvuspb.PlaceholderGroup{}
		//retValues := ret.Placeholders[0].GetValues()
		_ = proto.Unmarshal(s.PlaceholderGroup, ret)
		for i, grp := range s.OrigPlaceHolderGroups {
			if i == 0 {
				continue
			}
			x := &milvuspb.PlaceholderGroup{}
			_ = proto.Unmarshal(grp, x)
			ret.Placeholders[0].Values = append(ret.Placeholders[0].Values, x.Placeholders[0].Values...)
		}
		s.PlaceholderGroup, _ = proto.Marshal(ret)
	}
}

func (m *mergeSearchTask) PreExecute(ctx context.Context) error {
	m.combinePlaceHolderGroups()
	return m.searchTask.PreExecute(ctx)
}

func (m *mergeSearchTask) PostExecute(ctx context.Context) error {
	panic("not implemented")
}

func (m *mergeSearchTask) Merge(t *searchTask) *mergeSearchTask {
	target, _ := t.(*searchMsg)
	src, _ := s.(*searchMsg)

	newTopK := target.TopK
	if newTopK < src.TopK {
		newTopK = src.TopK
	}

	target.TopK = newTopK
	target.ReqIDs = append(target.ReqIDs, src.ReqIDs...)
	target.OrigTopKs = append(target.OrigTopKs, src.OrigTopKs...)
	target.OrigNQs = append(target.OrigNQs, src.OrigNQs...)
	target.SourceIDs = append(target.SourceIDs, src.SourceIDs...)
	target.OrigPlaceHolderGroups = append(target.OrigPlaceHolderGroups, src.OrigPlaceHolderGroups...)
	target.NQ += src.NQ
	return target

	return m
}

func newSearchTask(src *querypb.SearchRequest) *searchTask {
	target := &searchMsg{
		BaseMsg:               src.BaseMsg,
		Base:                  src.Base,
		ReqIDs:                []UniqueID{src.ReqID},
		DbID:                  src.DbID,
		CollectionID:          src.GetCollectionID(),
		PartitionIDs:          src.GetPartitionIDs(),
		Dsl:                   src.GetDsl(),
		DslType:               src.GetDslType(),
		PlaceholderGroup:      src.GetPlaceholderGroup(),
		OrigPlaceHolderGroups: [][]byte{src.GetPlaceholderGroup()},
		SerializedExprPlan:    src.GetSerializedExprPlan(),
		TravelTimestamp:       src.GetTravelTimestamp(),
		GuaranteeTimestamp:    src.GetGuaranteeTimestamp(),
		TimeoutTimestamp:      src.GetTimeoutTimestamp(),
		NQ:                    src.GetNq(),
		OrigNQs:               []int64{src.GetNq()},
		OrigTopKs:             []int64{src.GetTopk()},
		SourceIDs:             []UniqueID{src.SourceID()},
		TopK:                  src.GetTopk(),
		MetricType:            src.GetMetricType(),
	}
	return target
}
