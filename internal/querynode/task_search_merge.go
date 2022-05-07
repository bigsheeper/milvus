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

type mergeSearchTask struct {
	searchTask
	originTasks []*searchTask
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
