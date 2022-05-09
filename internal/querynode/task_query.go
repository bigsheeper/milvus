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


var _ sqTask = (*queryTask)(nil)

type queryTask struct {
	sqBaseTask

	Ret *internalpb.RetrieveResults
}

func (q *queryTask) PreExecute(ctx context.Context) error {
	panic("not implemented")
}

func (q *queryTask) Execute(ctx context.Context) error {
	panic("not implemented")
}

func (q *queryTask) PostExecute(ctx context.Context) error {
	panic("not implemented")
}

func (q *queryTask) CanMergeWith(t sqTask) bool {
	return false
}

func newQueryTask(src * querypb.QueryRequest) *queryTask {
	target := &retrieveMsg{
		BaseMsg:            src.BaseMsg,
		Base:               src.Base,
		DbID:               src.DbID,
		CollectionID:       src.GetCollectionID(),
		PartitionIDs:       src.GetPartitionIDs(),
		SerializedExprPlan: src.GetSerializedExprPlan(),
		TravelTimestamp:    src.GetTravelTimestamp(),
		GuaranteeTimestamp: src.GetGuaranteeTimestamp(),
		TimeoutTimestamp:   src.GetTimeoutTimestamp(),
		ReqID:              src.GetReqID(),
	}
	return target
}
