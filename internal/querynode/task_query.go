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
)

var _ sqTask = (*queryTask)(nil)

type queryTask struct {
	sqBaseTask
	iReq *internalpb.RetrieveRequest
	req  *querypb.QueryRequest
	Ret  *internalpb.RetrieveResults
}

func (q *queryTask) PreExecute(ctx context.Context) error {
	panic("not implemented")
}

func (q *queryTask) searchOnStreaming() error {
	return nil
}

func (q *queryTask) searchOnHistorical() error {
	return nil
}

func (q *queryTask) Execute(ctx context.Context) error {
	panic("not implemented")
}

func (q *queryTask) PostExecute(ctx context.Context) error {
	panic("not implemented")
}

func (q *queryTask) EstimateCpuUsage() int32 {
	// TODO according to number of segments
	return 50
}

func newQueryTask(ctx context.Context, src *querypb.QueryRequest) *queryTask {
	target := &queryTask{
		sqBaseTask: sqBaseTask{
			baseTask: baseTask{
				done: make(chan error),
				ctx:  ctx,
				id:   src.Req.Base.GetMsgID(),
				ts:   src.Req.Base.GetTimestamp(),
			},
			DbID:               src.Req.GetReqID(),
			CollectionID:       src.Req.GetCollectionID(),
			TravelTimestamp:    src.Req.GetTravelTimestamp(),
			GuaranteeTimestamp: src.Req.GetGuaranteeTimestamp(),
			TimeoutTimestamp:   src.Req.GetTimeoutTimestamp(),
		},
		iReq: src.Req,
		req:  src,
	}
	return target
}
