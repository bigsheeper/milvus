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
	"errors"
	"fmt"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"go.uber.org/zap"
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
	// check ctx timeout
	if !funcutil.CheckCtxValid(q.Ctx()) {
		return errors.New("search context timeout")
	}

	// lock streaming meta-replica for shard leader
	q.QS.streaming.replica.queryRLock()
	defer q.QS.streaming.replica.queryRUnlock()

	// check if collection has been released
	collection, err := q.QS.streaming.replica.getCollectionByID(q.CollectionID)
	if err != nil {
		return err
	}

	if q.iReq.GetGuaranteeTimestamp() >= collection.getReleaseTime() {
		log.Warn("collection release before query", zap.Int64("collectionID", q.CollectionID))
		return fmt.Errorf("retrieve failed, collection has been released, collectionID = %d", q.CollectionID)
	}
	// deserialize query plan
	plan, err := createRetrievePlanByExpr(collection, q.iReq.GetSerializedExprPlan(), q.TravelTimestamp)
	if err != nil {
		return err
	}
	defer plan.delete()

	// TODO: init vector chunk manager at most once
	if q.QS.vectorChunkManager == nil {
		if q.QS.localChunkManager == nil {
			return fmt.Errorf("can not create vector chunk manager for local chunk manager is nil")
		}
		if q.QS.remoteChunkManager == nil {
			return fmt.Errorf("can not create vector chunk manager for remote chunk manager is nil")
		}
		q.QS.vectorChunkManager, err = storage.NewVectorChunkManager(q.QS.localChunkManager, q.QS.remoteChunkManager,
			&etcdpb.CollectionMeta{
				ID:     collection.id,
				Schema: collection.schema,
			}, q.QS.localCacheSize, q.QS.localCacheEnabled)
		if err != nil {
			return err
		}
	}

	sResults, _, _, sErr := q.QS.streaming.retrieve(q.CollectionID, q.iReq.GetPartitionIDs(),
		plan, func(segment *Segment) bool { return segment.vChannelID == q.QS.channel })
	if sErr != nil {
		return sErr
	}
	mergedResult, err := mergeRetrieveResults(sResults)
	if err != nil {
		return err
	}

	q.Ret = &internalpb.RetrieveResults{
		Status:     &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		Ids:        mergedResult.Ids,
		FieldsData: mergedResult.FieldsData,
	}

	return nil
}

func (q *queryTask) searchOnHistorical() error {
	// check ctx timeout
	if !funcutil.CheckCtxValid(q.Ctx()) {
		return errors.New("search context timeout")
	}

	// lock streaming meta-replica for shard leader
	q.QS.historical.replica.queryRLock()
	defer q.QS.historical.replica.queryRUnlock()

	// check if collection has been released
	collection, err := q.QS.streaming.replica.getCollectionByID(q.CollectionID)
	if err != nil {
		return err
	}

	if q.iReq.GetGuaranteeTimestamp() >= collection.getReleaseTime() {
		log.Warn("collection release before query", zap.Int64("collectionID", q.CollectionID))
		return fmt.Errorf("retrieve failed, collection has been released, collectionID = %d", q.CollectionID)
	}
	// deserialize query plan
	plan, err := createRetrievePlanByExpr(collection, q.iReq.GetSerializedExprPlan(), q.TravelTimestamp)
	if err != nil {
		return err
	}
	defer plan.delete()

	// TODO: init vector chunk manager at most once
	if q.QS.vectorChunkManager == nil {
		if q.QS.localChunkManager == nil {
			return fmt.Errorf("can not create vector chunk manager for local chunk manager is nil")
		}
		if q.QS.remoteChunkManager == nil {
			return fmt.Errorf("can not create vector chunk manager for remote chunk manager is nil")
		}
		q.QS.vectorChunkManager, err = storage.NewVectorChunkManager(q.QS.localChunkManager, q.QS.remoteChunkManager,
			&etcdpb.CollectionMeta{
				ID:     collection.id,
				Schema: collection.schema,
			}, q.QS.localCacheSize, q.QS.localCacheEnabled)
		if err != nil {
			return err
		}
	}

	// validate segmentIDs in request
	err = q.QS.historical.validateSegmentIDs(q.req.SegmentIDs, q.CollectionID, q.iReq.GetPartitionIDs())
	if err != nil {
		log.Warn("segmentIDs in query request fails validation", zap.Int64s("segmentIDs", q.req.SegmentIDs))
		return err
	}
	retrieveResults, err := q.QS.historical.retrieveBySegmentIDs(q.CollectionID, q.req.SegmentIDs, q.QS.vectorChunkManager, plan)
	if err != nil {
		return err
	}
	mergedResult, err := mergeRetrieveResults(retrieveResults)
	if err != nil {
		return err
	}

	log.Debug("follower retrieve result", zap.String("ids", mergedResult.Ids.String()))
	q.Ret = &internalpb.RetrieveResults{
		Status:     &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		Ids:        mergedResult.Ids,
		FieldsData: mergedResult.FieldsData,
	}
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
