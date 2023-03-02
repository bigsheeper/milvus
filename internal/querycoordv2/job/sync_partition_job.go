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

package job

import (
	"context"
	"fmt"
	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"go.uber.org/zap"
	"time"
)

type SyncNewCreatedPartitionJob struct {
	*BaseJob
	req     *querypb.SyncNewCreatedPartitionRequest
	meta    *meta.Meta
	cluster session.Cluster
}

func NewSyncNewCreatedPartitionJob(
	ctx context.Context,
	req *querypb.SyncNewCreatedPartitionRequest,
	meta *meta.Meta,
	cluster session.Cluster,
) *SyncNewCreatedPartitionJob {
	return &SyncNewCreatedPartitionJob{
		BaseJob: NewBaseJob(ctx, req.Base.GetMsgID(), req.GetCollectionID()),
		req:     req,
		meta:    meta,
		cluster: cluster,
	}
}

func (job *SyncNewCreatedPartitionJob) Execute() error {
	req := job.req
	log := log.Ctx(job.ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.Int64("partitionID", req.GetPartitionID()),
	)

	// check if collection not load or loadType is loadPartition
	collection := job.meta.GetCollection(req.GetCollectionID())
	if collection == nil || collection.GetLoadType() == querypb.LoadType_LoadPartition {
		return nil
	}

	// check if partition already existed
	if partition := job.meta.GetPartition(req.GetPartitionID()); partition != nil {
		return nil
	}

	partition := &meta.Partition{
		PartitionLoadInfo: &querypb.PartitionLoadInfo{
			CollectionID: req.GetCollectionID(),
			PartitionID:  req.GetPartitionID(),
			Status:       querypb.LoadStatus_Loaded,
		},
		LoadPercentage: 100,
		CreatedAt:      time.Now(),
	}
	err := job.meta.CollectionManager.PutPartition(partition)
	if err != nil {
		msg := "failed to store partitions"
		log.Error(msg, zap.Error(err))
		return utils.WrapError(msg, err)
	}

	replicas := job.meta.ReplicaManager.GetByCollection(req.CollectionID)
	loadReq := &querypb.LoadPartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_LoadPartitions,
		},
		CollectionID: req.GetCollectionID(),
		PartitionIDs: []int64{req.GetPartitionID()},
	}
	for _, replica := range replicas {
		for _, node := range replica.GetNodes() {
			status, err := job.cluster.LoadPartitions(job.ctx, node, loadReq)
			if err != nil {
				return err
			}
			if status.GetErrorCode() != commonpb.ErrorCode_Success {
				return fmt.Errorf("failed to loadPartitions on QueryNode, nodeID=%d, err=%s", node, status.GetReason())
			}
		}
	}

	return nil
}

func (job *SyncNewCreatedPartitionJob) PostExecute() {
	if job.Error() != nil {
		job.meta.CollectionManager.RemovePartition(job.req.GetPartitionID())
		replicas := job.meta.ReplicaManager.GetByCollection(job.req.CollectionID)
		releaseReq := &querypb.ReleasePartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_ReleasePartitions,
			},
			CollectionID: job.req.GetCollectionID(),
			PartitionIDs: []int64{job.req.GetPartitionID()},
		}
		for _, replica := range replicas {
			for _, node := range replica.GetNodes() {
				job.cluster.ReleasePartitions(job.ctx, node, releaseReq)
			}
		}
	}
}
