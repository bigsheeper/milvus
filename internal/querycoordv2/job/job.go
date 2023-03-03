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
	"github.com/samber/lo"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/observers"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

// Job is request of loading/releasing collection/partitions,
// the execution flow is:
// 1. PreExecute()
// 2. Execute(), skip this step if PreExecute() failed
// 3. PostExecute()
type Job interface {
	MsgID() int64
	CollectionID() int64
	Context() context.Context
	// PreExecute does checks, DO NOT persists any thing within this stage,
	PreExecute() error
	// Execute processes the request
	Execute() error
	// PostExecute clears resources, it will be always processed
	PostExecute()
	Error() error
	SetError(err error)
	Done()
	Wait() error
}

type BaseJob struct {
	ctx          context.Context
	msgID        int64
	collectionID int64
	err          error
	doneCh       chan struct{}
}

func NewBaseJob(ctx context.Context, msgID, collectionID int64) *BaseJob {
	return &BaseJob{
		ctx:          ctx,
		msgID:        msgID,
		collectionID: collectionID,
		doneCh:       make(chan struct{}),
	}
}

func (job *BaseJob) MsgID() int64 {
	return job.msgID
}

func (job *BaseJob) CollectionID() int64 {
	return job.collectionID
}

func (job *BaseJob) Context() context.Context {
	return job.ctx
}

func (job *BaseJob) Error() error {
	return job.err
}

func (job *BaseJob) SetError(err error) {
	job.err = err
}

func (job *BaseJob) Done() {
	close(job.doneCh)
}

func (job *BaseJob) Wait() error {
	<-job.doneCh
	return job.err
}

func (job *BaseJob) PreExecute() error {
	return nil
}

func (job *BaseJob) PostExecute() {}

type LoadCollectionJob struct {
	*BaseJob
	req  *querypb.LoadCollectionRequest
	undo *UndoList

	dist           *meta.DistributionManager
	meta           *meta.Meta
	cluster        session.Cluster
	targetMgr      *meta.TargetManager
	targetObserver *observers.TargetObserver
	broker         meta.Broker
	nodeMgr        *session.NodeManager
}

func NewLoadCollectionJob(
	ctx context.Context,
	req *querypb.LoadCollectionRequest,
	dist *meta.DistributionManager,
	meta *meta.Meta,
	cluster session.Cluster,
	targetMgr *meta.TargetManager,
	targetObserver *observers.TargetObserver,
	broker meta.Broker,
	nodeMgr *session.NodeManager,
) *LoadCollectionJob {
	return &LoadCollectionJob{
		BaseJob:        NewBaseJob(ctx, req.Base.GetMsgID(), req.GetCollectionID()),
		req:            req,
		undo:           NewUndoList(ctx, meta, cluster, targetMgr, targetObserver),
		dist:           dist,
		meta:           meta,
		cluster:        cluster,
		targetMgr:      targetMgr,
		targetObserver: targetObserver,
		broker:         broker,
		nodeMgr:        nodeMgr,
	}
}

func (job *LoadCollectionJob) PreExecute() error {
	req := job.req
	log := log.Ctx(job.ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
	)

	if req.GetReplicaNumber() <= 0 {
		log.Info("request doesn't indicate the number of replicas, set it to 1",
			zap.Int32("replicaNumber", req.GetReplicaNumber()))
		req.ReplicaNumber = 1
	}

	collection := job.meta.GetCollection(req.GetCollectionID())
	if collection == nil {
		return nil
	}

	if collection.GetReplicaNumber() != req.GetReplicaNumber() {
		msg := fmt.Sprintf("collection with different replica number %d existed, release this collection first before changing its replica number",
			job.meta.GetReplicaNumber(req.GetCollectionID()),
		)
		log.Warn(msg)
		return utils.WrapError(msg, ErrLoadParameterMismatched)
	} else if !typeutil.MapEqual(collection.GetFieldIndexID(), req.GetFieldIndexID()) {
		msg := fmt.Sprintf("collection with different index %v existed, release this collection first before changing its index",
			collection.GetFieldIndexID())
		log.Warn(msg)
		return utils.WrapError(msg, ErrLoadParameterMismatched)
	}

	return nil
}

func (job *LoadCollectionJob) Execute() error {
	req := job.req
	log := log.Ctx(job.ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
	)
	meta.GlobalFailedLoadCache.Remove(req.GetCollectionID())

	// Fetch partitions from RootCoord
	partitionIDs, err := job.broker.GetPartitions(job.ctx, req.GetCollectionID())
	if err != nil {
		msg := "failed to get partitions from RootCoord"
		log.Error(msg, zap.Error(err))
		return utils.WrapError(msg, err)
	}
	lackPartitionIDs := lo.FilterMap(partitionIDs, func(partID int64, _ int) (int64, bool) {
		part := job.meta.CollectionManager.GetPartition(partID)
		return partID, part == nil
	})
	job.undo.CollectionID = req.GetCollectionID()
	job.undo.LackPartitions = lackPartitionIDs

	_, err = job.targetObserver.UpdateNextTarget(req.GetCollectionID(), lackPartitionIDs...)
	if err != nil {
		msg := "failed to update next target"
		log.Error(msg, zap.Error(err))
		return utils.WrapError(msg, err)
	}
	job.undo.TargetUpdated = true

	err = loadPartitions(job.ctx, job.meta, job.cluster, req.GetCollectionID(), lackPartitionIDs...)
	if err != nil {
		return err
	}
	job.undo.PartitionsLoaded = true

	if !job.meta.CollectionManager.Exist(req.GetCollectionID()) {
		job.undo.NeverLoad = true
		// Clear stale replicas, https://github.com/milvus-io/milvus/issues/20444
		err = job.meta.ReplicaManager.RemoveCollection(req.GetCollectionID())
		if err != nil {
			msg := "failed to clear stale replicas"
			log.Warn(msg, zap.Error(err))
			return utils.WrapError(msg, err)
		}
	}

	replicas := job.meta.ReplicaManager.GetByCollection(req.GetCollectionID())
	if len(replicas) == 0 { // replicas not exist, create replicas
		replicas, err = utils.SpawnReplicasWithRG(job.meta,
			req.GetCollectionID(),
			req.GetResourceGroups(),
			req.GetReplicaNumber(),
		)
		if err != nil {
			msg := "failed to spawn replica for collection"
			log.Error(msg, zap.Error(err))
			return utils.WrapError(msg, err)
		}
		for _, replica := range replicas {
			log.Info("replica created", zap.Int64("replicaID", replica.GetID()),
				zap.Int64s("nodes", replica.GetNodes()), zap.String("resourceGroup", replica.GetResourceGroup()))
		}
		job.undo.ReplicaCreated = true
	}

	partitions := lo.FilterMap(lackPartitionIDs, func(partID int64, _ int) (*meta.Partition, bool) {
		return &meta.Partition{
			PartitionLoadInfo: &querypb.PartitionLoadInfo{
				CollectionID: req.GetCollectionID(),
				PartitionID:  partID,
				Status:       querypb.LoadStatus_Loading,
			},
			LoadPercentage: 0,
			CreatedAt:      time.Now(),
		}, true
	})
	collection := &meta.Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{
			CollectionID:  req.GetCollectionID(),
			ReplicaNumber: req.GetReplicaNumber(),
			Status:        querypb.LoadStatus_Loading,
			FieldIndexID:  req.GetFieldIndexID(),
			LoadType:      querypb.LoadType_LoadCollection,
		},
		LoadPercentage: 0,
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}
	err = job.meta.CollectionManager.PutCollection(collection, partitions...)
	if err != nil {
		msg := "failed to store collection and partitions"
		log.Error(msg, zap.Error(err))
		return utils.WrapError(msg, err)
	}

	metrics.QueryCoordNumCollections.WithLabelValues().Inc()
	return nil
}

func (job *LoadCollectionJob) PostExecute() {
	if job.Error() != nil {
		job.undo.RollBack()
	}
}

type ReleaseCollectionJob struct {
	*BaseJob
	req            *querypb.ReleaseCollectionRequest
	dist           *meta.DistributionManager
	meta           *meta.Meta
	targetMgr      *meta.TargetManager
	targetObserver *observers.TargetObserver
}

func NewReleaseCollectionJob(ctx context.Context,
	req *querypb.ReleaseCollectionRequest,
	dist *meta.DistributionManager,
	meta *meta.Meta,
	targetMgr *meta.TargetManager,
	targetObserver *observers.TargetObserver,
) *ReleaseCollectionJob {
	return &ReleaseCollectionJob{
		BaseJob:        NewBaseJob(ctx, req.Base.GetMsgID(), req.GetCollectionID()),
		req:            req,
		dist:           dist,
		meta:           meta,
		targetMgr:      targetMgr,
		targetObserver: targetObserver,
	}
}

func (job *ReleaseCollectionJob) Execute() error {
	req := job.req
	log := log.Ctx(job.ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
	)
	if !job.meta.CollectionManager.Exist(req.GetCollectionID()) {
		log.Info("release collection end, the collection has not been loaded into QueryNode")
		return nil
	}

	err := job.meta.CollectionManager.RemoveCollection(req.GetCollectionID())
	if err != nil {
		msg := "failed to remove collection"
		log.Warn(msg, zap.Error(err))
		return utils.WrapError(msg, err)
	}

	err = job.meta.ReplicaManager.RemoveCollection(req.GetCollectionID())
	if err != nil {
		msg := "failed to remove replicas"
		log.Warn(msg, zap.Error(err))
	}

	job.targetMgr.RemoveCollection(req.GetCollectionID())
	job.targetObserver.ReleaseCollection(req.GetCollectionID())
	waitCollectionReleased(job.dist, req.GetCollectionID())
	metrics.QueryCoordNumCollections.WithLabelValues().Dec()
	return nil
}

type LoadPartitionJob struct {
	*BaseJob
	req  *querypb.LoadPartitionsRequest
	undo *UndoList

	dist           *meta.DistributionManager
	meta           *meta.Meta
	cluster        session.Cluster
	targetMgr      *meta.TargetManager
	targetObserver *observers.TargetObserver
	broker         meta.Broker
	nodeMgr        *session.NodeManager
}

func NewLoadPartitionJob(
	ctx context.Context,
	req *querypb.LoadPartitionsRequest,
	dist *meta.DistributionManager,
	meta *meta.Meta,
	cluster session.Cluster,
	targetMgr *meta.TargetManager,
	targetObserver *observers.TargetObserver,
	broker meta.Broker,
	nodeMgr *session.NodeManager,
) *LoadPartitionJob {
	return &LoadPartitionJob{
		BaseJob:        NewBaseJob(ctx, req.Base.GetMsgID(), req.GetCollectionID()),
		req:            req,
		undo:           NewUndoList(ctx, meta, cluster, targetMgr, targetObserver),
		dist:           dist,
		meta:           meta,
		cluster:        cluster,
		targetMgr:      targetMgr,
		targetObserver: targetObserver,
		broker:         broker,
		nodeMgr:        nodeMgr,
	}
}

func (job *LoadPartitionJob) PreExecute() error {
	req := job.req
	log := log.Ctx(job.ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
	)

	if req.GetReplicaNumber() <= 0 {
		log.Info("request doesn't indicate the number of replicas, set it to 1",
			zap.Int32("replicaNumber", req.GetReplicaNumber()))
		req.ReplicaNumber = 1
	}

	collection := job.meta.GetCollection(req.GetCollectionID())
	if collection == nil {
		return nil
	}
	if collection.GetReplicaNumber() != req.GetReplicaNumber() {
		msg := "collection with different replica number existed, release this collection first before changing its replica number"
		log.Warn(msg)
		return utils.WrapError(msg, ErrLoadParameterMismatched)
	} else if !typeutil.MapEqual(collection.GetFieldIndexID(), req.GetFieldIndexID()) {
		msg := fmt.Sprintf("collection with different index %v existed, release this collection first before changing its index",
			job.meta.GetFieldIndex(req.GetCollectionID()))
		log.Warn(msg)
		return utils.WrapError(msg, ErrLoadParameterMismatched)
	}

	return nil
}

func (job *LoadPartitionJob) Execute() error {
	req := job.req
	log := log.Ctx(job.ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.Int64s("partitionIDs", req.GetPartitionIDs()),
	)

	meta.GlobalFailedLoadCache.Remove(req.GetCollectionID())

	lackPartitionIDs := lo.FilterMap(req.GetPartitionIDs(), func(partID int64, _ int) (int64, bool) {
		part := job.meta.CollectionManager.GetPartition(partID)
		return partID, part == nil
	})
	job.undo.CollectionID = req.GetCollectionID()
	job.undo.LackPartitions = lackPartitionIDs

	_, err := job.targetObserver.UpdateNextTarget(req.GetCollectionID(), lackPartitionIDs...)
	if err != nil {
		msg := "failed to update next target"
		log.Error(msg, zap.Error(err))
		return utils.WrapError(msg, err)
	}
	job.undo.TargetUpdated = true

	err = loadPartitions(job.ctx, job.meta, job.cluster, req.GetCollectionID(), lackPartitionIDs...)
	if err != nil {
		return err
	}
	job.undo.PartitionsLoaded = true

	if !job.meta.CollectionManager.Exist(req.GetCollectionID()) {
		job.undo.NeverLoad = true
		// Clear stale replicas, https://github.com/milvus-io/milvus/issues/20444
		err = job.meta.ReplicaManager.RemoveCollection(req.GetCollectionID())
		if err != nil {
			msg := "failed to clear stale replicas"
			log.Warn(msg, zap.Error(err))
			return utils.WrapError(msg, err)
		}
	}

	replicas := job.meta.ReplicaManager.GetByCollection(req.GetCollectionID())
	if len(replicas) == 0 { // replicas not exist, create replicas
		replicas, err = utils.SpawnReplicasWithRG(job.meta,
			req.GetCollectionID(),
			req.GetResourceGroups(),
			req.GetReplicaNumber(),
		)
		if err != nil {
			msg := "failed to spawn replica for collection"
			log.Error(msg, zap.Error(err))
			return utils.WrapError(msg, err)
		}
		for _, replica := range replicas {
			log.Info("replica created", zap.Int64("replicaID", replica.GetID()),
				zap.Int64s("nodes", replica.GetNodes()), zap.String("resourceGroup", replica.GetResourceGroup()))
		}
		job.undo.ReplicaCreated = true
	}

	partitions := lo.FilterMap(lackPartitionIDs, func(partID int64, _ int) (*meta.Partition, bool) {
		return &meta.Partition{
			PartitionLoadInfo: &querypb.PartitionLoadInfo{
				CollectionID: req.GetCollectionID(),
				PartitionID:  partID,
				Status:       querypb.LoadStatus_Loading,
			},
			CreatedAt:      time.Now(),
			LoadPercentage: 0,
		}, true
	})

	if !job.meta.CollectionManager.Exist(req.GetCollectionID()) {
		collection := &meta.Collection{
			CollectionLoadInfo: &querypb.CollectionLoadInfo{
				CollectionID:  req.GetCollectionID(),
				ReplicaNumber: req.GetReplicaNumber(),
				Status:        querypb.LoadStatus_Loading,
				FieldIndexID:  req.GetFieldIndexID(),
				LoadType:      querypb.LoadType_LoadPartition,
			},
			LoadPercentage: 0,
			CreatedAt:      time.Now(),
			UpdatedAt:      time.Now(),
		}
		err = job.meta.CollectionManager.PutCollection(collection, partitions...)
		if err != nil {
			msg := "failed to store collection and partitions"
			log.Error(msg, zap.Error(err))
			return utils.WrapError(msg, err)
		}
	} else { // collection exists, put partitions only
		err = job.meta.CollectionManager.PutPartition(partitions...)
		if err != nil {
			msg := "failed to store partitions"
			log.Error(msg, zap.Error(err))
			return utils.WrapError(msg, err)
		}
	}

	metrics.QueryCoordNumCollections.WithLabelValues().Inc()
	return nil
}

func (job *LoadPartitionJob) PostExecute() {
	if job.Error() != nil {
		job.undo.RollBack()
	}
}

type ReleasePartitionJob struct {
	*BaseJob
	req            *querypb.ReleasePartitionsRequest
	dist           *meta.DistributionManager
	meta           *meta.Meta
	cluster        session.Cluster
	targetMgr      *meta.TargetManager
	targetObserver *observers.TargetObserver
}

func NewReleasePartitionJob(ctx context.Context,
	req *querypb.ReleasePartitionsRequest,
	dist *meta.DistributionManager,
	meta *meta.Meta,
	cluster session.Cluster,
	targetMgr *meta.TargetManager,
	targetObserver *observers.TargetObserver,
) *ReleasePartitionJob {
	return &ReleasePartitionJob{
		BaseJob:        NewBaseJob(ctx, req.Base.GetMsgID(), req.GetCollectionID()),
		req:            req,
		dist:           dist,
		meta:           meta,
		cluster:        cluster,
		targetMgr:      targetMgr,
		targetObserver: targetObserver,
	}
}

func (job *ReleasePartitionJob) Execute() error {
	req := job.req
	log := log.Ctx(job.ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.Int64s("partitionIDs", req.GetPartitionIDs()),
	)
	if !job.meta.CollectionManager.Exist(req.GetCollectionID()) {
		log.Info("release collection end, the collection has not been loaded into QueryNode")
		return nil
	}

	loadedPartitions := job.meta.CollectionManager.GetPartitionsByCollection(req.GetCollectionID())
	partitionIDs := typeutil.NewUniqueSet(req.GetPartitionIDs()...)
	toRelease := make([]int64, 0)
	for _, partition := range loadedPartitions {
		if partitionIDs.Contain(partition.GetPartitionID()) {
			toRelease = append(toRelease, partition.GetPartitionID())
		}
	}

	// If all partitions are released and LoadType is LoadPartition, clear all
	if len(toRelease) == len(loadedPartitions) &&
		job.meta.GetLoadType(req.GetCollectionID()) == querypb.LoadType_LoadPartition {
		log.Info("release partitions covers all partitions, will remove the whole collection")
		err := job.meta.CollectionManager.RemoveCollection(req.GetCollectionID())
		if err != nil {
			msg := "failed to release partitions from store"
			log.Warn(msg, zap.Error(err))
			return utils.WrapError(msg, err)
		}
		err = job.meta.ReplicaManager.RemoveCollection(req.GetCollectionID())
		if err != nil {
			log.Warn("failed to remove replicas", zap.Error(err))
		}
		job.targetMgr.RemoveCollection(req.GetCollectionID())
		job.targetObserver.ReleaseCollection(req.GetCollectionID())
		waitCollectionReleased(job.dist, req.GetCollectionID())
	} else {
		err := releasePartitions(job.ctx, job.meta, job.cluster, false, req.GetCollectionID(), req.GetPartitionIDs()...)
		if err != nil {
			return err
		}
		err = job.meta.CollectionManager.RemovePartition(toRelease...)
		if err != nil {
			msg := "failed to release partitions from store"
			log.Warn(msg, zap.Error(err))
			return utils.WrapError(msg, err)
		}
		job.targetMgr.RemovePartition(req.GetCollectionID(), toRelease...)
		waitCollectionReleased(job.dist, req.GetCollectionID(), toRelease...)
	}
	metrics.QueryCoordNumCollections.WithLabelValues().Dec()
	return nil
}
