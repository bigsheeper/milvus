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

package observers

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/samber/lo"
)

type CollectionObserver struct {
	stopCh chan struct{}

	dist                  *meta.DistributionManager
	meta                  *meta.Meta
	targetMgr             *meta.TargetManager
	broker                meta.Broker
	handoffOb             *HandoffObserver
	collectionLoadedCount map[int64]int
	partitionLoadedCount  map[int64]int

	refreshed map[int64]time.Time

	stopOnce sync.Once
}

func NewCollectionObserver(
	dist *meta.DistributionManager,
	meta *meta.Meta,
	targetMgr *meta.TargetManager,
	broker meta.Broker,
	handoffObserver *HandoffObserver,
) *CollectionObserver {
	return &CollectionObserver{
		stopCh:                make(chan struct{}),
		dist:                  dist,
		meta:                  meta,
		targetMgr:             targetMgr,
		broker:                broker,
		handoffOb:             handoffObserver,
		collectionLoadedCount: make(map[int64]int),
		partitionLoadedCount:  make(map[int64]int),

		refreshed: make(map[int64]time.Time),
	}
}

func (ob *CollectionObserver) Start(ctx context.Context) {
	const observePeriod = time.Second
	go func() {
		ticker := time.NewTicker(observePeriod)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				log.Info("CollectionObserver stopped due to context canceled")
				return

			case <-ob.stopCh:
				log.Info("CollectionObserver stopped")
				return

			case <-ticker.C:
				ob.Observe()
			}
		}
	}()
}

func (ob *CollectionObserver) Stop() {
	ob.stopOnce.Do(func() {
		close(ob.stopCh)
	})
}

func (ob *CollectionObserver) Observe() {
	ob.observeTimeout()
	ob.observeLoadStatus()
}

func (ob *CollectionObserver) observeTimeout() {
	collections := ob.meta.CollectionManager.GetAllCollections()
	for _, collection := range collections {
		if collection.GetStatus() != querypb.LoadStatus_Loading {
			continue
		}

		refreshTime := collection.UpdatedAt.Add(Params.QueryCoordCfg.RefreshTargetsIntervalSeconds)
		timeoutTime := collection.UpdatedAt.Add(Params.QueryCoordCfg.LoadTimeoutSeconds)

		now := time.Now()
		if now.After(timeoutTime) {
			log.Info("load collection timeout, cancel it",
				zap.Int64("collectionID", collection.GetCollectionID()),
				zap.Duration("loadTime", time.Since(collection.CreatedAt)))
			ob.meta.CollectionManager.RemoveCollection(collection.GetCollectionID())
			ob.meta.ReplicaManager.RemoveCollection(collection.GetCollectionID())
			ob.targetMgr.RemoveCollection(collection.GetCollectionID())
		} else if now.After(refreshTime) {
			if ob.refreshTargets(collection.UpdatedAt, collection.GetCollectionID()) {
				log.Info("load for long time, refresh targets of collection",
					zap.Duration("loadTime", time.Since(collection.CreatedAt)),
				)
			}
		}
	}

	partitions := utils.GroupPartitionsByCollection(
		ob.meta.CollectionManager.GetAllPartitions())
	if len(partitions) > 0 {
		log.Info("observes partitions timeout", zap.Int("partitionNum", len(partitions)))
	}
	for collection, partitions := range partitions {
		log := log.With(
			zap.Int64("collectionID", collection),
		)
		for _, partition := range partitions {
			if partition.GetStatus() != querypb.LoadStatus_Loading {
				continue
			}

			refreshTime := partition.UpdatedAt.Add(Params.QueryCoordCfg.RefreshTargetsIntervalSeconds)
			timeoutTime := partition.UpdatedAt.Add(Params.QueryCoordCfg.LoadTimeoutSeconds)

			now := time.Now()
			if now.After(timeoutTime) {
				log.Info("load partition timeout, cancel all partitions",
					zap.Int64("partitionID", partition.GetPartitionID()),
					zap.Duration("loadTime", time.Since(partition.CreatedAt)))
				// TODO(yah01): Now, releasing part of partitions is not allowed
				ob.meta.CollectionManager.RemoveCollection(partition.GetCollectionID())
				ob.meta.ReplicaManager.RemoveCollection(partition.GetCollectionID())
				ob.targetMgr.RemoveCollection(partition.GetCollectionID())
				break
			} else if now.After(refreshTime) {
				partitionIDs := lo.Map(partitions, func(partition *meta.Partition, _ int) int64 {
					return partition.GetPartitionID()
				})
				if ob.refreshTargets(partition.UpdatedAt, partition.GetCollectionID(), partitionIDs...) {
					log.Info("load for long time, refresh targets of partitions",
						zap.Duration("loadTime", time.Since(partition.CreatedAt)),
					)
				}
				break
			}
		}
	}
}

// refreshTargets refreshes the targets of the given collection,
// avoids repeated refreshing by checking the updatedAt,
// returns true if actually refreshed the targets,
// false otherwise
func (ob *CollectionObserver) refreshTargets(updatedAt time.Time, collectionID int64, partitions ...int64) bool {
	refreshedTime, ok := ob.refreshed[collectionID]
	if ok && refreshedTime.Equal(updatedAt) {
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ob.handoffOb.Unregister(ctx, collectionID)

	if len(partitions) == 0 {
		var err error
		partitions, err = ob.broker.GetPartitions(ctx, collectionID)
		if err != nil {
			log.Warn("failed to get partitions from RootCoord, will refresh targets later", zap.Error(err))
			return false
		}
	}

	ob.handoffOb.Register(collectionID)
	channels, segments, err := utils.FetchTargets(ctx, ob.targetMgr, ob.broker, collectionID, partitions)
	if err != nil {
		log.Warn("failed to fetch targets from DataCoord, will refresh targets later", zap.Error(err))
		return false
	}
	ob.targetMgr.Replace(collectionID, channels, segments)

	ob.refreshed[collectionID] = updatedAt
	return true
}

func (ob *CollectionObserver) observeLoadStatus() {
	collections := ob.meta.CollectionManager.GetAllCollections()
	for _, collection := range collections {
		if collection.LoadPercentage == 100 {
			continue
		}
		ob.observeCollectionLoadStatus(collection)
	}

	partitions := ob.meta.CollectionManager.GetAllPartitions()
	if len(partitions) > 0 {
		log.Info("observe partitions status", zap.Int("partitionNum", len(partitions)))
	}
	for _, partition := range partitions {
		if partition.LoadPercentage == 100 {
			continue
		}
		ob.observePartitionLoadStatus(partition)
	}
}

func (ob *CollectionObserver) observeCollectionLoadStatus(collection *meta.Collection) {
	log := log.With(zap.Int64("collectionID", collection.GetCollectionID()))

	segmentTargets := ob.targetMgr.GetSegmentsByCollection(collection.GetCollectionID())
	channelTargets := ob.targetMgr.GetDmChannelsByCollection(collection.GetCollectionID())
	targetNum := len(segmentTargets) + len(channelTargets)
	log.Info("collection targets",
		zap.Int("segmentTargetNum", len(segmentTargets)),
		zap.Int("channelTargetNum", len(channelTargets)),
		zap.Int("totalTargetNum", targetNum),
		zap.Int32("replicaNum", collection.GetReplicaNumber()),
	)
	if targetNum == 0 {
		log.Info("collection released, skip it")
		return
	}

	loadedCount := 0
	for _, channel := range channelTargets {
		group := utils.GroupNodesByReplica(ob.meta.ReplicaManager,
			collection.GetCollectionID(),
			ob.dist.LeaderViewManager.GetChannelDist(channel.GetChannelName()))
		loadedCount += len(group)
	}
	subChannelCount := loadedCount
	for _, segment := range segmentTargets {
		group := utils.GroupNodesByReplica(ob.meta.ReplicaManager,
			collection.GetCollectionID(),
			ob.dist.LeaderViewManager.GetSealedSegmentDist(segment.GetID()))
		loadedCount += len(group)
	}
	if loadedCount > 0 {
		log.Info("collection load progress",
			zap.Int("subChannelCount", subChannelCount),
			zap.Int("loadSegmentCount", loadedCount-subChannelCount),
		)
	}

	updated := collection.Clone()
	targetNum *= int(collection.GetReplicaNumber())
	updated.LoadPercentage = int32(loadedCount * 100 / targetNum)
	if loadedCount <= ob.collectionLoadedCount[collection.GetCollectionID()] {
		return
	}

	ob.collectionLoadedCount[collection.GetCollectionID()] = loadedCount
	if loadedCount >= targetNum {
		delete(ob.collectionLoadedCount, collection.GetCollectionID())
		updated.Status = querypb.LoadStatus_Loaded
		ob.meta.CollectionManager.UpdateCollection(updated)
		ob.handoffOb.StartHandoff(updated.GetCollectionID())

		elapsed := time.Since(updated.CreatedAt)
		metrics.QueryCoordLoadLatency.WithLabelValues().Observe(float64(elapsed.Milliseconds()))
	} else {
		ob.meta.CollectionManager.UpdateCollectionInMemory(updated)
	}
	log.Info("collection load status updated",
		zap.Int32("loadPercentage", updated.LoadPercentage),
		zap.Int32("collectionStatus", int32(updated.GetStatus())))
}

func (ob *CollectionObserver) observePartitionLoadStatus(partition *meta.Partition) {
	log := log.With(
		zap.Int64("collectionID", partition.GetCollectionID()),
		zap.Int64("partitionID", partition.GetPartitionID()),
	)

	segmentTargets := ob.targetMgr.GetSegmentsByCollection(partition.GetCollectionID(), partition.GetPartitionID())
	channelTargets := ob.targetMgr.GetDmChannelsByCollection(partition.GetCollectionID())
	targetNum := len(segmentTargets) + len(channelTargets)
	log.Info("partition targets",
		zap.Int("segmentTargetNum", len(segmentTargets)),
		zap.Int("channelTargetNum", len(channelTargets)),
		zap.Int("totalTargetNum", targetNum),
		zap.Int32("replicaNum", partition.GetReplicaNumber()),
	)
	if targetNum == 0 {
		log.Info("partition released, skip it")
		return
	}

	loadedCount := 0
	for _, channel := range channelTargets {
		group := utils.GroupNodesByReplica(ob.meta.ReplicaManager,
			partition.GetCollectionID(),
			ob.dist.LeaderViewManager.GetChannelDist(channel.GetChannelName()))
		loadedCount += len(group)
	}
	subChannelCount := loadedCount
	for _, segment := range segmentTargets {
		group := utils.GroupNodesByReplica(ob.meta.ReplicaManager,
			partition.GetCollectionID(),
			ob.dist.LeaderViewManager.GetSealedSegmentDist(segment.GetID()))
		loadedCount += len(group)
	}
	if loadedCount > 0 {
		log.Info("partition load progress",
			zap.Int("subChannelCount", subChannelCount),
			zap.Int("loadSegmentCount", loadedCount-subChannelCount))
	}

	updated := partition.Clone()
	targetNum *= int(partition.GetReplicaNumber())
	updated.LoadPercentage = int32(loadedCount * 100 / targetNum)
	if loadedCount <= ob.partitionLoadedCount[partition.GetPartitionID()] {
		return
	}

	ob.partitionLoadedCount[partition.GetPartitionID()] = loadedCount
	if loadedCount >= targetNum {
		delete(ob.partitionLoadedCount, partition.GetPartitionID())
		updated.Status = querypb.LoadStatus_Loaded
		ob.meta.CollectionManager.UpdatePartition(updated)
		ob.handoffOb.StartHandoff(updated.GetCollectionID())

		elapsed := time.Since(updated.CreatedAt)
		metrics.QueryCoordLoadLatency.WithLabelValues().Observe(float64(elapsed.Milliseconds()))
	} else {
		ob.meta.CollectionManager.UpdatePartitionInMemory(updated)
	}
	log.Info("partition load status updated",
		zap.Int32("loadPercentage", updated.LoadPercentage),
		zap.Int32("partitionStatus", int32(updated.GetStatus())))
}
