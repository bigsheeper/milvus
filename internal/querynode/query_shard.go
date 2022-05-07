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
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"math"
	"sync"
	"time"

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type queryShard struct {
	ctx    context.Context
	cancel context.CancelFunc

	collectionID UniqueID
	channel      Channel
	deltaChannel Channel
	replicaID    int64

	clusterService *ShardClusterService
	historical     *historical
	streaming      *streaming

	dmTSafeWatcher    *tSafeWatcher
	deltaTSafeWatcher *tSafeWatcher
	watcherCond       *sync.Cond
	serviceDmTs       atomic.Uint64
	serviceDeltaTs    atomic.Uint64
	startTickerOnce   sync.Once
	ticker            *time.Ticker // timed ticker for trigger timeout check

	localChunkManager  storage.ChunkManager
	remoteChunkManager storage.ChunkManager
	vectorChunkManager *storage.VectorChunkManager
	localCacheEnabled  bool
	localCacheSize     int64
	tsafeUpdateChan    chan bool
	once               sync.Once
}

func newQueryShard(
	ctx context.Context,
	collectionID UniqueID,
	channel Channel,
	replicaID int64,
	clusterService *ShardClusterService,
	historical *historical,
	streaming *streaming,
	localChunkManager storage.ChunkManager,
	remoteChunkManager storage.ChunkManager,
	localCacheEnabled bool,
	tsafeChan chan bool,
) *queryShard {
	ctx, cancel := context.WithCancel(ctx)
	qs := &queryShard{
		ctx:                ctx,
		cancel:             cancel,
		collectionID:       collectionID,
		channel:            channel,
		replicaID:          replicaID,
		clusterService:     clusterService,
		historical:         historical,
		streaming:          streaming,
		localChunkManager:  localChunkManager,
		remoteChunkManager: remoteChunkManager,
		localCacheEnabled:  localCacheEnabled,
		localCacheSize:     Params.QueryNodeCfg.CacheMemoryLimit,

		watcherCond:     sync.NewCond(&sync.Mutex{}),
		tsafeUpdateChan: tsafeChan,
	}
	deltaChannel, err := funcutil.ConvertChannelName(channel, Params.CommonCfg.RootCoordDml, Params.CommonCfg.RootCoordDelta)
	if err != nil {
		log.Warn("failed to convert dm channel to delta", zap.String("channel", channel), zap.Error(err))
	}
	qs.deltaChannel = deltaChannel
	qs.initVectorChunkManager()
	return qs
}

// Close cleans query shard
func (q *queryShard) Close() {
	q.cancel()
}

func (q *queryShard) watchDMLTSafe() error {
	q.dmTSafeWatcher = newTSafeWatcher()
	err := q.streaming.tSafeReplica.registerTSafeWatcher(q.channel, q.dmTSafeWatcher)
	if err != nil {
		log.Warn("failed to register dml tsafe watcher", zap.String("channel", q.channel), zap.Error(err))
		return err
	}
	go q.watchTs(q.dmTSafeWatcher.watcherChan(), q.dmTSafeWatcher.closeCh, tsTypeDML)

	q.startTsTicker()
	return nil
}

func (q *queryShard) watchDeltaTSafe() error {
	q.deltaTSafeWatcher = newTSafeWatcher()
	err := q.streaming.tSafeReplica.registerTSafeWatcher(q.deltaChannel, q.deltaTSafeWatcher)
	if err != nil {
		log.Warn("failed to register delta tsafe watcher", zap.String("channel", q.deltaChannel), zap.Error(err))
		return err
	}

	go q.watchTs(q.deltaTSafeWatcher.watcherChan(), q.deltaTSafeWatcher.closeCh, tsTypeDelta)
	q.startTsTicker()

	return nil
}

func (q *queryShard) startTsTicker() {
	q.startTickerOnce.Do(func() {
		go func() {
			q.ticker = time.NewTicker(time.Millisecond * 50) // check timeout every 50 milliseconds, need not to be to frequent
			defer q.ticker.Stop()
			for {
				select {
				case <-q.ticker.C:
					q.watcherCond.L.Lock()
					q.watcherCond.Broadcast()
					q.watcherCond.L.Unlock()
				case <-q.ctx.Done():
					return
				}
			}
		}()
	})
}

type tsType int32

const (
	tsTypeDML   tsType = 1
	tsTypeDelta tsType = 2
)

func (tp tsType) String() string {
	switch tp {
	case tsTypeDML:
		return "DML tSafe"
	case tsTypeDelta:
		return "Delta tSafe"
	}
	return ""
}

func (q *queryShard) watchTs(channel <-chan bool, closeCh <-chan struct{}, tp tsType) {
	for {
		select {
		case <-q.ctx.Done():
			log.Info("stop queryShard watcher due to ctx done", zap.Int64("collectionID", q.collectionID), zap.String("vChannel", q.channel))
			return
		case <-closeCh:
			log.Info("stop queryShard watcher due to watcher closed", zap.Int64("collectionID", q.collectionID), zap.String("vChannel", q.channel))
			return
		case _, ok := <-channel:
			if !ok {
				log.Warn("tsafe watcher channel closed", zap.Int64("collectionID", q.collectionID), zap.String("vChannel", q.channel))
				return
			}

			ts, err := q.getNewTSafe(tp)
			if err == nil {
				q.watcherCond.L.Lock()
				q.setServiceableTime(ts, tp)
				q.watcherCond.Broadcast()
				q.watcherCond.L.Unlock()
			}
		}
	}
}

func (q *queryShard) getNewTSafe(tp tsType) (Timestamp, error) {
	var channel string
	switch tp {
	case tsTypeDML:
		channel = q.channel
	case tsTypeDelta:
		channel = q.deltaChannel
	default:
		return 0, errors.New("invalid ts type")
	}
	t := Timestamp(math.MaxInt64)
	ts, err := q.streaming.tSafeReplica.getTSafe(channel)
	if err != nil {
		return 0, err
	}
	if ts <= t {
		t = ts
	}
	return t, nil
}

func (q *queryShard) waitUntilServiceable(ctx context.Context, guaranteeTs Timestamp, tp tsType) {
	q.watcherCond.L.Lock()
	defer q.watcherCond.L.Unlock()
	st := q.getServiceableTime(tp)
	for guaranteeTs > st {
		log.Debug("serviceable ts before guarantee ts", zap.Uint64("serviceable ts", st), zap.Uint64("guarantee ts", guaranteeTs), zap.String("channel", q.channel))
		q.watcherCond.Wait()
		if err := ctx.Err(); err != nil {
			log.Warn("waitUntialServiceable timeout", zap.Uint64("serviceable ts", st), zap.Uint64("guarantee ts", guaranteeTs), zap.String("channel", q.channel))
			return
		}
		st = q.getServiceableTime(tp)
	}
	log.Debug("wait serviceable ts done", zap.String("tsType", tp.String()), zap.Uint64("guarantee ts", guaranteeTs), zap.Uint64("serviceable ts", st), zap.String("channel", q.channel))
}

func (q *queryShard) getServiceableTime(tp tsType) Timestamp {
	gracefulTimeInMilliSecond := Params.QueryNodeCfg.GracefulTime
	gracefulTime := typeutil.ZeroTimestamp
	if gracefulTimeInMilliSecond > 0 {
		gracefulTime = tsoutil.ComposeTS(gracefulTimeInMilliSecond, 0)
	}
	var serviceTs Timestamp
	switch tp {
	case tsTypeDML: // use min value of dml & delta
		serviceTs = q.serviceDmTs.Load()
	case tsTypeDelta: // check delta ts only
		serviceTs = q.serviceDeltaTs.Load()
	}
	return serviceTs + gracefulTime
}

func (q *queryShard) setServiceableTime(t Timestamp, tp tsType) {
	updated := false
	defer func() {
		if updated && len(q.tsafeUpdateChan) == 0 {
			q.tsafeUpdateChan <- true
		}
	}()
	switch tp {
	case tsTypeDML:
		ts := q.serviceDmTs.Load()
		if t < ts {
			return
		}
		for !q.serviceDmTs.CAS(ts, t) {
			ts = q.serviceDmTs.Load()
			if t < ts {
				updated = true
				return
			}
		}
	case tsTypeDelta:
		ts := q.serviceDeltaTs.Load()
		if t < ts {
			return
		}
		for !q.serviceDeltaTs.CAS(ts, t) {
			ts = q.serviceDeltaTs.Load()
			if t < ts {
				updated = true
				return
			}
		}
	}
}

func (q *queryShard) initVectorChunkManager() {
	collection, _ := q.streaming.replica.getCollectionByID(q.collectionID)
	if q.vectorChunkManager == nil {
		var err error
		q.vectorChunkManager, err = storage.NewVectorChunkManager(q.localChunkManager, q.remoteChunkManager,
			&etcdpb.CollectionMeta{
				ID:     q.collectionID,
				Schema: collection.schema,
			}, q.localCacheSize, q.localCacheEnabled)
		if err != nil {
			panic(err)
		}
	}
}
