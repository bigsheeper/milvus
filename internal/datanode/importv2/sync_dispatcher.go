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

package importv2

import (
	"context"

	"github.com/milvus-io/milvus/internal/datanode/syncmgr"
	"github.com/milvus-io/milvus/pkg/util/conc"
)

type syncDispatcher struct {
	syncMgr    syncmgr.SyncManager
	workerPool *conc.Pool[error]
}

func newSyncDispatcher(maxParallel int, syncMgr syncmgr.SyncManager) *syncDispatcher {
	dispatcher := &syncDispatcher{
		syncMgr:    syncMgr,
		workerPool: conc.NewPool[error](maxParallel, conc.WithPreAlloc(false)),
	}
	return dispatcher
}

func (d *syncDispatcher) Submit(ctx context.Context, task syncmgr.Task) *conc.Future[error] {
	return d.workerPool.Submit(func() (error, error) {
		future := d.syncMgr.SyncData(ctx, task)
		return future.Await()
	})
}
