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

package task

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const NullNodeID = -1

type GlobalScheduler interface {
	Enqueue(task Task)
	AbortAndRemoveTask(taskID int64)

	Start()
	Stop()
}

var _ GlobalScheduler = (*globalTaskScheduler)(nil)

type globalTaskScheduler struct {
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	notifyChan chan struct{}

	pendingTasks FIFOQueue
	runningTasks *typeutil.ConcurrentMap[int64, Task]
	execPool     *conc.Pool[struct{}]
	checkPool    *conc.Pool[struct{}]
	cluster      session.Cluster
}

func (s *globalTaskScheduler) Enqueue(task Task) {
	if s.runningTasks.Contain(task.GetTaskID()) {
		return
	}
	if s.pendingTasks.Get(task.GetTaskID()) != nil {
		return
	}
	s.pendingTasks.Push(task)
}

func (s *globalTaskScheduler) AbortAndRemoveTask(taskID int64) {
	//TODO implement me
	panic("implement me")
}

func (s *globalTaskScheduler) Start() {
	dur := paramtable.Get().DataCoordCfg.TaskScheduleInterval.GetAsDuration(time.Millisecond)
	s.wg.Add(2)
	go func() {
		defer s.wg.Done()
		t := time.NewTicker(dur)
		defer t.Stop()
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-t.C:
				s.schedule()
			case <-s.notifyChan:
				s.schedule()
			}
		}
	}()
	go func() {
		defer s.wg.Done()
		t := time.NewTicker(dur)
		defer t.Stop()
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-t.C:
				s.check()
			}
		}
	}()
}

func (s *globalTaskScheduler) Stop() {
	s.cancel()
	s.wg.Wait()
}

func (s *globalTaskScheduler) notify() {
	select {
	case s.notifyChan <- struct{}{}:
	default:
	}
}

func (s *globalTaskScheduler) pickNode(nodeSlots map[int64]int64) int64 {
	for w, slots := range nodeSlots {
		if slots >= 1 {
			nodeSlots[w] = slots - 1
			return w
		}
	}
	return NullNodeID
}

func (s *globalTaskScheduler) schedule() {
	pendingNum := len(s.pendingTasks.TaskIDs())
	if pendingNum == 0 {
		return
	}
	nodeSlots := s.cluster.QuerySlots()
	log.Ctx(s.ctx).Info("scheduling pending tasks...", zap.Int("num", pendingNum), zap.Any("nodeSlots", nodeSlots))

	for {
		nodeID := s.pickNode(nodeSlots)
		if nodeID == NullNodeID {
			break
		}
		task := s.pendingTasks.Pop()
		if task == nil {
			break
		}
		s.execPool.Submit(func() (struct{}, error) {
			log.Ctx(s.ctx).Info("processing task...", WrapTaskLog(task)...)
			if task.GetTaskState() == Pending {
				task.CreateTaskOnWorker(nodeID, s.cluster)
				switch task.GetTaskState() {
				case Pending, Retry:
					s.pendingTasks.Push(task)
				case InProgress:
					s.runningTasks.Insert(task.GetTaskID(), task)
				}
			}
			return struct{}{}, nil
		})
	}
}

func (s *globalTaskScheduler) check() {
	if s.runningTasks.Len() <= 0 {
		return
	}
	log.Ctx(s.ctx).Info("check running tasks", zap.Int("num", s.runningTasks.Len()))

	tasks := s.runningTasks.Values()
	for _, task := range tasks {
		s.checkPool.Submit(func() (struct{}, error) {
			task.QueryTaskOnWorker(s.cluster)
			switch task.GetTaskState() {
			case Retry:
				s.pendingTasks.Push(task)
			case Finished, Failed:
				task.DropTaskOnWorker(s.cluster)
				s.runningTasks.Remove(task.GetTaskID())
			}
			return struct{}{}, nil
		})
	}
}

func NewGlobalTaskScheduler(ctx context.Context, cluster session.Cluster) GlobalScheduler {
	execPool := conc.NewPool[struct{}](128)
	checkPool := conc.NewPool[struct{}](128)
	ctx1, cancel := context.WithCancel(ctx)
	return &globalTaskScheduler{
		ctx:          ctx1,
		cancel:       cancel,
		wg:           sync.WaitGroup{},
		notifyChan:   make(chan struct{}, 1),
		pendingTasks: NewFIFOQueue(),
		runningTasks: typeutil.NewConcurrentMap[int64, Task](),
		execPool:     execPool,
		checkPool:    checkPool,
		cluster:      cluster,
	}
}
