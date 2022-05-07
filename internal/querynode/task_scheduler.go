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
	"container/list"
	"context"
	"fmt"
	"go.uber.org/zap"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/milvus-io/milvus/internal/log"
)

const (
	maxReceiveSQChanLen = 1024 * 10
	maxExecuteSQChanLen = 1024 * 10
)

type taskScheduler struct {
	ctx    context.Context
	cancel context.CancelFunc

	// for search and query start
	unsolvedSQTasks *list.List
	readySQTasks    *list.List

	receiveSQTaskChan chan sqTask
	executeSQTaskChan chan sqTask

	notifyChan      chan bool
	tsafeUpdateChan chan bool

	scheduleSQPolicy scheduleSQPolicy
	// for search and query end

	cpuUsage int32 // 1200 means 1200% 12 cores

	// for other tasks
	queue       taskQueue
	maxCpuUsage int32

	wg sync.WaitGroup
}

func newTaskScheduler(ctx context.Context) *taskScheduler {
	ctx1, cancel := context.WithCancel(ctx)
	s := &taskScheduler{
		ctx:               ctx1,
		cancel:            cancel,
		unsolvedSQTasks:   list.New(),
		readySQTasks:      list.New(),
		receiveSQTaskChan: make(chan sqTask, maxReceiveSQChanLen),
		executeSQTaskChan: make(chan sqTask, maxExecuteSQChanLen),
		notifyChan:        make(chan bool, 1),
		tsafeUpdateChan:   make(chan bool, 1),
		maxCpuUsage:       int32(runtime.NumCPU() * 100),
		scheduleSQPolicy:  defaultScheduleSQPolicy,
	}
	s.queue = newQueryNodeTaskQueue(s)
	return s
}

func (s *taskScheduler) processTask(t task, q taskQueue) {
	// TODO: ctx?
	err := t.PreExecute(s.ctx)

	defer func() {
		t.Notify(err)
	}()
	if err != nil {
		log.Warn(err.Error())
		return
	}

	q.AddActiveTask(t)
	defer func() {
		q.PopActiveTask(t.ID())
	}()

	err = t.Execute(s.ctx)
	if err != nil {
		log.Warn(err.Error())
		return
	}
	err = t.PostExecute(s.ctx)
}

func (s *taskScheduler) taskLoop() {
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.queue.utChan():
			if !s.queue.utEmpty() {
				t := s.queue.PopUnissuedTask()
				s.processTask(t, s.queue)
			}
		}
	}
}

func (s *taskScheduler) Start() {
	s.wg.Add(1)
	go s.taskLoop()

	s.wg.Add(1)
	go s.scheduleSQTasks()

	s.wg.Add(1)
	go s.executeSQTasks()
}

func (s *taskScheduler) scheduleSQTasks() {
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			log.Info("QueryNode sop schedulerSQTasks")
			return
		case <-s.notifyChan:
			s.tryMergeSQTasks()
			s.popAndAddToExecute()

		case <-s.receiveSQTaskChan:
			s.tryMergeSQTasks()
			s.popAndAddToExecute()

		case <-s.tsafeUpdateChan:
			s.tryMergeSQTasks()
			s.popAndAddToExecute()
		}
	}
}

func (s *taskScheduler) addSQTask(t sqTask, ctx context.Context) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("taskScheduler AddSQTask timeout")
	case <-s.ctx.Done():
		return fmt.Errorf("taskScheduler stoped")
	case s.executeSQTaskChan <- t:
		return nil
	}
}

func (s *taskScheduler) popAndAddToExecute() {
	readyLen := s.readySQTasks.Len()
	if readyLen <= 0 {
		return
	}
	curUsage := atomic.LoadInt32(&s.cpuUsage)
	targetUsage := s.maxCpuUsage - curUsage
	if targetUsage <= 0 {
		return
	}
	tasks, deltaUsage := s.scheduleSQPolicy(s.readySQTasks, targetUsage)
	if deltaUsage <= 0 || tasks == nil {
		return
	}
	for _, t := range tasks {
		s.executeSQTaskChan <- t
	}
	atomic.AddInt32(&s.cpuUsage, deltaUsage)
}

func (s *taskScheduler) executeSQTasks() {
	defer s.wg.Done()
	var taskWg sync.WaitGroup
	defer taskWg.Wait()
	for {
		select {
		case <-s.ctx.Done():
			log.Debug("QueryNode stop executeSQTasks", zap.Int64("NodeID", Params.QueryNodeCfg.GetNodeID()))
			return
		case t, ok := <-s.executeSQTaskChan:
			if ok {
				taskWg.Add(1)
				go func(t sqTask) {
					defer taskWg.Done()
					s.processSQTask(t)
					atomic.AddInt32(&s.cpuUsage, -t.EstimateCpuUsage())
					if len(s.notifyChan) == 0 {
						s.notifyChan <- true
					}
				}(t)

				pendingTaskLen := len(s.executeSQTaskChan)
				for i := 0; i < pendingTaskLen; i++ {
					taskWg.Add(1)
					t := <-s.executeSQTaskChan
					go func(t sqTask) {
						defer taskWg.Done()
						s.processSQTask(t)
						atomic.AddInt32(&s.cpuUsage, -t.EstimateCpuUsage())
						if len(s.notifyChan) == 0 {
							s.notifyChan <- true
						}
					}(t)
				}
				log.Info("QueryNode taskScheduler executeSQTasks process tasks done", zap.Int("numOfTasks", pendingTaskLen+1))
			} else {
				errMsg := "taskScheduler executeSQTaskChan has been closed"
				log.Warn(errMsg)
				return
			}
		}
	}
}

func (s *taskScheduler) processSQTask(t sqTask) {
	err := t.PreExecute(t.Ctx())

	defer func() {
		t.Notify(err)
	}()
	if err != nil {
		log.Warn(err.Error())
		return
	}

	err = t.Execute(s.ctx)
	if err != nil {
		log.Warn(err.Error())
	}
	err = t.PostExecute(s.ctx)
}

func (s *taskScheduler) Close() {
	s.cancel()
	s.wg.Wait()
}

func (s *taskScheduler) tryMergeSQTasks() {
	unsolvedLen := s.unsolvedSQTasks.Len()
	//metrics.QueryNodeWaitForMergeReqs.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID())).Set(float64(unsolvedLen))
	log.Debug("tryMergeSQTasks before", zap.Int("unsolvedSearchTasksLen", unsolvedLen))
	if unsolvedLen == 0 {
		return
	}

	for e := s.unsolvedSQTasks.Front(); e != nil; e = e.Next() {
		t, ok := e.Value.(sqTask)
		if !ok {
			log.Warn("can not cast to sTask")
			s.unsolvedSQTasks.Remove(e)
			continue
		}
		canDo, err := t.CanDo()
		if err != nil {
			s.unsolvedSQTasks.Remove(e)
			t.SetErr(err)
			t.PostExecute(t.Ctx())
			continue
		}
		if canDo {
			merged := false
			for m := s.readySQTasks.Back(); m != nil; m = e.Prev() {
				mTask, ok := m.Value.(sqTask)
				if !ok {
					continue
				}
				if mTask.CanMergeWith(t) {
					mTask.Merge(t)
					merged = true
					break
				}
			}
			if !merged {
				s.readySQTasks.PushBack(t)
			}
			s.unsolvedSQTasks.Remove(e)
		}
	}
	//mergedLen := s.readySQTasks.Len()
	//metrics.QueryNodeWaitForExecuteReqs.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID())).Set(float64(mergedLen))
}
