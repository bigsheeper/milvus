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
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"go.uber.org/zap"
	"sync"

	"github.com/milvus-io/milvus/internal/log"
)

const (
	maxReceiveSQChanLen = 1024 * 10
	maxExecuteSQChanLen = 1024 * 10
	maxPublishSQChanLen = 1024 * 10
)

type taskScheduler struct {
	ctx    context.Context
	cancel context.CancelFunc

	// for search and query start
	mergedSearchTasks    []sqTask

	unsolvedSearchTasks  *list.List
	unsolvedQueryTasks   *list.List

	receiveSQTaskChan  chan sqTask
	executeSQTaskChan  chan sqTask

	notifyChan      chan bool
	tsafeUpdateChan chan bool
	// for search and query end

	// for other tasks 
	queue taskQueue

	wg    sync.WaitGroup
}

func newTaskScheduler(ctx context.Context) *taskScheduler {
	ctx1, cancel := context.WithCancel(ctx)
	s := &taskScheduler{
		ctx:    ctx1,
		cancel: cancel,
		unsolvedSearchTasks: list.New(),
		unsolvedQueryTasks:  list.New(),
		receiveSQTaskChan:  make(chan sqTask, maxReceiveSQChanLen),
		executeSQTaskChan:     make(chan sqTask, maxExecuteSQChanLen),
		notifyChan:      make(chan bool, 1),
		tsafeUpdateChan: make(chan bool, 1),

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

// how to spawn tasks is in here
func (s *taskScheduler) popAndAddToExecute() {
	if len(q.executeSQTaskChan) < maxExecuteReqs && q.executeNQNum.Load().(int64) < maxExecuteNQ {
		if len(q.mergedMsgs) > 0 {
			msg := q.mergedMsgs[0]
			q.executeChan <- msg
			q.mergedMsgs = q.mergedMsgs[1:]
			sMsg, ok := msg.(*searchMsg)
			if ok {
				q.executeNQNum.Store(q.executeNQNum.Load().(int64) + sMsg.NQ)
			}
		}
	}
}

func (s *taskScheduler) scheduleSQTasks() {
	defer s.wg.Done()
	log.Debug("starting doUnsolvedMsg...", zap.Any("collectionID", q.collectionID))

	for {
		select {
		case <-s.releaseCtx.Done():
			log.Info("stop Collection's doUnsolvedMsg", zap.Int64("collectionID", q.collectionID))
			return
		case <-s.notifyChan:
			if len(s.needWaitNewTsafeMsgs) == 0 && len(s.mergedMsgs) == 0 {
				continue
			}
			s.mergedMsgs, s.needWaitNewTsafeMsgs = s.tryMergeSearchTasks(s.mergedMsgs, s.needWaitNewTsafeMsgs)
			s.popAndAddToQuery()

		case <-s.receiveSQTaskChan:
			s.needWaitNewTsafeMsgs = append(s.needWaitNewTsafeMsgs, s.popAllUnsolvedMsg()...)
			s.mergedMsgs, s.needWaitNewTsafeMsgs = s.tryMergeSearchTasks(s.mergedMsgs, s.needWaitNewTsafeMsgs)
			s.popAndAddToQuery()

		case <-s.tsafeUpdateChan:
			s.needWaitNewTsafeMsgs = append(s.needWaitNewTsafeMsgs, s.popAllUnsolvedMsg()...)
			if len(s.needWaitNewTsafeMsgs) == 0 && len(s.mergedMsgs) == 0 {
				continue
			}
			s.mergedMsgs, s.needWaitNewTsafeMsgs = s.tryMergeSearchTasks(s.mergedMsgs, s.needWaitNewTsafeMsgs)
			s.popAndAddToQuery()
			//runtime.GC()
		}
	}
}

func (s *taskScheduler) executeSQTasks() {
	defer s.wg.Done()
	for {
		select {
		case <-s.releaseCtx.Done():
			log.Debug("stop Collection's executeSearchMsg", zap.Int64("collectionID", q.collectionID))
			return
		case t, ok := <-s.executeSQTaskChan:
			if ok {
				var wg sync.WaitGroup
				wg.Add(1)
				go func(wg *sync.WaitGroup, t task){
					defer wg.Done()
					s.processSQTask(t)
				}(&wg, t)

				taskLen := len(s.executeSQTaskChan)
				for i:=0; i< taskLen; i++ {
					wg.Add(1)
					go func(wg *sync.WaitGroup, t task){
						defer wg.Done()
						s.processSQTask(t)
					}(&wg, t)
				}
				wg.Wait()
				log.Info("QueryNode taskScheduler executeSQTasks process tasks done", zap.Int64("numOfTasks", taskLen +1))
			} else {
				errMsg := "taskScheduler executeSQTaskChan has been closed"
				log.Warn(errMsg)
				return
			}
		}
	}
}

func (s *taskScheduler) processSQTask(t task) {
	err := t.PreExecute(t.ctx)

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

func (s *taskScheduler) tryMergeSearchTasks() {
//	s.mergedMsgs, s.needWaitNewTsafeMsgs
//mergedMsgs []queryMsg, queryMsgs []queryMsg
	unsolvedSearchTasksLen := s.unsolvedSearchTasks.Len()
	metrics.QueryNodeWaitForMergeReqs.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID())).Set(float64(unsolvedSearchTasksLen))
	log.Debug("tryMergeSearchTasks before", zap.Int("unsolvedSearchTasksLen", len(unsolvedSearchTasksLen)))
	if unsolvedSearchTasksLen == 0 {
		return
	}
	for e := s.unsolvedSearchTasks.Front(); e != nil; e = e.Next() {
		sTask, ok := e.Value.(*searchTask)
		if !ok {
			log.Warn("can not cast to sTask")
			s.unsolvedSearchTasks.Remove(e)
			continue
		}
		canDo, err := sTask.CanDo()
		if err != nil {
			sTask.SetErr(err)
			sTask.PostExecute()
			continue
		}
		if canDo {


		}
	}

	for i := 0; i < len(queryMsgs); i++ {
		msg := queryMsgs[i]
		ok, err := q.checkSearchCanDo(msg)
		log.Debug("judge if msg can do", zap.Int64("msgID", msg.ID()), zap.Int64("collectionID", q.collectionID),
			zap.Bool("ok", ok), zap.Error(err), zap.Bool("merge search requests", q.mergeMsgs))

		if err != nil {
			pubRet := &pubResult{
				Msg: msg,
				Err: err,
			}
			q.addToPublishChan(pubRet)
			log.Error("search request fast fail", zap.Int64("msgID", msg.ID()), zap.Error(err))
			continue
		}
		if ok {
			if q.mergeMsgs {
				merge := false
				for j, mergedMsg := range mergedMsgs {
					if canMerge(mergedMsg, msg) {
						mergedMsgs[j] = mergeSearchMsg(mergedMsg, msg)
						merge = true
						log.Info("Merge search message", zap.Int("num", mergedMsg.GetNumMerged()),
							zap.Int64("mergedMsgID", mergedMsg.ID()), zap.Int64("msgID", msg.ID()),
							zap.Int("merged msg nq", msg.GetNumMerged()))
						break
					}
				}
				if !merge {
					mergedMsgs = append(mergedMsgs, msg)
				}
			} else {
				mergedMsgs = append(mergedMsgs, msg)
			}

		} else {
			canNotDoMsg = append(canNotDoMsg, msg)
		}
	}
	metrics.QueryNodeWaitForExecuteReqs.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID())).Set(float64(len(mergedMsgs)))
	return mergedMsgs, canNotDoMsg
}
