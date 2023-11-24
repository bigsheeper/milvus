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

package _import

import (
	"context"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/datacoord"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"time"
)

const (
	ImportRpcTimeout = 10 * time.Second
	fakeNodeID       = 1
)

type Scheduler interface {
	Start(context.Context)
	Close()
}

type scheduler struct {
	ctx       context.Context
	manager   Manager
	sm        *datacoord.SegmentManager
	allocator *allocator.IDAllocator
	meta      datacoord.meta
	cluster   datacoord.Cluster
}

func (s *scheduler) Check() {
	tasks := s.manager.GetBy()
	for _, task := range tasks {
		switch task.State() {
		case datapb.ImportState_Pending:
			s.processPending(task)
		case datapb.ImportState_Preparing:
			s.processPreparing(task)
		case datapb.ImportState_InProgress:
			s.processInProgress(task)
		case datapb.ImportState_Failed, datapb.ImportState_Completed:
			s.processCompletedOrFailed(task)
		}
	}
}

func (s *scheduler) GetIdleNode() int64 {
	for _, nodeID := range s.cluster.GetNodes() {
		resp, err := s.cluster.GetImportState(s.GetCtx(), nodeID, &datapb.GetImportStateRequest{})
		if err != nil {
			log.Warn("")
			continue
		}
		if resp.GetSlots() > 0 {
			return nodeID
		}
	}
	return -1
}

func (s *scheduler) GetCtx() context.Context {
	ctx, _ := context.WithTimeout(s.ctx, ImportRpcTimeout)
	return ctx
}

func (s *scheduler) processPending(task Task) {
	nodeID := s.GetIdleNode()
	req := AssemblePreImportRequest(task)
	err := s.cluster.PreImport(s.GetCtx(), nodeID, req)
	if err != nil {
		log.Warn("")
		return
	}
	s.manager.Update(task.ID(), UpdateState(datapb.ImportState_Preparing))
}

func (s *scheduler) processPreparing(task Task) {
	if !IsPreImportDone(task) {
		return
	}
	nodeID := s.GetIdleNode()
	req, err := AssembleImportRequest(s.GetCtx(), task, s.sm, s.allocator)
	if err != nil {
		log.Warn("")
		return
	}
	err = s.cluster.ImportV2(s.GetCtx(), nodeID, req)
	if err != nil {
		log.Warn("")
		return
	}
	s.manager.Update(task.ID(), UpdateState(datapb.ImportState_InProgress))
}

func (s *scheduler) processInProgress(task Task) {
	if !IsImportDone(task) {
		return
	}
	// TODO: assign by vchannel
	s.cluster.AddImportSegment(s.GetCtx())
	s.meta.UnsetIsImporting()
	s.manager.Update(task.ID(), UpdateState(datapb.ImportState_Completed))
}

func (s *scheduler) processCompletedOrFailed(task Task) {
	if task.NodeID() == fakeNodeID {
		return
	}
	req := &datapb.DropImportTaskRequest{
		RequestID: task.ReqID(),
		TaskID:    task.ID(),
	}
	err := s.cluster.DropImport(s.GetCtx(), task.NodeID(), req)
	if err != nil {
		log.Warn("")
		return
	}
	s.manager.Update(task.ID(), UpdateNodeID(fakeNodeID))
}
