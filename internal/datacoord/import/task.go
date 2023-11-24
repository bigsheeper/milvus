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
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
)

type TaskFilter func(task Task) bool

func WithStates(states ...datapb.ImportState) TaskFilter {
	return func(task Task) bool {
		for _, state := range states {
			if task.State() == state {
				return true
			}
		}
		return false
	}
}

func WithRequestID(reqID int64) TaskFilter {
	return func(task Task) bool {
		return task.ReqID() == reqID
	}
}

type UpdateAction func(task Task)

func UpdateState(state datapb.ImportState) UpdateAction {
	return func(t Task) {
		t.(*task).ImportTaskV2.State = state
	}
}

func UpdateFileInfo(infos []*datapb.ImportFileInfo) UpdateAction {
	return func(t Task) {
		t.(*task).ImportTaskV2.FileInfos = infos
	}
}

func UpdateNodeID(nodeID int64) UpdateAction {
	return func(t Task) {
		t.(*task).ImportTaskV2.NodeID = nodeID
	}
}

type Task interface {
	ID() int64
	ReqID() int64
	NodeID() int64
	CollectionID() int64
	PartitionID() int64
	SegmentIDs() []int64
	State() datapb.ImportState
	Schema() *schemapb.CollectionSchema
	FileInfos() []*datapb.ImportFileInfo
	Clone() Task
}

type task struct {
	*datapb.ImportTaskV2
	schema *schemapb.CollectionSchema
}

func (t *task) State() datapb.ImportState {
	return t.GetState()
}
