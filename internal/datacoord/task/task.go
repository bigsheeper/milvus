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
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"go.uber.org/zap"
)

type Type string

const (
	Import     Type = "Import"
	Compaction Type = "Compaction"
	Index      Type = "Index"
	Stats      Type = "Stats"
)

type State int32

const (
	Invalid State = iota
	Pending
	InProgress
	Finished
	Failed
	Retry
)

var stateName = map[State]string{
	0: "Invalid",
	1: "Pending",
	2: "InProgress",
	3: "Finished",
	4: "Failed",
	5: "Retry",
}

func (s State) String() string {
	return stateName[s]
}

type Task interface {
	GetTaskID() int64
	GetTaskType() Type
	GetTaskState() State
	GetTaskSlot() int64

	CreateTaskOnWorker(nodeID int64, cluster session.Cluster)
	QueryTaskOnWorker(cluster session.Cluster)
	DropTaskOnWorker(cluster session.Cluster)
}

func WrapTaskLog(task Task, fields ...zap.Field) []zap.Field {
	res := []zap.Field{
		zap.Int64("ID", task.GetTaskID()),
		zap.String("type", string(task.GetTaskType())),
		zap.String("state", task.GetTaskState().String()),
	}
	res = append(res, fields...)
	return res
}
