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

package datacoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
)

func TestPreImportTask_CreateTaskOnWorker(t *testing.T) {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)

	im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
	assert.NoError(t, err)

	var job ImportJob = &importJob{
		ImportJob: &datapb.ImportJob{
			JobID: 1,
		},
	}
	err = im.AddJob(context.TODO(), job)
	assert.NoError(t, err)

	taskProto := &datapb.PreImportTask{
		JobID:        1,
		TaskID:       2,
		CollectionID: 3,
		State:        datapb.ImportTaskStateV2_Pending,
	}
	task := &preImportTask{
		imeta: im,
		tr:    timerecord.NewTimeRecorder(""),
	}
	task.task.Store(taskProto)
	err = im.AddTask(context.TODO(), task)
	assert.NoError(t, err)

	mockDNM := session.NewMockDataNodeManager(t)
	mockDNM.EXPECT().PreImport(mock.Anything, mock.Anything).Return(nil)

	cluster := session.Cluster{DataNodeManager: mockDNM}
	task.CreateTaskOnWorker(1, cluster)
	assert.Equal(t, datapb.ImportTaskStateV2_InProgress, task.GetState())
}

func TestPreImportTask_QueryTaskOnWorker(t *testing.T) {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil)

	im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
	assert.NoError(t, err)

	taskProto := &datapb.PreImportTask{
		JobID:        1,
		TaskID:       2,
		CollectionID: 3,
		NodeID:       7,
		State:        datapb.ImportTaskStateV2_InProgress,
	}
	task := &preImportTask{
		imeta: im,
		tr:    timerecord.NewTimeRecorder(""),
	}
	task.task.Store(taskProto)
	err = im.AddTask(context.TODO(), task)
	assert.NoError(t, err)

	mockDNM := session.NewMockDataNodeManager(t)
	mockDNM.EXPECT().QueryPreImport(mock.Anything, mock.Anything).Return(&datapb.QueryPreImportResponse{
		State: datapb.ImportTaskStateV2_Completed,
	}, nil)

	cluster := session.Cluster{DataNodeManager: mockDNM}
	task.QueryTaskOnWorker(cluster)
	assert.Equal(t, datapb.ImportTaskStateV2_Completed, task.GetState())
}
