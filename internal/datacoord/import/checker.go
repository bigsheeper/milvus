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
	"github.com/milvus-io/milvus/internal/datacoord"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

type Checker struct {
	manager Manager
	meta    datacoord.meta
	cluster datacoord.Cluster
}

func (c *Checker) checkErr(task Task, err error) {
	if !merr.IsRetryableErr(err) {
		c.manager.Update(task.ID(), UpdateState(datapb.ImportState_Failed))
		return
	}
	switch task.State() {
	case datapb.ImportState_Preparing:
		c.manager.Update(task.ID(), UpdateState(datapb.ImportState_Pending))
	case datapb.ImportState_InProgress:
		c.manager.Update(task.ID(), UpdateState(datapb.ImportState_Preparing))
	}
}

func (c *Checker) check() {
	for _, task := range c.manager.GetBy(WithStates(datapb.ImportState_Preparing, datapb.ImportState_InProgress)) {
		if task.NodeID() == fakeNodeID {
			continue
		}
		ctx, _ := context.WithTimeout(context.Background(), ImportRpcTimeout)
		req := &datapb.GetImportStateRequest{
			RequestID: task.ReqID(),
			TaskID:    task.ID(),
		}
		resp, err := c.cluster.GetImportState(ctx, task.NodeID(), req)
		if err != nil {
			log.Warn("")
			c.checkErr(task, err)
			return
		}
		c.manager.Update(task.ID(), UpdateFileInfo(resp.GetFileInfos()))

		for _, info := range resp.GetSegmentInfos() {
			operator := datacoord.UpdateBinlogsOperator(info.GetID(), info.GetBinlogs(), info.GetStatslogs(), info.GetDeltalogs())
			err := c.meta.UpdateSegmentsInfo(operator)
			if err != nil {
				log.Warn("")
				continue
			}
		}
	}
}
