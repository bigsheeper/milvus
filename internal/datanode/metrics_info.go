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

package datanode

import (
	"context"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

func getQuotaMetrics() (*metricsinfo.QuotaMetrics, error) {
	// TODO: get newest as default, support get by other strategy
	insertRate, err := rateCollector.Newest(commonpb.RateType_DMLInsert)
	if err != nil {
		return nil, err
	}
	deleteRate, err := rateCollector.Newest(commonpb.RateType_DMLDelete)
	if err != nil {
		return nil, err
	}
	// TODO: return throughput for now, support more types in the future
	rms := []metricsinfo.RateMetric{
		{Rt: commonpb.RateType_DMLInsert, ThroughPut: insertRate},
		{Rt: commonpb.RateType_DMLDelete, ThroughPut: deleteRate},
	}

	return &metricsinfo.QuotaMetrics{
		Rms: rms,
		Mm:  metricsinfo.MemMetric{},
	}, nil
}

func (node *DataNode) getSystemInfoMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	// TODO(dragondriver): add more metrics
	usedMem := metricsinfo.GetUsedMemoryCount()
	totalMem := metricsinfo.GetMemoryCount()

	quotaMetrics, err := getQuotaMetrics()
	if err != nil {
		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
			ComponentName: metricsinfo.ConstructComponentName(typeutil.DataNodeRole, Params.DataNodeCfg.GetNodeID()),
		}, nil
	}
	quotaMetrics.Mm.UsedMem = usedMem
	quotaMetrics.Mm.TotalMem = totalMem

	nodeInfos := metricsinfo.DataNodeInfos{
		BaseComponentInfos: metricsinfo.BaseComponentInfos{
			Name: metricsinfo.ConstructComponentName(typeutil.DataNodeRole, Params.DataNodeCfg.GetNodeID()),
			HardwareInfos: metricsinfo.HardwareMetrics{
				IP:           node.session.Address,
				CPUCoreCount: metricsinfo.GetCPUCoreCount(false),
				CPUCoreUsage: metricsinfo.GetCPUUsage(),
				Memory:       totalMem,
				MemoryUsage:  usedMem,
				Disk:         metricsinfo.GetDiskCount(),
				DiskUsage:    metricsinfo.GetDiskUsage(),
			},
			SystemInfo:  metricsinfo.DeployMetrics{},
			CreatedTime: Params.DataNodeCfg.CreatedTime.String(),
			UpdatedTime: Params.DataNodeCfg.UpdatedTime.String(),
			Type:        typeutil.DataNodeRole,
			ID:          node.session.ServerID,
		},
		SystemConfigurations: metricsinfo.DataNodeConfiguration{
			FlushInsertBufferSize: Params.DataNodeCfg.FlushInsertBufferSize,
		},
		QuotaMetrics: quotaMetrics,
	}

	metricsinfo.FillDeployMetricsWithEnv(&nodeInfos.SystemInfo)

	resp, err := metricsinfo.MarshalComponentInfos(nodeInfos)
	if err != nil {
		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
			Response:      "",
			ComponentName: metricsinfo.ConstructComponentName(typeutil.DataNodeRole, Params.DataNodeCfg.GetNodeID()),
		}, nil
	}

	return &milvuspb.GetMetricsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Response:      resp,
		ComponentName: metricsinfo.ConstructComponentName(typeutil.DataNodeRole, Params.DataNodeCfg.GetNodeID()),
	}, nil
}
