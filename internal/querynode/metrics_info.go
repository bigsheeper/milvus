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
	"context"
	"strconv"
	"time"

	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

//getComponentConfigurations returns the configurations of queryNode matching req.Pattern
func getComponentConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) *internalpb.ShowConfigurationsResponse {
	prefix := "querynode."
	matchedConfig := Params.QueryNodeCfg.Base.GetByPattern(prefix + req.Pattern)
	configList := make([]*commonpb.KeyValuePair, 0, len(matchedConfig))
	for key, value := range matchedConfig {
		configList = append(configList,
			&commonpb.KeyValuePair{
				Key:   key,
				Value: value,
			})
	}

	return &internalpb.ShowConfigurationsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Configuations: configList,
	}
}

func getQuotaMetrics() (*metricsinfo.QuotaMetrics, error) {
	nodeIDStr := strconv.FormatInt(Params.QueryNodeCfg.GetNodeID(), 10)
	toMegabytes := func(f float64) float64 {
		return f / 1024.0 / 1024.0
	}
	// TODO: get newest as default, support get by other strategy
	insertRate, err := rateCollector.Newest(internalpb.RateType_DMLInsert.String())
	if err != nil {
		return nil, err
	}
	metrics.QueryNodeConsumeInsertRate.WithLabelValues(nodeIDStr).Set(toMegabytes(insertRate))
	deleteRate, err := rateCollector.Newest(internalpb.RateType_DMLDelete.String())
	if err != nil {
		return nil, err
	}
	metrics.QueryNodeConsumeDeleteRate.WithLabelValues(nodeIDStr).Set(toMegabytes(deleteRate))
	searchRate, err := rateCollector.NewestAvg(internalpb.RateType_DQLSearch.String(), 3*time.Second)
	if err != nil {
		return nil, err
	}
	metrics.QueryNodeSearchRate.WithLabelValues(nodeIDStr).Set(toMegabytes(searchRate))
	queryRate, err := rateCollector.Newest(internalpb.RateType_DQLQuery.String())
	if err != nil {
		return nil, err
	}
	metrics.QueryNodeQueryRate.WithLabelValues(nodeIDStr).Set(toMegabytes(queryRate))
	// TODO: return throughput for now, support more types in the future
	rms := []metricsinfo.RateMetric{
		{Rt: internalpb.RateType_DMLInsert, ThroughPut: insertRate},
		{Rt: internalpb.RateType_DMLDelete, ThroughPut: deleteRate},
		{Rt: internalpb.RateType_DQLSearch, ThroughPut: searchRate},
		{Rt: internalpb.RateType_DQLQuery, ThroughPut: queryRate},
	}

	return &metricsinfo.QuotaMetrics{
		NodeID: Params.QueryNodeCfg.GetNodeID(),
		Rms:    rms,
		Mm:     metricsinfo.MemMetric{},
	}, nil
}

// getSystemInfoMetrics returns metrics info of QueryNode
func getSystemInfoMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest, node *QueryNode) (*milvuspb.GetMetricsResponse, error) {
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

	nodeInfos := metricsinfo.QueryNodeInfos{
		BaseComponentInfos: metricsinfo.BaseComponentInfos{
			Name: metricsinfo.ConstructComponentName(typeutil.QueryNodeRole, Params.QueryNodeCfg.GetNodeID()),
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
			CreatedTime: Params.QueryNodeCfg.CreatedTime.String(),
			UpdatedTime: Params.QueryNodeCfg.UpdatedTime.String(),
			Type:        typeutil.QueryNodeRole,
			ID:          node.session.ServerID,
		},
		SystemConfigurations: metricsinfo.QueryNodeConfiguration{
			SimdType: Params.CommonCfg.SimdType,
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
			ComponentName: metricsinfo.ConstructComponentName(typeutil.QueryNodeRole, Params.QueryNodeCfg.GetNodeID()),
		}, nil
	}

	return &milvuspb.GetMetricsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Response:      resp,
		ComponentName: metricsinfo.ConstructComponentName(typeutil.QueryNodeRole, Params.QueryNodeCfg.GetNodeID()),
	}, nil
}
