// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package querynode

import (
	"context"
	"errors"
	"fmt"
	"os"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

func getSystemInfoMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest, node *QueryNode) (*milvuspb.GetMetricsResponse, error) {
	usedMem, err := getUsedMemory()
	if err != nil {
		return nil, err
	}
	totalMem, err := getTotalMemory()
	if err != nil {
		return nil, err
	}
	nodeInfos := metricsinfo.QueryNodeInfos{
		BaseComponentInfos: metricsinfo.BaseComponentInfos{
			Name: metricsinfo.ConstructComponentName(typeutil.QueryNodeRole, Params.QueryNodeID),
			HardwareInfos: metricsinfo.HardwareMetrics{
				IP:           node.session.Address,
				CPUCoreCount: metricsinfo.GetCPUCoreCount(false),
				CPUCoreUsage: metricsinfo.GetCPUUsage(),
				Memory:       totalMem,
				MemoryUsage:  usedMem,
				Disk:         metricsinfo.GetDiskCount(),
				DiskUsage:    metricsinfo.GetDiskUsage(),
			},
			SystemInfo: metricsinfo.DeployMetrics{
				SystemVersion: os.Getenv(metricsinfo.GitCommitEnvKey),
				DeployMode:    os.Getenv(metricsinfo.DeployModeEnvKey),
			},
			CreatedTime: Params.CreatedTime.String(),
			UpdatedTime: Params.UpdatedTime.String(),
			Type:        typeutil.QueryNodeRole,
			ID:          node.session.ServerID,
		},
		SystemConfigurations: metricsinfo.QueryNodeConfiguration{
			SearchReceiveBufSize:         Params.SearchReceiveBufSize,
			SearchPulsarBufSize:          Params.SearchPulsarBufSize,
			SearchResultReceiveBufSize:   Params.SearchResultReceiveBufSize,
			RetrieveReceiveBufSize:       Params.RetrieveReceiveBufSize,
			RetrievePulsarBufSize:        Params.RetrievePulsarBufSize,
			RetrieveResultReceiveBufSize: Params.RetrieveResultReceiveBufSize,

			SimdType: Params.SimdType,
		},
	}
	resp, err := metricsinfo.MarshalComponentInfos(nodeInfos)
	if err != nil {
		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
			Response:      "",
			ComponentName: metricsinfo.ConstructComponentName(typeutil.QueryNodeRole, Params.QueryNodeID),
		}, nil
	}

	return &milvuspb.GetMetricsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Response:      resp,
		ComponentName: metricsinfo.ConstructComponentName(typeutil.QueryNodeRole, Params.QueryNodeID),
	}, nil
}

func getUsedMemory() (uint64, error) {
	if Params.InContainer {
		return metricsinfo.GetContainerMemUsed()
	}
	return metricsinfo.GetUsedMemoryCount(), nil
}

func getTotalMemory() (uint64, error) {
	if Params.InContainer {
		return metricsinfo.GetContainerMemLimit()
	}
	return metricsinfo.GetMemoryCount(), nil
}

func checkSegmentMemory(segmentLoadInfos []*querypb.SegmentLoadInfo, historicalReplica, streamingReplica ReplicaInterface) error {
	usedMem, err := getUsedMemory()
	if err != nil {
		return err
	}
	totalMem, err := getTotalMemory()
	if err != nil {
		return err
	}

	segmentTotalSize := int64(0)
	for _, segInfo := range segmentLoadInfos {
		collectionID := segInfo.CollectionID
		segmentID := segInfo.SegmentID

		col, err := historicalReplica.getCollectionByID(collectionID)
		if err != nil {
			return err
		}

		sizePerRecord, err := typeutil.EstimateSizePerRecord(col.schema)
		if err != nil {
			return err
		}

		segmentSize := int64(sizePerRecord) * segInfo.NumOfRows
		segmentTotalSize += segmentSize

		log.Debug("memory stats when load segment",
			zap.Any("collectionIDs", collectionID),
			zap.Any("segmentID", segmentID),
			zap.Any("numOfRows", segInfo.NumOfRows),
			zap.Any("totalMem", totalMem),
			zap.Any("usedMem", usedMem),
			zap.Any("segmentTotalSize", segmentTotalSize),
		)
		if int64(usedMem)+segmentTotalSize + segmentSize > int64(float64(totalMem) * 0.9) {
			return errors.New(fmt.Sprintln("load segment failed, OOM if load, "+
				"collectionID = ", collectionID, ", ",
				"usedMem = ", usedMem, ", ",
				"segmentTotalSize(MB) = ", segmentTotalSize, ", ",
				"totalMem = ", totalMem))
		}
	}

	return nil
}
