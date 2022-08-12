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

package rootcoord

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
)

type queryCoordMockForQuota struct {
	queryMock
	retErr        bool
	retFailStatus bool
}

type dataCoordMockForQuota struct {
	dataMock
	retErr        bool
	retFailStatus bool
}

func (q *queryCoordMockForQuota) GetMetrics(ctx context.Context, request *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	if q.retErr {
		return nil, fmt.Errorf("mock err")
	}
	if q.retFailStatus {
		return &milvuspb.GetMetricsResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "mock failure status"),
		}, nil
	}
	return &milvuspb.GetMetricsResponse{
		Status: succStatus(),
	}, nil
}

func (d *dataCoordMockForQuota) GetMetrics(ctx context.Context, request *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	if d.retErr {
		return nil, fmt.Errorf("mock err")
	}
	if d.retFailStatus {
		return &milvuspb.GetMetricsResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "mock failure status"),
		}, nil
	}
	return &milvuspb.GetMetricsResponse{
		// TODO: add metrics
		Status: succStatus(),
	}, nil
}

func TestQuotaCenter(t *testing.T) {
	Params.Init()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	core, err := NewCore(ctx, nil)
	assert.Nil(t, err)

	pcm := newProxyClientManager(core)

	t.Run("test QuotaCenter", func(t *testing.T) {
		quotaCenter := NewQuotaCenter(pcm, &queryCoordMockForQuota{}, &dataCoordMockForQuota{})
		go quotaCenter.run()
		quotaCenter.stop()
	})

	t.Run("test syncMetrics", func(t *testing.T) {
		quotaCenter := NewQuotaCenter(pcm, &queryCoordMockForQuota{}, &dataCoordMockForQuota{})
		err = quotaCenter.syncMetrics()
		assert.NoError(t, err)

		quotaCenter = NewQuotaCenter(pcm, &queryCoordMockForQuota{retErr: true}, &dataCoordMockForQuota{})
		err = quotaCenter.syncMetrics()
		assert.Error(t, err)

		quotaCenter = NewQuotaCenter(pcm, &queryCoordMockForQuota{}, &dataCoordMockForQuota{retErr: true})
		err = quotaCenter.syncMetrics()
		assert.Error(t, err)

		quotaCenter = NewQuotaCenter(pcm, &queryCoordMockForQuota{retFailStatus: true}, &dataCoordMockForQuota{})
		err = quotaCenter.syncMetrics()
		assert.Error(t, err)

		quotaCenter = NewQuotaCenter(pcm, &queryCoordMockForQuota{}, &dataCoordMockForQuota{retFailStatus: true})
		err = quotaCenter.syncMetrics()
		assert.Error(t, err)
	})

	t.Run("test memoryToWaterLevel", func(t *testing.T) {
		quotaCenter := NewQuotaCenter(pcm, &queryCoordMockForQuota{}, &dataCoordMockForQuota{})
		cb := quotaCenter.memoryToWaterLevel()
		assert.Equal(t, throttling, cb)
		quotaCenter.dataNodeMetrics = []*metricsinfo.QuotaMetrics{{Mm: metricsinfo.MemMetric{UsedMem: 100, TotalMem: 100}}}
		cb = quotaCenter.memoryToWaterLevel()
		assert.Equal(t, disableWrite, cb)
		quotaCenter.queryNodeMetrics = []*metricsinfo.QuotaMetrics{{Mm: metricsinfo.MemMetric{UsedMem: 100, TotalMem: 100}}}
		cb = quotaCenter.memoryToWaterLevel()
		assert.Equal(t, disableWrite, cb)
	})

	t.Run("test warmUp", func(t *testing.T) {
		rates := map[internalpb.RateType]float64{
			internalpb.RateType_DDLCollection: 100,
			internalpb.RateType_DMLInsert:     100,
			internalpb.RateType_DQLSearch:     100,
		}
		warmUp(rates)
		speed := Params.QuotaConfig.WarmUpSpeed
		assert.Equal(t, float64(100), rates[internalpb.RateType_DDLCollection])
		assert.Equal(t, 100+Params.QuotaConfig.DMLInsertRate*speed, rates[internalpb.RateType_DMLInsert])
		assert.Equal(t, 100+Params.QuotaConfig.DQLSearchRate*speed, rates[internalpb.RateType_DQLSearch])
	})

	t.Run("test getMinThroughput", func(t *testing.T) {
		quotaCenter := NewQuotaCenter(pcm, &queryCoordMockForQuota{}, &dataCoordMockForQuota{})
		quotaCenter.dataNodeMetrics = []*metricsinfo.QuotaMetrics{
			{Rms: []metricsinfo.RateMetric{
				{Rt: internalpb.RateType_DMLInsert, ThroughPut: 100},
				{Rt: internalpb.RateType_DMLInsert, ThroughPut: 200},
				{Rt: internalpb.RateType_DMLInsert, ThroughPut: 0},
			}},
		}
		res := quotaCenter.getMinThroughput()
		assert.Len(t, res, 1)
		throughput, ok := res[internalpb.RateType_DMLInsert]
		assert.True(t, ok)
		assert.Equal(t, float64(100), throughput)
	})

	t.Run("test setRates", func(t *testing.T) {
		quotaCenter := NewQuotaCenter(pcm, &queryCoordMockForQuota{}, &dataCoordMockForQuota{})
		quotaCenter.currentRates[internalpb.RateType_DMLInsert] = 100
		err = quotaCenter.setRates()
		assert.NoError(t, err)
	})
}
