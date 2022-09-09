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
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type queryCoordMockForQuota struct {
	mockQueryCoord
	retErr        bool
	retFailStatus bool
}

type dataCoordMockForQuota struct {
	mockDataCoord
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
		Status: succStatus(),
	}, nil
}

func TestQuotaCenter(t *testing.T) {
	Params.Init()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	core, err := NewCore(ctx, nil)
	assert.Nil(t, err)
	alloc := newMockTsoAllocator()
	core.tsoAllocator = alloc

	pcm := newProxyClientManager(core.proxyCreator)

	t.Run("test QuotaCenter", func(t *testing.T) {
		quotaCenter := NewQuotaCenter(pcm, &queryCoordMockForQuota{}, &dataCoordMockForQuota{}, core.tsoAllocator)
		go quotaCenter.run()
		time.Sleep(10 * time.Millisecond)
		quotaCenter.stop()
	})

	t.Run("test syncMetrics", func(t *testing.T) {
		quotaCenter := NewQuotaCenter(pcm, &queryCoordMockForQuota{}, &dataCoordMockForQuota{}, core.tsoAllocator)
		err = quotaCenter.syncMetrics()
		assert.Error(t, err) // for empty response

		quotaCenter = NewQuotaCenter(pcm, &queryCoordMockForQuota{retErr: true}, &dataCoordMockForQuota{}, core.tsoAllocator)
		err = quotaCenter.syncMetrics()
		assert.Error(t, err)

		quotaCenter = NewQuotaCenter(pcm, &queryCoordMockForQuota{}, &dataCoordMockForQuota{retErr: true}, core.tsoAllocator)
		err = quotaCenter.syncMetrics()
		assert.Error(t, err)

		quotaCenter = NewQuotaCenter(pcm, &queryCoordMockForQuota{retFailStatus: true}, &dataCoordMockForQuota{}, core.tsoAllocator)
		err = quotaCenter.syncMetrics()
		assert.Error(t, err)

		quotaCenter = NewQuotaCenter(pcm, &queryCoordMockForQuota{}, &dataCoordMockForQuota{retFailStatus: true}, core.tsoAllocator)
		err = quotaCenter.syncMetrics()
		assert.Error(t, err)
	})

	t.Run("test calculateRates", func(t *testing.T) {
		quotaCenter := NewQuotaCenter(pcm, &queryCoordMockForQuota{}, &dataCoordMockForQuota{}, core.tsoAllocator)
		err = quotaCenter.calculateRates()
		assert.NoError(t, err)
		alloc.GenerateTSOF = func(count uint32) (typeutil.Timestamp, error) {
			return 0, fmt.Errorf("mock err")
		}
		core.tsoAllocator = alloc
		err = quotaCenter.calculateRates()
		assert.Error(t, err)
	})

	t.Run("test timeTickDelay", func(t *testing.T) {
		// test MaxTimestamp
		quotaCenter := NewQuotaCenter(pcm, &queryCoordMockForQuota{}, &dataCoordMockForQuota{}, core.tsoAllocator)
		factor, err := quotaCenter.timeTickDelay()
		assert.NoError(t, err)
		assert.Equal(t, float64(1), factor)

		now := time.Now()

		bak := Params.QuotaConfig.MaxTimeTickDelay
		Params.QuotaConfig.MaxTimeTickDelay = 3 * time.Second
		// test force deny writing
		alloc.GenerateTSOF = func(count uint32) (typeutil.Timestamp, error) {
			added := now.Add(Params.QuotaConfig.MaxTimeTickDelay)
			ts := tsoutil.ComposeTSByTime(added, 0)
			return ts, nil
		}
		core.tsoAllocator = alloc
		quotaCenter.queryNodeMetrics = []*metricsinfo.QueryNodeQuotaMetrics{{
			Fgm: metricsinfo.FlowGraphMetric{
				MinFlowGraphTt: tsoutil.ComposeTSByTime(now, 0),
			},
		}}
		factor, err = quotaCenter.timeTickDelay()
		assert.NoError(t, err)
		assert.Equal(t, float64(0), factor)

		// test one-third time tick delay
		alloc.GenerateTSOF = func(count uint32) (typeutil.Timestamp, error) {
			oneThirdDelay := Params.QuotaConfig.MaxTimeTickDelay / 3
			added := now.Add(oneThirdDelay)
			oneThirdTs := tsoutil.ComposeTSByTime(added, 0)
			return oneThirdTs, nil
		}
		core.tsoAllocator = alloc
		quotaCenter.queryNodeMetrics = []*metricsinfo.QueryNodeQuotaMetrics{{
			Fgm: metricsinfo.FlowGraphMetric{
				MinFlowGraphTt: tsoutil.ComposeTSByTime(now, 0),
			},
		}}
		factor, err = quotaCenter.timeTickDelay()
		assert.NoError(t, err)
		ok := math.Abs(factor-2.0/3.0) < 0.0001
		assert.True(t, ok)

		// test with error
		alloc.GenerateTSOF = func(count uint32) (typeutil.Timestamp, error) {
			return 0, fmt.Errorf("mock err")
		}
		core.tsoAllocator = alloc
		_, err = quotaCenter.timeTickDelay()
		assert.Error(t, err)
		Params.QuotaConfig.MaxTimeTickDelay = bak
	})

	t.Run("test checkNQInQuery", func(t *testing.T) {
		quotaCenter := NewQuotaCenter(pcm, &queryCoordMockForQuota{}, &dataCoordMockForQuota{}, core.tsoAllocator)
		factor := quotaCenter.checkNQInQuery()
		assert.Equal(t, float64(1), factor)

		// test cool off
		bak := Params.QuotaConfig.NQInQueueThreshold
		Params.QuotaConfig.NQInQueueThreshold = 100
		quotaCenter.queryNodeMetrics = []*metricsinfo.QueryNodeQuotaMetrics{{
			SearchQueue: metricsinfo.ReadInfoInQueue{
				UnsolvedQueue: Params.QuotaConfig.NQInQueueThreshold,
			},
		}}
		factor = quotaCenter.checkNQInQuery()
		assert.Equal(t, RateCoolOffSpeed, factor)

		// test no cool off
		quotaCenter.queryNodeMetrics = []*metricsinfo.QueryNodeQuotaMetrics{{
			SearchQueue: metricsinfo.ReadInfoInQueue{
				UnsolvedQueue: Params.QuotaConfig.NQInQueueThreshold - 1,
			},
		}}
		factor = quotaCenter.checkNQInQuery()
		assert.Equal(t, 1.0, factor)
		//ok := math.Abs(factor-1.0) < 0.0001
		//assert.True(t, ok)

		Params.QuotaConfig.NQInQueueThreshold = bak
	})

	t.Run("test checkQueryLatency", func(t *testing.T) {
		quotaCenter := NewQuotaCenter(pcm, &queryCoordMockForQuota{}, &dataCoordMockForQuota{}, core.tsoAllocator)
		factor := quotaCenter.checkQueryLatency()
		assert.Equal(t, float64(1), factor)

		// test cool off
		bak := Params.QuotaConfig.QueueLatencyThreshold
		Params.QuotaConfig.QueueLatencyThreshold = float64(3 * time.Second)
		quotaCenter.queryNodeMetrics = []*metricsinfo.QueryNodeQuotaMetrics{{
			SearchQueue: metricsinfo.ReadInfoInQueue{
				AvgQueueDuration: time.Duration(Params.QuotaConfig.QueueLatencyThreshold),
			},
		}}
		factor = quotaCenter.checkQueryLatency()
		assert.Equal(t, RateCoolOffSpeed, factor)

		// test no cool off
		quotaCenter.queryNodeMetrics = []*metricsinfo.QueryNodeQuotaMetrics{{
			SearchQueue: metricsinfo.ReadInfoInQueue{
				AvgQueueDuration: 1 * time.Second,
			},
		}}
		factor = quotaCenter.checkQueryLatency()
		assert.Equal(t, 1.0, factor)
		//ok := math.Abs(factor-1.0) < 0.0001
		//assert.True(t, ok)

		Params.QuotaConfig.QueueLatencyThreshold = bak
	})

	t.Run("test memoryToWaterLevel", func(t *testing.T) {
		quotaCenter := NewQuotaCenter(pcm, &queryCoordMockForQuota{}, &dataCoordMockForQuota{}, core.tsoAllocator)
		factor := quotaCenter.memoryToWaterLevel()
		assert.Equal(t, float64(1), factor)
		quotaCenter.dataNodeMetrics = []*metricsinfo.DataNodeQuotaMetrics{{Hms: metricsinfo.HardwareMetrics{MemoryUsage: 100, Memory: 100}}}
		factor = quotaCenter.memoryToWaterLevel()
		assert.Equal(t, float64(0), factor)
		quotaCenter.queryNodeMetrics = []*metricsinfo.QueryNodeQuotaMetrics{{Hms: metricsinfo.HardwareMetrics{MemoryUsage: 100, Memory: 100}}}
		factor = quotaCenter.memoryToWaterLevel()
		assert.Equal(t, float64(0), factor)
	})

	t.Run("test setRates", func(t *testing.T) {
		quotaCenter := NewQuotaCenter(pcm, &queryCoordMockForQuota{}, &dataCoordMockForQuota{}, core.tsoAllocator)
		quotaCenter.currentRates[internalpb.RateType_DMLInsert] = 100
		err = quotaCenter.setRates()
		assert.NoError(t, err)
	})
}
