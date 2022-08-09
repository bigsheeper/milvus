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
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"math"
	"time"
)

// TODO: get from config
const (
	QuotaMemoryWaterMarker = 0.9
	GetMetricsTimeout      = 10 // in seconds
	SetRatesTimeout        = 10 // in seconds
)

type controlBehavior int32

const (
	disableWrite controlBehavior = 0
	disableRead  controlBehavior = 1
	throttling   controlBehavior = 2
)

type quotaEvent int32

const (
	memoryReachWaterMarker quotaEvent = 0
	tSafeDelayed           quotaEvent = 1
	growingPredominated    quotaEvent = 2
	dmlPerfChanged         quotaEvent = 3
	dqlPerfChanged         quotaEvent = 4
)

type QuotaCenter struct {
	ctx    context.Context
	cancel context.CancelFunc

	// clients
	proxies    *proxyClientManager
	queryCoord types.QueryCoord
	dataCoord  types.DataCoord

	// metrics
	queryNodeMetrics []*metricsinfo.QuotaMetrics
	dataNodeMetrics  []*metricsinfo.QuotaMetrics
	proxyMetrics     []*metricsinfo.QuotaMetrics

	currentRates map[commonpb.RateType]float64
}

func (q *QuotaCenter) Run() {
	for {
		select {
		case <-q.ctx.Done():
			fmt.Println("QuotaCenter exit")
			return
		case <-time.After(time.Duration(Params.QuotaConfig.QuotaCenterCollectInterval) * time.Millisecond):
			err := q.syncMetrics()
			if err != nil {
				fmt.Println(fmt.Errorf("quotaCenter collect metrics failed"))
				continue
			}
			err = q.Execute()
			if err != nil {
				fmt.Println(fmt.Errorf("quotaCenter execute failed"))
			}
		}
	}
}

func (q *QuotaCenter) doTriggerStrategy(ts quotaEvent) {
	switch ts {
	case memoryReachWaterMarker:
		q.doControl(disableWrite)
	case tSafeDelayed:
		q.doControl(throttling)
	case growingPredominated:
		q.doControl(throttling)
	case dmlPerfChanged:
		q.doControl(throttling)
	case dqlPerfChanged:
		q.doControl(throttling)
	}
}

func (q *QuotaCenter) doControl(cb controlBehavior) {
	switch cb {
	case disableWrite:
		q.currentRates[commonpb.RateType_DMLInsert] = 0
		q.currentRates[commonpb.RateType_DMLDelete] = 0
	case disableRead:
		q.currentRates[commonpb.RateType_DQLSearch] = 0
		q.currentRates[commonpb.RateType_DQLQuery] = 0
	case throttling:

	}
}

func (q *QuotaCenter) Execute() error {
	rateMap := map[commonpb.RateType]float64{
		commonpb.RateType_DDLCollection: math.MaxFloat64,
		commonpb.RateType_DDLPartition:  math.MaxFloat64,
		commonpb.RateType_DDLIndex:      math.MaxFloat64,
		commonpb.RateType_DMLDelete:     math.MaxFloat64,
		commonpb.RateType_DMLInsert:     math.MaxFloat64,
		commonpb.RateType_DQLSearch:     math.MaxFloat64,
		commonpb.RateType_DQLQuery:      math.MaxFloat64,
	}

	// disable write
	for _, metric := range q.metrics {
		for _, mm := range metric.Mms {
			if float64(mm.TotalMem-mm.FreeMem)/float64(mm.TotalMem) > QuotaMemoryWaterMarker {
				rateMap[commonpb.RateType_DMLInsert] = 0
				rateMap[commonpb.RateType_DMLDelete] = 0
				break
			}
		}
	}

	// backpressure
	for _, metric := range q.metrics {
		for _, rm := range metric.Rms {
			if _, ok := rateMap[rm.Rt]; ok {
				if rm.ThroughPut > 0 && rm.ThroughPut < rateMap[rm.Rt] {
					rateMap[rm.Rt] = rm.ThroughPut
				}
			}
		}
	}

	// notify proxies to set rates
	ctx, cancel := context.WithTimeout(q.ctx, time.Second*GetMetricsTimeout)
	defer cancel()
	map2List := func() []*commonpb.Rate {
		rates := make([]*commonpb.Rate, 0, len(rateMap))
		for rt, r := range rateMap {
			rates = append(rates, &commonpb.Rate{Rt: rt, R: r})
		}
		return rates
	}
	timestamp := tsoutil.ComposeTSByTime(time.Now(), 0)
	req := &proxypb.SetRatesRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_Undefined,
			MsgID:     int64(timestamp),
			Timestamp: timestamp,
		},
		Rates: map2List(),
	}
	return q.proxies.SetRates(ctx, req)
}

func NewQuotaCenter(ctx context.Context,
	proxies *proxyClientManager,
	queryCoord types.QueryCoord,
	dataCoord types.DataCoord) *QuotaCenter {
	ctx1, cancel := context.WithCancel(ctx)
	return &QuotaCenter{
		ctx:    ctx1,
		cancel: cancel,

		proxies:    proxies,
		queryCoord: queryCoord,
		dataCoord:  dataCoord,
	}
}
