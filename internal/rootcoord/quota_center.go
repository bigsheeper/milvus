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
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	QuotaMemoryWaterMarker = 0.9 // TODO: add to config
	GetMetricsTimeout      = 10  // TODO: add to config
	SetRatesTimeout        = 10  // TODO: add to config
)

type controlBehavior int32

const (
	disableWrite controlBehavior = iota
	disableRead
	throttling
)

type quotaEvent int32

const (
	none quotaEvent = iota
	memoryReachWaterMarker
	tSafeDelayed
	growingPredominated
	dmlPerfChanged
	dqlPerfChanged
	// TODO: support more...
)

type QuotaCenter struct {
	// clients
	proxies    *proxyClientManager
	queryCoord types.QueryCoord
	dataCoord  types.DataCoord

	// metrics
	queryNodeMetrics []*metricsinfo.QuotaMetrics
	dataNodeMetrics  []*metricsinfo.QuotaMetrics
	proxyMetrics     []*metricsinfo.QuotaMetrics

	currentRates map[commonpb.RateType]float64

	stopOnce sync.Once
	stopChan chan struct{}
}

func (q *QuotaCenter) run() {
	for {
		select {
		case <-q.stopChan:
			log.Info("QuotaCenter exit")
			return
		//case <-time.After(time.Duration(Params.QuotaConfig.QuotaCenterCollectInterval) * time.Millisecond):
		case <-time.After(1 * time.Second):
			fmt.Println("run..........")
			err := q.syncMetrics()
			if err != nil {
				log.Error("quotaCenter sync metrics failed", zap.Error(err))
				continue
			}
			q.calculateRates()
			err = q.setRates()
			if err != nil {
				log.Error("quotaCenter setRates failed", zap.Error(err))
			}
		}
	}
}

func (q *QuotaCenter) stop() {
	q.stopOnce.Do(func() {
		q.stopChan <- struct{}{}
	})
}

type getMetricsInterface interface {
	GetMetrics(ctx context.Context, request *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error)
}

func (q *QuotaCenter) clearMetrics() {
	q.dataNodeMetrics = make([]*metricsinfo.QuotaMetrics, 0)
	q.queryNodeMetrics = make([]*metricsinfo.QuotaMetrics, 0)
	q.proxyMetrics = make([]*metricsinfo.QuotaMetrics, 0)
}

func (q *QuotaCenter) syncMetrics() error {
	q.clearMetrics()
	var rspsMu sync.Mutex
	rsps := make([]*milvuspb.GetMetricsResponse, 0)
	getMetricFunc := func(client getMetricsInterface) error {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*GetMetricsTimeout)
		defer cancel()
		timestamp := tsoutil.ComposeTSByTime(time.Now(), 0)
		metricReq := map[string]string{metricsinfo.MetricTypeKey: metricsinfo.SystemInfoMetrics}
		metricReqStr, err := json.Marshal(metricReq)
		req := &milvuspb.GetMetricsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     int64(timestamp),
				Timestamp: timestamp,
			},
			Request: string(metricReqStr), // TODO: get quota metrics only
		}
		rsp, err := client.GetMetrics(ctx, req)
		if err != nil {
			return err
		}
		if rsp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
			return fmt.Errorf("quotaCenter call GetMetrics failed")
		}
		rspsMu.Lock()
		rsps = append(rsps, rsp)
		rspsMu.Unlock()
		return nil
	}

	group := &errgroup.Group{}
	group.Go(func() error { return getMetricFunc(q.dataCoord) })
	group.Go(func() error { return getMetricFunc(q.queryCoord) })
	// TODO: get proxy metrics
	err := group.Wait()
	if err != nil {
		return err
	}

	for _, rsp := range rsps {
		name := metricsinfo.GetRoleNameByComponentName(rsp.GetComponentName())
		switch name {
		case typeutil.QueryCoordRole:
			queryCoordTopo := &metricsinfo.QueryCoordTopology{}
			err = metricsinfo.UnmarshalTopology(rsp.GetResponse(), queryCoordTopo)
			if err != nil {
				return err
			}
			for _, queryNodeMetric := range queryCoordTopo.Cluster.ConnectedNodes {
				q.queryNodeMetrics = append(q.queryNodeMetrics, queryNodeMetric.QuotaMetrics)
			}
		case typeutil.DataCoordRole:
			dataCoordTopo := &metricsinfo.DataCoordTopology{}
			err = metricsinfo.UnmarshalTopology(rsp.GetResponse(), dataCoordTopo)
			if err != nil {
				return err
			}
			for _, dataNodeMetric := range dataCoordTopo.Cluster.ConnectedNodes {
				q.dataNodeMetrics = append(q.dataNodeMetrics, dataNodeMetric.QuotaMetrics)
			}
		case typeutil.ProxyRole:
			proxyMetric := &metricsinfo.ProxyInfos{}
			err = metricsinfo.UnmarshalComponentInfos(rsp.GetResponse(), proxyMetric)
			if err != nil {
				return err
			}
			q.proxyMetrics = append(q.proxyMetrics, proxyMetric.QuotaMetrics)
		}
	}
	log.Debug("QuotaCenter sync metrics done",
		zap.Any("dataNodeMetrics", q.dataNodeMetrics),
		zap.Any("queryNodeMetrics", q.queryNodeMetrics),
		zap.Any("proxyMetrics", q.proxyMetrics))
	return nil
}

func (q *QuotaCenter) calculateRates() {
	cb := q.checkMemory()
	switch cb {
	case disableWrite:
		q.currentRates[commonpb.RateType_DMLInsert] = 0
	case throttling:
		rates := q.getMinThroughput()
		for rt, r := range rates {
			q.currentRates[rt] = r
		}
	}
}

func (q *QuotaCenter) checkMemory() controlBehavior {
	return throttling
	for _, metric := range q.queryNodeMetrics {
		if float64(metric.Mm.UsedMem)/float64(metric.Mm.TotalMem) >= QuotaMemoryWaterMarker {
			return disableWrite
		}
		// TODO: check growing segments, ...
	}
	for _, metric := range q.dataNodeMetrics {
		// TODO: nil check
		if float64(metric.Mm.UsedMem)/float64(metric.Mm.TotalMem) >= QuotaMemoryWaterMarker {
			return disableWrite
		}
		// TODO: check bloom filter, ...
	}
	return throttling
}

func (q *QuotaCenter) getMinThroughput() map[commonpb.RateType]float64 {
	minThroughput := make(map[commonpb.RateType]float64)
	metrics := make([]*metricsinfo.QuotaMetrics, 0, len(q.dataNodeMetrics)+len(q.queryNodeMetrics))
	metrics = append(metrics, q.dataNodeMetrics...)
	metrics = append(metrics, q.queryNodeMetrics...)
	for _, metric := range metrics {
		for _, rate := range metric.Rms {
			if _, ok := minThroughput[rate.Rt]; !ok {
				minThroughput[rate.Rt] = math.MaxFloat64
			}
			if rate.ThroughPut == 0 {
				// ignore 0 value here, in case of QueryNode didn't be loaded and still return 0 rates
				// over and over again. We should limit the rate in DataNodes in this case.
				continue
			}
			if rate.ThroughPut < minThroughput[rate.Rt] {
				minThroughput[rate.Rt] = rate.ThroughPut
			}
		}
	}
	for rt, t := range minThroughput {
		if t == math.MaxFloat64 {
			minThroughput[rt] = 0
		}
	}
	return minThroughput
}

func (q *QuotaCenter) setRates() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*SetRatesTimeout)
	defer cancel()
	map2List := func() []*commonpb.Rate {
		rates := make([]*commonpb.Rate, 0, len(q.currentRates))
		for rt, r := range q.currentRates {
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

func NewQuotaCenter(proxies *proxyClientManager, queryCoord types.QueryCoord, dataCoord types.DataCoord) *QuotaCenter {
	return &QuotaCenter{
		proxies:      proxies,
		queryCoord:   queryCoord,
		dataCoord:    dataCoord,
		currentRates: make(map[commonpb.RateType]float64),
	}
}
