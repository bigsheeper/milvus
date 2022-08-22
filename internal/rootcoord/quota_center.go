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
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	GetMetricsTimeout = 10 * time.Second
	SetRatesTimeout   = 10 * time.Second
)

type controlBehavior int32

const (
	disableWrite controlBehavior = iota
	disableRead
	throttling
)

const (
	// TODO: add to conf
	MemoryLowWaterLevel = 0.8
	MaxNQInQueue        = 1024 * 100
	MaxQueriesInQueue   = 1024
	MaxTSafeDelay       = 10 * time.Second
)

//type quotaEvent int32
//
//const (
//	none quotaEvent = iota
//	memoryReachWaterLevel
//	tSafeDelayed
//	growingPredominated
//	dmlPerfChanged
//	dqlPerfChanged
//	// TODO: support more...
//)

// QuotaCenter manages the quota and limitations of the whole cluster,
// it receives metrics info from DataNodes, QueryNodes and Proxies, and
// notifies Proxies to limit rate of requests from clients or reject
// all data manipulated requests when cluster met resources issues.
type QuotaCenter struct {
	// clients
	proxies    *proxyClientManager
	queryCoord types.QueryCoord
	dataCoord  types.DataCoord

	// metrics
	queryNodeMetrics []*metricsinfo.QueryNodeQuotaMetrics
	dataNodeMetrics  []*metricsinfo.DataNodeQuotaMetrics
	proxyMetrics     []*metricsinfo.ProxyQuotaMetrics

	currentRates map[internalpb.RateType]float64
	TSOAllocator func(count uint32) (typeutil.Timestamp, error)

	stopOnce sync.Once
	stopChan chan struct{}
}

// NewQuotaCenter returns a new QuotaCenter.
func NewQuotaCenter(proxies *proxyClientManager, queryCoord types.QueryCoord, dataCoord types.DataCoord, TSOAllocator func(count uint32) (typeutil.Timestamp, error)) *QuotaCenter {
	return &QuotaCenter{
		proxies:      proxies,
		queryCoord:   queryCoord,
		dataCoord:    dataCoord,
		currentRates: make(map[internalpb.RateType]float64),
		TSOAllocator: TSOAllocator,
	}
}

// run starts the service of QuotaCenter.
func (q *QuotaCenter) run() {
	log.Info("Start QuotaCenter", zap.Int64("collectInterval[ms]", Params.QuotaConfig.QuotaCenterCollectInterval))
	ticker := time.NewTicker(time.Duration(Params.QuotaConfig.QuotaCenterCollectInterval) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-q.stopChan:
			log.Info("QuotaCenter exit")
			return
		case <-ticker.C:
			err := q.syncMetrics()
			if err != nil {
				log.Error("quotaCenter sync metrics failed", zap.Error(err))
				continue
			}
			err = q.calculateRates()
			if err != nil {
				log.Error("quotaCenter calculate rates failed", zap.Error(err))
				continue
			}
			err = q.setRates()
			if err != nil {
				log.Error("quotaCenter setRates failed", zap.Error(err))
			}
		}
	}
}

// stop would stop the service of QuotaCenter.
func (q *QuotaCenter) stop() {
	q.stopOnce.Do(func() {
		q.stopChan <- struct{}{}
	})
}

// getMetricsInterface defines the interface of GetMetrics.
type getMetricsInterface interface {
	GetMetrics(ctx context.Context, request *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error)
}

//  clearMetrics removes all metrics stored in QuotaCenter.
func (q *QuotaCenter) clearMetrics() {
	q.dataNodeMetrics = make([]*metricsinfo.DataNodeQuotaMetrics, 0)
	q.queryNodeMetrics = make([]*metricsinfo.QueryNodeQuotaMetrics, 0)
	q.proxyMetrics = make([]*metricsinfo.ProxyQuotaMetrics, 0)
}

// syncMetrics sends GetMetrics requests to DataCoord and QueryCoord to sync the metrics in DataNodes and QueryNodes.
func (q *QuotaCenter) syncMetrics() error {
	q.clearMetrics()
	ctx, cancel := context.WithTimeout(context.Background(), GetMetricsTimeout)
	defer cancel()

	var rspsMu sync.Mutex
	rsps := make([]*milvuspb.GetMetricsResponse, 0)
	getMetricFunc := func(client getMetricsInterface) error {
		req, err := metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
		if err != nil {
			return err
		}
		rsp, err := client.GetMetrics(ctx, req)
		if err != nil {
			return err
		}
		if rsp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
			return fmt.Errorf("quotaCenter call GetMetrics failed, err = %s", rsp.GetStatus().GetReason())
		}
		rspsMu.Lock()
		rsps = append(rsps, rsp)
		rspsMu.Unlock()
		return nil
	}

	group := &errgroup.Group{}
	group.Go(func() error { return getMetricFunc(q.dataCoord) })
	group.Go(func() error { return getMetricFunc(q.queryCoord) })
	group.Go(func() error {
		metricRsps, err := q.proxies.GetMetrics(ctx)
		if err != nil {
			return err
		}
		rspsMu.Lock()
		rsps = append(rsps, metricRsps...)
		rspsMu.Unlock()
		return nil
	})
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
				if queryNodeMetric.QuotaMetrics != nil {
					q.queryNodeMetrics = append(q.queryNodeMetrics, queryNodeMetric.QuotaMetrics)
				}
			}
		case typeutil.DataCoordRole:
			dataCoordTopo := &metricsinfo.DataCoordTopology{}
			err = metricsinfo.UnmarshalTopology(rsp.GetResponse(), dataCoordTopo)
			if err != nil {
				return err
			}
			for _, dataNodeMetric := range dataCoordTopo.Cluster.ConnectedNodes {
				if dataNodeMetric.QuotaMetrics != nil {
					q.dataNodeMetrics = append(q.dataNodeMetrics, dataNodeMetric.QuotaMetrics)
				}
			}
		case typeutil.ProxyRole:
			// TODO: fix proxy metric, unmarshal by topology way
			proxyMetric := &metricsinfo.ProxyInfos{}
			err = metricsinfo.UnmarshalComponentInfos(rsp.GetResponse(), proxyMetric)
			if err != nil {
				return err
			}
			if proxyMetric.QuotaMetrics != nil {
				q.proxyMetrics = append(q.proxyMetrics, proxyMetric.QuotaMetrics)
			}
		}
	}
	log.Debug("QuotaCenter sync metrics done",
		zap.Any("dataNodeMetrics", q.dataNodeMetrics),
		zap.Any("queryNodeMetrics", q.queryNodeMetrics),
		zap.Any("proxyMetrics", q.proxyMetrics))
	return nil
}

func (q *QuotaCenter) disableDML() {
	q.currentRates[internalpb.RateType_DMLInsert] = 0
	q.currentRates[internalpb.RateType_DMLDelete] = 0
}

func (q *QuotaCenter) disableDQL() {
	q.currentRates[internalpb.RateType_DQLSearch] = 0
	q.currentRates[internalpb.RateType_DQLQuery] = 0
	log.Info("", zap.String("triggerReason", ""))
}

// calculateRates calculates target rates by different strategies.
func (q *QuotaCenter) calculateRates() error {
	q.resetCurrentRates()

	if Params.QuotaConfig.ForceDenyWriting {
		q.disableDML()
	} else {
		percentage, err := q.tSafeDelayed()
		if err != nil {
			return err
		}
		memPercent := q.memoryToWaterLevel()
		if percentage < memPercent {
			percentage = memPercent
		}
		q.currentRates[internalpb.RateType_DMLInsert] *= percentage
		q.currentRates[internalpb.RateType_DMLDelete] *= percentage
		// TODO: add more strategies
	}

	if Params.QuotaConfig.ForceDenyReading {
		q.disableDQL()
	} else {
		percentage := q.checkQueryQueueSize()
		q.currentRates[internalpb.RateType_DQLSearch] *= percentage
		q.currentRates[internalpb.RateType_DQLQuery] *= percentage
		// TODO: add strategies
	}

	log.Debug("QuotaCenter calculates rate done", zap.Any("rates", q.currentRates))
	return nil
}

// resetCurrentRates resets all current rates to configured rates
func (q *QuotaCenter) resetCurrentRates() {
	for _, rateType := range internalpb.RateType_value {
		rt := internalpb.RateType(rateType)
		switch rt {
		case internalpb.RateType_DMLInsert:
			q.currentRates[rt] = Params.QuotaConfig.DMLInsertRate
		case internalpb.RateType_DMLDelete:
			q.currentRates[rt] = Params.QuotaConfig.DMLDeleteRate
		case internalpb.RateType_DQLSearch:
			q.currentRates[rt] = Params.QuotaConfig.DQLSearchRate
		case internalpb.RateType_DQLQuery:
			q.currentRates[rt] = Params.QuotaConfig.DQLQueryRate
		}
	}
}

// tSafeDelayed gets QueryNode's tSafe delayed and return the percentage according to max tolerable tSafe delay.
func (q *QuotaCenter) tSafeDelayed() (float64, error) {
	minTSafe := typeutil.MaxTimestamp
	for _, metric := range q.queryNodeMetrics {
		if metric.MinTSafe < minTSafe {
			minTSafe = metric.MinTSafe
		}
	}
	ts, err := q.TSOAllocator(1)
	if err != nil {
		return 0, err
	}
	if minTSafe >= ts {
		return 1, nil
	}
	delay, _ := tsoutil.ParseTS(minTSafe - ts)
	return float64(delay.Nanosecond()) / float64(MaxTSafeDelay.Nanoseconds()), nil
}

// checkQueryQueueSize checks search and query tasks number in QueryNode,
// and return the percentage according to max query task number and max search nq number.
func (q *QuotaCenter) checkQueryQueueSize() float64 {
	percentage := float64(1)
	for _, metric := range q.queryNodeMetrics {
		if metric.SearchNQInQueue >= MaxNQInQueue {
			return 0
		} else if metric.QueriesInQueue >= MaxQueriesInQueue {
			return 0
		} else {
			if (float64(metric.SearchNQInQueue) / MaxNQInQueue) < percentage {
				percentage = float64(metric.SearchNQInQueue) / MaxNQInQueue
			}
			if (float64(metric.QueriesInQueue) / MaxQueriesInQueue) < percentage {
				percentage = float64(metric.QueriesInQueue) / MaxQueriesInQueue
			}
		}
	}
	return percentage
}

// memoryToWaterLevel checks whether any node has memory resource issue,
// and return the percentage according to max memory water level.
func (q *QuotaCenter) memoryToWaterLevel() float64 {
	percentage := float64(1)
	for _, metric := range q.queryNodeMetrics {
		memoryWaterLevel := float64(metric.Mm.UsedMem) / float64(metric.Mm.TotalMem)
		if memoryWaterLevel <= MemoryLowWaterLevel {
			continue
		}
		if memoryWaterLevel >= Params.QuotaConfig.QueryNodeMemoryWaterLevel {
			log.Debug("QuotaCenter: QueryNode memory to high water level",
				zap.Uint64("UsedMem", metric.Mm.UsedMem),
				zap.Uint64("TotalMem", metric.Mm.TotalMem),
				zap.Float64("QueryNodeMemoryWaterLevel", Params.QuotaConfig.QueryNodeMemoryWaterLevel))
			return 0
		}
		p := (memoryWaterLevel - MemoryLowWaterLevel) / (Params.QuotaConfig.QueryNodeMemoryWaterLevel - MemoryLowWaterLevel)
		if p < percentage {
			percentage = p
		}
	}
	for _, metric := range q.dataNodeMetrics {
		memoryWaterLevel := float64(metric.Mm.UsedMem) / float64(metric.Mm.TotalMem)
		if memoryWaterLevel <= MemoryLowWaterLevel {
			continue
		}
		if memoryWaterLevel >= Params.QuotaConfig.DataNodeMemoryWaterLevel {
			log.Debug("QuotaCenter: DataNode memory to high water level",
				zap.Uint64("UsedMem", metric.Mm.UsedMem),
				zap.Uint64("TotalMem", metric.Mm.TotalMem),
				zap.Float64("DataNodeMemoryWaterLevel", Params.QuotaConfig.DataNodeMemoryWaterLevel))
			return 0
		}
		p := (memoryWaterLevel - MemoryLowWaterLevel) / (Params.QuotaConfig.QueryNodeMemoryWaterLevel - MemoryLowWaterLevel)
		if p < percentage {
			percentage = p
		}
	}
	return percentage
}

// setRates notifies Proxies to set rates for different rate types.
func (q *QuotaCenter) setRates() error {
	ctx, cancel := context.WithTimeout(context.Background(), SetRatesTimeout)
	defer cancel()
	map2List := func() []*internalpb.Rate {
		rates := make([]*internalpb.Rate, 0, len(q.currentRates))
		for rt, r := range q.currentRates {
			rates = append(rates, &internalpb.Rate{Rt: rt, R: r})
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
