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
	queryNodeMetrics []*metricsinfo.QuotaMetrics
	dataNodeMetrics  []*metricsinfo.QuotaMetrics
	proxyMetrics     []*metricsinfo.QuotaMetrics

	currentRates map[internalpb.RateType]float64

	stopOnce sync.Once
	stopChan chan struct{}
}

// NewQuotaCenter returns a new QuotaCenter.
func NewQuotaCenter(proxies *proxyClientManager, queryCoord types.QueryCoord, dataCoord types.DataCoord) *QuotaCenter {
	return &QuotaCenter{
		proxies:      proxies,
		queryCoord:   queryCoord,
		dataCoord:    dataCoord,
		currentRates: make(map[internalpb.RateType]float64),
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
			q.calculateRates()
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
	q.dataNodeMetrics = make([]*metricsinfo.QuotaMetrics, 0)
	q.queryNodeMetrics = make([]*metricsinfo.QuotaMetrics, 0)
	q.proxyMetrics = make([]*metricsinfo.QuotaMetrics, 0)
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
			// TODO: fix proxy metric, unmarshal by topology way
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

func (q *QuotaCenter) disableDML() {
	q.currentRates[internalpb.RateType_DMLInsert] = 0
	q.currentRates[internalpb.RateType_DMLDelete] = 0
}

func (q *QuotaCenter) disableDQL() {
	q.currentRates[internalpb.RateType_DQLSearch] = 0
	q.currentRates[internalpb.RateType_DQLQuery] = 0
}

// calculateRates calculates target rates by different strategies.
func (q *QuotaCenter) calculateRates() {
	q.resetCurrentRates()

	if Params.QuotaConfig.ForceDenyWriting {
		q.disableDML()
	} else {
		if q.memoryToWaterLevel() {
			q.disableDML()
		}
		// TODO: add more strategies
	}

	if Params.QuotaConfig.ForceDenyReading {
		q.disableDQL()
	} else {
		// TODO: add strategies
	}

	log.Debug("QuotaCenter calculates rate done", zap.Any("rates", q.currentRates))
}

func (q *QuotaCenter) resetCurrentRates() {
	for _, rt := range internalpb.RateType_value {
		q.currentRates[internalpb.RateType(rt)] = q.getRateConfigByRateType(internalpb.RateType(rt))
	}
}

// getRateConfigByRateType returns rate by the specified rateType.
func (q *QuotaCenter) getRateConfigByRateType(rt internalpb.RateType) float64 {
	switch rt {
	case internalpb.RateType_DDLCollection:
		return Params.QuotaConfig.DDLCollectionRate
	case internalpb.RateType_DDLPartition:
		return Params.QuotaConfig.DDLPartitionRate
	case internalpb.RateType_DDLIndex:
		return Params.QuotaConfig.DDLIndexRate
	case internalpb.RateType_DDLSegments:
		return Params.QuotaConfig.DDLSegmentsRate
	case internalpb.RateType_DMLInsert:
		return Params.QuotaConfig.DMLInsertRate
	case internalpb.RateType_DMLDelete:
		return Params.QuotaConfig.DMLDeleteRate
	case internalpb.RateType_DQLSearch:
		return Params.QuotaConfig.DQLSearchRate
	case internalpb.RateType_DQLQuery:
		return Params.QuotaConfig.DQLQueryRate
	}
	panic(fmt.Errorf("QuotaCenter: invalid rateType, rt = %d", rt))
}

// memoryToWaterLevel checks whether any node has memory resource issue.
func (q *QuotaCenter) memoryToWaterLevel() bool {
	for _, metric := range q.queryNodeMetrics {
		if float64(metric.Mm.UsedMem)/float64(metric.Mm.TotalMem) >= Params.QuotaConfig.QueryNodeMemoryWaterLevel {
			log.Debug("QuotaCenter: QueryNode memory to water level",
				zap.Uint64("UsedMem", metric.Mm.UsedMem),
				zap.Uint64("TotalMem", metric.Mm.TotalMem),
				zap.Float64("QueryNodeMemoryWaterLevel", Params.QuotaConfig.QueryNodeMemoryWaterLevel))
			return true
		}
		// TODO: check growing segments, ...
	}
	for _, metric := range q.dataNodeMetrics {
		// TODO: check if Mm is nil
		if float64(metric.Mm.UsedMem)/float64(metric.Mm.TotalMem) >= Params.QuotaConfig.DataNodeMemoryWaterLevel {
			log.Debug("QuotaCenter: DataNode memory to water level",
				zap.Uint64("UsedMem", metric.Mm.UsedMem),
				zap.Uint64("TotalMem", metric.Mm.TotalMem),
				zap.Float64("DataNodeMemoryWaterLevel", Params.QuotaConfig.DataNodeMemoryWaterLevel))
			return true
		}
		// TODO: check bloom filter, ...
	}
	return false
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
