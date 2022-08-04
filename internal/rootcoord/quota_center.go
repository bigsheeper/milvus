package rootcoord

import (
	"context"
	"fmt"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"golang.org/x/sync/errgroup"
	"math"
	"sync"
	"time"
)

// TODO: get from config
const (
	QuotaCollectInterval   = 1
	QuotaMemoryWaterMarker = 0.9
	GetMetricsTimeout      = 10 // in seconds
	SetRatesTimeout        = 10 // in seconds
)

type QuotaCenter struct {
	ctx    context.Context
	cancel context.CancelFunc

	// clients
	proxies    *proxyClientManager
	queryCoord types.QueryCoord
	dataCoord  types.DataCoord

	//// metrics
	metricsMu sync.Mutex
	metrics   []*metricsinfo.QuotaMetrics // TODO: sort to datanode, querynode, proxy?
}

type GetMetricsInterface interface {
	GetMetrics(ctx context.Context, request *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error)
}

func (q *QuotaCenter) Run() {
	for {
		select {
		case <-q.ctx.Done():
			fmt.Println("QuotaCenter exit")
			return
		case <-time.After(QuotaCollectInterval * time.Second):
			err := q.Collect()
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

func (q *QuotaCenter) Collect() error {
	var rspsMu sync.Mutex
	rsps := make([]*milvuspb.GetMetricsResponse, 0)
	getMetrics := func(client GetMetricsInterface) error {
		ctx, cancel := context.WithTimeout(q.ctx, time.Second*GetMetricsTimeout)
		defer cancel()
		timestamp := tsoutil.ComposeTSByTime(time.Now(), 0)
		req := &milvuspb.GetMetricsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Undefined,
				MsgID:     int64(timestamp),
				Timestamp: timestamp,
			},
			Request: "", // TODO: get quota metrics only
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
	group.Go(func() error { return getMetrics(q.dataCoord) })
	group.Go(func() error { return getMetrics(q.queryCoord) })
	// TODO: get proxy metrics
	err := group.Wait()
	if err != nil {
		return err
	}

	for _, rsp := range rsps {
		name := rsp.GetComponentName()
		switch name {
		case typeutil.QueryCoordRole:
			queryCoordTopo := &metricsinfo.QueryCoordTopology{}
			err = metricsinfo.UnmarshalTopology(rsp.GetResponse(), queryCoordTopo)
			if err != nil {
				return err
			}
			for _, queryNodeMetric := range queryCoordTopo.Cluster.ConnectedNodes {
				q.metricsMu.Lock()
				q.metrics = append(q.metrics, &queryNodeMetric.QuotaMetrics)
				q.metricsMu.Unlock()
			}
		case typeutil.DataCoordRole:
			dataCoordTopo := &metricsinfo.DataCoordTopology{}
			err = metricsinfo.UnmarshalTopology(rsp.GetResponse(), dataCoordTopo)
			if err != nil {
				return err
			}
			for _, dataNodeMetric := range dataCoordTopo.Cluster.ConnectedNodes {
				q.metricsMu.Lock()
				q.metrics = append(q.metrics, &dataNodeMetric.QuotaMetrics)
				q.metricsMu.Unlock()
			}
		case typeutil.ProxyRole:
			proxyMetrics := &metricsinfo.ProxyInfos{}
			err = metricsinfo.UnmarshalComponentInfos(rsp.GetResponse(), proxyMetrics)
			if err != nil {
				return err
			}
			// TODO: handle proxy metrics
		}
	}
	return nil
}

func (q *QuotaCenter) Execute() error {
	rateMap := map[commonpb.RateType]float32{
		commonpb.RateType_DDLCollection: math.MaxFloat32,
		commonpb.RateType_DDLPartition:  math.MaxFloat32,
		commonpb.RateType_DDLIndex:      math.MaxFloat32,
		commonpb.RateType_DMLDelete:     math.MaxFloat32,
		commonpb.RateType_DMLInsert:     math.MaxFloat32,
		commonpb.RateType_DQLSearch:     math.MaxFloat32,
		commonpb.RateType_DQLQuery:      math.MaxFloat32,
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

		metrics: make([]*metricsinfo.QuotaMetrics, 0),
	}
}
