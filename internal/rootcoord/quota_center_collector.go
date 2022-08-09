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

	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

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
	group.Go(func() error { return getMetricFunc(q.dataCoord) })
	group.Go(func() error { return getMetricFunc(q.queryCoord) })
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
				q.queryNodeMetrics = append(q.queryNodeMetrics, &queryNodeMetric.QuotaMetrics)
			}
		case typeutil.DataCoordRole:
			dataCoordTopo := &metricsinfo.DataCoordTopology{}
			err = metricsinfo.UnmarshalTopology(rsp.GetResponse(), dataCoordTopo)
			if err != nil {
				return err
			}
			for _, dataNodeMetric := range dataCoordTopo.Cluster.ConnectedNodes {
				q.dataNodeMetrics = append(q.dataNodeMetrics, &dataNodeMetric.QuotaMetrics)
			}
		case typeutil.ProxyRole:
			proxyMetric := &metricsinfo.ProxyInfos{}
			err = metricsinfo.UnmarshalComponentInfos(rsp.GetResponse(), proxyMetric)
			if err != nil {
				return err
			}
			q.proxyMetrics = append(q.proxyMetrics, &proxyMetric.QuotaMetrics)
		}
	}
	return nil
}
