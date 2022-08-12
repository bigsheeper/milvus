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

package metrics

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

// limiter's rates
var (
	//DDLCollectionLimiterRate = prometheus.NewGaugeVec(
	//	prometheus.GaugeOpts{
	//		Namespace: milvusNamespace,
	//		Subsystem: typeutil.ProxyRole,
	//		Name:      "ddl_collection_limiter_rate",
	//		Help:      "",
	//	}, []string{nodeIDLabelName})
	//
	//DDLPartitionLimiterRate = prometheus.NewGaugeVec(
	//	prometheus.GaugeOpts{
	//		Namespace: milvusNamespace,
	//		Subsystem: typeutil.ProxyRole,
	//		Name:      "ddl_partition_limiter_rate",
	//		Help:      "",
	//	}, []string{nodeIDLabelName})
	//
	//DDLIndexLimiterRate = prometheus.NewGaugeVec(
	//	prometheus.GaugeOpts{
	//		Namespace: milvusNamespace,
	//		Subsystem: typeutil.ProxyRole,
	//		Name:      "ddl_index_limiter_rate",
	//		Help:      "",
	//	}, []string{nodeIDLabelName})
	//
	//DDLSegmentsLimiterRate = prometheus.NewGaugeVec(
	//	prometheus.GaugeOpts{
	//		Namespace: milvusNamespace,
	//		Subsystem: typeutil.ProxyRole,
	//		Name:      "ddl_segments_limiter_rate",
	//		Help:      "",
	//	}, []string{nodeIDLabelName})

	LimiterRate = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "limiter_rate",
			Help:      "",
		}, []string{nodeIDLabelName, msgTypeLabelName})

	//DeleteLimiterRate = prometheus.NewGaugeVec(
	//	prometheus.GaugeOpts{
	//		Namespace: milvusNamespace,
	//		Subsystem: typeutil.ProxyRole,
	//		Name:      "dml_delete_limiter_rate",
	//		Help:      "",
	//	}, []string{nodeIDLabelName})
	//
	//SearchLimiterRate = prometheus.NewGaugeVec(
	//	prometheus.GaugeOpts{
	//		Namespace: milvusNamespace,
	//		Subsystem: typeutil.ProxyRole,
	//		Name:      "dml_search_limiter_rate",
	//		Help:      "",
	//	}, []string{nodeIDLabelName})
	//
	//QueryLimiterRate = prometheus.NewGaugeVec(
	//	prometheus.GaugeOpts{
	//		Namespace: milvusNamespace,
	//		Subsystem: typeutil.ProxyRole,
	//		Name:      "dml_query_limiter_rate",
	//		Help:      "",
	//	}, []string{nodeIDLabelName})
)

// proxy rates
var (
	ProxyExecuteRate = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "execute_rates",
			Help:      "",
		}, []string{nodeIDLabelName, msgTypeLabelName})

	//ProxyDeleteRate = prometheus.NewCounterVec(
	//	prometheus.CounterOpts{
	//		Namespace: milvusNamespace,
	//		Subsystem: typeutil.ProxyRole,
	//		Name:      "delete_rate",
	//		Help:      "",
	//	}, []string{nodeIDLabelName})
	//
	//ProxySearchRate = prometheus.NewCounterVec(
	//	prometheus.CounterOpts{
	//		Namespace: milvusNamespace,
	//		Subsystem: typeutil.ProxyRole,
	//		Name:      "search_rate",
	//		Help:      "",
	//	}, []string{nodeIDLabelName})
	//
	//ProxyQueryRate = prometheus.NewCounterVec(
	//	prometheus.CounterOpts{
	//		Namespace: milvusNamespace,
	//		Subsystem: typeutil.ProxyRole,
	//		Name:      "query_rate",
	//		Help:      "",
	//	}, []string{nodeIDLabelName})
)

// nodes rates
var (
	DataNodeConsumeRate = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataNodeRole,
			Name:      "consume_rate",
			Help:      "",
		}, []string{nodeIDLabelName, msgTypeLabelName})

	//DataNodeConsumeDeleteRate = prometheus.NewCounterVec(
	//	prometheus.CounterOpts{
	//		Namespace: milvusNamespace,
	//		Subsystem: typeutil.DataNodeRole,
	//		Name:      "consume_delete_rate",
	//		Help:      "",
	//	}, []string{nodeIDLabelName})

	// Use DataNodeFlushedSize instead
	//DataNodeSyncInsertRate = prometheus.NewCounterVec(
	//	prometheus.CounterOpts{
	//		Namespace: milvusNamespace,
	//		Subsystem: typeutil.DataNodeRole,
	//		Name:      "sync_insert_rate",
	//		Help:      "",
	//	}, []string{nodeIDLabelName})
	//
	//DataNodeSyncDeleteRate = prometheus.NewCounterVec(
	//	prometheus.CounterOpts{
	//		Namespace: milvusNamespace,
	//		Subsystem: typeutil.DataNodeRole,
	//		Name:      "sync_delete_rate",
	//		Help:      "",
	//	}, []string{nodeIDLabelName})

	QueryNodeConsumeRate = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "consume_rate",
			Help:      "",
		}, []string{nodeIDLabelName, msgTypeLabelName})

	//QueryNodeConsumeDeleteRate = prometheus.NewCounterVec(
	//	prometheus.CounterOpts{
	//		Namespace: milvusNamespace,
	//		Subsystem: typeutil.QueryNodeRole,
	//		Name:      "consume_delete_rate",
	//		Help:      "",
	//	}, []string{nodeIDLabelName})

	QueryNodeExecuteRate = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.QueryNodeRole,
			Name:      "execute_rates",
			Help:      "",
		}, []string{nodeIDLabelName, msgTypeLabelName})

	//QueryNodeQueryRate = prometheus.NewCounterVec(
	//	prometheus.CounterOpts{
	//		Namespace: milvusNamespace,
	//		Subsystem: typeutil.QueryNodeRole,
	//		Name:      "query_rate",
	//		Help:      "",
	//	}, []string{nodeIDLabelName})
)

//RegisterQuota registers Quota metrics
func RegisterQuota(registry *prometheus.Registry) {
	//registry.MustRegister(DDLCollectionLimiterRate)
	//registry.MustRegister(DDLPartitionLimiterRate)
	//registry.MustRegister(DDLIndexLimiterRate)
	//registry.MustRegister(DDLSegmentsLimiterRate)
	registry.MustRegister(LimiterRate)
	//registry.MustRegister(DeleteLimiterRate)
	//registry.MustRegister(SearchLimiterRate)
	//registry.MustRegister(QueryLimiterRate)

	registry.MustRegister(ProxyExecuteRate)
	//registry.MustRegister(ProxyDeleteRate)
	//registry.MustRegister(ProxySearchRate)
	//registry.MustRegister(ProxyQueryRate)

	registry.MustRegister(DataNodeConsumeRate)
	//registry.MustRegister(DataNodeConsumeDeleteRate)
	//registry.MustRegister(DataNodeSyncInsertRate)
	//registry.MustRegister(DataNodeSyncDeleteRate)
	registry.MustRegister(QueryNodeConsumeRate)
	//registry.MustRegister(QueryNodeConsumeDeleteRate)
	registry.MustRegister(QueryNodeExecuteRate)
	//registry.MustRegister(QueryNodeQueryRate)
}

func SetRateGaugeByRateType(rateType internalpb.RateType, nodeID int64, rate float64) {
	nodeIDStr := strconv.FormatInt(nodeID, 10)
	log.Debug("set rates", zap.Int64("nodeID", nodeID), zap.String("rateType", rateType.String()), zap.Float64("rate", rate))
	switch rateType {
	//case internalpb.RateType_DDLCollection:
	//	DDLCollectionLimiterRate.WithLabelValues(nodeIDStr).Set(rate)
	//case internalpb.RateType_DDLPartition:
	//	DDLPartitionLimiterRate.WithLabelValues(nodeIDStr).Set(rate)
	//case internalpb.RateType_DDLIndex:
	//	DDLIndexLimiterRate.WithLabelValues(nodeIDStr).Set(rate)
	//case internalpb.RateType_DDLSegments:
	//	DDLSegmentsLimiterRate.WithLabelValues(nodeIDStr).Set(rate)
	case internalpb.RateType_DMLInsert:
		LimiterRate.WithLabelValues(nodeIDStr, InsertLabel).Set(rate)
	case internalpb.RateType_DMLDelete:
		LimiterRate.WithLabelValues(nodeIDStr, DeleteLabel).Set(rate)
	case internalpb.RateType_DQLSearch:
		LimiterRate.WithLabelValues(nodeIDStr, SearchLabel).Set(rate)
	case internalpb.RateType_DQLQuery:
		LimiterRate.WithLabelValues(nodeIDStr, QueryLabel).Set(rate)
	}
}
