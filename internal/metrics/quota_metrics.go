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
	"fmt"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

var (
	InsertRate = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "dml_insert_rate",
			Help:      "",
		}, []string{nodeIDLabelName})

	DeleteRate = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "dml_delete_rate",
			Help:      "",
		}, []string{nodeIDLabelName})

	SearchRate = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "dml_search_rate",
			Help:      "",
		}, []string{nodeIDLabelName})

	QueryRate = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.ProxyRole,
			Name:      "dml_query_rate",
			Help:      "",
		}, []string{nodeIDLabelName})
)

//RegisterQuota registers Quota metrics
func RegisterQuota(registry *prometheus.Registry) {
	registry.MustRegister(InsertRate)
	registry.MustRegister(DeleteRate)
	registry.MustRegister(SearchRate)
	registry.MustRegister(QueryRate)
}

func toMegabytes(rate float64) float64 {
	return rate / 1024.0 / 1024.0
}

func SetRateGaugeByRateType(rateType commonpb.RateType, nodeID int64, rate float64) {
	nodeIDStr := strconv.FormatInt(nodeID, 10)
	rate = toMegabytes(rate)
	fmt.Printf("set rates for %s, rate = %f\n", rateType.String(), rate)
	switch rateType {
	case commonpb.RateType_DMLInsert:
		InsertRate.WithLabelValues(nodeIDStr).Set(rate)
	case commonpb.RateType_DMLDelete:
		DeleteRate.WithLabelValues(nodeIDStr).Set(rate)
	case commonpb.RateType_DQLSearch:
		SearchRate.WithLabelValues(nodeIDStr).Set(rate)
	case commonpb.RateType_DQLQuery:
		QueryRate.WithLabelValues(nodeIDStr).Set(rate)
	}
}
