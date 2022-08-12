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

package metricsinfo

import (
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type QuotaMetrics struct {
	NodeID typeutil.UniqueID
	Rms    []RateMetric
	Mm     MemMetric
	// TODO: add more quota metrics
}

type RateMetric struct {
	Rt         internalpb.RateType
	ThroughPut float64
	// TODO: add more rate metrics
}

type MemMetric struct {
	UsedMem  uint64
	TotalMem uint64
	Buffers  map[string]uint64 // growing segments, bloom filter, etc..., TODO: add more
}
