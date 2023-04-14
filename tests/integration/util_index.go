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

package integration

import (
	"fmt"
	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/indexparamcheck"
	"strconv"
)

const (
	IndexRaftIvfFlat     = indexparamcheck.IndexRaftIvfFlat
	IndexRaftIvfPQ       = indexparamcheck.IndexRaftIvfPQ
	IndexFaissIDMap      = indexparamcheck.IndexFaissIDMap
	IndexFaissIvfFlat    = indexparamcheck.IndexFaissIvfFlat
	IndexFaissIvfPQ      = indexparamcheck.IndexFaissIvfPQ
	IndexFaissIvfSQ8     = indexparamcheck.IndexFaissIvfSQ8
	IndexFaissBinIDMap   = indexparamcheck.IndexFaissBinIDMap
	IndexFaissBinIvfFlat = indexparamcheck.IndexFaissBinIvfFlat
	IndexHNSW            = indexparamcheck.IndexHNSW
	IndexDISKANN         = indexparamcheck.IndexDISKANN

	L2 = indexparamcheck.L2
	IP = indexparamcheck.IP

	nlist          = "100"
	m              = "4"
	nbits          = "8"
	nprobe         = "8"
	efConstruction = "200"
	ef             = "200"
)

func constructIndexParam(dim int, indexType string, metricType string) []*commonpb.KeyValuePair {
	params := []*commonpb.KeyValuePair{
		{
			Key:   common.DimKey,
			Value: strconv.Itoa(dim),
		},
		{
			Key:   common.MetricTypeKey,
			Value: metricType,
		},
		{
			Key:   common.IndexTypeKey,
			Value: indexType,
		},
	}
	switch indexType {
	case IndexFaissIDMap:
	// no index param is required
	case IndexFaissIvfFlat, IndexFaissIvfSQ8:
		params = append(params, &commonpb.KeyValuePair{
			Key:   "nlist",
			Value: nlist,
		})
	case IndexFaissIvfPQ:
		params = append(params, &commonpb.KeyValuePair{
			Key:   "nlist",
			Value: nlist,
		})
		params = append(params, &commonpb.KeyValuePair{
			Key:   "m",
			Value: m,
		})
		params = append(params, &commonpb.KeyValuePair{
			Key:   "nbits",
			Value: nbits,
		})
	case IndexHNSW:
		params = append(params, &commonpb.KeyValuePair{
			Key:   "m",
			Value: m,
		})
		params = append(params, &commonpb.KeyValuePair{
			Key:   "efConstruction",
			Value: efConstruction,
		})
	default:
		panic(fmt.Sprintf("unimplemented index param for %s, please help to improve it", indexType))
	}
	return params
}
