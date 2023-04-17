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
	"context"
	"fmt"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/distance"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type TestGetVectorSuite struct {
	suite.Suite

	ctx     context.Context
	cancel  context.CancelFunc
	cluster *MiniCluster

	// test params
	nq         int
	topK       int
	indexType  string
	metricType string
	pkType     schemapb.DataType
}

func (suite *TestGetVectorSuite) SetupTest() {
	suite.ctx, suite.cancel = context.WithTimeout(context.Background(), time.Second*600)

	var err error
	suite.cluster, err = StartMiniCluster(suite.ctx)
	suite.Require().NoError(err)
	err = suite.cluster.Start()
	suite.Require().NoError(err)
}

func (suite *TestGetVectorSuite) run() {
	collection := fmt.Sprintf("TestGetVector_%d_%d_%s_%s_%s",
		suite.nq, suite.topK, suite.indexType, suite.metricType, funcutil.GenRandomStr())

	const (
		NB  = 10000
		dim = 128
	)

	pkFieldName := "pkField"
	pk := &schemapb.FieldSchema{
		FieldID:      100,
		Name:         pkFieldName,
		IsPrimaryKey: true,
		Description:  "",
		DataType:     suite.pkType,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "max_length",
				Value: "100",
			},
		},
		IndexParams: nil,
		AutoID:      false,
	}
	fVec := &schemapb.FieldSchema{
		FieldID:      101,
		Name:         floatVecField,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: fmt.Sprintf("%d", dim),
			},
		},
		IndexParams: nil,
	}
	schema := constructSchema(collection, dim, false, pk, fVec)
	marshaledSchema, err := proto.Marshal(schema)
	suite.Require().NoError(err)

	createCollectionStatus, err := suite.cluster.proxy.CreateCollection(suite.ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: collection,
		Schema:         marshaledSchema,
		ShardsNum:      2,
	})
	suite.Require().NoError(err)
	suite.Require().Equal(createCollectionStatus.GetErrorCode(), commonpb.ErrorCode_Success)

	fieldsData := make([]*schemapb.FieldData, 0)
	if suite.pkType == schemapb.DataType_Int64 {
		fieldsData = append(fieldsData, newInt64FieldData(pkFieldName, NB))
	} else {
		fieldsData = append(fieldsData, newStringFieldData(pkFieldName, NB))
	}
	vecData := newFloatVectorFieldData(floatVecField, NB, dim)
	fieldsData = append(fieldsData, vecData)
	hashKeys := generateHashKeys(NB)
	_, err = suite.cluster.proxy.Insert(suite.ctx, &milvuspb.InsertRequest{
		CollectionName: collection,
		FieldsData:     fieldsData,
		HashKeys:       hashKeys,
		NumRows:        uint32(NB),
	})
	suite.Require().NoError(err)
	suite.Require().Equal(createCollectionStatus.GetErrorCode(), commonpb.ErrorCode_Success)

	// flush
	flushResp, err := suite.cluster.proxy.Flush(suite.ctx, &milvuspb.FlushRequest{
		CollectionNames: []string{collection},
	})
	suite.Require().NoError(err)
	segmentIDs, has := flushResp.GetCollSegIDs()[collection]
	ids := segmentIDs.GetData()
	suite.Require().NotEmpty(segmentIDs)
	suite.Require().True(has)

	segments, err := suite.cluster.metaWatcher.ShowSegments()
	suite.Require().NoError(err)
	suite.Require().NotEmpty(segments)

	waitingForFlush(suite.ctx, suite.cluster, ids)

	// create index
	_, err = suite.cluster.proxy.CreateIndex(suite.ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collection,
		FieldName:      floatVecField,
		IndexName:      "_default",
		ExtraParams:    constructIndexParam(dim, suite.indexType, suite.metricType),
	})
	suite.Require().NoError(err)
	suite.Require().Equal(createCollectionStatus.GetErrorCode(), commonpb.ErrorCode_Success)

	// load
	_, err = suite.cluster.proxy.LoadCollection(suite.ctx, &milvuspb.LoadCollectionRequest{
		CollectionName: collection,
	})
	suite.Require().NoError(err)
	suite.Require().Equal(createCollectionStatus.GetErrorCode(), commonpb.ErrorCode_Success)
	waitingForLoad(suite.ctx, suite.cluster, collection)

	// search
	nq := suite.nq
	topk := suite.topK

	outputFields := []string{floatVecField}
	params := getSearchParams(suite.indexType, suite.metricType)
	searchReq := constructSearchRequest("", collection, "",
		floatVecField, outputFields, suite.metricType, params, nq, dim, topk, -1)

	searchResp, err := suite.cluster.proxy.Search(suite.ctx, searchReq)
	suite.Require().NoError(err)
	suite.Require().Equal(searchResp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)

	result := searchResp.GetResults()
	if suite.pkType == schemapb.DataType_Int64 {
		suite.Require().Len(result.GetIds().GetIntId().GetData(), nq*topk)
	} else {
		suite.Require().Len(result.GetIds().GetStrId().GetData(), nq*topk)
	}
	suite.Require().Len(result.GetScores(), nq*topk)
	suite.Require().GreaterOrEqual(len(result.GetFieldsData()), 1)
	var vecFieldIndex = -1
	for i, fieldData := range result.GetFieldsData() {
		if typeutil.IsVectorType(fieldData.GetType()) {
			vecFieldIndex = i
			break
		}
	}
	suite.Require().Len(result.GetFieldsData()[vecFieldIndex].GetVectors().GetFloatVector().GetData(), nq*topk*dim)
	suite.Require().EqualValues(nq, result.GetNumQueries())
	suite.Require().EqualValues(topk, result.GetTopK())

	// check output vectors
	rawData := vecData.GetVectors().GetFloatVector().GetData()
	resData := result.GetFieldsData()[vecFieldIndex].GetVectors().GetFloatVector().GetData()
	for i, id := range result.GetIds().GetIntId().GetData() {
		expect := rawData[int(id)*dim : (int(id)+1)*dim]
		actual := resData[i*dim : (i+1)*dim]
		suite.Require().ElementsMatch(expect, actual)
	}

	status, err := suite.cluster.proxy.DropCollection(suite.ctx, &milvuspb.DropCollectionRequest{
		CollectionName: collection,
	})
	suite.Require().NoError(err)
	suite.Require().Equal(status.GetErrorCode(), commonpb.ErrorCode_Success)
}

func (suite *TestGetVectorSuite) TestGetVector_FLAT() {
	suite.nq = 10
	suite.topK = 10
	suite.indexType = IndexFaissIDMap
	suite.metricType = distance.L2
	suite.pkType = schemapb.DataType_Int64
	suite.run()
}

func (suite *TestGetVectorSuite) TestGetVector_IVF_FLAT() {
	suite.nq = 10
	suite.topK = 10
	suite.indexType = IndexFaissIvfFlat
	suite.metricType = distance.L2
	suite.pkType = schemapb.DataType_Int64
	suite.run()
}

func (suite *TestGetVectorSuite) TestGetVector_IVF_PQ() {
	suite.nq = 10
	suite.topK = 10
	suite.indexType = IndexFaissIvfPQ
	suite.metricType = distance.L2
	suite.pkType = schemapb.DataType_Int64
	suite.run()
}

func (suite *TestGetVectorSuite) TestGetVector_IVF_SQ8() {
	suite.nq = 10
	suite.topK = 10
	suite.indexType = IndexFaissIvfSQ8
	suite.metricType = distance.L2
	suite.pkType = schemapb.DataType_Int64
	suite.run()
}

func (suite *TestGetVectorSuite) TestGetVector_HNSW() {
	suite.nq = 10
	suite.topK = 10
	suite.indexType = IndexHNSW
	suite.metricType = distance.L2
	suite.pkType = schemapb.DataType_Int64
	suite.run()
}

func (suite *TestGetVectorSuite) TestGetVector_IP() {
	suite.nq = 10
	suite.topK = 10
	suite.indexType = IndexHNSW
	suite.metricType = distance.IP
	suite.pkType = schemapb.DataType_Int64
	suite.run()
}

func (suite *TestGetVectorSuite) TestGetVector_StringPK() {
	suite.nq = 10
	suite.topK = 10
	suite.indexType = IndexHNSW
	suite.metricType = distance.L2
	suite.pkType = schemapb.DataType_VarChar
	suite.run()
}

//func (suite *TestGetVectorSuite) TestGetVector_DISKANN() {
//	suite.nq = 10
//	suite.topK = 10
//	suite.indexType = IndexDISKANN
//	suite.metricType = distance.L2
//	suite.run()
//}

func (suite *TestGetVectorSuite) TearDownTest() {
	//err := suite.cluster.Stop()
	//suite.Require().NoError(err)
	//suite.cancel()
}

func TestGetVector(t *testing.T) {
	suite.Run(t, new(TestGetVectorSuite))
}
