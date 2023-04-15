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
	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"github.com/stretchr/testify/suite"
	"testing"
	"time"
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
}

func (suite *TestGetVectorSuite) SetupTest() {
	suite.ctx, suite.cancel = context.WithTimeout(context.Background(), time.Second*180)

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

	schema := constructSchema(collection, dim, false)
	marshaledSchema, err := proto.Marshal(schema)
	suite.Require().NoError(err)

	createCollectionStatus, err := suite.cluster.proxy.CreateCollection(suite.ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: collection,
		Schema:         marshaledSchema,
		ShardsNum:      2,
	})
	suite.Require().NoError(err)
	suite.Require().Equal(createCollectionStatus.GetErrorCode(), commonpb.ErrorCode_Success)

	pkData := newInt64FieldData(int64Field, NB)
	vecData := newFloatVectorFieldData(floatVecField, NB, dim)
	hashKeys := generateHashKeys(NB)
	_, err = suite.cluster.proxy.Insert(suite.ctx, &milvuspb.InsertRequest{
		CollectionName: collection,
		FieldsData:     []*schemapb.FieldData{pkData, vecData},
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
		floatVecField, outputFields, nq, dim, params, topk, -1)

	searchResp, err := suite.cluster.proxy.Search(suite.ctx, searchReq)
	suite.Require().NoError(err)
	suite.Require().Equal(searchResp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)

	result := searchResp.GetResults()
	suite.Require().Len(result.GetIds().GetIntId().GetData(), nq*topk)
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
	suite.metricType = L2
	suite.run()
}

func (suite *TestGetVectorSuite) TestGetVector_IVF_FLAT() {
	suite.nq = 10
	suite.topK = 10
	suite.indexType = IndexFaissIvfFlat
	suite.metricType = L2
	suite.run()
}

func (suite *TestGetVectorSuite) TestGetVector_IVF_PQ() {
	suite.nq = 10
	suite.topK = 10
	suite.indexType = IndexFaissIvfPQ
	suite.metricType = L2
	suite.run()
}

func (suite *TestGetVectorSuite) TestGetVector_IVF_SQ8() {
	suite.nq = 10
	suite.topK = 10
	suite.indexType = IndexFaissIvfSQ8
	suite.metricType = L2
	suite.run()
}

func (suite *TestGetVectorSuite) TestGetVector_HNSW() {
	suite.nq = 10
	suite.topK = 10
	suite.indexType = IndexHNSW
	suite.metricType = L2
	suite.run()
}

func (suite *TestGetVectorSuite) TearDownTest() {
	suite.cancel()
	//err = suite.cluster.Stop()
	//suite.Require().NoError(err) // TODO: cluster.Stop hangs, fix it
}

func TestGetVector(t *testing.T) {
	suite.Run(t, new(TestGetVectorSuite))
}
