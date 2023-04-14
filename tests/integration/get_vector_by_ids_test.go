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
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/distance"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/stretchr/testify/suite"
	"strconv"
	"testing"
	"time"
)

type TestGetVectorSuite struct {
	suite.Suite

	ctx     context.Context
	cancel  context.CancelFunc
	cluster *MiniCluster

	// schema and meta
	dim         int
	collection  string
	pkField     string
	vectorField string

	// data
	pk      *schemapb.FieldData
	rawData *schemapb.FieldData
}

func (suite *TestGetVectorSuite) SetupTest() {
	suite.ctx, suite.cancel = context.WithTimeout(context.Background(), time.Second*180)

	var err error
	suite.cluster, err = StartMiniCluster(suite.ctx)
	suite.Require().NoError(err)
	err = suite.cluster.Start()
	suite.Require().NoError(err)

	suite.collection = "TestGetVector" + funcutil.GenRandomStr()
	suite.pkField = "int64"
	suite.vectorField = "fvec"
	suite.dim = 128
	rowNum := 3000

	constructCollectionSchema := func() *schemapb.CollectionSchema {
		pk := &schemapb.FieldSchema{
			FieldID:      0,
			Name:         suite.pkField,
			IsPrimaryKey: true,
			Description:  "",
			DataType:     schemapb.DataType_Int64,
			TypeParams:   nil,
			IndexParams:  nil,
			AutoID:       false,
		}
		fVec := &schemapb.FieldSchema{
			FieldID:      0,
			Name:         suite.vectorField,
			IsPrimaryKey: false,
			Description:  "",
			DataType:     schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   "dim",
					Value: strconv.Itoa(suite.dim),
				},
			},
			IndexParams: nil,
			AutoID:      false,
		}
		return &schemapb.CollectionSchema{
			Name:        suite.collection,
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				pk,
				fVec,
			},
		}
	}
	schema := constructCollectionSchema()
	marshaledSchema, err := proto.Marshal(schema)
	suite.Require().NoError(err)

	createCollectionStatus, err := suite.cluster.proxy.CreateCollection(suite.ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: suite.collection,
		Schema:         marshaledSchema,
		ShardsNum:      2,
	})
	suite.Require().NoError(err)
	suite.Require().Equal(createCollectionStatus.GetErrorCode(), commonpb.ErrorCode_Success)

	suite.pk = newPkFieldData(suite.pkField, rowNum)
	suite.rawData = newFloatVectorFieldData(suite.vectorField, rowNum, suite.dim)
	hashKeys := generateHashKeys(rowNum)
	_, err = suite.cluster.proxy.Insert(suite.ctx, &milvuspb.InsertRequest{
		CollectionName: suite.collection,
		FieldsData:     []*schemapb.FieldData{suite.pk, suite.rawData},
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	suite.Require().NoError(err)
	suite.Require().Equal(createCollectionStatus.GetErrorCode(), commonpb.ErrorCode_Success)

	// flush
	flushResp, err := suite.cluster.proxy.Flush(suite.ctx, &milvuspb.FlushRequest{
		CollectionNames: []string{suite.collection},
	})
	suite.Require().NoError(err)
	segmentIDs, has := flushResp.GetCollSegIDs()[suite.collection]
	ids := segmentIDs.GetData()
	suite.Require().NotEmpty(segmentIDs)

	segments, err := suite.cluster.metaWatcher.ShowSegments()
	suite.Require().NoError(err)
	suite.Require().NotEmpty(segments)

	if has && len(ids) > 0 {
		flushed := func() bool {
			resp, err := suite.cluster.proxy.GetFlushState(suite.ctx, &milvuspb.GetFlushStateRequest{
				SegmentIDs: ids,
			})
			if err != nil {
				return false
			}
			return resp.GetFlushed()
		}
		for !flushed() {
			select {
			case <-suite.ctx.Done():
				panic("flush timeout")
			default:
				time.Sleep(500 * time.Millisecond)
			}
		}
	}

	// create index
	_, err = suite.cluster.proxy.CreateIndex(suite.ctx, &milvuspb.CreateIndexRequest{
		CollectionName: suite.collection,
		FieldName:      suite.vectorField,
		IndexName:      "_default",
		ExtraParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: strconv.Itoa(suite.dim),
			},
			{
				Key:   common.MetricTypeKey,
				Value: distance.L2,
			},
			{
				Key:   "index_type",
				Value: "IVF_FLAT",
			},
			{
				Key:   "nlist",
				Value: strconv.Itoa(10),
			},
		},
	})
	suite.Require().NoError(err)
	suite.Require().Equal(createCollectionStatus.GetErrorCode(), commonpb.ErrorCode_Success)

	// load
	_, err = suite.cluster.proxy.LoadCollection(suite.ctx, &milvuspb.LoadCollectionRequest{
		CollectionName: suite.collection,
	})
	suite.Require().NoError(err)
	suite.Require().Equal(createCollectionStatus.GetErrorCode(), commonpb.ErrorCode_Success)
	getLoadingProgress := func() *milvuspb.GetLoadingProgressResponse {
		loadProgress, err := suite.cluster.proxy.GetLoadingProgress(suite.ctx, &milvuspb.GetLoadingProgressRequest{
			CollectionName: suite.collection,
		})
		if err != nil {
			panic("GetLoadingProgress fail")
		}
		return loadProgress
	}
	for getLoadingProgress().GetProgress() != 100 {
		select {
		case <-suite.ctx.Done():
			panic("load timeout")
		default:
			time.Sleep(500 * time.Millisecond)
		}
	}
}

func (suite *TestGetVectorSuite) TestGetVector() {
	// search
	expr := fmt.Sprintf("%s > 0", "int64")
	nq := 10
	topk := 10
	roundDecimal := -1
	nprobe := 10

	outputFields := []string{suite.vectorField}
	searchReq := constructSearchRequest("", suite.collection, expr,
		suite.vectorField, outputFields, nq, suite.dim, nprobe, topk, roundDecimal)

	searchResp, err := suite.cluster.proxy.Search(suite.ctx, searchReq)
	suite.Require().NoError(err)
	suite.Require().Equal(searchResp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)

	result := searchResp.GetResults()
	suite.CheckSearchResult(result, nq, topk)
	suite.Require().Len(result.GetIds().GetIntId().GetData(), nq*topk)
	suite.Require().Len(result.GetScores(), nq*topk)
	suite.Require().EqualValues(nq, result.GetNumQueries())
	suite.Require().EqualValues(topk, result.GetTopK())
}

func (suite *TestGetVectorSuite) CheckSearchResult(result *schemapb.SearchResultData, nq, topk int) {
	suite.Require().Len(result.GetIds().GetIntId().GetData(), nq*topk)
	suite.Require().Len(result.GetScores(), nq*topk)
	suite.Require().Len(result.GetFieldsData(), 1)
	suite.Require().Len(result.GetFieldsData()[0].GetVectors().GetFloatVector().GetData(), nq*topk*suite.dim)
	suite.Require().EqualValues(nq, result.GetNumQueries())
	suite.Require().EqualValues(topk, result.GetTopK())

	// check output vectors
	dim := suite.dim
	rawData := suite.rawData.GetVectors().GetFloatVector().GetData()
	resData := result.GetFieldsData()[0].GetVectors().GetFloatVector().GetData()
	for i, id := range result.GetIds().GetIntId().GetData() {
		expect := rawData[int(id)*dim : (int(id)+1)*dim]
		actual := resData[i*dim : (i+1)*dim]
		suite.Require().ElementsMatch(expect, actual)
	}
}

func (suite *TestGetVectorSuite) TearDownTest() {
	status, err := suite.cluster.proxy.DropCollection(suite.ctx, &milvuspb.DropCollectionRequest{
		CollectionName: suite.collection,
	})
	suite.Require().NoError(err)
	suite.Require().Equal(status.GetErrorCode(), commonpb.ErrorCode_Success)

	suite.cancel()
	//err = suite.cluster.Stop()
	//suite.Require().NoError(err)
}

func TestGetVector(t *testing.T) {
	suite.Run(t, new(TestGetVectorSuite))
}
