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

package pkindex

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/tests/integration"
)

func (s *WriteDedupSuite) TestOverwriteAcrossShards() {
	ctx := s.Cluster.GetContext()
	name := "PKIndexShards" + funcutil.GenRandomStr()
	s.createAndLoadCollectionWithShards(ctx, name, 2)
	s.insert(ctx, name, 0, 1000, 1)
	s.insert(ctx, name, 0, 1000, 2)
	s.Equal(int64(1000), s.queryCount(ctx, name))
	s.Equal(map[int64]int64{0: 2, 500: 2, 999: 2}, s.queryTags(ctx, name, []int64{0, 500, 999}))
	s.flush(ctx, name)
	s.Len(s.waitForManifest(ctx, name), 2)
	s.Equal(int64(1000), s.queryCount(ctx, name))
}

func (s *WriteDedupSuite) TestOverwriteAcrossPartitions() {
	ctx := s.Cluster.GetContext()
	name := "PKIndexPartitions" + funcutil.GenRandomStr()
	s.createAndLoadCollectionWithShards(ctx, name, 2)
	status, err := s.Cluster.MilvusClient.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{CollectionName: name, PartitionName: "second"})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(status), status.GetReason())
	status, err = s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: name})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(status), status.GetReason())
	s.WaitForLoad(ctx, name)
	s.insert(ctx, name, 0, 100, 1)
	s.insertIntoPartition(ctx, name, "second", 0, 100, 2)
	s.Equal(int64(100), s.queryCount(ctx, name), "a replacement in another partition must remove the previous row")
	s.Equal(map[int64]int64{0: 2, 50: 2, 99: 2}, s.queryTags(ctx, name, []int64{0, 50, 99}))
	s.flush(ctx, name)
	s.Equal(int64(100), s.queryCount(ctx, name))
}

func (s *WriteDedupSuite) TestConcurrentOverwriteAcrossShards() {
	ctx := s.Cluster.GetContext()
	name := "PKIndexConcurrent" + funcutil.GenRandomStr()
	s.createAndLoadCollectionWithShards(ctx, name, 2)
	results := make(chan struct {
		response *milvuspb.MutationResult
		err      error
	}, 4)
	var wg sync.WaitGroup
	for writer := int64(1); writer <= 4; writer++ {
		wg.Add(1)
		go func(tag int64) {
			defer wg.Done()
			response, err := s.Cluster.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{CollectionName: name, NumRows: 1000, HashKeys: integration.GenerateHashKeys(1000), FieldsData: []*schemapb.FieldData{
				integration.NewInt64FieldDataWithStart(dedupPKField, 1000, 0),
				integration.NewFloatVectorFieldData(integration.FloatVecField, 1000, dedupDim),
				integration.NewInt64SameFieldData(dedupTagField, 1000, tag),
			}})
			results <- struct {
				response *milvuspb.MutationResult
				err      error
			}{response, err}
		}(writer)
	}
	wg.Wait()
	close(results)
	for result := range results {
		s.Require().NoError(result.err)
		s.Require().True(merr.Ok(result.response.GetStatus()), result.response.GetStatus().GetReason())
		s.Require().EqualValues(1000, result.response.GetInsertCnt())
	}
	s.Equal(int64(1000), s.queryCount(ctx, name))
	s.flush(ctx, name)
	s.Equal(int64(1000), s.queryCount(ctx, name))
}

// SmallHandoverSuite also runs the write regressions with a small handover threshold.
type SmallHandoverSuite struct{ WriteDedupSuite }

func (s *SmallHandoverSuite) SetupSuite() {
	s.WithMilvusConfig("streaming.pkindex.handover.activeBytes", "16k")
	s.WriteDedupSuite.SetupSuite()
}

func TestSmallHandover(t *testing.T) { suite.Run(t, new(SmallHandoverSuite)) }

func (s *SmallHandoverSuite) TestAutomaticHandoverWithoutFlush() {
	ctx := s.Cluster.GetContext()
	name := "PKIndexAutomatic" + funcutil.GenRandomStr()
	s.createAndLoadCollection(ctx, name)
	s.insert(ctx, name, 0, 2000, 1)
	s.waitForManifest(ctx, name)
	s.insert(ctx, name, 0, 1000, 2)
	s.Equal(int64(2000), s.queryCount(ctx, name))
	s.Equal(map[int64]int64{0: 2, 999: 2, 1000: 1}, s.queryTags(ctx, name, []int64{0, 999, 1000}))
}

func (s *WriteDedupSuite) TestDedupSurvivesForcedStreamingNodeStop() {
	s.checkDedupAfterStreamingNodeRestart(true)
}
