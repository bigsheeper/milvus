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

package commit_timestamp

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
	"github.com/milvus-io/milvus/tests/integration"
)

const dim = 128

func TestCommitTimestampSuite(t *testing.T) {
	suite.Run(t, new(CommitTimestampSuite))
}

type CommitTimestampSuite struct {
	integration.MiniClusterSuite
}

// ─── Helper: modify segment metadata in etcd ──────────────────────────────

// setCommitTimestamp reads a segment's metadata from etcd, sets its
// CommitTimestamp to commitTs, then writes it back. This simulates an
// import segment without going through the actual import pipeline.
func (s *CommitTimestampSuite) setCommitTimestamp(
	collectionID int64,
	commitTs uint64,
) []int64 {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	prefix := path.Join(s.Cluster.RootPath(), "meta/datacoord-meta/s",
		fmt.Sprintf("%d", collectionID)) + "/"

	resp, err := s.Cluster.EtcdCli.Get(ctx, prefix, clientv3.WithPrefix())
	s.Require().NoError(err, "failed to list segments from etcd")
	s.Require().NotEmpty(resp.Kvs, "no segments found in etcd")

	var segmentIDs []int64
	for _, kv := range resp.Kvs {
		var seg datapb.SegmentInfo
		err := proto.Unmarshal(kv.Value, &seg)
		if err != nil {
			continue
		}
		// Only modify flushed L1 segments with data
		if seg.GetState() != commonpb.SegmentState_Flushed && seg.GetState() != commonpb.SegmentState_Flushing {
			continue
		}
		if len(seg.GetBinlogs()) == 0 {
			continue
		}

		log.Info("setCommitTimestamp: modifying segment",
			zap.Int64("segmentID", seg.GetID()),
			zap.Uint64("commitTs", commitTs))

		seg.CommitTimestamp = commitTs

		data, err := proto.Marshal(&seg)
		s.Require().NoError(err)
		_, err = s.Cluster.EtcdCli.Put(ctx, string(kv.Key), string(data))
		s.Require().NoError(err)
		segmentIDs = append(segmentIDs, seg.GetID())
	}
	s.Require().NotEmpty(segmentIDs, "no flushed segments were modified")
	return segmentIDs
}

// getSegmentCommitTimestamps reads CommitTimestamp for all flushed segments.
func (s *CommitTimestampSuite) getSegmentCommitTimestamps(collectionID int64) map[int64]uint64 {
	segments, err := s.Cluster.ShowSegments(s.collectionName(collectionID))
	s.Require().NoError(err)
	result := make(map[int64]uint64)
	for _, seg := range segments {
		if seg.GetState() == commonpb.SegmentState_Flushed {
			result[seg.GetID()] = seg.GetCommitTimestamp()
		}
	}
	return result
}

func (s *CommitTimestampSuite) collectionName(collectionID int64) string {
	// We track collection name directly in tests, this is a placeholder
	return ""
}

// createCollectionAndInsert creates a collection, inserts rows, flushes, builds index, and loads.
// Returns (collectionName, collectionID).
func (s *CommitTimestampSuite) createCollectionAndInsert(
	ctx context.Context,
	rowNum int,
) (string, int64) {
	collName := "CommitTs_" + funcutil.RandomString(6)

	schema := integration.ConstructSchema(collName, dim, false)
	marshaledSchema, err := proto.Marshal(schema)
	s.Require().NoError(err)

	createResp, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: collName,
		Schema:         marshaledSchema,
		ShardsNum:      1,
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(createResp))

	// Insert data
	pkColumn := integration.NewInt64FieldDataWithStart(integration.Int64Field, rowNum, 1)
	vecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, dim)
	insertResp, err := s.Cluster.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		CollectionName: collName,
		FieldsData:     []*schemapb.FieldData{pkColumn, vecColumn},
		NumRows:        uint32(rowNum),
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(insertResp.GetStatus()))

	// Flush
	flushResp, err := s.Cluster.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{
		CollectionNames: []string{collName},
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(flushResp.GetStatus()))
	segIDs := flushResp.GetCollSegIDs()[collName].GetData()
	flushTs := flushResp.GetCollFlushTs()[collName]
	s.WaitForFlush(ctx, segIDs, flushTs, "", collName)

	// Get collection ID
	showResp, err := s.Cluster.MilvusClient.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{
		CollectionNames: []string{collName},
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(showResp.GetStatus()))
	collectionID := showResp.GetCollectionIds()[0]

	return collName, collectionID
}

// buildIndexAndLoad creates an index and loads the collection.
func (s *CommitTimestampSuite) buildIndexAndLoad(ctx context.Context, collName string) {
	indexResp, err := s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collName,
		FieldName:      integration.FloatVecField,
		IndexName:      "vec_idx",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexFaissIvfFlat, metric.L2),
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(indexResp))
	s.WaitForIndexBuilt(ctx, collName, integration.FloatVecField)

	loadResp, err := s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		CollectionName: collName,
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(loadResp))
	s.WaitForLoad(ctx, collName)
}

func (s *CommitTimestampSuite) queryCount(ctx context.Context, collName string) int64 {
	queryResp, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName:     collName,
		Expr:               "",
		OutputFields:       []string{"count(*)"},
		ConsistencyLevel:   commonpb.ConsistencyLevel_Strong,
		QueryParams: []*commonpb.KeyValuePair{
			{Key: "reduce_stop_for_best", Value: "false"},
		},
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(queryResp.GetStatus()), queryResp.GetStatus().GetReason())

	for _, field := range queryResp.GetFieldsData() {
		if field.GetFieldName() == "count(*)" {
			return field.GetScalars().GetLongData().GetData()[0]
		}
	}
	s.Fail("count(*) field not found in query response")
	return 0
}

// ─── S4: MVCC visibility with commit_ts ──────────────────────────────────

func (s *CommitTimestampSuite) TestS4_MVCC_Visibility() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	const rowNum = 100

	collName, collectionID := s.createCollectionAndInsert(ctx, rowNum)

	// Record a "before" timestamp
	tBefore := tsoutil.ComposeTSByTime(time.Now().Add(-1*time.Second), 0)

	// Set commit_ts = a future TSO to test MVCC
	tCommit := tsoutil.ComposeTSByTime(time.Now().Add(10*time.Second), 0)
	s.setCommitTimestamp(collectionID, tCommit)

	// Build index and load (triggers C++ timestamp overwrite)
	s.buildIndexAndLoad(ctx, collName)

	// Query with guarantee_timestamp < commit_ts → should return 0 rows
	queryResp, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName:     collName,
		Expr:               "",
		OutputFields:       []string{"count(*)"},
		GuaranteeTimestamp: tBefore,
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(queryResp.GetStatus()), queryResp.GetStatus().GetReason())
	for _, field := range queryResp.GetFieldsData() {
		if field.GetFieldName() == "count(*)" {
			count := field.GetScalars().GetLongData().GetData()[0]
			s.Equal(int64(0), count,
				"MVCC: query before commit_ts should return 0 rows")
		}
	}

	// Query with Strong consistency → should return all rows
	count := s.queryCount(ctx, collName)
	s.Equal(int64(rowNum), count,
		"MVCC: Strong consistency query should return all rows")
}

// ─── S5: Delete on import segment ──────────────────────────────────────

func (s *CommitTimestampSuite) TestS5_Delete_OnImportSegment() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	const rowNum = 100
	const deleteCount = 10

	collName, collectionID := s.createCollectionAndInsert(ctx, rowNum)

	commitTs := tsoutil.ComposeTSByTime(time.Now(), 0)
	s.setCommitTimestamp(collectionID, commitTs)

	s.buildIndexAndLoad(ctx, collName)

	// Verify all rows present before delete
	count := s.queryCount(ctx, collName)
	s.Equal(int64(rowNum), count, "should have all rows before delete")

	// Delete first 10 PKs
	pks := make([]string, deleteCount)
	for i := 0; i < deleteCount; i++ {
		pks[i] = strconv.FormatInt(int64(i+1), 10)
	}
	expr := fmt.Sprintf("%s in [%s]", integration.Int64Field, strings.Join(pks, ","))
	deleteResp, err := s.Cluster.MilvusClient.Delete(ctx, &milvuspb.DeleteRequest{
		CollectionName: collName,
		Expr:           expr,
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(deleteResp.GetStatus()))

	// Wait a bit for delete propagation
	time.Sleep(2 * time.Second)

	// Query with strong consistency — should return rowNum - deleteCount
	count = s.queryCount(ctx, collName)
	s.Equal(int64(rowNum-deleteCount), count,
		"after deleting %d rows from import segment, should have %d rows", deleteCount, rowNum-deleteCount)
}

// ─── S6: Upsert on import segment ──────────────────────────────────────

func (s *CommitTimestampSuite) TestS6_Upsert_OnImportSegment() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	const rowNum = 100
	const upsertCount = 20

	collName, collectionID := s.createCollectionAndInsert(ctx, rowNum)

	commitTs := tsoutil.ComposeTSByTime(time.Now(), 0)
	s.setCommitTimestamp(collectionID, commitTs)

	s.buildIndexAndLoad(ctx, collName)

	// Upsert first 20 rows with new vector values
	pkColumn := integration.NewInt64FieldDataWithStart(integration.Int64Field, upsertCount, 1)
	vecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, upsertCount, dim)
	upsertResp, err := s.Cluster.MilvusClient.Upsert(ctx, &milvuspb.UpsertRequest{
		CollectionName: collName,
		FieldsData:     []*schemapb.FieldData{pkColumn, vecColumn},
		NumRows:        uint32(upsertCount),
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(upsertResp.GetStatus()))

	// Wait for upsert to propagate
	time.Sleep(2 * time.Second)

	// Total row count should still be 100
	count := s.queryCount(ctx, collName)
	s.Equal(int64(rowNum), count,
		"after upsert, total row count should remain %d", rowNum)
}

// ─── S7: Delete before commit_ts should not take effect ──────────────────

func (s *CommitTimestampSuite) TestS7_Delete_BeforeCommitTs() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	const rowNum = 100
	const deleteCount = 10

	collName, collectionID := s.createCollectionAndInsert(ctx, rowNum)

	// Set commit_ts to a future time — deletes happening "now" are before commit_ts
	commitTs := tsoutil.ComposeTSByTime(time.Now().Add(10*time.Second), 0)
	s.setCommitTimestamp(collectionID, commitTs)

	s.buildIndexAndLoad(ctx, collName)

	// Delete first 10 PKs — these deletes have ts < commit_ts
	pks := make([]string, deleteCount)
	for i := 0; i < deleteCount; i++ {
		pks[i] = strconv.FormatInt(int64(i+1), 10)
	}
	expr := fmt.Sprintf("%s in [%s]", integration.Int64Field, strings.Join(pks, ","))
	deleteResp, err := s.Cluster.MilvusClient.Delete(ctx, &milvuspb.DeleteRequest{
		CollectionName: collName,
		Expr:           expr,
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(deleteResp.GetStatus()))

	time.Sleep(2 * time.Second)

	// Deletes before commit_ts should NOT take effect — row didn't exist yet
	count := s.queryCount(ctx, collName)
	s.Equal(int64(rowNum), count,
		"delete before commit_ts should not take effect — all %d rows should remain", rowNum)
}

// ─── S8: Upsert before commit_ts should not take effect ──────────────────

func (s *CommitTimestampSuite) TestS8_Upsert_BeforeCommitTs() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	const rowNum = 100
	const upsertCount = 20

	collName, collectionID := s.createCollectionAndInsert(ctx, rowNum)

	// Set commit_ts to a future time — upserts happening "now" are before commit_ts
	commitTs := tsoutil.ComposeTSByTime(time.Now().Add(10*time.Second), 0)
	s.setCommitTimestamp(collectionID, commitTs)

	s.buildIndexAndLoad(ctx, collName)

	// Upsert first 20 rows — these upserts have ts < commit_ts
	pkColumn := integration.NewInt64FieldDataWithStart(integration.Int64Field, upsertCount, 1)
	vecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, upsertCount, dim)
	upsertResp, err := s.Cluster.MilvusClient.Upsert(ctx, &milvuspb.UpsertRequest{
		CollectionName: collName,
		FieldsData:     []*schemapb.FieldData{pkColumn, vecColumn},
		NumRows:        uint32(upsertCount),
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(upsertResp.GetStatus()))

	time.Sleep(2 * time.Second)

	// Upsert before commit_ts: the delete part should not take effect on the
	// import segment (row didn't exist yet), so we should see rowNum + upsertCount
	// rows (original import rows + new upserted rows as separate inserts).
	count := s.queryCount(ctx, collName)
	s.Equal(int64(rowNum+upsertCount), count,
		"upsert before commit_ts: delete part should not apply, expect %d rows", rowNum+upsertCount)
}

// ─── S2: Compaction normalizes commit_ts ──────────────────────────────────

func (s *CommitTimestampSuite) TestS2_Compaction_PreservesCommitTs() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	const rowsPerSegment = 50

	collName := "CommitTs_Compact_" + funcutil.RandomString(6)

	schema := integration.ConstructSchema(collName, dim, false)
	marshaledSchema, err := proto.Marshal(schema)
	s.Require().NoError(err)

	createResp, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: collName,
		Schema:         marshaledSchema,
		ShardsNum:      1,
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(createResp))

	// Insert two batches to create two segments
	for batch := 0; batch < 2; batch++ {
		startPK := int64(batch*rowsPerSegment + 1)
		pkColumn := integration.NewInt64FieldDataWithStart(integration.Int64Field, rowsPerSegment, startPK)
		vecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, rowsPerSegment, dim)
		insertResp, err := s.Cluster.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
			CollectionName: collName,
			FieldsData:     []*schemapb.FieldData{pkColumn, vecColumn},
			NumRows:        uint32(rowsPerSegment),
		})
		s.Require().NoError(err)
		s.Require().True(merr.Ok(insertResp.GetStatus()))

		flushResp, err := s.Cluster.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{
			CollectionNames: []string{collName},
		})
		s.Require().NoError(err)
		s.Require().True(merr.Ok(flushResp.GetStatus()))
		segIDs := flushResp.GetCollSegIDs()[collName].GetData()
		flushTs := flushResp.GetCollFlushTs()[collName]
		s.WaitForFlush(ctx, segIDs, flushTs, "", collName)
	}

	// Get collection ID
	showResp, err := s.Cluster.MilvusClient.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{
		CollectionNames: []string{collName},
	})
	s.Require().NoError(err)
	collectionID := showResp.GetCollectionIds()[0]

	// Set commit_ts on both segments
	commitTs := tsoutil.ComposeTSByTime(time.Now(), 0)
	modifiedSegIDs := s.setCommitTimestamp(collectionID, commitTs)
	s.Require().GreaterOrEqual(len(modifiedSegIDs), 2,
		"should have at least 2 segments to compact")

	// Trigger compaction
	compactResp, err := s.Cluster.MilvusClient.ManualCompaction(ctx, &milvuspb.ManualCompactionRequest{
		CollectionID: collectionID,
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(compactResp.GetStatus()))

	compactionID := compactResp.GetCompactionID()

	// Wait for compaction to complete
	compactionCompleted := false
	for i := 0; i < 60; i++ {
		time.Sleep(2 * time.Second)
		stateResp, err := s.Cluster.MilvusClient.GetCompactionState(ctx, &milvuspb.GetCompactionStateRequest{
			CompactionID: compactionID,
		})
		if err != nil {
			continue
		}
		if stateResp.GetState() == commonpb.CompactionState_Completed {
			log.Info("compaction completed", zap.Int64("compactionID", compactionID))
			compactionCompleted = true
			break
		}
	}
	s.Require().True(compactionCompleted, "compaction did not complete within timeout")

	// Verify output segments have CommitTimestamp cleared (normalized)
	segments, err := s.Cluster.ShowSegments(collName)
	s.Require().NoError(err)

	for _, seg := range segments {
		if seg.GetState() == commonpb.SegmentState_Flushed {
			s.Equal(uint64(0), seg.GetCommitTimestamp(),
				"compaction output segment %d must have CommitTimestamp=0 (normalized)", seg.GetID())
		}
	}
}
