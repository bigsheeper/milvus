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

package indexnode

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
	"go.uber.org/zap/zapcore"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/compaction"
	"github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/proto/workerpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func Benchmark_SegmentWriter(b *testing.B) {
	log.SetLevel(zapcore.InfoLevel)
	paramtable.Init()

	const (
		dim     = 128
		numRows = 1000000
	)

	var (
		rId  = &schemapb.FieldSchema{FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64}
		ts   = &schemapb.FieldSchema{FieldID: common.TimeStampField, Name: common.TimeStampFieldName, DataType: schemapb.DataType_Int64}
		pk   = &schemapb.FieldSchema{FieldID: 100, Name: "pk", IsPrimaryKey: true, DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "100"}}}
		f    = &schemapb.FieldSchema{FieldID: 101, Name: "random", DataType: schemapb.DataType_Double}
		fVec = &schemapb.FieldSchema{FieldID: 102, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: strconv.Itoa(dim)}}}
	)
	schema := &schemapb.CollectionSchema{Name: "test-aaa", Fields: []*schemapb.FieldSchema{rId, ts, pk, f, fVec}}

	// prepare data values
	start := time.Now()
	vec := make([]float32, dim)
	for j := 0; j < dim; j++ {
		vec[j] = rand.Float32()
	}
	values := make([]*storage.Value, numRows)
	for i := 0; i < numRows; i++ {
		value := &storage.Value{}
		value.Value = make(map[int64]interface{}, len(schema.GetFields()))
		m := value.Value.(map[int64]interface{})
		for _, field := range schema.GetFields() {
			switch field.GetDataType() {
			case schemapb.DataType_Int64:
				m[field.GetFieldID()] = int64(i)
			case schemapb.DataType_VarChar:
				k := fmt.Sprintf("test_pk_%d", i)
				m[field.GetFieldID()] = k
				value.PK = &storage.VarCharPrimaryKey{
					Value: k,
				}
			case schemapb.DataType_Double:
				m[field.GetFieldID()] = float64(i)
			case schemapb.DataType_FloatVector:
				m[field.GetFieldID()] = vec
			}
		}
		value.ID = int64(i)
		value.Timestamp = int64(0)
		value.IsDeleted = false
		value.Value = m
		values[i] = value
	}
	sort.Slice(values, func(i, j int) bool {
		return values[i].PK.LT(values[j].PK)
	})
	log.Info("prepare data done", zap.Int("len", len(values)), zap.Duration("dur", time.Since(start)))

	writer, err := compaction.NewSegmentWriter(schema, numRows, 100, 200, 300, nil)
	assert.NoError(b, err)

	b.N = 10
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start = time.Now()
		for _, v := range values {
			_ = writer.Write(v)
			assert.NoError(b, err)
		}
		log.Info("write done", zap.Int("len", len(values)), zap.Duration("dur", time.Since(start)))
	}
	b.StopTimer()
}

func TestTaskStatsSuite(t *testing.T) {
	suite.Run(t, new(TaskStatsSuite))
}

type TaskStatsSuite struct {
	suite.Suite

	collectionID int64
	partitionID  int64
	clusterID    string
	schema       *schemapb.CollectionSchema

	mockBinlogIO *io.MockBinlogIO
	segWriter    *compaction.SegmentWriter
}

func (s *TaskStatsSuite) SetupSuite() {
	s.collectionID = 100
	s.partitionID = 101
	s.clusterID = "102"
}

func (s *TaskStatsSuite) SetupSubTest() {
	paramtable.Init()
	s.mockBinlogIO = io.NewMockBinlogIO(s.T())
}

func (s *TaskStatsSuite) GenSegmentWriterWithBM25(magic int64) {
	segWriter, err := compaction.NewSegmentWriter(s.schema, 100, magic, s.partitionID, s.collectionID, []int64{102})
	s.Require().NoError(err)

	v := storage.Value{
		PK:        storage.NewInt64PrimaryKey(magic),
		Timestamp: int64(tsoutil.ComposeTSByTime(getMilvusBirthday(), 0)),
		Value:     genRowWithBM25(magic),
	}
	err = segWriter.Write(&v)
	s.Require().NoError(err)
	segWriter.FlushAndIsFull()

	s.segWriter = segWriter
}

func (s *TaskStatsSuite) Testbm25SerializeWriteError() {
	s.Run("normal case", func() {
		s.schema = genCollectionSchemaWithBM25()
		s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil).Once()
		s.GenSegmentWriterWithBM25(0)
		cnt, binlogs, err := bm25SerializeWrite(context.Background(), s.mockBinlogIO, 0, s.segWriter, 1)
		s.Require().NoError(err)
		s.Equal(int64(1), cnt)
		s.Equal(1, len(binlogs))
	})

	s.Run("upload failed", func() {
		s.schema = genCollectionSchemaWithBM25()
		s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(fmt.Errorf("mock error")).Once()
		s.GenSegmentWriterWithBM25(0)
		_, _, err := bm25SerializeWrite(context.Background(), s.mockBinlogIO, 0, s.segWriter, 1)
		s.Error(err)
	})
}

func (s *TaskStatsSuite) TestSortSegmentWithBM25() {
	s.Run("normal case", func() {
		s.schema = genCollectionSchemaWithBM25()
		s.GenSegmentWriterWithBM25(0)
		_, kvs, fBinlogs, err := serializeWrite(context.TODO(), 0, s.segWriter)
		s.NoError(err)
		s.mockBinlogIO.EXPECT().Download(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, paths []string) ([][]byte, error) {
			result := make([][]byte, len(paths))
			for i, path := range paths {
				result[i] = kvs[path]
			}
			return result, nil
		})
		s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil)

		ctx, cancel := context.WithCancel(context.Background())

		testTaskKey := taskKey{ClusterID: s.clusterID, TaskID: 100}
		node := &IndexNode{statsTasks: map[taskKey]*statsTaskInfo{testTaskKey: {segID: 1}}}
		task := newStatsTask(ctx, cancel, &workerpb.CreateStatsRequest{
			CollectionID:    s.collectionID,
			PartitionID:     s.partitionID,
			ClusterID:       s.clusterID,
			TaskID:          testTaskKey.TaskID,
			TargetSegmentID: 1,
			InsertLogs:      lo.Values(fBinlogs),
			Schema:          s.schema,
			NumRows:         1,
		}, node, s.mockBinlogIO)
		err = task.PreExecute(ctx)
		s.Require().NoError(err)
		binlog, err := task.sortSegment(ctx)
		s.Require().NoError(err)
		s.Equal(5, len(binlog))

		// check bm25 log
		s.Equal(1, len(node.statsTasks))
		for key, task := range node.statsTasks {
			s.Equal(testTaskKey.ClusterID, key.ClusterID)
			s.Equal(testTaskKey.TaskID, key.TaskID)
			s.Equal(1, len(task.bm25Logs))
		}
	})

	s.Run("upload bm25 binlog failed", func() {
		s.schema = genCollectionSchemaWithBM25()
		s.GenSegmentWriterWithBM25(0)
		_, kvs, fBinlogs, err := serializeWrite(context.TODO(), 0, s.segWriter)
		s.NoError(err)
		s.mockBinlogIO.EXPECT().Download(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, paths []string) ([][]byte, error) {
			result := make([][]byte, len(paths))
			for i, path := range paths {
				result[i] = kvs[path]
			}
			return result, nil
		})
		s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil).Times(2)
		s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(fmt.Errorf("mock error")).Once()

		ctx, cancel := context.WithCancel(context.Background())

		testTaskKey := taskKey{ClusterID: s.clusterID, TaskID: 100}
		node := &IndexNode{statsTasks: map[taskKey]*statsTaskInfo{testTaskKey: {segID: 1}}}
		task := newStatsTask(ctx, cancel, &workerpb.CreateStatsRequest{
			CollectionID:    s.collectionID,
			PartitionID:     s.partitionID,
			ClusterID:       s.clusterID,
			TaskID:          testTaskKey.TaskID,
			TargetSegmentID: 1,
			InsertLogs:      lo.Values(fBinlogs),
			Schema:          s.schema,
			NumRows:         1,
		}, node, s.mockBinlogIO)
		err = task.PreExecute(ctx)
		s.Require().NoError(err)
		_, err = task.sortSegment(ctx)
		s.Error(err)
	})
}

func genCollectionSchemaWithBM25() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name:        "schema",
		Description: "schema",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:  common.RowIDField,
				Name:     "row_id",
				DataType: schemapb.DataType_Int64,
			},
			{
				FieldID:  common.TimeStampField,
				Name:     "Timestamp",
				DataType: schemapb.DataType_Int64,
			},
			{
				FieldID:      100,
				Name:         "pk",
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: true,
			},
			{
				FieldID:  101,
				Name:     "text",
				DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.MaxLengthKey,
						Value: "8",
					},
				},
			},
			{
				FieldID:  102,
				Name:     "sparse",
				DataType: schemapb.DataType_SparseFloatVector,
			},
		},
		Functions: []*schemapb.FunctionSchema{{
			Name:             "BM25",
			Id:               100,
			Type:             schemapb.FunctionType_BM25,
			InputFieldNames:  []string{"text"},
			InputFieldIds:    []int64{101},
			OutputFieldNames: []string{"sparse"},
			OutputFieldIds:   []int64{102},
		}},
	}
}

func genRowWithBM25(magic int64) map[int64]interface{} {
	ts := tsoutil.ComposeTSByTime(getMilvusBirthday(), 0)
	return map[int64]interface{}{
		common.RowIDField:     magic,
		common.TimeStampField: int64(ts),
		100:                   magic,
		101:                   "varchar",
		102:                   typeutil.CreateAndSortSparseFloatRow(map[uint32]float32{1: 1}),
	}
}

func getMilvusBirthday() time.Time {
	return time.Date(2019, time.Month(5), 30, 0, 0, 0, 0, time.UTC)
}
