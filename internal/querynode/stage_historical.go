// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package querynode

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	oplog "github.com/opentracing/opentracing-go/log"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type historicalStage struct {
	ctx context.Context

	collectionID UniqueID

	input  chan queryMsg
	output chan queryResult

	historical *historical
	vcm        *storage.VectorChunkManager
}

func newHistoricalStage(ctx context.Context,
	collectionID UniqueID,
	input chan queryMsg,
	output chan queryResult,
	historical *historical,
	vcm *storage.VectorChunkManager) *historicalStage {

	return &historicalStage{
		ctx:          ctx,
		collectionID: collectionID,
		input:        input,
		output:       output,
		historical:   historical,
		vcm:          vcm,
	}
}

func (q *historicalStage) start() {
	for {
		select {
		case <-q.ctx.Done():
			log.Debug("stop historicalStage", zap.Int64("collectionID", q.collectionID))
			return
		case msg := <-q.input:
			msgType := msg.Type()
			sp, ctx := trace.StartSpanFromContext(msg.TraceCtx())
			msg.SetTraceCtx(ctx)
			log.Debug("doing query in historicalStage...",
				zap.Any("collectionID", q.collectionID),
				zap.Any("msgID", msg.ID()),
				zap.Any("msgType", msgType),
			)
			switch msgType {
			case commonpb.MsgType_Retrieve:
				rm := msg.(*retrieveMsg)
				segmentRetrieved, res, err := q.retrieve(rm)
				retrieveRes := &retrieveResult{
					msg:              rm,
					err:              err,
					segmentRetrieved: segmentRetrieved,
					res:              res,
				}
				if err != nil {
					log.Warn("retrieve error in historicalStage",
						zap.Any("collectionID", q.collectionID),
						zap.Any("msgID", msg.ID()),
						zap.Error(err),
					)
				}
				q.output <- retrieveRes
			case commonpb.MsgType_Search:
				sm := msg.(*searchMsg)
				searchResults, _, sealedSegmentSearched, err := q.search(sm)
				searchRes := &searchResult{
					reqs:                  sm.reqs,
					msg:                   sm,
					err:                   err,
					searchResults:         searchResults,
					sealedSegmentSearched: sealedSegmentSearched,
				}
				if err != nil {
					log.Warn("search error in historicalStage",
						zap.Any("collectionID", q.collectionID),
						zap.Any("msgID", msg.ID()),
						zap.Error(err),
					)
				}
				q.output <- searchRes
			default:
				err := fmt.Errorf("receive invalid msgType = %d", msgType)
				log.Warn(err.Error())
			}

			sp.Finish()
		}
	}
}

func (q *historicalStage) retrieve(retrieveMsg *retrieveMsg) ([]UniqueID, []*segcorepb.RetrieveResults, error) {
	collectionID := retrieveMsg.CollectionID
	tr := timerecord.NewTimeRecorder(fmt.Sprintf("retrieve %d", collectionID))

	var partitionIDsInHistorical []UniqueID
	partitionIDsInQuery := retrieveMsg.PartitionIDs
	if len(partitionIDsInQuery) == 0 {
		partitionIDsInHistoricalCol, err := q.historical.replica.getPartitionIDs(collectionID)
		if err != nil {
			return nil, nil, err
		}
		partitionIDsInHistorical = partitionIDsInHistoricalCol
	} else {
		for _, id := range partitionIDsInQuery {
			_, err := q.historical.replica.getPartitionByID(id)
			if err != nil {
				return nil, nil, err
			}
			partitionIDsInHistorical = append(partitionIDsInHistorical, id)
		}
	}
	sealedSegmentRetrieved := make([]UniqueID, 0)
	var mergeList []*segcorepb.RetrieveResults
	for _, partitionID := range partitionIDsInHistorical {
		segmentIDs, err := q.historical.replica.getSegmentIDs(partitionID)
		if err != nil {
			return nil, nil, err
		}
		for _, segmentID := range segmentIDs {
			segment, err := q.historical.replica.getSegmentByID(segmentID)
			if err != nil {
				return nil, nil, err
			}
			result, err := segment.getEntityByIds(retrieveMsg.plan)
			if err != nil {
				return nil, nil, err
			}
			if err = q.fillVectorFieldsData(segment, result); err != nil {
				return nil, nil, err
			}
			mergeList = append(mergeList, result)
			sealedSegmentRetrieved = append(sealedSegmentRetrieved, segmentID)
		}
	}
	tr.Record("historical retrieve done")
	tr.Elapse("all done")
	return sealedSegmentRetrieved, mergeList, nil
}

func (q *historicalStage) search(searchMsg *searchMsg) ([]*SearchResult, []*Segment, []UniqueID, error) {
	sp, ctx := trace.StartSpanFromContext(searchMsg.TraceCtx())
	defer sp.Finish()
	searchMsg.SetTraceCtx(ctx)

	travelTimestamp := searchMsg.TravelTimestamp
	collectionID := searchMsg.CollectionID

	// multiple search is not supported for now
	if len(searchMsg.reqs) != 1 {
		return nil, nil, nil, errors.New("illegal search requests, collectionID = " + fmt.Sprintln(collectionID))
	}
	queryNum := searchMsg.reqs[0].getNumOfQuery()
	topK := searchMsg.plan.getTopK()

	if searchMsg.GetDslType() == commonpb.DslType_BoolExprV1 {
		sp.LogFields(oplog.String("statistical time", "stats start"),
			oplog.Object("nq", queryNum),
			oplog.Object("expr", searchMsg.SerializedExprPlan))
	} else {
		sp.LogFields(oplog.String("statistical time", "stats start"),
			oplog.Object("nq", queryNum),
			oplog.Object("dsl", searchMsg.Dsl))
	}

	tr := timerecord.NewTimeRecorder(fmt.Sprintf("search %d(nq=%d, k=%d)", searchMsg.CollectionID, queryNum, topK))
	sealedSegmentSearched := make([]UniqueID, 0)

	// historical search
	hisSearchResults, hisSegmentResults, err := q.historical.search(searchMsg.reqs,
		collectionID,
		searchMsg.PartitionIDs,
		searchMsg.plan,
		travelTimestamp)
	if err != nil {
		log.Warn(err.Error())
		return nil, nil, nil, err
	}

	for _, seg := range hisSegmentResults {
		sealedSegmentSearched = append(sealedSegmentSearched, seg.segmentID)
	}

	tr.Record("historical search done")
	sp.LogFields(oplog.String("statistical time", "historical search done"))
	tr.Elapse("all done")
	return hisSearchResults, hisSegmentResults, sealedSegmentSearched, nil
}

func (q *historicalStage) fillVectorFieldsData(segment *Segment, result *segcorepb.RetrieveResults) error {
	// If segment is growing, vector data is in memory
	if segment.segmentType == segmentTypeGrowing {
		log.Debug("Segment is growing, all vector data is in memory")
		log.Debug("FillVectorFieldData", zap.Any("segmentType", segment.segmentType))
		return nil
	}
	collection, _ := q.historical.replica.getCollectionByID(q.collectionID)
	schema := &etcdpb.CollectionMeta{
		ID:     q.collectionID,
		Schema: collection.schema}
	schemaHelper, err := typeutil.CreateSchemaHelper(collection.schema)
	if err != nil {
		return err
	}
	for _, resultFieldData := range result.FieldsData {
		log.Debug("FillVectorFieldData for fieldID", zap.Any("fieldID", resultFieldData.FieldId))
		// If the vector field doesn't have index. Vector data is in memory for
		// brute force search. No need to download data from remote.
		_, ok := segment.indexInfos[resultFieldData.FieldId]
		if !ok {
			log.Debug("FillVectorFieldData fielD doesn't have index",
				zap.Any("fielD", resultFieldData.FieldId))
			continue
		}

		vecFieldInfo, err := segment.getVectorFieldInfo(resultFieldData.FieldId)
		if err != nil {
			continue
		}
		dim := resultFieldData.GetVectors().GetDim()
		log.Debug("FillVectorFieldData", zap.Any("dim", dim))
		fieldSchema, err := schemaHelper.GetFieldFromID(resultFieldData.FieldId)
		if err != nil {
			return err
		}
		dataType := fieldSchema.DataType
		log.Debug("FillVectorFieldData", zap.Any("datatype", dataType))

		data := resultFieldData.GetVectors().GetData()

		for i, offset := range result.Offset {
			var vecPath string
			for index, idBinlogRowSize := range segment.idBinlogRowSizes {
				if offset < idBinlogRowSize {
					vecPath = vecFieldInfo.fieldBinlog.Binlogs[index]
					break
				} else {
					offset -= idBinlogRowSize
				}
			}
			log.Debug("FillVectorFieldData", zap.Any("path", vecPath))
			err := q.vcm.DownloadVectorFile(vecPath, schema)
			if err != nil {
				return err
			}

			switch dataType {
			case schemapb.DataType_BinaryVector:
				rowBytes := dim / 8
				content := make([]byte, rowBytes)
				_, err := q.vcm.ReadAt(vecPath, content, offset*rowBytes)
				if err != nil {
					return err
				}
				log.Debug("FillVectorFieldData", zap.Any("binaryVectorResult", content))

				resultLen := dim / 8
				copy(data.(*schemapb.VectorField_BinaryVector).BinaryVector[i*int(resultLen):(i+1)*int(resultLen)], content)
			case schemapb.DataType_FloatVector:
				rowBytes := dim * 4
				content := make([]byte, rowBytes)
				_, err := q.vcm.ReadAt(vecPath, content, offset*rowBytes)
				if err != nil {
					return err
				}
				floatResult := make([]float32, dim)
				buf := bytes.NewReader(content)
				err = binary.Read(buf, binary.LittleEndian, &floatResult)
				if err != nil {
					return err
				}
				log.Debug("FillVectorFieldData", zap.Any("floatVectorResult", floatResult))

				resultLen := dim
				copy(data.(*schemapb.VectorField_FloatVector).FloatVector.Data[i*int(resultLen):(i+1)*int(resultLen)], floatResult)
			}
		}
	}
	return nil
}
