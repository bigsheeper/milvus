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

package querynode

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"io"
	"reflect"
	"sort"
	"strconv"

	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/milvus-io/milvus/internal/util/trace"
)

// filterDmNode is one of the nodes in query node flow graph
type filterDmNode struct {
	baseNode
	collection  *Collection
	metaReplica ReplicaInterface
}

// Name returns the name of filterDmNode
func (fdmNode *filterDmNode) Name() string {
	return fmt.Sprintf("fdmNode-%d", fdmNode.collection.ID())
}

// Operate handles input messages, to filter invalid insert messages
func (fdmNode *filterDmNode) Operate(in []flowgraph.Msg) []flowgraph.Msg {
	if len(in) != 1 {
		log.Warn("Invalid operate message input in filterDmNode", zap.Int("input length", len(in)))
		return []Msg{}
	}

	msgStreamMsg, ok := in[0].(*MsgStreamMsg)
	if !ok {
		if in[0] == nil {
			log.Debug("type assertion failed for MsgStreamMsg because it's nil")
		} else {
			log.Warn("type assertion failed for MsgStreamMsg", zap.String("name", reflect.TypeOf(in[0]).Name()))
		}
		return []Msg{}
	}

	if msgStreamMsg == nil {
		return []Msg{}
	}

	var spans []opentracing.Span
	for _, msg := range msgStreamMsg.TsMessages() {
		sp, ctx := trace.StartSpanFromContext(msg.TraceCtx())
		spans = append(spans, sp)
		msg.SetTraceCtx(ctx)
	}

	var iMsg = insertMsg{
		insertData: newInsertData(),
		deleteData: newDeleteData(),
		timeRange: TimeRange{
			timestampMin: msgStreamMsg.TimestampMin(),
			timestampMax: msgStreamMsg.TimestampMax(),
		},
	}

	fdmNode.collection.RLock()
	defer fdmNode.collection.RUnlock()

	insertMessages := make([]*msgstream.InsertMsg, 0)
	for i, msg := range msgStreamMsg.TsMessages() {
		traceID, _, _ := trace.InfoFromSpan(spans[i])
		log.Debug("Filter invalid message in QueryNode", zap.String("traceID", traceID))
		switch msg.Type() {
		case commonpb.MsgType_Insert:
			resMsg, err := fdmNode.filterInvalidInsertMessage(msg.(*msgstream.InsertMsg))
			if err != nil {
				// error occurs when missing meta info or data is misaligned, should not happen
				err = fmt.Errorf("filterInvalidInsertMessage failed, err = %s", err)
				log.Error(err.Error())
				panic(err)
			}
			if resMsg != nil {
				insertMessages = append(insertMessages, resMsg)
			}
		case commonpb.MsgType_Delete:
			resMsg, err := fdmNode.filterInvalidDeleteMessage(msg.(*msgstream.DeleteMsg))
			if err != nil {
				// error occurs when missing meta info or data is misaligned, should not happen
				err = fmt.Errorf("filterInvalidDeleteMessage failed, err = %s", err)
				log.Error(err.Error())
				panic(err)
			}
			log.Debug("delete in streaming replica",
				zap.Any("collectionID", resMsg.CollectionID),
				zap.Any("collectionName", resMsg.CollectionName),
				zap.Int64("numPKs", resMsg.NumRows),
				zap.Int("numTS", len(resMsg.Timestamps)),
				zap.Any("timestampBegin", resMsg.BeginTs()),
				zap.Any("timestampEnd", resMsg.EndTs()),
			)
			err = processDeleteMessages(fdmNode.metaReplica, segmentTypeGrowing, resMsg, iMsg.deleteData)
			if err != nil {
				// error occurs when missing meta info or unexpected pk type, should not happen
				err = fmt.Errorf("processDeleteMessages failed, collectionID = %d, err = %s", resMsg.CollectionID, err)
				log.Error(err.Error())
				panic(err)
			}
		default:
			log.Warn("invalid message type in filterDmNode", zap.String("message type", msg.Type().String()))
		}
	}

	err := processInsertMessages(fdmNode.collection, fdmNode.metaReplica, insertMessages, iMsg.insertData)
	if err != nil {
		// error occurs when missing meta info, should not happen
		err = fmt.Errorf("processInsertMessages failed, collectionID = %d, err = %s", fdmNode.collection.ID(), err)
		log.Error(err.Error())
		panic(err)
	}

	var res Msg = &iMsg
	for _, sp := range spans {
		sp.Finish()
	}
	return []Msg{res}
}

// filterInvalidDeleteMessage would filter out invalid delete messages
func (fdmNode *filterDmNode) filterInvalidDeleteMessage(msg *msgstream.DeleteMsg) (*msgstream.DeleteMsg, error) {
	if err := msg.CheckAligned(); err != nil {
		return nil, fmt.Errorf("CheckAligned failed, err = %s", err)
	}

	if len(msg.Timestamps) <= 0 {
		log.Debug("filter invalid delete message, no message",
			zap.Any("collectionID", msg.CollectionID),
			zap.Any("partitionID", msg.PartitionID))
		return nil, nil
	}

	sp, ctx := trace.StartSpanFromContext(msg.TraceCtx())
	msg.SetTraceCtx(ctx)
	defer sp.Finish()

	if msg.CollectionID != fdmNode.collection.ID() {
		// filter out msg which not belongs to the current collection
		return nil, nil
	}

	if fdmNode.collection.getLoadType() == loadTypePartition {
		if !fdmNode.metaReplica.hasPartition(msg.PartitionID) {
			// filter out msg which not belongs to the loaded partitions
			return nil, nil
		}
	}

	return msg, nil
}

// filterInvalidInsertMessage would filter out invalid insert messages
func (fdmNode *filterDmNode) filterInvalidInsertMessage(msg *msgstream.InsertMsg) (*msgstream.InsertMsg, error) {
	if err := msg.CheckAligned(); err != nil {
		return nil, fmt.Errorf("CheckAligned failed, err = %s", err)
	}

	if len(msg.Timestamps) <= 0 {
		log.Debug("filter invalid insert message, no message",
			zap.Any("collectionID", msg.CollectionID),
			zap.Any("partitionID", msg.PartitionID))
		return nil, nil
	}

	sp, ctx := trace.StartSpanFromContext(msg.TraceCtx())
	msg.SetTraceCtx(ctx)
	defer sp.Finish()

	// check if the collection from message is target collection
	if msg.CollectionID != fdmNode.collection.ID() {
		//log.Debug("filter invalid insert message, collection is not the target collection",
		//	zap.Any("collectionID", msg.CollectionID),
		//	zap.Any("partitionID", msg.PartitionID))
		return nil, nil
	}

	if fdmNode.collection.getLoadType() == loadTypePartition {
		if !fdmNode.metaReplica.hasPartition(msg.PartitionID) {
			// filter out msg which not belongs to the loaded partitions
			return nil, nil
		}
	}

	// Check if the segment is in excluded segments,
	// messages after seekPosition may contain the redundant data from flushed slice of segment,
	// so we need to compare the endTimestamp of received messages and position's timestamp.
	excludedSegments, err := fdmNode.metaReplica.getExcludedSegments(fdmNode.collection.ID())
	if err != nil {
		// QueryNode should addExcludedSegments for the current collection before start flow graph
		return nil, err
	}
	for _, segmentInfo := range excludedSegments {
		// unFlushed segment may not have checkPoint, so `segmentInfo.DmlPosition` may be nil
		if segmentInfo.DmlPosition == nil {
			log.Warn("filter unFlushed segment without checkPoint",
				zap.Any("collectionID", msg.CollectionID),
				zap.Any("partitionID", msg.PartitionID))
			continue
		}
		if msg.SegmentID == segmentInfo.ID && msg.EndTs() < segmentInfo.DmlPosition.Timestamp {
			log.Debug("filter invalid insert message, segments are excluded segments",
				zap.Any("collectionID", msg.CollectionID),
				zap.Any("partitionID", msg.PartitionID))
			return nil, nil
		}
	}

	return msg, nil
}

// processDeleteMessages would execute delete operations for growing segments
func processDeleteMessages(replica ReplicaInterface, segType segmentType, msg *msgstream.DeleteMsg, delData *deleteData) error {
	var partitionIDs []UniqueID
	var err error
	if msg.PartitionID != common.InvalidPartitionID {
		partitionIDs = []UniqueID{msg.PartitionID}
	} else {
		partitionIDs, err = replica.getPartitionIDs(msg.CollectionID)
		if err != nil {
			return err
		}
	}
	resultSegmentIDs := make([]UniqueID, 0)
	for _, partitionID := range partitionIDs {
		segmentIDs, err := replica.getSegmentIDs(partitionID, segType)
		if err != nil {
			return err
		}
		resultSegmentIDs = append(resultSegmentIDs, segmentIDs...)
	}

	primaryKeys := storage.ParseIDs2PrimaryKeys(msg.PrimaryKeys)
	for _, segmentID := range resultSegmentIDs {
		segment, err := replica.getSegmentByID(segmentID, segType)
		if err != nil {
			return err
		}
		pks, tss, err := filterSegmentsByPKs(primaryKeys, msg.Timestamps, segment)
		if err != nil {
			return err
		}
		if len(pks) > 0 {
			delData.deleteIDs[segmentID] = append(delData.deleteIDs[segmentID], pks...)
			delData.deleteTimestamps[segmentID] = append(delData.deleteTimestamps[segmentID], tss...)
			delData.deleteSegments[segmentID] = segment
		}
	}
	return nil
}

func processInsertMessages(col *Collection, replica ReplicaInterface, insertMessages []*msgstream.InsertMsg, iData *insertData) error {
	// hash insertMessages to insertData
	// sort timestamps ensures that the data in iData.insertRecords is sorted in ascending order of timestamp
	// avoiding re-sorting in segCore, which will need data copying
	sort.Slice(insertMessages, func(i, j int) bool {
		return insertMessages[i].BeginTs() < insertMessages[j].BeginTs()
	})
	for _, insertMsg := range insertMessages {
		// if loadType is loadCollection, check if partition exists, if not, create partition
		if col.getLoadType() == loadTypeCollection {
			err := replica.addPartition(insertMsg.CollectionID, insertMsg.PartitionID)
			if err != nil {
				// error occurs only when collection cannot be found, should not happen
				return err
			}
		}

		// check if segment exists, if not, create this segment
		has, err := replica.hasSegment(insertMsg.SegmentID, segmentTypeGrowing)
		if err != nil {
			return err
		}
		if !has {
			err = replica.addSegment(insertMsg.SegmentID, insertMsg.PartitionID, insertMsg.CollectionID, insertMsg.ShardName, segmentTypeGrowing)
			if err != nil {
				// error occurs when collection or partition cannot be found, collection and partition should be created before
				return err
			}
		}

		insertRecord, err := storage.TransferInsertMsgToInsertRecord(col.schema, insertMsg)
		if err != nil {
			// occurs only when schema doesn't have dim param, this should not happen
			return err
		}

		iData.insertIDs[insertMsg.SegmentID] = append(iData.insertIDs[insertMsg.SegmentID], insertMsg.RowIDs...)
		iData.insertTimestamps[insertMsg.SegmentID] = append(iData.insertTimestamps[insertMsg.SegmentID], insertMsg.Timestamps...)
		if _, ok := iData.insertRecords[insertMsg.SegmentID]; !ok {
			iData.insertRecords[insertMsg.SegmentID] = insertRecord.FieldsData
		} else {
			typeutil.MergeFieldData(iData.insertRecords[insertMsg.SegmentID], insertRecord.FieldsData)
		}
		pks, err := getPrimaryKeys(insertMsg, col)
		if err != nil {
			// error occurs when cannot find collection or data is misaligned, should not happen
			return err
		}
		iData.insertPKs[insertMsg.SegmentID] = append(iData.insertPKs[insertMsg.SegmentID], pks...)
		if _, ok := iData.insertSegments[insertMsg.SegmentID]; !ok {
			segment, err := replica.getSegmentByID(insertMsg.GetSegmentID(), segmentTypeGrowing)
			if err != nil {
				return err
			}
			iData.insertSegments[insertMsg.SegmentID] = segment
		}
	}
	return nil
}

// TODO: remove this function to proper file
// getPrimaryKeys would get primary keys by insert messages
func getPrimaryKeys(msg *msgstream.InsertMsg, collection *Collection) ([]primaryKey, error) {
	if err := msg.CheckAligned(); err != nil {
		log.Warn("misaligned messages detected", zap.Error(err))
		return nil, err
	}
	return getPKs(msg, collection.schema)
}

func getPKs(msg *msgstream.InsertMsg, schema *schemapb.CollectionSchema) ([]primaryKey, error) {
	if msg.IsRowBased() {
		return getPKsFromRowBasedInsertMsg(msg, schema)
	}
	return getPKsFromColumnBasedInsertMsg(msg, schema)
}

func getPKsFromRowBasedInsertMsg(msg *msgstream.InsertMsg, schema *schemapb.CollectionSchema) ([]primaryKey, error) {
	offset := 0
	for _, field := range schema.Fields {
		if field.IsPrimaryKey {
			break
		}
		switch field.DataType {
		case schemapb.DataType_Bool:
			offset++
		case schemapb.DataType_Int8:
			offset++
		case schemapb.DataType_Int16:
			offset += 2
		case schemapb.DataType_Int32:
			offset += 4
		case schemapb.DataType_Int64:
			offset += 8
		case schemapb.DataType_Float:
			offset += 4
		case schemapb.DataType_Double:
			offset += 8
		case schemapb.DataType_FloatVector:
			for _, t := range field.TypeParams {
				if t.Key == "dim" {
					dim, err := strconv.Atoi(t.Value)
					if err != nil {
						return nil, fmt.Errorf("strconv wrong on get dim, err = %s", err)
					}
					offset += dim * 4
					break
				}
			}
		case schemapb.DataType_BinaryVector:
			for _, t := range field.TypeParams {
				if t.Key == "dim" {
					dim, err := strconv.Atoi(t.Value)
					if err != nil {
						return nil, fmt.Errorf("strconv wrong on get dim, err = %s", err)
					}
					offset += dim / 8
					break
				}
			}
		}
	}

	blobReaders := make([]io.Reader, len(msg.RowData))
	for i, blob := range msg.RowData {
		blobReaders[i] = bytes.NewReader(blob.GetValue()[offset : offset+8])
	}
	pks := make([]primaryKey, len(blobReaders))

	for i, reader := range blobReaders {
		var int64PkValue int64
		err := binary.Read(reader, common.Endian, &int64PkValue)
		if err != nil {
			log.Warn("binary read blob value failed", zap.Error(err))
			return nil, err
		}
		pks[i] = newInt64PrimaryKey(int64PkValue)
	}

	return pks, nil
}

func getPKsFromColumnBasedInsertMsg(msg *msgstream.InsertMsg, schema *schemapb.CollectionSchema) ([]primaryKey, error) {
	primaryFieldSchema, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		return nil, err
	}

	primaryFieldData, err := typeutil.GetPrimaryFieldData(msg.GetFieldsData(), primaryFieldSchema)
	if err != nil {
		return nil, err
	}

	pks, err := storage.ParseFieldData2PrimaryKeys(primaryFieldData)
	if err != nil {
		return nil, err
	}

	return pks, nil
}

// filterSegmentsByPKs would filter segments by primary keys
func filterSegmentsByPKs(pks []primaryKey, timestamps []Timestamp, segment *Segment) ([]primaryKey, []Timestamp, error) {
	if segment == nil {
		return nil, nil, fmt.Errorf("segments is nil when getSegmentsByPKs")
	}

	retPks := make([]primaryKey, 0)
	retTss := make([]Timestamp, 0)
	buf := make([]byte, 8)
	for index, pk := range pks {
		exist := false
		switch pk.Type() {
		case schemapb.DataType_Int64:
			int64Pk := pk.(*int64PrimaryKey)
			common.Endian.PutUint64(buf, uint64(int64Pk.Value))
			exist = segment.pkFilter.Test(buf)
		case schemapb.DataType_VarChar:
			varCharPk := pk.(*varCharPrimaryKey)
			exist = segment.pkFilter.TestString(varCharPk.Value)
		default:
			return nil, nil, fmt.Errorf("invalid data type of delete primary keys")
		}
		if exist {
			retPks = append(retPks, pk)
			retTss = append(retTss, timestamps[index])
		}
	}
	return retPks, retTss, nil
}

// newFilteredDmNode returns a new filterDmNode
func newFilteredDmNode(metaReplica ReplicaInterface, collectionID UniqueID) (*filterDmNode, error) {

	maxQueueLength := Params.QueryNodeCfg.FlowGraphMaxQueueLength
	maxParallelism := Params.QueryNodeCfg.FlowGraphMaxParallelism

	baseNode := baseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	col, err := metaReplica.getCollectionByID(collectionID)
	if err != nil {
		// QueryNode should add collection before start flow graph
		return nil, fmt.Errorf("getCollectionByID failed, collectionID = %d", collectionID)
	}

	return &filterDmNode{
		baseNode:    baseNode,
		collection:  col,
		metaReplica: metaReplica,
	}, nil
}
