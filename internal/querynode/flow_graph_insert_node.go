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
	"fmt"
	"reflect"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
)

// insertNode is one of the nodes in query flow graph
type insertNode struct {
	baseNode
}

// Name returns the name of insertNode
func (iNode *insertNode) Name() string {
	return "iNode"
}

// Operate handles input messages, to execute insert operations
func (iNode *insertNode) Operate(in []flowgraph.Msg) []flowgraph.Msg {
	if len(in) != 1 {
		log.Warn("Invalid operate message input in insertNode", zap.Int("input length", len(in)))
		return []Msg{}
	}

	iMsg, ok := in[0].(*insertMsg)
	if !ok {
		if in[0] == nil {
			log.Debug("type assertion failed for insertMsg because it's nil")
		} else {
			log.Warn("type assertion failed for insertMsg", zap.String("name", reflect.TypeOf(in[0]).Name()))
		}
		return []Msg{}
	}

	if iMsg == nil {
		return []Msg{}
	}

	// 1. do preInsert
	iData := iMsg.insertData
	for segmentID := range iData.insertRecords {
		targetSegment := iData.insertSegments[segmentID]
		var numOfRecords = len(iData.insertIDs[segmentID])
		if targetSegment != nil {
			offset, err := targetSegment.segmentPreInsert(numOfRecords)
			if err != nil {
				// error occurs when cgo function `PreInsert` failed
				err = fmt.Errorf("segmentPreInsert failed, segmentID = %d, err = %s", segmentID, err)
				log.Error(err.Error())
				panic(err)
			}
			iData.insertOffset[segmentID] = offset
			log.Debug("insertNode operator", zap.Int("insert size", numOfRecords), zap.Int64("insert offset", offset), zap.Int64("segment id", segmentID))
			targetSegment.updateBloomFilter(iData.insertPKs[segmentID])
		}
	}

	// 2. do insert
	wg := sync.WaitGroup{}
	for segmentID := range iData.insertRecords {
		segmentID := segmentID
		segment := iData.insertSegments[segmentID]
		wg.Add(1)
		go func() {
			err := iNode.insert(iData, segment, &wg)
			if err != nil {
				// error occurs when segment cannot be found or cgo function `Insert` failed
				err = fmt.Errorf("segment insert failed, segmentID = %d, err = %s", segmentID, err)
				log.Error(err.Error())
				panic(err)
			}
		}()
	}
	wg.Wait()

	// 3. do preDelete
	delData := iMsg.deleteData
	for segmentID, pks := range delData.deleteIDs {
		segment := delData.deleteSegments[segmentID]
		offset := segment.segmentPreDelete(len(pks))
		delData.deleteOffset[segmentID] = offset
	}

	// 4. do delete
	for segmentID := range delData.deleteOffset {
		segmentID := segmentID
		segment := delData.deleteSegments[segmentID]
		wg.Add(1)
		go func() {
			err := iNode.delete(delData, segment, &wg)
			if err != nil {
				// error occurs when segment cannot be found, calling cgo function delete failed and etc...
				err = fmt.Errorf("segment delete failed, segmentID = %d, err = %s", segmentID, err)
				log.Error(err.Error())
				panic(err)
			}
		}()
	}
	wg.Wait()

	var res Msg = &serviceTimeMsg{
		timeRange: iMsg.timeRange,
	}

	return []Msg{res}
}

// insert would execute insert operations for specific growing segment
func (iNode *insertNode) insert(iData *insertData, segment *Segment, wg *sync.WaitGroup) error {
	defer wg.Done()
	segmentID := segment.ID()
	ids := iData.insertIDs[segmentID]
	timestamps := iData.insertTimestamps[segmentID]
	offsets := iData.insertOffset[segmentID]
	insertRecord := &segcorepb.InsertRecord{
		FieldsData: iData.insertRecords[segmentID],
		NumRows:    int64(len(ids)),
	}

	err := segment.segmentInsert(offsets, ids, timestamps, insertRecord)
	if err != nil {
		return fmt.Errorf("segmentInsert failed, segmentID = %d, err = %s", segmentID, err)
	}

	log.Debug("Do insert done", zap.Int("len", len(iData.insertIDs[segmentID])), zap.Int64("collectionID", segment.collectionID), zap.Int64("segmentID", segmentID))
	return nil
}

// delete would execute delete operations for specific growing segment
func (iNode *insertNode) delete(deleteData *deleteData, segment *Segment, wg *sync.WaitGroup) error {
	defer wg.Done()
	segmentID := segment.ID()
	if segment.segmentType != segmentTypeGrowing {
		return fmt.Errorf("unexpected segmentType when delete, segmentType = %s", segment.getType().String())
	}

	ids := deleteData.deleteIDs[segmentID]
	timestamps := deleteData.deleteTimestamps[segmentID]
	offset := deleteData.deleteOffset[segmentID]

	err := segment.segmentDelete(offset, ids, timestamps)
	if err != nil {
		return fmt.Errorf("segmentDelete failed, err = %s", err)
	}

	log.Debug("Do delete done", zap.Int("len", len(deleteData.deleteIDs[segmentID])), zap.Int64("segmentID", segmentID))
	return nil
}

// newInsertNode returns a new insertNode
func newInsertNode(metaReplica ReplicaInterface) *insertNode {
	maxQueueLength := Params.QueryNodeCfg.FlowGraphMaxQueueLength
	maxParallelism := Params.QueryNodeCfg.FlowGraphMaxParallelism

	baseNode := baseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &insertNode{
		baseNode: baseNode,
	}
}
