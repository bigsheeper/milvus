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
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
)

type primaryKey = storage.PrimaryKey
type int64PrimaryKey = storage.Int64PrimaryKey
type varCharPrimaryKey = storage.VarCharPrimaryKey

var newInt64PrimaryKey = storage.NewInt64PrimaryKey
var newVarCharPrimaryKey = storage.NewVarCharPrimaryKey

// deleteNode is the one of nodes in delta flow graph
type deleteNode struct {
	baseNode
	//metaReplica ReplicaInterface // historical
}

// Name returns the name of deleteNode
func (dNode *deleteNode) Name() string {
	return "dNode"
}

// Operate handles input messages, do delete operations
func (dNode *deleteNode) Operate(in []flowgraph.Msg) []flowgraph.Msg {
	if len(in) != 1 {
		log.Warn("Invalid operate message input in deleteNode", zap.Int("input length", len(in)))
		return []Msg{}
	}

	dMsg, ok := in[0].(*deleteMsg)
	if !ok {
		if in[0] == nil {
			log.Debug("type assertion failed for deleteMsg because it's nil")
		} else {
			log.Warn("type assertion failed for deleteMsg", zap.String("name", reflect.TypeOf(in[0]).Name()))
		}
		return []Msg{}
	}

	delData := &deleteData{
		deleteIDs:        map[UniqueID][]primaryKey{},
		deleteTimestamps: map[UniqueID][]Timestamp{},
		deleteOffset:     map[UniqueID]int64{},
	}

	if dMsg == nil {
		return []Msg{}
	}

	// 1. do preDelete
	for segmentID, pks := range delData.deleteIDs {
		segment := delData.deleteSegments[segmentID]
		offset := segment.segmentPreDelete(len(pks))
		delData.deleteOffset[segmentID] = offset
	}

	// 2. do delete
	wg := sync.WaitGroup{}
	for segmentID := range delData.deleteOffset {
		segmentID := segmentID
		segment := delData.deleteSegments[segmentID]
		wg.Add(1)
		go func() {
			err := dNode.delete(delData, segment, &wg)
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
		timeRange: dMsg.timeRange,
	}

	return []Msg{res}
}

// delete will do delete operation on Segment
func (dNode *deleteNode) delete(deleteData *deleteData, segment *Segment, wg *sync.WaitGroup) error {
	defer wg.Done()
	segmentID := segment.ID()
	ids := deleteData.deleteIDs[segmentID]
	timestamps := deleteData.deleteTimestamps[segmentID]
	offset := deleteData.deleteOffset[segmentID]

	err := segment.segmentDelete(offset, ids, timestamps)
	if err != nil {
		return fmt.Errorf("segmentDelete failed, segmentID = %d", segmentID)
	}

	log.Debug("Do delete done",
		zap.Int("len", len(deleteData.deleteIDs[segmentID])),
		zap.Int64("segmentID", segmentID),
		zap.Any("SegmentType", segment.getType().String()))
	return nil
}

// newDeleteNode returns a new deleteNode
func newDeleteNode(metaReplica ReplicaInterface) *deleteNode {
	maxQueueLength := Params.QueryNodeCfg.FlowGraphMaxQueueLength
	maxParallelism := Params.QueryNodeCfg.FlowGraphMaxParallelism

	baseNode := baseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &deleteNode{
		baseNode: baseNode,
	}
}
