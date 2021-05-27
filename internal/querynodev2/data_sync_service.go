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
	"context"
	"errors"
	"fmt"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
)

type flowGraphType = int32

const (
	flowGraphTypeCollection = 0
	flowGraphTypePartition  = 1
)

type dataSyncService struct {
	ctx context.Context

	mu                   sync.Mutex                                    // guards FlowGraphs
	collectionFlowGraphs map[UniqueID][]*flowgraph.TimeTickedFlowGraph // map[collectionID]flowGraphs
	partitionFlowGraphs  map[UniqueID][]*flowgraph.TimeTickedFlowGraph // map[partitionID]flowGraph

	streamingReplica ReplicaInterface
	tSafeReplica     TSafeReplicaInterface
	msFactory        msgstream.Factory
}

func (dsService *dataSyncService) addCollectionFlowGraph(collectionID UniqueID,
	vChannels []string,
	subName ConsumeSubName) error {

	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	if _, ok := dsService.collectionFlowGraphs[collectionID]; ok {
		return errors.New("collection flow graph has been existed, collectionID = " + fmt.Sprintln(collectionID))
	}
	dsService.collectionFlowGraphs[collectionID] = make([]*flowgraph.TimeTickedFlowGraph, 0)
	for _, vChannel := range vChannels {
		// collection flow graph doesn't need partition id
		partitionID := UniqueID(0)
		newFlowGraph := dsService.newDataSyncFlowGraph(flowGraphTypeCollection, collectionID, partitionID, vChannel, subName)
		dsService.collectionFlowGraphs[collectionID] = append(dsService.collectionFlowGraphs[collectionID], newFlowGraph)
		// start flow graph
		newFlowGraph.Start()
	}
	return nil
}

func (dsService *dataSyncService) addPartitionFlowGraph(collectionID UniqueID,
	partitionID UniqueID,
	vChannels []string,
	subName ConsumeSubName) error {

	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	if _, ok := dsService.partitionFlowGraphs[partitionID]; ok {
		return errors.New("partition flow graph has been existed, partitionID = " + fmt.Sprintln(partitionID))
	}
	dsService.partitionFlowGraphs[partitionID] = make([]*flowgraph.TimeTickedFlowGraph, 0)
	for _, vChannel := range vChannels {
		newFlowGraph := dsService.newDataSyncFlowGraph(flowGraphTypePartition, collectionID, partitionID, vChannel, subName)
		dsService.partitionFlowGraphs[partitionID] = append(dsService.partitionFlowGraphs[partitionID], newFlowGraph)
		// start flow graph
		newFlowGraph.Start()
	}
	return nil
}

func (dsService *dataSyncService) removeCollectionFlowGraph(collectionID UniqueID) {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	if _, ok := dsService.collectionFlowGraphs[collectionID]; ok {
		for _, fg := range dsService.collectionFlowGraphs[collectionID] {
			// close flow graph
			fg.Close()
		}
		dsService.collectionFlowGraphs[collectionID] = nil
	}
	delete(dsService.collectionFlowGraphs, collectionID)
}

func (dsService *dataSyncService) removePartitionFlowGraph(partitionID UniqueID) {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	if _, ok := dsService.partitionFlowGraphs[partitionID]; ok {
		for _, fg := range dsService.partitionFlowGraphs[partitionID] {
			// close flow graph
			fg.Close()
		}
		dsService.partitionFlowGraphs[partitionID] = nil
	}
	delete(dsService.partitionFlowGraphs, partitionID)
}

func (dsService *dataSyncService) newDataSyncFlowGraph(flowGraphType flowGraphType,
	collectionID UniqueID,
	partitionID UniqueID,
	channel VChannel,
	subName ConsumeSubName) *flowgraph.TimeTickedFlowGraph {

	fg := flowgraph.NewTimeTickedFlowGraph(dsService.ctx)

	var dmStreamNode node = dsService.newDmInputNode(dsService.ctx, channel, subName)
	var filterDmNode node = newFilteredDmNode(dsService.streamingReplica,
		flowGraphType,
		collectionID,
		partitionID)
	var insertNode node = newInsertNode(dsService.streamingReplica)
	var serviceTimeNode node = newServiceTimeNode(dsService.ctx,
		dsService.tSafeReplica,
		channel,
		dsService.msFactory)

	fg.AddNode(dmStreamNode)
	fg.AddNode(filterDmNode)
	fg.AddNode(insertNode)
	fg.AddNode(serviceTimeNode)

	// dmStreamNode
	var err = fg.SetEdges(dmStreamNode.Name(),
		[]string{},
		[]string{filterDmNode.Name()},
	)
	if err != nil {
		log.Error("set edges failed in node:", zap.String("node name", dmStreamNode.Name()))
	}

	// filterDmNode
	err = fg.SetEdges(filterDmNode.Name(),
		[]string{dmStreamNode.Name()},
		[]string{insertNode.Name()},
	)
	if err != nil {
		log.Error("set edges failed in node:", zap.String("node name", filterDmNode.Name()))
	}

	// insertNode
	err = fg.SetEdges(insertNode.Name(),
		[]string{filterDmNode.Name()},
		[]string{serviceTimeNode.Name()},
	)
	if err != nil {
		log.Error("set edges failed in node:", zap.String("node name", insertNode.Name()))
	}

	// serviceTimeNode
	err = fg.SetEdges(serviceTimeNode.Name(),
		[]string{insertNode.Name()},
		[]string{},
	)
	if err != nil {
		log.Error("set edges failed in node:", zap.String("node name", serviceTimeNode.Name()))
	}

	return fg
}

func newDataSyncService(ctx context.Context,
	streamingReplica ReplicaInterface,
	tSafeReplica TSafeReplicaInterface,
	factory msgstream.Factory) *dataSyncService {

	return &dataSyncService{
		ctx:                  ctx,
		collectionFlowGraphs: make(map[UniqueID][]*flowgraph.TimeTickedFlowGraph),
		partitionFlowGraphs:  make(map[UniqueID][]*flowgraph.TimeTickedFlowGraph),
		streamingReplica:     streamingReplica,
		tSafeReplica:         tSafeReplica,
		msFactory:            factory,
	}
}
