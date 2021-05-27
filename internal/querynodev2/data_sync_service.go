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
	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"
	"sync"

	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
)

type dataSyncService struct {
	ctx    context.Context
	cancel context.CancelFunc

	mu                   sync.Mutex                                    // guards FlowGraphs
	collectionFlowGraphs map[UniqueID][]*flowgraph.TimeTickedFlowGraph // map[collectionID]flowGraphs
	partitionFlowGraphs  map[UniqueID]*flowgraph.TimeTickedFlowGraph   // map[partitionID]flowGraph

	msFactory        msgstream.Factory
	streamingReplica ReplicaInterface
}

func (dsService *dataSyncService) addCollectionFlowGraph(collectionID UniqueID, vChannels []string) error {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	if _, ok := dsService.collectionFlowGraphs[collectionID]; ok {
		return errors.New("collection flow graph has been existed, collectionID = " + fmt.Sprintln(collectionID))
	}
	dsService.collectionFlowGraphs[collectionID] = make([]*flowgraph.TimeTickedFlowGraph, 0)
	for _, vChannel := range vChannels {
		fmt.Println(vChannel)
		newFlowGraph := dsService.newDataSyncFlowGraph()
		dsService.collectionFlowGraphs[collectionID] = append(dsService.collectionFlowGraphs[collectionID], newFlowGraph)
		// start flow graph
		newFlowGraph.Start()
	}
	return nil
}

func (dsService *dataSyncService) addPartitionFlowGraph(partitionID UniqueID) error {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	if _, ok := dsService.partitionFlowGraphs[partitionID]; ok {
		return errors.New("partition flow graph has been existed, partitionID = " + fmt.Sprintln(partitionID))
	}
	dsService.partitionFlowGraphs[partitionID] = dsService.newDataSyncFlowGraph()
	// start flow graph
	dsService.partitionFlowGraphs[partitionID].Start()
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
		// close flow graph
		dsService.partitionFlowGraphs[partitionID].Close()
	}
	delete(dsService.partitionFlowGraphs, partitionID)
}

func (dsService *dataSyncService) newDataSyncFlowGraph(channel VChannel) *flowgraph.TimeTickedFlowGraph {
	fg := flowgraph.NewTimeTickedFlowGraph(dsService.ctx)

	var dmStreamNode node = dsService.newDmInputNode(dsService.ctx, channel)
	var filterDmNode node = newFilteredDmNode(dsService.streamingReplica, dsService.collectionID)
	var insertNode node = newInsertNode(dsService.streamingReplica, dsService.collectionID)
	var serviceTimeNode node = newServiceTimeNode(dsService.ctx, dsService.streamingReplica, dsService.msFactory, dsService.collectionID)

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
