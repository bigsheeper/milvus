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
	"context"
	"errors"
	"fmt"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
)

// loadType is load collection or load partition
type loadType = int32

const (
	loadTypeCollection loadType = 0
	loadTypePartition  loadType = 1
)

// dataSyncService manages a lot of flow graphs
type dataSyncService struct {
	ctx context.Context

	mu                      sync.Mutex // guards FlowGraphs
	dmlChannel2FlowGraphs   map[Channel]*queryNodeFlowGraph
	deltaChannel2FlowGraphs map[Channel]*queryNodeFlowGraph

	streamingReplica  ReplicaInterface
	historicalReplica ReplicaInterface
	tSafeReplica      TSafeReplicaInterface
	msFactory         msgstream.Factory
}

// addFlowGraphsForDMLChannels add a flowGraph to dmlChannel2FlowGraphs
func (dsService *dataSyncService) addFlowGraphsForDMLChannels(collectionID UniqueID, partitionID UniqueID, loadType loadType, vChannels []string) {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	for _, vChannel := range vChannels {
		if _, ok := dsService.dmlChannel2FlowGraphs[vChannel]; ok {
			log.Warn("dml flow graph has been existed", zap.Any("Channel", vChannel))
			continue
		}
		newFlowGraph := newQueryNodeFlowGraph(dsService.ctx,
			collectionID,
			dsService.streamingReplica,
			dsService.tSafeReplica,
			vChannel,
			dsService.msFactory)
		dsService.dmlChannel2FlowGraphs[vChannel] = newFlowGraph
		log.Debug("add DML flow graph",
			zap.Any("collectionID", collectionID),
			zap.Any("partitionID", partitionID),
			zap.Any("channel", vChannel))
	}
}

// addFlowGraphsForDeltaChannels add a flowGraph to deltaChannel2FlowGraphs
func (dsService *dataSyncService) addFlowGraphsForDeltaChannels(collectionID UniqueID, vChannels []string) {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	for _, vChannel := range vChannels {
		if _, ok := dsService.deltaChannel2FlowGraphs[vChannel]; ok {
			log.Warn("delta flow graph has been existed", zap.Any("Channel", vChannel))
			continue
		}
		// delta flow graph doesn't need partition id
		newFlowGraph := newQueryNodeDeltaFlowGraph(dsService.ctx,
			collectionID,
			dsService.historicalReplica,
			dsService.tSafeReplica,
			vChannel,
			dsService.msFactory)
		dsService.deltaChannel2FlowGraphs[vChannel] = newFlowGraph
		log.Debug("add delta flow graph",
			zap.Any("collectionID", collectionID),
			zap.Any("channel", vChannel))
	}
}

// getFlowGraphByDMLChannel returns the DML flowGraph by collectionID
func (dsService *dataSyncService) getFlowGraphByDMLChannel(collectionID UniqueID, channel Channel) (*queryNodeFlowGraph, error) {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	if _, ok := dsService.dmlChannel2FlowGraphs[channel]; !ok {
		return nil, errors.New("DML flow graph doesn't existed, collectionID = " + fmt.Sprintln(collectionID))
	}

	// TODO: return clone?
	return dsService.dmlChannel2FlowGraphs[channel], nil
}

// getFlowGraphByDeltaChannel returns the delta flowGraph by collectionID
func (dsService *dataSyncService) getFlowGraphByDeltaChannel(collectionID UniqueID, channel Channel) (*queryNodeFlowGraph, error) {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	if _, ok := dsService.deltaChannel2FlowGraphs[channel]; !ok {
		return nil, errors.New("delta flow graph doesn't existed, collectionID = " + fmt.Sprintln(collectionID))
	}

	// TODO: return clone?
	return dsService.deltaChannel2FlowGraphs[channel], nil
}

// startFlowGraphByDMLChannel starts the DML flow graph by collectionID
func (dsService *dataSyncService) startFlowGraphByDMLChannel(collectionID UniqueID, channel Channel) error {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	if _, ok := dsService.dmlChannel2FlowGraphs[channel]; !ok {
		return errors.New("DML flow graph doesn't existed, collectionID = " + fmt.Sprintln(collectionID))
	}
	log.Debug("start DML flow graph", zap.Any("channel", channel))
	dsService.dmlChannel2FlowGraphs[channel].flowGraph.Start()
	return nil
}

// startFlowGraphForDeltaChannel would start the delta flow graph by collectionID
func (dsService *dataSyncService) startFlowGraphForDeltaChannel(collectionID UniqueID, channel Channel) error {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	if _, ok := dsService.deltaChannel2FlowGraphs[channel]; !ok {
		return errors.New("delta flow graph doesn't existed, collectionID = " + fmt.Sprintln(collectionID))
	}
	log.Debug("start delta flow graph", zap.Any("channel", channel))
	dsService.deltaChannel2FlowGraphs[channel].flowGraph.Start()
	return nil
}

// removeFlowGraphByDMLChannel would remove the DML flow graph by collectionID
func (dsService *dataSyncService) removeFlowGraphByDMLChannel(channel Channel) {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	if _, ok := dsService.dmlChannel2FlowGraphs[channel]; ok {
		// close flow graph
		dsService.dmlChannel2FlowGraphs[channel].close()
	}
	delete(dsService.dmlChannel2FlowGraphs, channel)
}

// removeFlowGraphByDeltaChannel would remove the delta delta flow graph by collectionID
func (dsService *dataSyncService) removeFlowGraphByDeltaChannel(channel Channel) {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	if _, ok := dsService.deltaChannel2FlowGraphs[channel]; ok {
		// close flow graph
		dsService.deltaChannel2FlowGraphs[channel].close()
	}
	delete(dsService.deltaChannel2FlowGraphs, channel)
}

// newDataSyncService returns a new dataSyncService
func newDataSyncService(ctx context.Context,
	streamingReplica ReplicaInterface,
	historicalReplica ReplicaInterface,
	tSafeReplica TSafeReplicaInterface,
	factory msgstream.Factory) *dataSyncService {

	return &dataSyncService{
		ctx:                     ctx,
		dmlChannel2FlowGraphs:   make(map[Channel]*queryNodeFlowGraph),
		deltaChannel2FlowGraphs: make(map[Channel]*queryNodeFlowGraph),
		streamingReplica:        streamingReplica,
		historicalReplica:       historicalReplica,
		tSafeReplica:            tSafeReplica,
		msFactory:               factory,
	}
}

// close would close and remove all flow graphs in dataSyncService
func (dsService *dataSyncService) close() {
	// close DML flow graphs
	for _, nodeFG := range dsService.dmlChannel2FlowGraphs {
		if nodeFG != nil {
			nodeFG.flowGraph.Close()
		}
	}
	dsService.dmlChannel2FlowGraphs = make(map[Channel]*queryNodeFlowGraph)
	// close delta flow graphs
	for _, nodeFG := range dsService.deltaChannel2FlowGraphs {
		if nodeFG != nil {
			nodeFG.flowGraph.Close()
		}
	}
	dsService.deltaChannel2FlowGraphs = make(map[Channel]*queryNodeFlowGraph)
}
