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

	mu              sync.Mutex                                   // guards FlowGraphs
	dmlFlowGraphs   map[Channel]*queryNodeFlowGraph // map[collectionID]flowGraphs
	deltaFlowGraphs map[Channel]*queryNodeFlowGraph // map[collectionID]flowGraphs

	streamingReplica  ReplicaInterface
	historicalReplica ReplicaInterface
	tSafeReplica      TSafeReplicaInterface
	msFactory         msgstream.Factory
}

// addDMLFlowGraph add a flowGraph to dmlFlowGraphs
func (dsService *dataSyncService) addDMLFlowGraph(collectionID UniqueID, partitionID UniqueID, loadType loadType, vChannels []string) {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	for _, vChannel := range vChannels {
		if _, ok := dsService.dmlFlowGraphs[vChannel]; ok {
			log.Warn("dml flow graph has been existed", zap.Any("Channel", vChannel))
			continue
		}
		newFlowGraph := newQueryNodeFlowGraph(dsService.ctx,
			loadType,
			collectionID,
			partitionID,
			dsService.streamingReplica,
			dsService.tSafeReplica,
			vChannel,
			dsService.msFactory)
		dsService.dmlFlowGraphs[vChannel] = newFlowGraph
		log.Debug("add DML flow graph",
			zap.Any("collectionID", collectionID),
			zap.Any("partitionID", partitionID),
			zap.Any("channel", vChannel))
	}
}

// addDeltaFlowGraph add a flowGraph to deltaFlowGraphs
func (dsService *dataSyncService) addDeltaFlowGraph(collectionID UniqueID, vChannels []string) {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	for _, vChannel := range vChannels {
		if _, ok := dsService.deltaFlowGraphs[vChannel]; ok {
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
		dsService.deltaFlowGraphs[vChannel] = newFlowGraph
		log.Debug("add delta flow graph",
			zap.Any("collectionID", collectionID),
			zap.Any("channel", vChannel))
	}
}

// getDMLFlowGraph returns the DML flowGraph by collectionID
func (dsService *dataSyncService) getDMLFlowGraph(collectionID UniqueID, channel Channel) (*queryNodeFlowGraph, error) {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	if _, ok := dsService.dmlFlowGraphs[channel]; !ok {
		return nil, errors.New("DML flow graph doesn't existed, collectionID = " + fmt.Sprintln(collectionID))
	}

	return dsService.dmlFlowGraphs[channel], nil
}

// getDeltaFlowGraphs returns the delta flowGraph by collectionID
func (dsService *dataSyncService) getDeltaFlowGraphs(collectionID UniqueID, vChannels []string) (map[Channel]*queryNodeFlowGraph, error) {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	if _, ok := dsService.deltaFlowGraphs[collectionID]; !ok {
		return nil, errors.New("delta flow graph doesn't existed, collectionID = " + fmt.Sprintln(collectionID))
	}

	tmpFGs := make(map[Channel]*queryNodeFlowGraph)
	for _, channel := range vChannels {
		if _, ok := dsService.deltaFlowGraphs[collectionID][channel]; ok {
			tmpFGs[channel] = dsService.deltaFlowGraphs[collectionID][channel]
		}
	}

	return tmpFGs, nil
}

// startDMLFlowGraph starts the DML flow graph by collectionID
func (dsService *dataSyncService) startDMLFlowGraph(collectionID UniqueID, vChannels []string) error {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	if _, ok := dsService.dmlFlowGraphs[collectionID]; !ok {
		return errors.New("DML flow graph doesn't existed, collectionID = " + fmt.Sprintln(collectionID))
	}
	for _, channel := range vChannels {
		if _, ok := dsService.dmlFlowGraphs[collectionID][channel]; ok {
			// start flow graph
			log.Debug("start DML flow graph", zap.Any("channel", channel))
			dsService.dmlFlowGraphs[collectionID][channel].flowGraph.Start()
		}
	}
	return nil
}

// startDeltaFlowGraph would start the delta flow graph by collectionID
func (dsService *dataSyncService) startDeltaFlowGraph(collectionID UniqueID, vChannels []string) error {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	if _, ok := dsService.deltaFlowGraphs[collectionID]; !ok {
		return errors.New("delta flow graph doesn't existed, collectionID = " + fmt.Sprintln(collectionID))
	}
	for _, channel := range vChannels {
		if _, ok := dsService.deltaFlowGraphs[collectionID][channel]; ok {
			// start flow graph
			log.Debug("start delta flow graph", zap.Any("channel", channel))
			dsService.deltaFlowGraphs[collectionID][channel].flowGraph.Start()
		}
	}
	return nil
}

// removeDMLFlowGraph would remove the DML flow graph by collectionID
func (dsService *dataSyncService) removeDMLFlowGraph(collectionID UniqueID) {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	if _, ok := dsService.dmlFlowGraphs[collectionID]; ok {
		for _, nodeFG := range dsService.dmlFlowGraphs[collectionID] {
			// close flow graph
			nodeFG.close()
		}
		dsService.dmlFlowGraphs[collectionID] = nil
	}
	delete(dsService.dmlFlowGraphs, collectionID)
}

// removeDeltaFlowGraph would remove the delta delta flow graph by collectionID
func (dsService *dataSyncService) removeDeltaFlowGraph(collectionID UniqueID) {
	dsService.mu.Lock()
	defer dsService.mu.Unlock()

	if _, ok := dsService.deltaFlowGraphs[collectionID]; ok {
		for _, nodeFG := range dsService.deltaFlowGraphs[collectionID] {
			// close flow graph
			nodeFG.close()
		}
		dsService.deltaFlowGraphs[collectionID] = nil
	}
	delete(dsService.deltaFlowGraphs, collectionID)
}

// newDataSyncService returns a new dataSyncService
func newDataSyncService(ctx context.Context,
	streamingReplica ReplicaInterface,
	historicalReplica ReplicaInterface,
	tSafeReplica TSafeReplicaInterface,
	factory msgstream.Factory) *dataSyncService {

	return &dataSyncService{
		ctx:               ctx,
		dmlFlowGraphs:     make(map[Channel]*queryNodeFlowGraph),
		deltaFlowGraphs:   make(map[Channel]*queryNodeFlowGraph),
		streamingReplica:  streamingReplica,
		historicalReplica: historicalReplica,
		tSafeReplica:      tSafeReplica,
		msFactory:         factory,
	}
}

// close would close and remove all flow graphs in dataSyncService
func (dsService *dataSyncService) close() {
	// close DML flow graphs
	for _, nodeFGs := range dsService.dmlFlowGraphs {
		for _, nodeFG := range nodeFGs {
			if nodeFG != nil {
				nodeFG.flowGraph.Close()
			}
		}
	}
	dsService.dmlFlowGraphs = make(map[UniqueID]map[Channel]*queryNodeFlowGraph)
	// close delta flow graphs
	for _, nodeFGs := range dsService.deltaFlowGraphs {
		for _, nodeFG := range nodeFGs {
			if nodeFG != nil {
				nodeFG.flowGraph.Close()
			}
		}
	}
	dsService.deltaFlowGraphs = make(map[UniqueID]map[Channel]*queryNodeFlowGraph)
}
