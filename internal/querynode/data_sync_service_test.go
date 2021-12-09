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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDataSyncService_DMLFlowGraphs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	streamingReplica, err := genSimpleReplica()
	assert.NoError(t, err)

	historicalReplica, err := genSimpleReplica()
	assert.NoError(t, err)

	fac, err := genFactory()
	assert.NoError(t, err)

	tSafe := newTSafeReplica()
	dataSyncService := newDataSyncService(ctx, streamingReplica, historicalReplica, tSafe, fac)
	assert.NotNil(t, dataSyncService)

	dataSyncService.addDMLFlowGraph(defaultCollectionID, defaultPartitionID, loadTypeCollection, []Channel{defaultVChannel})
	assert.Len(t, 1, len(dataSyncService.dmlFlowGraphs))

	fg, err := dataSyncService.getDMLFlowGraph(defaultCollectionID, []Channel{defaultVChannel})
	assert.NotNil(t, fg)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(fg))

	fg, err = dataSyncService.getDMLFlowGraph(UniqueID(1000), []Channel{defaultVChannel})
	assert.Nil(t, fg)
	assert.Error(t, err)

	fg, err = dataSyncService.getDMLFlowGraph(defaultCollectionID, []Channel{"invalid-vChannel"})
	assert.NotNil(t, fg)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(fg))

	fg, err = dataSyncService.getDMLFlowGraph(UniqueID(1000), []Channel{"invalid-vChannel"})
	assert.Nil(t, fg)
	assert.Error(t, err)

	err = dataSyncService.startDMLFlowGraph(defaultCollectionID, []Channel{defaultVChannel})
	assert.NoError(t, err)

	dataSyncService.removeDMLFlowGraph(defaultCollectionID)
	assert.Len(t, 0, len(dataSyncService.dmlFlowGraphs))

	fg, err = dataSyncService.getDMLFlowGraph(defaultCollectionID, []Channel{defaultVChannel})
	assert.Nil(t, fg)
	assert.Error(t, err)

	dataSyncService.addDMLFlowGraph(defaultCollectionID, defaultPartitionID, loadTypeCollection, []Channel{defaultVChannel})
	assert.Len(t, 1, len(dataSyncService.dmlFlowGraphs))

	dataSyncService.close()
	assert.Len(t, 0, len(dataSyncService.dmlFlowGraphs))
}

func TestDataSyncService_DeltaFlowGraphs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	streamingReplica, err := genSimpleReplica()
	assert.NoError(t, err)

	historicalReplica, err := genSimpleReplica()
	assert.NoError(t, err)

	fac, err := genFactory()
	assert.NoError(t, err)

	tSafe := newTSafeReplica()
	dataSyncService := newDataSyncService(ctx, streamingReplica, historicalReplica, tSafe, fac)
	assert.NotNil(t, dataSyncService)

	dataSyncService.addDeltaFlowGraph(defaultCollectionID, []Channel{defaultVChannel})
	assert.Len(t, 1, len(dataSyncService.deltaFlowGraphs))

	fg, err := dataSyncService.getDeltaFlowGraphs(defaultCollectionID, []Channel{defaultVChannel})
	assert.NotNil(t, fg)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(fg))

	fg, err = dataSyncService.getDeltaFlowGraphs(UniqueID(1000), []Channel{defaultVChannel})
	assert.Nil(t, fg)
	assert.Error(t, err)

	fg, err = dataSyncService.getDeltaFlowGraphs(defaultCollectionID, []Channel{"invalid-vChannel"})
	assert.NotNil(t, fg)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(fg))

	fg, err = dataSyncService.getDeltaFlowGraphs(UniqueID(1000), []Channel{"invalid-vChannel"})
	assert.Nil(t, fg)
	assert.Error(t, err)

	err = dataSyncService.startDeltaFlowGraph(defaultCollectionID, []Channel{defaultVChannel})
	assert.NoError(t, err)

	dataSyncService.removeDeltaFlowGraph(defaultCollectionID)
	assert.Len(t, 0, len(dataSyncService.deltaFlowGraphs))

	fg, err = dataSyncService.getDeltaFlowGraphs(defaultCollectionID, []Channel{defaultVChannel})
	assert.Nil(t, fg)
	assert.Error(t, err)

	dataSyncService.addDeltaFlowGraph(defaultCollectionID, []Channel{defaultVChannel})
	assert.Len(t, 1, len(dataSyncService.deltaFlowGraphs))

	dataSyncService.close()
	assert.Len(t, 0, len(dataSyncService.deltaFlowGraphs))
}
