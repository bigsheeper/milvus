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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/msgstream"
)

func TestHistoricalStage_TestSearch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	his, err := genSimpleHistorical(ctx)
	assert.NoError(t, err)
	inputChan := make(chan queryMsg, queryBufferSize)
	outputChan := make(chan queryResult, queryBufferSize)

	localManager, err := genLocalChunkManager()
	assert.NoError(t, err)

	remoteManager, err := genRemoteChunkManager(ctx)
	assert.NoError(t, err)

	hs := newHistoricalStage(ctx, defaultCollectionID, inputChan, outputChan, his, localManager, remoteManager, false)
	go hs.start()

	// construct searchMsg
	searchReq, err := genSimpleSearchRequest()
	assert.NoError(t, err)
	plan, reqs, err := genSimpleSearchPlanAndRequests()
	assert.NoError(t, err)
	msg := &searchMsg{
		SearchMsg: &msgstream.SearchMsg{
			BaseMsg: msgstream.BaseMsg{
				HashValues: []uint32{0},
			},
			SearchRequest: *searchReq,
		},
		plan: plan,
		reqs: reqs,
	}

	go func() {
		inputChan <- msg
	}()

	//result check
	res := <-outputChan
	sr, ok := res.(*searchResult)
	assert.True(t, ok)
	assert.NoError(t, sr.err)
	assert.Equal(t, 1, len(sr.sealedSegmentSearched))
	assert.Equal(t, defaultSegmentID, sr.sealedSegmentSearched[0])
}

func TestHistoricalStage_TestRetrieve(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	his, err := genSimpleHistorical(ctx)
	assert.NoError(t, err)
	inputChan := make(chan queryMsg, queryBufferSize)
	outputChan := make(chan queryResult, queryBufferSize)

	localManager, err := genLocalChunkManager()
	assert.NoError(t, err)

	remoteManager, err := genRemoteChunkManager(ctx)
	assert.NoError(t, err)

	hs := newHistoricalStage(ctx, defaultCollectionID, inputChan, outputChan, his, localManager, remoteManager, false)
	go hs.start()

	// construct retrieveMsg
	retrieveReq, err := genSimpleRetrieveRequest()
	assert.NoError(t, err)
	plan, err := genSimpleRetrievePlan()
	assert.NoError(t, err)
	msg := &retrieveMsg{
		RetrieveMsg: &msgstream.RetrieveMsg{
			BaseMsg: msgstream.BaseMsg{
				HashValues: []uint32{0},
			},
			RetrieveRequest: *retrieveReq,
		},
		plan: plan,
	}

	go func() {
		inputChan <- msg
	}()

	//result check
	res := <-outputChan
	sr, ok := res.(*retrieveResult)
	assert.True(t, ok)
	assert.NoError(t, sr.err)
	assert.Equal(t, 1, len(sr.segmentRetrieved))
	assert.Equal(t, defaultSegmentID, sr.segmentRetrieved[0])
}
