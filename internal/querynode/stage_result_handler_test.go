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
)

func TestResultHandlerStage_ResultHandlerStage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, err := genSimpleStreaming(ctx)
	assert.NoError(t, err)
	h, err := genSimpleHistorical(ctx)
	assert.NoError(t, err)

	inputChan := make(chan queryResult, queryBufferSize)
	stream, err := genQueryMsgStream(ctx)
	assert.NoError(t, err)
	stream.AsProducer([]string{defaultQueryResultChannel})
	stream.Start()

	resStage := newResultHandlerStage(ctx,
		defaultCollectionID,
		s,
		h,
		inputChan,
		stream,
		0)
	go resStage.start()

	resMsg, err := genSimpleSearchResult()
	assert.NoError(t, err)
	go func() {
		inputChan <- resMsg
	}()

	res, err := consumeSimpleSearchResult(ctx)
	assert.NoError(t, err)
	assert.Equal(t, defaultTopK, len(res.Hits))
	assert.Equal(t, 0, len(res.ChannelIDsSearched))
	assert.Equal(t, 1, len(res.SealedSegmentIDsSearched))
	assert.Equal(t, defaultSegmentID, res.SealedSegmentIDsSearched[0])
}
