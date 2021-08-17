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
	"sync/atomic"

	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/internal/storage"
)

const queryBufferSize = 1024

type queryMsg interface {
	msgstream.TsMsg
	GuaranteeTs() Timestamp
	TravelTs() Timestamp
}

type searchMsg struct {
	*msgstream.SearchMsg
	plan *SearchPlan
	reqs []*searchRequest
}

type retrieveMsg struct {
	*msgstream.RetrieveMsg
	plan *RetrievePlan
}

type queryResult interface {
	ID() UniqueID
	Type() msgstream.MsgType
}

type retrieveResult struct {
	err              error
	msg              *retrieveMsg
	segmentRetrieved []UniqueID
	res              []*segcorepb.RetrieveResults
}

type searchResult struct {
	err                   error
	msg                   *searchMsg
	reqs                  []*searchRequest
	searchResults         []*SearchResult
	sealedSegmentSearched []UniqueID
}

type queryCollection struct {
	releaseCtx context.Context
	cancel     context.CancelFunc

	collectionID UniqueID
	historical   *historical
	streaming    *streaming

	queryMsgStream       msgstream.MsgStream
	queryResultMsgStream msgstream.MsgStream

	vcm *storage.VectorChunkManager

	inputStage       *inputStage
	loadBalanceStage *loadBalanceStage
	reqStage         *requestHandlerStage
	historicalStage  *historicalStage
	vChannelStages   map[Channel]*vChannelStage
	resStage         *resultHandlerStage

	channelNum   int32
	vChannelChan map[Channel]chan queryMsg
	resChan      chan queryResult
}

type ResultEntityIds []UniqueID

func newQueryCollection(releaseCtx context.Context,
	cancel context.CancelFunc,
	collectionID UniqueID,
	historical *historical,
	streaming *streaming,
	factory msgstream.Factory,
	lcm storage.ChunkManager,
	rcm storage.ChunkManager) (*queryCollection, error) {

	queryStream, _ := factory.NewQueryMsgStream(releaseCtx)
	queryResultStream, _ := factory.NewQueryMsgStream(releaseCtx)

	vcm := storage.NewVectorChunkManager(lcm, rcm)

	qc := &queryCollection{
		releaseCtx: releaseCtx,
		cancel:     cancel,

		collectionID: collectionID,
		historical:   historical,
		streaming:    streaming,

		queryMsgStream:       queryStream,
		queryResultMsgStream: queryResultStream,

		vcm: vcm,
	}

	// create query stages
	col, err := qc.streaming.replica.getCollectionByID(qc.collectionID)
	if err != nil {
		return nil, err
	}
	channels := col.getVChannels()

	lbChan := make(chan *msgstream.LoadBalanceSegmentsMsg, queryBufferSize)
	reqChan := make(chan queryMsg, queryBufferSize)
	iStage := newInputStage(qc.releaseCtx,
		qc.collectionID,
		qc.queryMsgStream,
		lbChan,
		reqChan)
	qc.inputStage = iStage
	lbStage := newLoadBalanceStage(qc.releaseCtx,
		qc.collectionID,
		lbChan)
	qc.loadBalanceStage = lbStage

	hisChan := make(chan queryMsg, queryBufferSize)
	qc.vChannelChan = make(map[Channel]chan queryMsg)
	for _, c := range channels {
		qc.vChannelChan[c] = make(chan queryMsg, queryBufferSize)
	}
	reqStage := newRequestHandlerStage(qc.releaseCtx,
		qc.collectionID,
		reqChan,
		hisChan,
		qc.vChannelChan,
		qc.streaming,
		qc.historical,
		qc.queryResultMsgStream)
	qc.reqStage = reqStage
	// expand channel's capacity is not allowed
	qc.resChan = make(chan queryResult, queryBufferSize*(len(channels)+1)) // vChannels + historical
	hisStage := newHistoricalStage(qc.releaseCtx,
		qc.collectionID,
		hisChan,
		qc.resChan,
		qc.historical,
		qc.vcm)
	qc.historicalStage = hisStage

	vcStages := make(map[Channel]*vChannelStage)
	for _, c := range channels {
		vcStages[c] = newVChannelStage(qc.releaseCtx,
			qc.collectionID,
			c,
			qc.vChannelChan[c],
			qc.resChan,
			qc.streaming)
	}
	qc.vChannelStages = vcStages
	qc.channelNum = int32(len(channels))
	resStage := newResultHandlerStage(qc.releaseCtx,
		qc.collectionID,
		qc.streaming,
		qc.historical,
		qc.resChan,
		qc.queryResultMsgStream,
		&qc.channelNum)
	qc.resStage = resStage

	return qc, nil
}

func (q *queryCollection) start() {
	q.queryMsgStream.Start()
	q.queryResultMsgStream.Start()

	// start stages
	go q.inputStage.start()
	go q.loadBalanceStage.start()
	go q.reqStage.start()
	go q.historicalStage.start()
	for _, s := range q.vChannelStages {
		go s.start()
	}
	go q.resStage.start()
}

func (q *queryCollection) close() {
	if q.queryMsgStream != nil {
		q.queryMsgStream.Close()
	}
	if q.queryResultMsgStream != nil {
		q.queryResultMsgStream.Close()
	}
}

// vChannel stage management
func (q *queryCollection) addVChannelStage(channel Channel) error {
	if _, ok := q.vChannelStages[channel]; ok {
		return errors.New("vChannelStage has been existed, collectionID = " + fmt.Sprintln(q.collectionID) +
			", vChannel = " + fmt.Sprintln(channel))
	}
	q.vChannelChan[channel] = make(chan queryMsg, queryBufferSize)
	stage := newVChannelStage(q.releaseCtx,
		q.collectionID,
		channel,
		q.vChannelChan[channel],
		q.resChan,
		q.streaming)

	q.vChannelStages[channel] = stage
	atomic.AddInt32(&q.channelNum, 1)
	go stage.start()
	return nil
}

func (q *queryCollection) removeVChannelStage(channel Channel) {
	delete(q.vChannelStages, channel)
	delete(q.vChannelChan, channel)
	atomic.AddInt32(&q.channelNum, -1)
}

// result functions
func (r *retrieveResult) Type() msgstream.MsgType {
	return commonpb.MsgType_Retrieve
}

func (s *searchResult) Type() msgstream.MsgType {
	return commonpb.MsgType_Search
}

func (r *retrieveResult) ID() UniqueID {
	return r.msg.ID()
}

func (s *searchResult) ID() UniqueID {
	return s.msg.ID()
}
