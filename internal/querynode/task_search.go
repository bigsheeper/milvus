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
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"go.uber.org/zap"
)

const (
	maxTopKMergeRatio = 10.0
	maxNQ             = 10000
)

var _ sqTask = (*searchTask)(nil)

type searchTask struct {
	sqBaseTask
	iReq *internalpb.SearchRequest
	req  *querypb.SearchRequest

	//Dsl                string
	//DslType            commonpb.DslType
	//SerializedExprPlan []byte

	MetricType            string
	PlaceholderGroup      []byte
	OrigPlaceHolderGroups [][]byte
	NQ                    int64
	OrigNQs               []int64
	TopK                  int64
	OrigTopKs             []int64
	Ret                   *internalpb.SearchResults

	originTasks []*searchTask
}

func (s *searchTask) PreExecute(ctx context.Context) error {
	s.combinePlaceHolderGroups()
	return nil
}

func (s *searchTask) searchOnStreaming() error {
	// deserialize query plan
	// check if collection has been released
	collection, err := s.QS.streaming.replica.getCollectionByID(s.CollectionID)
	if err != nil {
		return err
	}
	var plan *SearchPlan
	if s.iReq.GetDslType() == commonpb.DslType_BoolExprV1 {
		expr := s.iReq.SerializedExprPlan
		plan, err = createSearchPlanByExpr(collection, expr)
		if err != nil {
			return err
		}
	} else {
		dsl := s.iReq.GetDsl()
		plan, err = createSearchPlan(collection, dsl)
		if err != nil {
			return err
		}
	}
	defer plan.delete()

	// validate top-k
	topK := plan.getTopK()
	if topK <= 0 || topK >= 16385 {
		return fmt.Errorf("limit should be in range [1, 16385], but got %d", topK)
	}

	// parse plan to search request
	searchReq, err := parseSearchRequest(plan, s.iReq.GetPlaceholderGroup())
	if err != nil {
		return err
	}
	defer searchReq.delete()
	queryNum := searchReq.getNumOfQuery()
	searchRequests := []*searchRequest{searchReq}

	// TODO add context
	sResults, _, _, sErr := s.QS.streaming.search(searchRequests, s.CollectionID, s.iReq.GetPartitionIDs(), s.req.GetDmlChannel(),
		plan, s.TravelTimestamp)

	if sErr != nil {
		log.Warn("failed to search streaming data", zap.Int64("collectionID", s.CollectionID), zap.Error(sErr))
		return sErr
	}

	var results []*internalpb.SearchResults
	var streamingResults []*SearchResult
	streamingResults = sResults

	defer deleteSearchResults(streamingResults)

	results = append(results, &internalpb.SearchResults{
		Status:         &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		MetricType:     plan.getMetricType(),
		NumQueries:     queryNum,
		TopK:           topK,
		SlicedBlob:     nil,
		SlicedOffset:   1,
		SlicedNumCount: 1,
	})

	if len(streamingResults) > 0 {
		// reduce search results
		numSegment := int64(len(streamingResults))
		err = reduceSearchResultsAndFillData(plan, streamingResults, numSegment)
		if err != nil {
			return err
		}

		//nq := searchRequests[0].getNumOfQuery()
		//nqOfReqs := []int64{nq}
		//nqPerSlice := nq
		//reqSlices, err := getReqSlices(nqOfReqs, nqPerSlice)
		//if err != nil {
		//	log.Warn("getReqSlices for streaming results error", zap.Error(err))
		//	return  err
		//}

		sInfo := parseSliceInfo(s.OrigNQs, s.OrigTopKs, s.NQ)
		blobs, err := marshal(s.CollectionID, s.ID(), streamingResults, int(numSegment), sInfo.sliceNQs, sInfo.sliceTopKs)

		//blobs, err := marshal(s.CollectionID, 0, streamingResults, int(numSegment), reqSlices)
		defer deleteSearchResultDataBlobs(blobs)
		if err != nil {
			log.Warn("marshal for streaming results error", zap.Error(err))
			return err
		}

		// assume only one blob will be sent back
		blob, err := getSearchResultDataBlob(blobs, 0)
		if err != nil {
			log.Warn("getSearchResultDataBlob for streaming results error", zap.Error(err))
		}

		results[len(results)-1].SlicedBlob = blob
	}

	// reduce shard search results: unmarshal -> reduce -> marshal
	searchResultData, err := decodeSearchResults(results)
	if err != nil {
		log.Warn("shard leader decode search results errors", zap.Error(err))
		return err
	}
	log.Debug("shard leader get valid search results", zap.Int("numbers", len(searchResultData)))

	for i, sData := range searchResultData {
		log.Debug("reduceSearchResultData",
			zap.Int("result No.", i),
			zap.Int64("nq", sData.NumQueries),
			zap.Int64("topk", sData.TopK),
			zap.Int64s("topks", sData.Topks))
	}

	reducedResultData, err := reduceSearchResultData(searchResultData, queryNum, plan.getTopK())
	if err != nil {
		log.Warn("shard leader reduce errors", zap.Error(err))
		return err
	}
	s.Ret, err = encodeSearchResultData(reducedResultData, queryNum, plan.getTopK(), plan.getMetricType())
	if err != nil {
		log.Warn("shard leader encode search result errors", zap.Error(err))
		return err
	}
	return nil
}

func (s *searchTask) searchOnHistorical() error {

	// check ctx timeout
	if !funcutil.CheckCtxValid(s.Ctx()) {
		return errors.New("search context timeout")
	}
	segmentIDs := s.req.GetSegmentIDs()

	// lock historic meta-replica
	s.QS.historical.replica.queryRLock()
	defer s.QS.historical.replica.queryRUnlock()

	// check if collection has been released
	collection, err := s.QS.historical.replica.getCollectionByID(s.CollectionID)
	if err != nil {
		return err
	}
	if s.GuaranteeTimestamp >= collection.getReleaseTime() {
		log.Warn("collection release before search", zap.Int64("collectionID", s.CollectionID))
		return fmt.Errorf("retrieve failed, collection has been released, collectionID = %d", s.CollectionID)
	}

	// deserialize query plan
	var plan *SearchPlan
	if s.iReq.GetDslType() == commonpb.DslType_BoolExprV1 {
		expr := s.iReq.GetSerializedExprPlan()
		plan, err = createSearchPlanByExpr(collection, expr)
		if err != nil {
			return err
		}
	} else {
		dsl := s.iReq.GetDsl()
		plan, err = createSearchPlan(collection, dsl)
		if err != nil {
			return err
		}
	}
	defer plan.delete()

	// validate top-k
	topK := plan.getTopK()
	if topK <= 0 || topK >= 16385 {
		return fmt.Errorf("limit should be in range [1, 16385], but got %d", topK)
	}

	// parse plan to search request
	searchReq, err := parseSearchRequest(plan, s.PlaceholderGroup)
	if err != nil {
		return err
	}
	defer searchReq.delete()
	queryNum := searchReq.getNumOfQuery()
	searchRequests := []*searchRequest{searchReq}

	err = s.QS.historical.validateSegmentIDs(segmentIDs, s.CollectionID, s.iReq.GetPartitionIDs())
	if err != nil {
		log.Warn("segmentIDs in search request fails validation", zap.Int64s("segmentIDs", segmentIDs))
		return err
	}

	historicalResults, _, err := s.QS.historical.searchSegments(segmentIDs, searchRequests, plan, s.TravelTimestamp)
	if err != nil {
		return err
	}
	defer deleteSearchResults(historicalResults)

	// reduce search results
	numSegment := int64(len(historicalResults))
	err = reduceSearchResultsAndFillData(plan, historicalResults, numSegment)
	if err != nil {
		return err
	}

	sInfo := parseSliceInfo(s.OrigNQs, s.OrigTopKs, s.NQ)
	blobs, err := marshal(s.CollectionID, s.ID(), historicalResults, int(numSegment), sInfo.sliceNQs, sInfo.sliceTopKs)

	//blobs, err := marshal(s.CollectionID, 0, historicalResults, int(numSegment), reqSlices)
	defer deleteSearchResultDataBlobs(blobs)
	if err != nil {
		log.Warn("marshal for historical results error", zap.Error(err))
		return err
	}

	// assume only one blob will be sent back
	blob, err := getSearchResultDataBlob(blobs, 0)
	if err != nil {
		log.Warn("getSearchResultDataBlob for historical results error", zap.Error(err))
	}
	bs := make([]byte, len(blob))
	copy(bs, blob)

	s.Ret = &internalpb.SearchResults{
		Status:         &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		MetricType:     plan.getMetricType(),
		NumQueries:     queryNum,
		TopK:           topK,
		SlicedBlob:     bs,
		SlicedOffset:   1,
		SlicedNumCount: 1,
	}
	log.Debug("shard follower send search result to leader")
	return nil
}

func (s *searchTask) Execute(ctx context.Context) error {
	panic("not implemented")
}

func (s *searchTask) PostExecute(ctx context.Context) error {
	panic("not implemented")
}

func (s *searchTask) CanMergeWith(t sqTask) bool {
	s2, ok := t.(*searchTask)
	if !ok {
		return false
	}

	if s.DbID != s2.DbID {
		return false
	}

	if s.CollectionID != s2.CollectionID {
		return false
	}

	if s.iReq.GetDslType() != s2.iReq.GetDslType() {
		return false
	}

	if s.MetricType != s2.MetricType {
		return false
	}

	if !funcutil.SortedSliceEqual(s.iReq.GetPartitionIDs(), s2.iReq.GetPartitionIDs()) {
		return false
	}

	if !bytes.Equal(s.iReq.GetSerializedExprPlan(), s2.iReq.GetSerializedExprPlan()) {
		return false
	}

	if s.TravelTimestamp != s2.TravelTimestamp {
		return false
	}

	pre := s.NQ * s.TopK * 1.0
	newTopK := s.TopK
	if newTopK < s2.TopK {
		newTopK = s2.TopK
	}
	after := (s.NQ + s2.NQ) * newTopK * 1.0

	if pre == 0 {
		return false
	}
	if after/pre > maxTopKMergeRatio {
		return false
	}
	if s.NQ+s2.NQ > maxNQ {
		return false
	}
	return true
}

func (s *searchTask) Mergeable() bool {
	return true
}

func (s *searchTask) EstimateCpuUsage() int32 {
	// TODO according to number of segments
	ret := s.NQ * 5
	return int32(ret)
}

func (s *searchTask) Merge(t sqTask) {
	src, ok := t.(*searchTask)
	if !ok {
		return
	}
	newTopK := s.TopK
	if newTopK < src.TopK {
		newTopK = src.TopK
	}

	s.TopK = newTopK
	s.OrigTopKs = append(s.OrigTopKs, src.OrigTopKs...)
	s.OrigNQs = append(s.OrigNQs, src.OrigNQs...)
	s.OrigPlaceHolderGroups = append(s.OrigPlaceHolderGroups, src.OrigPlaceHolderGroups...)
	s.NQ += src.NQ
}

// combinePlaceHolderGroups combine all the placeholder groups.
func (s *searchTask) combinePlaceHolderGroups() {
	if len(s.OrigPlaceHolderGroups) > 1 {
		ret := &milvuspb.PlaceholderGroup{}
		//retValues := ret.Placeholders[0].GetValues()
		_ = proto.Unmarshal(s.PlaceholderGroup, ret)
		for i, grp := range s.OrigPlaceHolderGroups {
			if i == 0 {
				continue
			}
			x := &milvuspb.PlaceholderGroup{}
			_ = proto.Unmarshal(grp, x)
			ret.Placeholders[0].Values = append(ret.Placeholders[0].Values, x.Placeholders[0].Values...)
		}
		s.PlaceholderGroup, _ = proto.Marshal(ret)
	}
}

func newSearchTask(ctx context.Context, src *querypb.SearchRequest) *searchTask {
	target := &searchTask{
		sqBaseTask: sqBaseTask{
			baseTask: baseTask{
				done: make(chan error),
				ctx:  ctx,
				id:   src.Req.Base.GetMsgID(),
				ts:   src.Req.Base.GetTimestamp(),
			},
			DbID:               src.Req.GetReqID(),
			CollectionID:       src.Req.GetCollectionID(),
			TravelTimestamp:    src.Req.GetTravelTimestamp(),
			GuaranteeTimestamp: src.Req.GetGuaranteeTimestamp(),
			TimeoutTimestamp:   src.Req.GetTimeoutTimestamp(),
		},
		iReq:                  src.Req,
		req:                   src,
		TopK:                  src.Req.GetTopk(),
		OrigTopKs:             []int64{src.Req.GetTopk()},
		NQ:                    src.Req.GetNq(),
		OrigNQs:               []int64{src.Req.GetNq()},
		OrigPlaceHolderGroups: [][]byte{src.Req.GetPlaceholderGroup()},
	}
	return target
}
