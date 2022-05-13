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

	"go.uber.org/zap"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/timerecord"
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

	MetricType            string
	PlaceholderGroup      []byte
	OrigPlaceHolderGroups [][]byte
	NQ                    int64
	OrigNQs               []int64
	TopK                  int64
	OrigTopKs             []int64
	Ret                   *internalpb.SearchResults
	originTasks           []*searchTask
}

func (s *searchTask) PreExecute(ctx context.Context) error {
	topK := s.TopK
	if topK <= 0 || topK >= 16385 {
		return fmt.Errorf("limit should be in range [1, 16385], but got %d", topK)
	}
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

	searchReq, err2 := newSearchRequest(collection, s.req, s.PlaceholderGroup)
	if err2 != nil {
		return err2
	}
	defer searchReq.delete()

	// TODO add context
	partResults, _, _, sErr := s.QS.streaming.search(searchReq, s.CollectionID, s.iReq.GetPartitionIDs(), s.req.GetDmlChannel())

	if sErr != nil {
		log.Warn("failed to search streaming data", zap.Int64("collectionID", s.CollectionID), zap.Error(sErr))
		return sErr
	}
	defer deleteSearchResults(partResults)
	return s.reduceResults(searchReq, partResults)
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

	searchReq, err2 := newSearchRequest(collection, s.req, s.PlaceholderGroup)
	if err2 != nil {
		return err2
	}
	defer searchReq.delete()
	err = s.QS.historical.validateSegmentIDs(segmentIDs, s.CollectionID, s.iReq.GetPartitionIDs())
	if err != nil {
		log.Warn("segmentIDs in search request fails validation", zap.Int64s("segmentIDs", segmentIDs))
		return err
	}

	partResults, _, err := s.QS.historical.searchSegments(searchReq, segmentIDs)
	fmt.Println("CC:", partResults)
	fmt.Println("CC-err:", partResults)
	if err != nil {
		return err
	}
	defer deleteSearchResults(partResults)
	return s.reduceResults(searchReq, partResults)
}

func (s *searchTask) Execute(ctx context.Context) error {
	if s.req.GetScope() == querypb.DataScope_Streaming {
		log.Info("searchTask Execute DataScope Streaming")
		return s.searchOnStreaming()
	} else if s.req.GetScope() == querypb.DataScope_Historical {
		log.Info("searchTask Execute DataScope Historical")
		return s.searchOnHistorical()
	}
	panic("searchTask not implement search on all data scope")
}

func (s *searchTask) SetErr(err error) {
	s.err = err
	for i := 1; i < len(s.originTasks); i++ {
		s.originTasks[i].SetErr(err)
	}
}

func (s *searchTask) Notify(err error) {
	s.done <- err
	for i := 1; i < len(s.originTasks); i++ {
		s.originTasks[i].Notify(err)
	}
}

// reduceResults reduce search results
func (s *searchTask) reduceResults(searchReq *searchRequest, results []*SearchResult) error {
	isEmpty := len(results) == 0
	if !isEmpty {
		fmt.Println("s.OrigNQs:", s.OrigNQs, ", s.OrigTopKs:", s.OrigTopKs, ", s.NQ:", s.NQ)
		sInfo := parseSliceInfo(s.OrigNQs, s.OrigTopKs, s.NQ)
		numSegment := int64(len(results))
		fmt.Println("sInfo::", sInfo)
		blobs, err := reduceSearchResultsAndFillData(searchReq.plan, results, numSegment, sInfo.sliceNQs, sInfo.sliceTopKs)
		if err != nil {
			return err
		}
		defer deleteSearchResultDataBlobs(blobs)
		if err != nil {
			log.Warn("marshal for historical results error", zap.Error(err))
			return err
		}
		for i := 0; i < len(s.originTasks); i++ {
			blob, err := getSearchResultDataBlob(blobs, i)
			if err != nil {
				log.Warn("getSearchResultDataBlob for historical results error", zap.Error(err))
				return err
			}
			bs := make([]byte, len(blob))
			copy(bs, blob)
			s.originTasks[i].Ret = &internalpb.SearchResults{
				Status:         &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
				MetricType:     s.MetricType,
				NumQueries:     s.OrigNQs[i],
				TopK:           s.OrigTopKs[i],
				SlicedBlob:     bs,
				SlicedOffset:   1,
				SlicedNumCount: 1,
			}
		}
	} else {
		for i := 0; i < len(s.originTasks); i++ {
			s.originTasks[i].Ret = &internalpb.SearchResults{
				Status:         &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
				MetricType:     s.MetricType,
				NumQueries:     s.OrigNQs[i],
				TopK:           s.OrigTopKs[i],
				SlicedBlob:     nil,
				SlicedOffset:   1,
				SlicedNumCount: 1,
			}
		}
	}
	return nil
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

/*
func (s *searchTask) CanDo() (bool, error) {
	if s.TopK <= 0 || s.TopK >= 16385 {
		return false, fmt.Errorf("limit should be in range [1, 16385], but got %d", s.TopK)
	}
	return s.sqBaseTask.CanDo()
}
*/

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
	s.originTasks = append(s.originTasks, src)
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
	fmt.Println("sheep >>>>>>>>>>>> src.Req.GetNq():", src.Req.GetNq())
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
			tr:                 timerecord.NewTimeRecorder("searchTask"),
			DataScope:          src.GetScope(),
		},
		iReq:                  src.Req,
		req:                   src,
		TopK:                  src.Req.GetTopk(),
		OrigTopKs:             []int64{src.Req.GetTopk()},
		NQ:                    src.Req.GetNq(),
		OrigNQs:               []int64{src.Req.GetNq()},
		OrigPlaceHolderGroups: [][]byte{src.Req.GetPlaceholderGroup()},
		PlaceholderGroup:      src.Req.GetPlaceholderGroup(),
		MetricType:            src.Req.GetMetricType(),
	}
	target.originTasks = append(target.originTasks, target)
	return target
}
