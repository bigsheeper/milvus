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

/*
#cgo CFLAGS: -I${SRCDIR}/../core/output/include
#cgo darwin LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_segcore -Wl,-rpath,"${SRCDIR}/../core/output/lib"
#cgo linux LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_segcore -Wl,-rpath=${SRCDIR}/../core/output/lib

#include "segcore/plan_c.h"
#include "segcore/reduce_c.h"

*/
import "C"
import (
	"errors"
	"fmt"
	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"
	"unsafe"
)

// SearchResult contains a pointer to the search result in C++ memory
type SearchResult struct {
	cSearchResult C.CSearchResult
}

// searchResultDataBlobs is the CSearchResultsDataBlobs in C++
type searchResultDataBlobs = C.CSearchResultDataBlobs

// MarshaledHits contains a pointer to the marshaled hits in C++ memory
type MarshaledHits struct {
	cMarshaledHits C.CMarshaledHits
}

// RetrieveResult contains a pointer to the retrieve result in C++ memory
type RetrieveResult struct {
	cRetrieveResult C.CRetrieveResult
}

func reduceSearchResultsAndFillData(plan *SearchPlan, searchResults []*SearchResult, numSegments int64) error {
	if plan.cSearchPlan == nil {
		return errors.New("nil search plan")
	}

	cSearchResults := make([]C.CSearchResult, 0)
	for _, res := range searchResults {
		cSearchResults = append(cSearchResults, res.cSearchResult)
	}
	cSearchResultPtr := (*C.CSearchResult)(&cSearchResults[0])
	cNumSegments := C.int64_t(numSegments)

	status := C.ReduceSearchResultsAndFillData(plan.cSearchPlan, cSearchResultPtr, cNumSegments)
	if err := HandleCStatus(&status, "ReduceSearchResultsAndFillData failed"); err != nil {
		return err
	}
	return nil
}

func marshal(collectionID UniqueID, msgID UniqueID,
	searchResults []*SearchResult, numSegments int,
	nqOfReqs []int, nqPerSlice int) (*searchResultDataBlobs, error) {
	if nqPerSlice == 0 {
		return nil, fmt.Errorf("zero nqPerSlice is not allowed")
	}

	cSearchResults := make([]C.CSearchResult, 0)
	for _, res := range searchResults {
		cSearchResults = append(cSearchResults, res.cSearchResult)
	}
	cSearchResultPtr := (*C.CSearchResult)(&cSearchResults[0])

	slices := make([]int32, 0)
	for i := 0; i < len(nqOfReqs); i++ {
		for j := 0; j < nqOfReqs[i]/nqPerSlice; j++ {
			slices = append(slices, int32(nqPerSlice))
		}
		if tailSliceSize := nqOfReqs[i] % nqPerSlice; tailSliceSize > 0 {
			slices = append(slices, int32(tailSliceSize))
		}
	}

	log.Debug("start marshal...",
		zap.Int64("collectionID", collectionID),
		zap.Int64("msgID", msgID),
		zap.Int32s("slices", slices))

	var cNumSegments = C.int32_t(numSegments)
	var cSlicesPtr = (*C.int32_t)(&slices[0])
	var cNumSlices = C.int32_t(len(slices))

	var cSearchResultDataBlobs searchResultDataBlobs

	status := C.Marshal(&cSearchResultDataBlobs, cSearchResultPtr, cNumSegments, cSlicesPtr, cNumSlices)
	if err := HandleCStatus(&status, "ReorganizeSearchResults failed"); err != nil {
		return nil, err
	}
	return &cSearchResultDataBlobs, nil
}

func getNumSearchResultDataBlobs(cSearchResultsDataBlobs *searchResultDataBlobs) int {
	return int(cSearchResultsDataBlobs.num_blobs)
}

func getSearchResultDataBlob(cSearchResultDataBlobs *searchResultDataBlobs, blobIndex int) ([]byte, error) {
	var blob C.CProto
	status := C.GetSearchResultDataBlob(&blob, cSearchResultDataBlobs, C.int32_t(blobIndex))
	if err := HandleCStatus(&status, "marshal failed"); err != nil {
		return nil, err
	}
	// TODO: prevent copy?
	return GetCProtoBlob(&blob), nil
}

func deleteSearchResultDataBlobs(cSearchResultDataBlobs *searchResultDataBlobs) {
	C.DeleteSearchResultDataBlobs(cSearchResultDataBlobs)
}

// deprecated
func reorganizeSearchResults(searchResults []*SearchResult, numSegments int64) (*MarshaledHits, error) {
	cSearchResults := make([]C.CSearchResult, 0)
	for _, res := range searchResults {
		cSearchResults = append(cSearchResults, res.cSearchResult)
	}
	cSearchResultPtr := (*C.CSearchResult)(&cSearchResults[0])

	var cNumSegments = C.int64_t(numSegments)
	var cMarshaledHits C.CMarshaledHits

	status := C.ReorganizeSearchResults(&cMarshaledHits, cSearchResultPtr, cNumSegments)
	if err := HandleCStatus(&status, "ReorganizeSearchResults failed"); err != nil {
		return nil, err
	}
	return &MarshaledHits{cMarshaledHits: cMarshaledHits}, nil
}

// deprecated
func (mh *MarshaledHits) getHitsBlobSize() int64 {
	res := C.GetHitsBlobSize(mh.cMarshaledHits)
	return int64(res)
}

// deprecated
func (mh *MarshaledHits) getHitsBlob() ([]byte, error) {
	byteSize := mh.getHitsBlobSize()
	result := make([]byte, byteSize)
	cResultPtr := unsafe.Pointer(&result[0])
	C.GetHitsBlob(mh.cMarshaledHits, cResultPtr)
	return result, nil
}

// deprecated
func (mh *MarshaledHits) hitBlobSizeInGroup(groupOffset int64) ([]int64, error) {
	cGroupOffset := (C.int64_t)(groupOffset)
	numQueries := C.GetNumQueriesPerGroup(mh.cMarshaledHits, cGroupOffset)
	result := make([]int64, int64(numQueries))
	cResult := (*C.int64_t)(&result[0])
	C.GetHitSizePerQueries(mh.cMarshaledHits, cGroupOffset, cResult)
	return result, nil
}

// deprecated
func deleteMarshaledHits(hits *MarshaledHits) {
	C.DeleteMarshaledHits(hits.cMarshaledHits)
}

func deleteSearchResults(results []*SearchResult) {
	for _, result := range results {
		C.DeleteSearchResult(result.cSearchResult)
	}
}
