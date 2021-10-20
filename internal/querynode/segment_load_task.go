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

//import (
//	"github.com/milvus-io/milvus/internal/proto/datapb"
//)
//
//type segmentLoadTask struct {
//	msgID     UniqueID
//	loadInfos map[UniqueID]*segmentLoadInfo
//}
//
//type segmentLoadInfo struct {
//	segmentID         UniqueID
//	indexedFieldIDs   []FieldID
//	fieldsBinLogPaths []*datapb.FieldBinlog
//	segmentSize       int64
//}
//
//func newSegmentLoadTask(msgID UniqueID) *segmentLoadTask {
//	return &segmentLoadTask{
//		msgID:             msgID,
//		indexedFieldIDs:   make([]FieldID, 0),
//		fieldsBinLogPaths: make([]*datapb.FieldBinlog, 0),
//		segmentSize:       0,
//	}
//}
//
//func (s *segmentLoadTask) set()

//func (loader *segmentLoader) getFieldAndIndexInfo(segment *Segment,
//	segmentLoadInfo *querypb.SegmentLoadInfo) ([]*datapb.FieldBinlog, []FieldID, error) {
//	collectionID := segment.collectionID
//	vectorFieldIDs, err := loader.historicalReplica.getVecFieldIDsByCollectionID(collectionID)
//	if err != nil {
//		return nil, nil, err
//	}
//	if len(vectorFieldIDs) <= 0 {
//		return nil, nil, fmt.Errorf("no vector field in collection %d", collectionID)
//	}
//
//	// add VectorFieldInfo for vector fields
//	for _, fieldBinlog := range segmentLoadInfo.BinlogPaths {
//		if funcutil.SliceContain(vectorFieldIDs, fieldBinlog.FieldID) {
//			vectorFieldInfo := newVectorFieldInfo(fieldBinlog)
//			segment.setVectorFieldInfo(fieldBinlog.FieldID, vectorFieldInfo)
//		}
//	}
//
//	indexedFieldIDs := make([]FieldID, 0)
//	for _, vecFieldID := range vectorFieldIDs {
//		err = loader.indexLoader.setIndexInfo(collectionID, segment, vecFieldID)
//		if err != nil {
//			log.Warn(err.Error())
//			continue
//		}
//		indexedFieldIDs = append(indexedFieldIDs, vecFieldID)
//	}
//
//	// we don't need to load raw data for indexed vector field
//	fieldBinlogs := loader.filterFieldBinlogs(segmentLoadInfo.BinlogPaths, indexedFieldIDs)
//	return fieldBinlogs, indexedFieldIDs, nil
//}
//
//func (loader *segmentLoader) estimateSegmentSize(segment *Segment,
//	fieldBinLogs []*datapb.FieldBinlog,
//	indexFieldIDs []FieldID) (int64, error) {
//	segmentSize := int64(0)
//	// get fields data size, if len(indexFieldIDs) == 0, vector field would be involved in fieldBinLogs
//	for _, fb := range fieldBinLogs {
//		log.Debug("estimate segment fields size",
//			zap.Any("collectionID", segment.collectionID),
//			zap.Any("segmentID", segment.ID()),
//			zap.Any("fieldID", fb.FieldID),
//			zap.Any("paths", fb.Binlogs),
//		)
//		for _, path := range fb.Binlogs {
//			logSize, err := storage.EstimateMemorySize(nil, path)
//			if err != nil {
//				return 0, err
//			}
//			segmentSize += logSize
//		}
//	}
//
//	// // get index size
//	// for _, fieldID := range indexFieldIDs {
//	// 	indexSize, err := loader.indexLoader.estimateIndexBinlogSize(segment, fieldID)
//	// }
//
//	log.Debug("loading bloom filter...")
//	err := loader.loadSegmentBloomFilter(segment)
//	if err != nil {
//		return 0, err
//	}
//	for _, id := range indexedFieldIDs {
//		log.Debug("loading index...")
//		err = loader.indexLoader.loadIndex(segment, id)
//		if err != nil {
//			return 0, err
//		}
//		segmentSize += indexSize
//	}
//	return segmentSize, nil
//}
