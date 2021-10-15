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
	"github.com/milvus-io/milvus/internal/proto/datapb"
)

type segmentLoadTask struct {
	msgID     UniqueID
	loadInfos map[UniqueID]*segmentLoadInfo
}

type segmentLoadInfo struct {
	segmentID         UniqueID
	indexedFieldIDs   []FieldID
	fieldsBinLogPaths []*datapb.FieldBinlog
	segmentSize       int64
}

func newSegmentLoadTask(msgID UniqueID) *segmentLoadTask {
	return &segmentLoadTask{
		msgID:             msgID,
		indexedFieldIDs:   make([]FieldID, 0),
		fieldsBinLogPaths: make([]*datapb.FieldBinlog, 0),
		segmentSize:       0,
	}
}

func (s *segmentLoadTask) set()
