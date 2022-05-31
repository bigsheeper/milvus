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
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
)

// Msg is an interface which has a function named TimeTick
type Msg = flowgraph.Msg

// MsgStreamMsg is an implementation of interface Msg
type MsgStreamMsg = flowgraph.MsgStreamMsg

// insertMsg is an implementation of interface Msg
type insertMsg struct {
	insertData *insertData
	deleteData *deleteData
	timeRange  TimeRange
}

// deleteMsg is an implementation of interface Msg
type deleteMsg struct {
	deleteData *deleteData
	timeRange  TimeRange
}

// serviceTimeMsg is an implementation of interface Msg
type serviceTimeMsg struct {
	timeRange TimeRange
}

// insertData stores the valid insert data
type insertData struct {
	insertIDs        map[UniqueID][]int64 // rowIDs
	insertTimestamps map[UniqueID][]Timestamp
	insertRecords    map[UniqueID][]*schemapb.FieldData
	insertOffset     map[UniqueID]int64
	insertPKs        map[UniqueID][]primaryKey // pks
	insertSegments   map[UniqueID]*Segment     // pks
}

// deleteData stores the valid delete data
type deleteData struct {
	deleteIDs        map[UniqueID][]primaryKey // pks
	deleteTimestamps map[UniqueID][]Timestamp
	deleteSegments   map[UniqueID]*Segment
	deleteOffset     map[UniqueID]int64
}

func newDeleteData() *deleteData {
	return &deleteData{
		deleteIDs:        make(map[UniqueID][]primaryKey),
		deleteTimestamps: make(map[UniqueID][]Timestamp),
		deleteSegments:   make(map[UniqueID]*Segment),
		deleteOffset:     make(map[UniqueID]int64),
	}
}

func newInsertData() *insertData {
	return &insertData{
		insertIDs:        make(map[UniqueID][]int64),
		insertTimestamps: make(map[UniqueID][]Timestamp),
		insertRecords:    make(map[UniqueID][]*schemapb.FieldData),
		insertOffset:     make(map[UniqueID]int64),
		insertPKs:        make(map[UniqueID][]primaryKey),
		insertSegments:   make(map[UniqueID]*Segment),
	}
}

// TimeTick returns timestamp of insertMsg
func (iMsg *insertMsg) TimeTick() Timestamp {
	return iMsg.timeRange.timestampMax
}

// TimeTick returns timestamp of deleteMsg
func (dMsg *deleteMsg) TimeTick() Timestamp {
	return dMsg.timeRange.timestampMax
}

// TimeTick returns timestamp of serviceTimeMsg
func (stMsg *serviceTimeMsg) TimeTick() Timestamp {
	return stMsg.timeRange.timestampMax
}
