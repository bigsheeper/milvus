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
	"fmt"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type sqTask interface {
	task

	SetErr(error)
	GetErr() error
	Ctx() context.Context

	GetTimeRecorder() *timerecord.TimeRecorder
	GetCollectionID() UniqueID

	CanDo() (bool, error)
	Merge(sqTask)
	CanMergeWith(sqTask) bool
	Mergeable() bool
	EstimateCpuUsage() int32 //
}

var _ sqTask = (*sqBaseTask)(nil)

type sqBaseTask struct {
	baseTask

	QS *queryShard

	DbID               int64
	CollectionID       int64
	DataScope          querypb.DataScope
	TravelTimestamp    uint64
	GuaranteeTimestamp uint64
	TimeoutTimestamp   uint64
	tr                 *timerecord.TimeRecorder
	err                error
}

func (s *sqBaseTask) Execute(ctx context.Context) error {
	return nil
}

func (s *sqBaseTask) SetErr(err error) {
	s.err = err
}

func (s *sqBaseTask) GetErr() error {
	return s.err
}

// GetCollectionID return CollectionID.
func (s *sqBaseTask) GetCollectionID() UniqueID {
	return s.CollectionID
}

func (s *sqBaseTask) GetTimeRecorder() *timerecord.TimeRecorder {
	return s.tr
}

func (s *sqBaseTask) Timeout() bool {
	curTime := tsoutil.GetCurrentTime()
	curTimePhysical, _ := tsoutil.ParseTS(curTime)
	timeoutTsPhysical, _ := tsoutil.ParseTS(s.TimeoutTimestamp)
	log.Debug("check if query timeout",
		zap.Int64("collectionID", s.CollectionID),
		zap.Int64("taskID", s.ID()),
		zap.Uint64("TimeoutTs", s.TimeoutTimestamp),
		zap.Uint64("curTime", curTime),
		zap.Time("timeoutTsPhysical", timeoutTsPhysical),
		zap.Time("curTimePhysical", curTimePhysical),
	)
	return s.TimeoutTimestamp > typeutil.ZeroTimestamp && curTime >= s.TimeoutTimestamp
}

func (s *sqBaseTask) Mergeable() bool {
	return false
}

func (s *sqBaseTask) CanMergeWith(t sqTask) bool {
	return false
}

func (s *sqBaseTask) Merge(t sqTask) {
	return
}

func (s *sqBaseTask) EstimateCpuUsage() int32 {
	return 0
}

func (s *sqBaseTask) CanDo() (bool, error) {
	var collection *Collection
	var err error
	var tType tsType
	if s.DataScope == querypb.DataScope_Streaming {
		tType = tsTypeDML
		collection, err = s.QS.streaming.replica.getCollectionByID(s.CollectionID)
	} else if s.DataScope == querypb.DataScope_Historical {
		tType = tsTypeDelta
		collection, err = s.QS.historical.replica.getCollectionByID(s.CollectionID)
	}
	if err != nil {
		return false, err
	}

	guaranteeTs := s.GuaranteeTimestamp
	if guaranteeTs >= collection.getReleaseTime() {
		err = fmt.Errorf("collection has been released, taskID = %d, collectionID = %d", s.ID(), s.CollectionID)
		return false, err
	}

	serviceTime := s.QS.getServiceableTime(tType)
	gt, _ := tsoutil.ParseTS(guaranteeTs)
	st, _ := tsoutil.ParseTS(serviceTime)
	if guaranteeTs > serviceTime {
		log.Debug("query msg can't do",
			zap.Any("collectionID", s.CollectionID),
			zap.Any("sm.GuaranteeTimestamp", gt),
			zap.Any("serviceTime", st),
			zap.Any("delta seconds", (guaranteeTs-serviceTime)/(1000*1000*1000)),
			zap.Any("msgID", s.ID()))
		return false, nil
	}
	return true, nil
}
