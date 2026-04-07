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

package compaction

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
)

func TestEntityFilterTaskSuite(t *testing.T) {
	suite.Run(t, new(EntityFilterSuite))
}

type EntityFilterSuite struct {
	suite.Suite
}

func (s *EntityFilterSuite) TestEntityFilterByTTL() {
	milvusBirthday := getMilvusBirthday()

	tests := []struct {
		description string
		collTTL     int64
		nowTime     time.Time
		entityTime  time.Time

		expect bool
	}{
		// ttl == maxInt64, dur is 1hour, no entities should expire
		{"ttl=maxInt64, now<entity", math.MaxInt64, milvusBirthday, milvusBirthday.Add(time.Hour), false},
		{"ttl=maxInt64, now>entity", math.MaxInt64, milvusBirthday, milvusBirthday.Add(-time.Hour), false},
		{"ttl=maxInt64, now==entity", math.MaxInt64, milvusBirthday, milvusBirthday, false},
		// ttl == 0, no entities should expire
		{"ttl=0, now==entity", 0, milvusBirthday, milvusBirthday, false},
		{"ttl=0, now>entity", 0, milvusBirthday, milvusBirthday.Add(-time.Hour), false},
		{"ttl=0, now<entity", 0, milvusBirthday, milvusBirthday.Add(time.Hour), false},
		// ttl == 10days
		{"ttl=10days, nowTs-entityTs>10days", 864000000000000, milvusBirthday.AddDate(0, 0, 11), milvusBirthday, true},
		{"ttl=10days, nowTs-entityTs==10days", 864000000000000, milvusBirthday.AddDate(0, 0, 10), milvusBirthday, true},
		{"ttl=10days, nowTs-entityTs<10days", 864000000000000, milvusBirthday.AddDate(0, 0, 9), milvusBirthday, false},
		// ttl is maxInt64
		{"ttl=maxInt64, nowTs-entityTs>1000years", math.MaxInt64, milvusBirthday.AddDate(1000, 0, 11), milvusBirthday, true},
		{"ttl=maxInt64, nowTs-entityTs==1000years", math.MaxInt64, milvusBirthday.AddDate(1000, 0, 0), milvusBirthday, true},
		{"ttl=maxInt64, nowTs-entityTs==240year", math.MaxInt64, milvusBirthday.AddDate(240, 0, 0), milvusBirthday, false},
		{"ttl=maxInt64, nowTs-entityTs==maxDur", math.MaxInt64, milvusBirthday.Add(math.MaxInt64), milvusBirthday, true},
		{"ttl<maxInt64, nowTs-entityTs==1000years", math.MaxInt64 - 1, milvusBirthday.AddDate(1000, 0, 0), milvusBirthday, true},
	}
	for _, test := range tests {
		s.Run(test.description, func() {
			filter := newEntityFilter(nil, test.collTTL, test.nowTime, 0)

			entityTs := tsoutil.ComposeTSByTime(test.entityTime, 0)
			got := filter.Filtered("mockpk", entityTs, -1)
			s.Equal(test.expect, got)

			if got {
				s.Equal(1, filter.GetExpiredCount())
				s.Equal(0, filter.GetDeletedCount())
			} else {
				s.Equal(0, filter.GetExpiredCount())
				s.Equal(0, filter.GetDeletedCount())
			}
		})
	}
}

// TestEntityFilterByTTLWithCommitTs verifies that import/CDC segments (commitTs != 0)
// are protected from premature TTL expiry caused by stale row timestamps.
func (s *EntityFilterSuite) TestEntityFilterByTTLWithCommitTs() {
	// Entity was written 5 years ago (outdated timestamp).
	staleEntityTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	staleEntityTs := tsoutil.ComposeTSByTime(staleEntityTime, 0)

	// Collection TTL is 1 year.
	ttlOneYear := int64(365 * 24 * time.Hour)

	// Current time is 2026 — stale row would appear expired (2026-2020 = 6 years > 1 year TTL).
	nowTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	s.Run("without commitTs: stale row expires (baseline)", func() {
		filter := newEntityFilter(nil, ttlOneYear, nowTime, 0)
		got := filter.Filtered("pk1", staleEntityTs, -1)
		s.True(got, "stale row should expire without commitTs protection")
	})

	s.Run("with commitTs recent enough: stale row is protected", func() {
		// commitTs is 6 months ago — well within TTL.
		recentCommitTime := nowTime.AddDate(0, -6, 0)
		commitTs := tsoutil.ComposeTSByTime(recentCommitTime, 0)
		filter := newEntityFilter(nil, ttlOneYear, nowTime, commitTs)
		got := filter.Filtered("pk1", staleEntityTs, -1)
		s.False(got, "row should not expire when commitTs is within TTL window")
	})

	s.Run("with commitTs also expired: row expires", func() {
		// commitTs is also 2 years ago — beyond TTL.
		expiredCommitTime := nowTime.AddDate(-2, 0, 0)
		commitTs := tsoutil.ComposeTSByTime(expiredCommitTime, 0)
		filter := newEntityFilter(nil, ttlOneYear, nowTime, commitTs)
		got := filter.Filtered("pk1", staleEntityTs, -1)
		s.True(got, "row should expire when both row_ts and commitTs are beyond TTL")
	})

	s.Run("with commitTs: ttl_field expiration still applies", func() {
		// Per-row TTL field claims expiration 1 day ago. TTL field represents
		// user intent and should be honored regardless of commitTs.
		expiredTimeMicros := nowTime.Add(-24 * time.Hour).UnixMicro()
		recentCommitTime := nowTime.AddDate(0, -6, 0)
		commitTs := tsoutil.ComposeTSByTime(recentCommitTime, 0)
		filter := newEntityFilter(nil, ttlOneYear, nowTime, commitTs)
		got := filter.Filtered("pk1", tsoutil.ComposeTSByTime(nowTime.AddDate(0, -6, 0), 0), expiredTimeMicros)
		s.True(got, "ttl_field expiration should apply even when commitTs is set")
	})

	s.Run("without commitTs: ttl_field expiration applies", func() {
		expiredTimeMicros := nowTime.Add(-24 * time.Hour).UnixMicro()
		filter := newEntityFilter(nil, ttlOneYear, nowTime, 0)
		// Use a recent entity ts so TTL by timestamp alone would NOT expire it.
		recentTs := tsoutil.ComposeTSByTime(nowTime.AddDate(0, -6, 0), 0)
		got := filter.Filtered("pk1", recentTs, expiredTimeMicros)
		s.True(got, "ttl_field should expire the row when commitTs is 0")
	})
}

func getMilvusBirthday() time.Time {
	return time.Date(2019, time.Month(5), 30, 0, 0, 0, 0, time.UTC)
}
