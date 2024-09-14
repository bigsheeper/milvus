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

package tsoutil

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParseHybridTs(t *testing.T) {
	var tss = []uint64{
		452470657037893633,
	}

	for _, ts := range tss {
		fmt.Println(PhysicalTime(ts))
	}
}

func Test_Tso(t *testing.T) {
	t.Run("test ComposeTSByTime", func(t *testing.T) {
		physical := time.Now()
		logical := int64(1000)
		timestamp := ComposeTSByTime(physical, logical)
		pRes, lRes := ParseTS(timestamp)
		assert.Equal(t, physical.Unix(), pRes.Unix())
		assert.Equal(t, uint64(logical), lRes)
	})
}

func TestCalculateDuration(t *testing.T) {
	now := time.Now()
	ts1 := ComposeTSByTime(now, 0)
	durationInMilliSecs := int64(20 * 1000)
	ts2 := ComposeTSByTime(now.Add(time.Duration(durationInMilliSecs)*time.Millisecond), 0)
	diff := CalculateDuration(ts2, ts1)
	assert.Equal(t, durationInMilliSecs, diff)
}

func TestAddPhysicalDurationOnTs(t *testing.T) {
	now := time.Now()
	ts1 := ComposeTSByTime(now, 0)
	duration := time.Millisecond * (20 * 1000)
	ts2 := AddPhysicalDurationOnTs(ts1, duration)
	ts3 := ComposeTSByTime(now.Add(duration), 0)
	// diff := CalculateDuration(ts2, ts1)
	assert.Equal(t, ts3, ts2)

	ts2 = AddPhysicalDurationOnTs(ts1, -duration)
	ts3 = ComposeTSByTime(now.Add(-duration), 0)
	// diff := CalculateDuration(ts2, ts1)
	assert.Equal(t, ts3, ts2)
}
