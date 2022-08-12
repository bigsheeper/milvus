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

package ratecollector

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/internalpb"
)

func TestRateCollector(t *testing.T) {
	t.Run("test newRateCollector", func(t *testing.T) {
		_, err := NewRateCollector(DefaultWindow, DefaultGranularity)
		assert.NoError(t, err)

		_, err = NewRateCollector(0, DefaultGranularity)
		assert.Error(t, err)

		_, err = NewRateCollector(DefaultWindow, 0)
		assert.Error(t, err)

		_, err = NewRateCollector(1*time.Second, 2*time.Second)
		assert.Error(t, err)

		_, err = NewRateCollector(10*time.Second, 3*time.Second)
		assert.Error(t, err)
	})

	t.Run("test start stop and register", func(t *testing.T) {
		rc, err := NewRateCollector(DefaultWindow, DefaultGranularity)
		assert.NoError(t, err)
		rc.Register(internalpb.RateType_DMLInsert.String())
		rc.Start()
		rc.Stop()
	})

	t.Run("test add and get", func(t *testing.T) {
		rc, err := NewRateCollector(DefaultWindow, DefaultGranularity)
		assert.NoError(t, err)
		rt := internalpb.RateType_DMLInsert.String()
		rc.Register(rt)
		unregisterRt := internalpb.RateType_DQLSearch.String()

		// test add
		rc.Add(rt, 10)
		rc.position++
		rc.Add(rt, 20)

		// test avg
		_, err = rc.Avg(unregisterRt)
		assert.Error(t, err)
		v, err := rc.Avg(rt)
		assert.NoError(t, err)
		assert.Equal(t, float64((10+20)/(DefaultWindow/DefaultGranularity)), v)

		// test max
		_, err = rc.Max(unregisterRt)
		assert.Error(t, err)
		v, err = rc.Max(rt)
		assert.NoError(t, err)
		assert.EqualValues(t, float64(20), v)

		// test min
		_, err = rc.Min(unregisterRt)
		assert.Error(t, err)
		v, err = rc.Min(rt)
		assert.NoError(t, err)
		assert.EqualValues(t, float64(0), v)

		// test newest
		_, err = rc.Newest(unregisterRt)
		assert.Error(t, err)
		v, err = rc.Newest(rt)
		assert.NoError(t, err)
		assert.EqualValues(t, float64(20), v)
	})
}
