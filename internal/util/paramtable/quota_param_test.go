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

package paramtable

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestQuotaParam(t *testing.T) {
	qc := quotaConfig{}
	qc.init(&baseParams)

	t.Run("test quota", func(t *testing.T) {
		assert.True(t, qc.EnableQuotaAndLimits)
		assert.Equal(t, int64(1000), qc.QuotaCenterCollectInterval)
	})

	t.Run("test ddl", func(t *testing.T) {
		assert.Equal(t, float64(10), qc.DDLCollectionRate)
		assert.Equal(t, float64(10), qc.DDLPartitionRate)
		assert.Equal(t, float64(10), qc.DDLIndexRate)
		assert.Equal(t, float64(10), qc.DDLSegmentsRate)
	})

	t.Run("test dml", func(t *testing.T) {
		assert.Equal(t, megaBytesRate2Bytes(64), qc.DMLInsertRate)
		assert.Equal(t, megaBytesRate2Bytes(1), qc.DMLDeleteRate)
		assert.Equal(t, int(megaBytesRate2Bytes(64)), qc.MaxInsertSize)
		assert.Equal(t, int(megaBytesRate2Bytes(1)), qc.MaxDeleteSize)
	})

	t.Run("test dql", func(t *testing.T) {
		assert.Equal(t, megaBytesRate2Bytes(0.1), qc.DQLSearchRate)
		assert.Equal(t, megaBytesRate2Bytes(0.01), qc.DQLQueryRate)
		assert.Equal(t, int(megaBytesRate2Bytes(0.1)), qc.MaxSearchSize)
		assert.Equal(t, int(megaBytesRate2Bytes(0.01)), qc.MaxQuerySize)
	})

	t.Run("test limits", func(t *testing.T) {
		assert.Equal(t, 32768, qc.MaxCollectionNum)
	})

	t.Run("test force deny writing", func(t *testing.T) {
		assert.False(t, qc.ForceDenyWriting)
		assert.Equal(t, 10*time.Second, qc.MaxTSafeDelay)
		assert.Equal(t, 0.8, qc.DataNodeMemoryLowWaterLevel)
		assert.Equal(t, 0.9, qc.DataNodeMemoryHighWaterLevel)
		assert.Equal(t, 0.8, qc.QueryNodeMemoryLowWaterLevel)
		assert.Equal(t, 0.9, qc.QueryNodeMemoryHighWaterLevel)
	})

	t.Run("test force deny reading", func(t *testing.T) {
		assert.False(t, qc.ForceDenyReading)
		assert.Equal(t, int64(100000), qc.MaxNQInQueue)
		assert.Equal(t, int64(1024), qc.MaxQueryTasksInQueue)
	})
}
