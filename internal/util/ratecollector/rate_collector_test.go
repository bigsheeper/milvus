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

package ratecollector

import (
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
	"time"
)

func TestRateCollector(t *testing.T) {
	t.Run("test rateCollector", func(t *testing.T) {
		rc, err := NewRateCollector(DefaultWindow*time.Second, DefaultGranularity*time.Second)
		assert.NoError(t, err)

		rt := commonpb.RateType_DMLInsert
		rc.RegisterForRateType(rt)
		rc.Start()

		//go func() {
		//	for {
		//		fmt.Println(rc.values[rt])
		//		time.Sleep(1 * time.Second)
		//	}
		//}()

		for i := 0; i < 100; i++ {
			rc.Add(rt, float64(rand.Int()%10+1))
			rc.Newest(rt)
			//rc.Max(rt)
			time.Sleep(time.Second)
		}
	})
}
