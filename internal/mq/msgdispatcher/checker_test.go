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

package msgdispatcher

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper"
)

func TestChecker(t *testing.T) {
	t.Run("test add and remove dispatcher", func(t *testing.T) {
		c := newChecker("mock_pchannel_0", "mock_sub_prefix_0", newMockFactory())
		assert.NotNil(t, c)
		assert.Equal(t, 0, c.dispatcherNum())

		var offset int
		for i := 0; i < 100; i++ {
			r := rand.Intn(100) + 1
			for j := 0; j < r; j++ {
				offset++
				_, err := c.addDispatcher(fmt.Sprintf("mock_vchannel_%d", offset), nil, mqwrapper.SubscriptionPositionUnknown)
				assert.NoError(t, err)
				assert.Equal(t, offset, c.dispatcherNum())
			}
			for j := 0; j < rand.Intn(r); j++ {
				c.removeDispatcher(fmt.Sprintf("mock_vchannel_%d", offset))
				offset--
				assert.Equal(t, offset, c.dispatcherNum())
			}
		}
	})

	t.Run("test merge and split", func(t *testing.T) {
		c := newChecker("mock_pchannel_0", "mock_sub_prefix_0", newMockFactory())
		assert.NotNil(t, c)
		_, err := c.addDispatcher("mock_vchannel_0", nil, mqwrapper.SubscriptionPositionUnknown)
		assert.NoError(t, err)
		_, err = c.addDispatcher("mock_vchannel_1", nil, mqwrapper.SubscriptionPositionUnknown)
		assert.NoError(t, err)
		_, err = c.addDispatcher("mock_vchannel_2", nil, mqwrapper.SubscriptionPositionUnknown)
		assert.NoError(t, err)
		assert.Equal(t, 3, c.dispatcherNum())

		c.merge(map[string]struct{}{
			"mock_vchannel_1": {},
			"mock_vchannel_2": {},
		})
		assert.Equal(t, 1, c.dispatcherNum())

		c.split("mock_vchannel_2", nil)
		assert.Equal(t, 2, c.dispatcherNum())
	})

	t.Run("test run and close", func(t *testing.T) {
		c := newChecker("mock_pchannel_0", "mock_sub_prefix_0", newMockFactory())
		assert.NotNil(t, c)
		_, err := c.addDispatcher("mock_vchannel_0", nil, mqwrapper.SubscriptionPositionUnknown)
		assert.NoError(t, err)
		_, err = c.addDispatcher("mock_vchannel_1", nil, mqwrapper.SubscriptionPositionUnknown)
		assert.NoError(t, err)
		_, err = c.addDispatcher("mock_vchannel_2", nil, mqwrapper.SubscriptionPositionUnknown)
		assert.NoError(t, err)
		assert.Equal(t, 3, c.dispatcherNum())

		c.checkPeriod = 10 * time.Millisecond
		go c.run()
		time.Sleep(15 * time.Millisecond)
		assert.Equal(t, 1, c.dispatcherNum()) // expected merged

		c.lagChan <- &msgstream.MsgPosition{ChannelName: "mock_vchannel_2"}
		time.Sleep(1 * time.Millisecond)
		assert.Equal(t, 2, c.dispatcherNum()) // expected split

		assert.NotPanics(t, func() {
			c.close()
		})
	})
}
