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
	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestChecker(t *testing.T) {
	t.Run("test new and run", func(t *testing.T) {
		c := newChecker("mock_pchannel_0", "mock_sub_prefix_0", newMockFactory())
		assert.NotNil(t, c)
		assert.NotPanics(t, func() {
			c.run()
			c.close()
		})
	})

	t.Run("test add and remove dispatcher", func(t *testing.T) {
		c := newChecker("mock_pchannel_0", "mock_sub_prefix_0", newMockFactory())
		assert.NotNil(t, c)
		assert.True(t, c.isEmpty())
		_, err := c.addDispatcher("mock_vchannel_0", nil, mqwrapper.SubscriptionPositionUnknown)
		assert.NoError(t, err)
		assert.False(t, c.isEmpty())
		c.removeDispatcher("mock_vchannel_0")
		assert.True(t, c.isEmpty())
	})

	t.Run("test merge", func(t *testing.T) {
		c := newChecker("mock_pchannel_0", "mock_sub_prefix_0", newMockFactory())
		assert.NotNil(t, c)
		_, err := c.addDispatcher("mock_vchannel_0", nil, mqwrapper.SubscriptionPositionUnknown)
		assert.NoError(t, err)
		_, err = c.addDispatcher("mock_vchannel_1", nil, mqwrapper.SubscriptionPositionUnknown)
		assert.NoError(t, err)
		_, err = c.addDispatcher("mock_vchannel_2", nil, mqwrapper.SubscriptionPositionUnknown)
		assert.NoError(t, err)

		c.merge(map[string]struct{}{
			"mock_vchannel_1": {},
			"mock_vchannel_2": {},
		})
	})
}
