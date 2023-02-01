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
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

func TestDispatcher(t *testing.T) {
	t.Run("test new and handle", func(t *testing.T) {
		d, err := newDispatcher(newMockFactory(), "mock_pchannel_0", nil,
			"mock_subName_0", mqwrapper.SubscriptionPositionEarliest, nil)
		assert.NoError(t, err)
		assert.NotPanics(t, func() {
			d.handle(start)
			d.handle(pause)
			d.handle(resume)
			d.handle(terminate)
		})
	})

	t.Run("test set-get position", func(t *testing.T) {
		d, err := newDispatcher(newMockFactory(), "mock_pchannel_0", nil,
			"mock_subName_0", mqwrapper.SubscriptionPositionEarliest, nil)
		assert.NoError(t, err)
		pos := &msgstream.MsgPosition{
			ChannelName: "mock_vchannel_0",
			MsgGroup:    "mock_msg_group",
			Timestamp:   100,
		}
		d.setCurPosition(pos)
		curPos := d.getCurPosition()
		assert.Equal(t, pos.ChannelName, curPos.ChannelName)
		assert.Equal(t, pos.MsgGroup, curPos.MsgGroup)
		assert.Equal(t, pos.Timestamp, curPos.Timestamp)
	})

	t.Run("test target", func(t *testing.T) {
		d, err := newDispatcher(newMockFactory(), "mock_pchannel_0", nil,
			"mock_subName_0", mqwrapper.SubscriptionPositionEarliest, nil)
		assert.NoError(t, err)
		output := make(chan *msgstream.MsgPack, 1024)
		d.addTarget("mock_vchannel_0", output)
		d.addTarget("mock_vchannel_1", nil)
		num := d.targetNum()
		assert.Equal(t, 2, num)

		out, err := d.getTarget("mock_vchannel_0")
		assert.NoError(t, err)
		assert.Equal(t, cap(output), cap(out))

		wg := &sync.WaitGroup{}
		wg.Add(1)
		go func() {
			select {
			case _, ok := <-output:
				assert.False(t, ok)
			case <-time.After(1 * time.Second):
				assert.Fail(t, "close target timeout!")
			}
			wg.Done()
		}()
		d.closeTarget("mock_vchannel_0")
		wg.Wait()

		d.removeTarget("mock_vchannel_0")
		num = d.targetNum()
		assert.Equal(t, 1, num)
	})
}

func BenchmarkDispatcher_handle(b *testing.B) {
	d, err := newDispatcher(newMockFactory(), "mock_pchannel_0", nil,
		"mock_subName_0", mqwrapper.SubscriptionPositionEarliest, nil)
	assert.NoError(b, err)

	for i := 0; i < b.N; i++ {
		d.handle(start)
		d.handle(pause)
		d.handle(resume)
		d.handle(terminate)
	}
	// BenchmarkDispatcher_handle-12    	    9568	    122123 ns/op
	// PASS
}
