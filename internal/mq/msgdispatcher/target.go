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
	"sync"
	"time"

	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/mq/msgstream"
)

// TODO: move to config
var (
	MaxTolerantLag        = 3 * time.Second
	DefaultTargetChanSize = 1024
)

type target struct {
	vchannel string
	ch       chan *msgstream.MsgPack
	pos      *msgstream.MsgPosition

	closeOnce sync.Once
	closed    atomic.Bool
}

func newTarget(vchannel string, pos *msgstream.MsgPosition) *target {
	t := &target{
		vchannel: vchannel,
		ch:       make(chan *msgstream.MsgPack, DefaultTargetChanSize),
		pos:      pos,
	}
	t.closed.Store(false)
	return t
}

func (t *target) close() {
	t.closed.Store(true)
}

func (t *target) send(pack *msgstream.MsgPack, timeout time.Duration) error {
	if t.closed.Load() {
		t.closeOnce.Do(func() {
			close(t.ch)
		})
		return nil
	}
	select {
	case <-time.After(timeout):
		return fmt.Errorf("send target timeout, vchannel=%s", t.vchannel)
	case t.ch <- pack:
		return nil
	}
}
