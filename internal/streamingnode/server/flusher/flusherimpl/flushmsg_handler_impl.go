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

package flusherimpl

import (
	"context"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/streaming/util/message/adaptor"
)

func flushMsgHandlerImpl(wbMgr writebuffer.BufferManager) func(vchannel string, flushMsg *adaptor.FlushMessageBody) {
	return func(vchannel string, flushMsg *adaptor.FlushMessageBody) {
		err := wbMgr.SealSegments(context.Background(), vchannel, flushMsg.GetSegmentId())
		if err != nil {
			log.Warn("failed to seal segments", zap.String("vchannel", vchannel), zap.Error(err))
		}
		err = wbMgr.FlushChannel(context.Background(), vchannel, flushMsg.GetFlushTs())
		if err != nil {
			log.Warn("failed to flush channel", zap.String("vchannel", vchannel), zap.Error(err))
		}
	}
}
