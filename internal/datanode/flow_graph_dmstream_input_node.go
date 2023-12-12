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

package datanode

import (
	"context"
	"fmt"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"runtime"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/mq/msgdispatcher"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// DmInputNode receives messages from message streams, packs messages between two timeticks, and passes all
//
// messages between two timeticks to the following flowgraph node. In DataNode, the following flow graph node is
// flowgraph ddNode.
func newDmInputNode(initCtx context.Context, dispatcherClient msgdispatcher.Client, seekPos *msgpb.MsgPosition, dmNodeConfig *nodeConfig) (*flowgraph.InputNode, error) {
	log := log.With(zap.Int64("nodeID", paramtable.GetNodeID()),
		zap.Int64("collectionID", dmNodeConfig.collectionID),
		zap.String("vchannel", dmNodeConfig.vChannelName))
	var err error
	//var input <-chan *msgstream.MsgPack
	stream, err := dmNodeConfig.msFactory.NewTtMsgStream(initCtx)
	if err != nil {
		panic(err)
		return nil, err
	}
	runtime.SetFinalizer(stream, func(stream msgstream.MsgStream) {
		log.Info("dyh hit Finalizer", zap.Any("vchan", dmNodeConfig.vChannelName), zap.Any("addr", fmt.Sprintf("%p", stream)))
	})
	log.Info("dyh create newDmInputNode", zap.Any("vchan", dmNodeConfig.vChannelName), zap.Any("addr", fmt.Sprintf("%p", stream)))
	if seekPos != nil && len(seekPos.MsgID) != 0 {
		position := seekPos
		ctx := initCtx
		position.ChannelName = funcutil.ToPhysicalChannel(position.ChannelName)
		err = stream.AsConsumer(ctx, []string{position.ChannelName}, dmNodeConfig.vChannelName, mqwrapper.SubscriptionPositionUnknown)
		if err != nil {
			log.Error("asConsumer failed", zap.Error(err))
			return nil, err
		}

		err = stream.Seek(ctx, []*msgpb.MsgPosition{position})
		if err != nil {
			panic(err)
			stream.Close()
			log.Error("seek failed", zap.Error(err))
			return nil, err
		}
		posTime := tsoutil.PhysicalTime(position.GetTimestamp())
		log.Info("seek successfully", zap.Time("posTime", posTime),
			zap.Duration("tsLag", time.Since(posTime)))

		//input, err = dispatcherClient.Register(initCtx, dmNodeConfig.vChannelName, seekPos, mqwrapper.SubscriptionPositionUnknown)
		//if err != nil {
		//	return nil, err
		//}
		log.Info("datanode seek successfully when register to msgDispatcher",
			zap.ByteString("msgID", seekPos.GetMsgID()),
			zap.Time("tsTime", tsoutil.PhysicalTime(seekPos.GetTimestamp())),
			zap.Duration("tsLag", time.Since(tsoutil.PhysicalTime(seekPos.GetTimestamp()))))
	} else {
		panic("should not be here")
		//input, err = dispatcherClient.Register(initCtx, dmNodeConfig.vChannelName, nil, mqwrapper.SubscriptionPositionEarliest)
		//if err != nil {
		//	return nil, err
		//}
		//log.Info("datanode consume successfully when register to msgDispatcher")
	}

	name := fmt.Sprintf("dmInputNode-data-%d-%s", dmNodeConfig.collectionID, dmNodeConfig.vChannelName)
	node := flowgraph.NewInputNode(
		stream,
		stream.Chan(),
		name,
		Params.DataNodeCfg.FlowGraphMaxQueueLength.GetAsInt32(),
		Params.DataNodeCfg.FlowGraphMaxParallelism.GetAsInt32(),
		typeutil.DataNodeRole,
		paramtable.GetNodeID(),
		dmNodeConfig.collectionID,
		metrics.AllLabel,
	)
	return node, nil
}
