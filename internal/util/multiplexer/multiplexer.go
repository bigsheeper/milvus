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

package multiplexer

import (
	"context"
	"fmt"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type vChannel = string
type pChannel = string

type Multiplexing func(msgPack *msgstream.MsgPack) map[vChannel]*msgstream.MsgPack

type MuxConfig struct {
	channelBufferSize uint64 // TODO: add buffer size
	//startPosition     *msgstream.MsgPosition
}

type Multiplexer struct {
	config  *MuxConfig
	muxFunc Multiplexing // TODO: *?

	Inputs  map[pChannel]typeutil.Timestamp      // TODO: add lock
	Outputs map[vChannel]chan *msgstream.MsgPack // TODO: add lock

	role    string
	nodeID  int64
	factory msgstream.Factory
}

func NewMultiplexer() *Multiplexer {
	return &Multiplexer{}
}

func (mux *Multiplexer) AddVChannel(ctx context.Context, channel vChannel, subName string, position *msgstream.MsgPosition) (<-chan *msgstream.MsgPack, error) {
	if _, ok := mux.Outputs[channel]; ok {
		log.Info("exists")
		return nil, nil // TODO: check nil chan
	}

	stream, err := mux.factory.NewTtMsgStream(ctx)
	if err != nil {
		return nil, err
	}

	mux.Outputs[channel] = make(chan *msgstream.MsgPack, mux.config.channelBufferSize)

	ch := position.ChannelName
	if position.Timestamp < mux.Inputs[ch] {
		stream.AsConsumer([]string{ch}, subName)
		err = stream.Seek([]*internalpb.MsgPosition{position})
		if err != nil {
			return nil, err
		}
		mux.Read(ctx, ch, stream)
		log.Info("seek done")
	}

	return mux.Outputs[channel], nil // TODO: close channel
}

func (mux *Multiplexer) Read(ctx context.Context, ch pChannel, stream msgstream.MsgStream) {
	func() {
		for {
			select {
			case <-ctx.Done():
				log.Info("done")
			case msgPack := <-stream.Chan():
				if msgPack.BeginTs >= mux.Inputs[ch] {
					return
				}
				for ch, pack := range mux.muxFunc(msgPack) {
					if _, ok := mux.Outputs[ch]; ok {
						mux.Outputs[ch] <- pack // TODO: what if block here
					}
				}
			}
		}
	}()
}

func (mux *Multiplexer) AddPChannel(ctx context.Context, channel pChannel) error {
	if _, ok := mux.Inputs[channel]; ok {
		log.Info("exists")
		return nil
	}

	stream, err := mux.factory.NewTtMsgStream(ctx)
	if err != nil {
		return err
	}

	subName := fmt.Sprintf("%s-%d-%s", mux.role, mux.nodeID, channel)
	stream.AsConsumer([]pChannel{channel}, subName)

	// TODO: add wait
	go mux.Demux(ctx, stream)
	mux.Inputs[channel] = struct{}{}

	return nil
}

func (mux *Multiplexer) Demux(ctx context.Context /*TODO: add ref count*/, stream msgstream.MsgStream) {
	for {
		select {
		case <-ctx.Done():
			log.Info("done")
		case msgPack := <-stream.Chan():
			for ch, pack := range mux.muxFunc(msgPack) {
				// send msg
				if _, ok := mux.Outputs[ch]; ok {
					mux.Outputs[ch] <- pack // TODO: what if block here
				}
			}
		}
	}
}
