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

package msgstream

import (
	"context"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

const (
	producePackNum = 10
	msgNumEachPack = 10
)

var collectionIDList = []UniqueID{0, 1, 2, 3, 4, 5}

func mockDispatcherFunc(msgPack *MsgPack) map[vchannel]*MsgPack {
	packs := make(map[vchannel]*MsgPack)
	getVChannelName := func(collectionID UniqueID) string {
		return strconv.FormatInt(collectionID, 10)
	}
	initPack := func(beginTs, endTs Timestamp, vchannel vchannel) {
		packs[vchannel] = &MsgPack{
			BeginTs:        beginTs,
			EndTs:          endTs,
			Msgs:           make([]TsMsg, 0),
			StartPositions: make([]*MsgPosition, 0),
			EndPositions:   make([]*MsgPosition, 0),
		}
	}
	for i := range msgPack.Msgs {
		switch msgPack.Msgs[i].Type() {
		case commonpb.MsgType_Insert:
			iMsg := msgPack.Msgs[i].(*InsertMsg)
			vchannel := getVChannelName(iMsg.CollectionID)
			if _, ok := packs[vchannel]; !ok {
				initPack(msgPack.BeginTs, msgPack.EndTs, vchannel)
			}
			packs[vchannel].Msgs = append(packs[vchannel].Msgs, msgPack.Msgs[i])
			packs[vchannel].StartPositions = append(packs[vchannel].StartPositions, msgPack.StartPositions[i])
			packs[vchannel].EndPositions = append(packs[vchannel].EndPositions, msgPack.EndPositions[i])
		}
	}
	return packs
}

func genFactory() dependency.Factory {
	factory := dependency.NewDefaultFactory(true)
	return factory
}

func genTsMsg(collectionID UniqueID, msgType commonpb.MsgType, timestamp Timestamp) TsMsg {
	base := &commonpb.MsgBase{
		MsgType:   msgType,
		Timestamp: timestamp,
	}
	switch msgType {
	case commonpb.MsgType_Insert:
		return &InsertMsg{
			InsertRequest: internalpb.InsertRequest{
				Base:         base,
				CollectionID: collectionID,
			},
		}
	case commonpb.MsgType_Delete:
		return &DeleteMsg{
			DeleteRequest: internalpb.DeleteRequest{
				Base:         base,
				CollectionID: collectionID,
			},
		}
	case commonpb.MsgType_TimeTick:
		return &TimeTickMsg{
			TimeTickMsg: internalpb.TimeTickMsg{
				Base: base,
			},
		}
	case commonpb.MsgType_DropCollection:
		return &DropCollectionMsg{
			DropCollectionRequest: internalpb.DropCollectionRequest{
				Base:         base,
				CollectionID: collectionID,
			},
		}
	}
	return nil
}

func genTimestampByIndex(i int) Timestamp {
	return Timestamp((i + 1) * 100)
}

func produceMsg(pchannel pchannel) error {
	factory := genFactory()
	stream, err := factory.NewMsgStream(context.Background())
	if err != nil {
		return err
	}
	stream.AsProducer([]string{pchannel})
	for i := 0; i < producePackNum; i++ {
		pack := &MsgPack{
			BeginTs:        genTimestampByIndex(i),
			EndTs:          genTimestampByIndex(i + 1),
			Msgs:           make([]TsMsg, 0),
			StartPositions: make([]*MsgPosition, 0),
			EndPositions:   make([]*MsgPosition, 0),
		}
		for j := 0; j < msgNumEachPack; j++ {
			pack.Msgs = append(pack.Msgs, genTsMsg())
		}
	}
}

func TestTtMsgDispatcher(t *testing.T) {
	t.Run("test TtMsgDispatcher", func(t *testing.T) {
		pchannel := "testTtMsgDispatcher-pchannel-0"
		dispatcher := NewTtMsgDispatcher(0, "role", pchannel, mockDispatcherFunc, genFactory())
		go func() {
			err := dispatcher.Run()
			assert.NoError(t, err)
		}()
	})
}
