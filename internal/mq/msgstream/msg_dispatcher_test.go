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
	"fmt"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"
)

const (
	producePackNum = 10
	msgNumEachPack = 10
)

var collectionIDList = []UniqueID{0, 1, 2, 3, 4, 5}

func collectionID2VChannelName(collectionID UniqueID) string {
	return strconv.FormatInt(collectionID, 10)
}

func vchannelName2collectionID(vchannel vchannel) UniqueID {
	id, err := strconv.ParseInt(vchannel, 10, 64)
	if err != nil {
		panic(err)
	}
	return id
}

func mockDispatcherFunc(msgPack *MsgPack) map[vchannel]*MsgPack {
	packs := make(map[vchannel]*MsgPack)
	initPack := func(beginTs, endTs Timestamp, vchannel vchannel) {
		packs[vchannel] = &MsgPack{
			BeginTs: beginTs,
			EndTs:   endTs,
			Msgs:    make([]TsMsg, 0),
		}
	}
	fmt.Println("============== consume msg pack, beginTs = ", msgPack.BeginTs, ", endTs = ", msgPack.EndTs)
	for i := range msgPack.Msgs {
		switch msgPack.Msgs[i].Type() {
		case commonpb.MsgType_Insert:
			iMsg := msgPack.Msgs[i].(*InsertMsg)
			vchannel := collectionID2VChannelName(iMsg.CollectionID)
			if _, ok := packs[vchannel]; !ok {
				initPack(msgPack.BeginTs, msgPack.EndTs, vchannel)
			}
			fmt.Println("*********** collectionID", iMsg.CollectionID, "ts = ", msgPack.Msgs[i].BeginTs())
			packs[vchannel].Msgs = append(packs[vchannel].Msgs, msgPack.Msgs[i])
		}
	}
	return packs
}

func genFactory(t *testing.T) Factory {
	defer func() {
		err := os.Unsetenv("ROCKSMQ_PATH")
		if err != nil {
			panic(err)
		}
	}()
	dir := t.TempDir()
	factory := NewRmsFactory(dir)
	return factory
}

func genTsMsg(collectionID UniqueID, msgType commonpb.MsgType, timestamp Timestamp, msgID UniqueID) TsMsg {
	base := &commonpb.MsgBase{
		MsgType:   msgType,
		MsgID:     msgID,
		Timestamp: timestamp,
	}
	switch msgType {
	case commonpb.MsgType_Insert:
		return &InsertMsg{
			BaseMsg: BaseMsg{
				BeginTimestamp: timestamp,
				EndTimestamp:   timestamp,
				HashValues:     []uint32{0},
			},
			InsertRequest: internalpb.InsertRequest{
				Base:         base,
				CollectionID: collectionID,
				Timestamps:   []Timestamp{timestamp},
				RowIDs:       []int64{0},
				RowData:      []*commonpb.Blob{&commonpb.Blob{}},
			},
		}
	case commonpb.MsgType_Delete:
		return &DeleteMsg{
			BaseMsg: BaseMsg{
				BeginTimestamp: timestamp,
				EndTimestamp:   timestamp,
				HashValues:     []uint32{0},
			},
			DeleteRequest: internalpb.DeleteRequest{
				Base:             base,
				CollectionID:     collectionID,
				Timestamps:       []Timestamp{timestamp},
				Int64PrimaryKeys: []int64{0},
				NumRows:          1,
			},
		}
	case commonpb.MsgType_TimeTick:
		return &TimeTickMsg{
			BaseMsg: BaseMsg{
				BeginTimestamp: timestamp,
				EndTimestamp:   timestamp,
				HashValues:     []uint32{0},
			},
			TimeTickMsg: internalpb.TimeTickMsg{
				Base: base,
			},
		}
	case commonpb.MsgType_DropCollection:
		return &DropCollectionMsg{
			BaseMsg: BaseMsg{
				BeginTimestamp: timestamp,
				EndTimestamp:   timestamp,
				HashValues:     []uint32{0},
			},
			DropCollectionRequest: internalpb.DropCollectionRequest{
				Base:         base,
				CollectionID: collectionID,
			},
		}
	}
	panic("should not get here")
}

func genTimestampByIndex(i int) Timestamp {
	return Timestamp((i + 1) * 10000)
}

func produceMsg(t *testing.T, pchannel pchannel) map[UniqueID]int {
	factory := genFactory(t)
	stream, err := factory.NewMsgStream(context.Background())
	assert.NoError(t, err)
	stream.AsProducer([]string{pchannel})
	msgID := UniqueID(0)
	counter := make(map[UniqueID]int)
	for i := 0; i < producePackNum+1; i++ {
		beginTs := genTimestampByIndex(i)
		endTs := genTimestampByIndex(i + 1)
		pack := &MsgPack{
			BeginTs: beginTs,
			EndTs:   endTs,
			Msgs:    make([]TsMsg, 0),
		}
		pack.Msgs = append(pack.Msgs, genTsMsg(0, commonpb.MsgType_TimeTick, beginTs, msgID))
		msgID++
		err = stream.Produce(pack)
		assert.NoError(t, err)
		fmt.Printf("===== produce timeTick msg, timestamp = %d\n", beginTs)

		if i < producePackNum {
			pack.Msgs = make([]TsMsg, 0)
			for j := 0; j < msgNumEachPack; j++ {
				collectionID := collectionIDList[rand.Int()%len(collectionIDList)]
				timestamp := beginTs + rand.Uint64()%(endTs-beginTs) // beginTs ~ endTs
				pack.Msgs = append(pack.Msgs, genTsMsg(collectionID, commonpb.MsgType_Insert, timestamp, msgID))
				msgID++
				counter[collectionID]++
				fmt.Printf("-- collectionID = %d, timestamp = %d\n", collectionID, timestamp)
			}
			err = stream.Produce(pack)
			assert.NoError(t, err)
		}
	}
	return counter
}

func getPosition(t *testing.T, ctx context.Context, msgIndex int, pchannel pchannel, factory Factory) *MsgPosition {
	readStream, err := factory.NewMsgStream(ctx)
	assert.NoError(t, err)
	defer readStream.Close()
	subName := "getPosition-randSubName-" + funcutil.RandomString(8)
	readStream.AsConsumer([]string{pchannel}, subName)
	for i := 0; i <= msgIndex; i++ {
		select {
		case <-ctx.Done():
			assert.False(t, false) // get position timeout
		case pack := <-readStream.Chan():
			if i == msgIndex {
				return pack.EndPositions[0]
			}
		}
	}
	assert.False(t, false) // get position failed
	return nil
}

func TestTtMsgDispatcher(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("test TtMsgDispatcher", func(t *testing.T) {
		pchannel := "testTtMsgDispatcher-pchannel-" + funcutil.RandomString(8)
		dispatcher := newTtMsgDispatcher(0, "role", pchannel, mockDispatcherFunc, genFactory(t))
		outputs := make(map[vchannel]<-chan *MsgPack)
		for _, collectionID := range collectionIDList {
			vchannel := collectionID2VChannelName(collectionID)
			output, err := dispatcher.register(vchannel, nil)
			assert.NoError(t, err)
			outputs[vchannel] = output
		}

		go func() {
			err := dispatcher.run()
			assert.NoError(t, err)
		}()

		time.Sleep(100 * time.Millisecond)
		expected := produceMsg(t, pchannel)

		result := make(map[UniqueID]int)
		time.Sleep(2 * time.Second)
		for vchannel, output := range outputs {
			func() {
				for {
					select {
					case <-ctx.Done():
						panic("test timeout")
					case res := <-output:
						for range res.Msgs {
							collectionID := vchannelName2collectionID(vchannel)
							result[collectionID]++
							fmt.Println("collectionID ", collectionID, ", ResultNum:", result[collectionID], ", expectedNum:", expected[collectionID])
							if result[collectionID] == expected[collectionID] {
								return
							}
						}
					}
				}
			}()
		}
	})

	t.Run("test chaseToCurrent", func(t *testing.T) {
		pchannel := "testChaseToCurrent-pchannel-" + funcutil.RandomString(8)
		chaseCollectionID := UniqueID(0)

		time.Sleep(100 * time.Millisecond)
		expected := produceMsg(t, pchannel)

		time.Sleep(2 * time.Second)
		for i := 0; i < producePackNum; i++ {
			fmt.Println("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", i)
			dispatcher := newTtMsgDispatcher(0, "role", pchannel, mockDispatcherFunc, genFactory(t))
			for _, collectionID := range collectionIDList {
				if collectionID != chaseCollectionID {
					vchannel := collectionID2VChannelName(collectionID)
					_, err := dispatcher.register(vchannel, nil)
					assert.NoError(t, err)
				}
			}
			go func() {
				err := dispatcher.run()
				assert.NoError(t, err)
			}()

			result := make(map[UniqueID]int)

			vchannel := collectionID2VChannelName(chaseCollectionID)
			err := dispatcher.addConsumer(vchannel)
			assert.NoError(t, err)

			position := getPosition(t, ctx, i, pchannel, genFactory(t))
			err = dispatcher.chaseToCurrent(ctx, collectionID2VChannelName(chaseCollectionID), position)
			assert.NoError(t, err)

			dispatcher.consumersMu.Lock()
			output := dispatcher.consumers[vchannel]
			dispatcher.consumersMu.Unlock()
			func() {
				for {
					select {
					case <-ctx.Done():
						panic("test timeout")
					case res := <-output:
						for range res.Msgs {
							collectionID := vchannelName2collectionID(vchannel)
							result[collectionID]++
							fmt.Println("collectionID ", collectionID, ", ResultNum:", result[collectionID], ", expectedNum:", expected[collectionID])
							if result[collectionID] == expected[collectionID]-i*msgNumEachPack {
								return
							}
						}
					}
				}
			}()
		}
	})
}
