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

package flowgraph

import (
	"context"
	"errors"
	"fmt"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/paramtable"
)

const (
	testChannelName = "flow-graph-unittest-channel-0"
	msgLength       = 10
	readPosition    = 5
)

var Params paramtable.BaseTable

func TestMain(m *testing.M) {
	Params.Init()
	exitCode := m.Run()
	os.Exit(exitCode)
}

func genFactory() (msgstream.Factory, error) {
	const receiveBufSize = 1024
	pulsarAddress, _ := Params.Load("_PulsarAddress")

	msFactory := msgstream.NewPmsFactory()
	m := map[string]interface{}{
		"receiveBufSize": receiveBufSize,
		"pulsarAddress":  pulsarAddress,
		"pulsarBufSize":  1024}
	err := msFactory.SetParams(m)
	if err != nil {
		return nil, err
	}
	return msFactory, nil
}

func prepareToRead(ctx context.Context) (*internalpb.MsgPosition, error) {
	msFactory, err := genFactory()
	if err != nil {
		return nil, err
	}
	stream, err := msFactory.NewMsgStream(ctx)
	if err != nil {
		return nil, err
	}
	stream.AsProducer([]string{testChannelName})
	stream.Start()

	msgPack := &msgstream.MsgPack{}
	for i := 0; i < msgLength; i++ {
		insertMsg := &msgstream.InsertMsg{
			BaseMsg: msgstream.BaseMsg{
				HashValues: []uint32{},
			},
			InsertRequest: internalpb.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Insert,
				},
			},
		}
		msgPack.Msgs = append(msgPack.Msgs, insertMsg)
	}

	err = stream.Produce(msgPack)
	if err != nil {
		return nil, err
	}
	time.Sleep(2 * time.Second)
	fmt.Println("produce done!!!!!!!!!!")

	aaa,err := msFactory.NewTtMsgStream(ctx)
	if err != nil {
		panic(err)
	}
	aaa.AsConsumer([]string{testChannelName}, "ut-sub-name-0")
	aaa.Start()
	pack := aaa.Consume()
	if len(pack.Msgs) == 0 {
		panic("aaa")
	}
	fmt.Println("ddddaaaaaaaaaaaaaaaaa")

	readStream, err := msFactory.NewMsgStream(ctx)
	if err != nil {
		return nil, err
	}
	readStream.AsReader([]string{testChannelName}, "ut-sub-name-0")
	var seekPosition *internalpb.MsgPosition
	for i := 0; i < msgLength; i++ {
		hasNext := readStream.HasNext(testChannelName)
		if !hasNext {
			return nil, errors.New("read failed")
		}
		result, err := readStream.Next(ctx, testChannelName)
		if err != nil {
			return nil, err
		}
		if i == readPosition {
			seekPosition = result.Position()
		}
	}
	return seekPosition, nil
}

func TestReaderNode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	position, err := prepareToRead(ctx)
	assert.NoError(t, err)

	msFactory, err := genFactory()
	assert.NoError(t, err)

	stream, err := msFactory.NewMsgStream(ctx)
	assert.NoError(t, err)

	stream.AsReader([]string{testChannelName}, "ut-sub-name-1")
	err = stream.SeekReaders([]*internalpb.MsgPosition{position})
	assert.NoError(t, err)

	nodeName := "readerNode"
	readerNode := NewReaderNode(ctx, stream, testChannelName, nodeName, 1024, 1024)
	assert.NotNil(t, readerNode)

	isInputNode := readerNode.IsInputNode()
	assert.True(t, isInputNode)

	name := readerNode.Name()
	assert.Equal(t, name, nodeName)

	res := readerNode.Operate([]Msg{})
	assert.Len(t, res, 1)

	resMsg, ok := res[0].(*MsgStreamMsg)
	assert.True(t, ok)
	assert.Len(t, resMsg.tsMessages, msgLength-readPosition)
}
