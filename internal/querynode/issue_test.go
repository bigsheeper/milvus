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

package querynode

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
)

func TestIssue_16941(t *testing.T) {
	Params.PulsarCfg.Address = "pulsar://172.18.50.9:6650"
	//issueChannel := "by-dev-rootcoord-dml_27"
	issueChannel := "by-dev-rootcoord-delta_27"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	factory := msgstream.NewPmsFactory(&Params.PulsarCfg)
	stream, err := factory.NewTtMsgStream(ctx)
	assert.NoError(t, err)

	randSubName := "test_issue_16941_" + funcutil.RandomString(8)
	stream.AsConsumer([]Channel{issueChannel}, randSubName)

	packLastTime := Timestamp(0)
	msgCount := 0
	for {
		select {
		case pack := <-stream.Chan():
			msgCount++
			fmt.Println("==============================================")
			packBegin, _ := tsoutil.ParseTS(pack.BeginTs)
			packEnd, _ := tsoutil.ParseTS(pack.EndTs)
			for _, msg := range pack.Msgs {
				if del, ok := msg.(*msgstream.DeleteMsg); ok {
					delBegin, _ := tsoutil.ParseTS(del.BeginTimestamp)
					delEnd, _ := tsoutil.ParseTS(del.EndTimestamp)
					fmt.Println("deleteMsg in MsgPack, time = ", delEnd)
					if del.BeginTimestamp < pack.BeginTs {
						panic(fmt.Sprintf("Check beginTs failed!!!, delBegin = %s, packBegin = %s", delBegin, packBegin))
					}
					if del.EndTimestamp > pack.EndTs {
						panic(fmt.Sprintf("Check endTs failed!!!, delEnd = %s, packEnd = %s", delEnd, packEnd))
					}
				}
			}
			fmt.Println("MsgPack, time = ", packEnd, ", msgCount = ", msgCount)
			if pack.EndTs < packLastTime {
				pt2, _ := tsoutil.ParseTS(packLastTime)
				panic(fmt.Sprintf("Consumed invalid msg!!!, last = %s, current = %s", pt2, packEnd))
			}
			packLastTime = pack.EndTs
		}
	}
}
