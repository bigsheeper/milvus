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

package proxy

import (
	"context"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
)

type limiterMock struct {
	limit bool
}

func (l *limiterMock) Limit(_ internalpb.RateType, _ int) bool {
	return l.limit
}

func TestRateLimitInterceptor(t *testing.T) {
	t.Run("test getRequestInfo", func(t *testing.T) {
		rt, size, err := getRequestInfo(&milvuspb.InsertRequest{})
		assert.NoError(t, err)
		assert.Equal(t, proto.Size(&milvuspb.InsertRequest{}), size)
		assert.Equal(t, internalpb.RateType_DMLInsert, rt)

		rt, size, err = getRequestInfo(&milvuspb.DeleteRequest{})
		assert.NoError(t, err)
		assert.Equal(t, proto.Size(&milvuspb.DeleteRequest{}), size)
		assert.Equal(t, internalpb.RateType_DMLDelete, rt)

		rt, size, err = getRequestInfo(&milvuspb.SearchRequest{})
		assert.NoError(t, err)
		assert.Equal(t, proto.Size(&milvuspb.SearchRequest{}), size)
		assert.Equal(t, internalpb.RateType_DQLSearch, rt)

		rt, size, err = getRequestInfo(&milvuspb.QueryRequest{})
		assert.NoError(t, err)
		assert.Equal(t, proto.Size(&milvuspb.QueryRequest{}), size)
		assert.Equal(t, internalpb.RateType_DQLQuery, rt)

		rt, size, err = getRequestInfo(&milvuspb.CreateCollectionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, 1, size)
		assert.Equal(t, internalpb.RateType_DDLCollection, rt)

		rt, size, err = getRequestInfo(&milvuspb.CreatePartitionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, 1, size)
		assert.Equal(t, internalpb.RateType_DDLPartition, rt)

		rt, size, err = getRequestInfo(&milvuspb.CreateIndexRequest{})
		assert.NoError(t, err)
		assert.Equal(t, 1, size)
		assert.Equal(t, internalpb.RateType_DDLIndex, rt)

		rt, size, err = getRequestInfo(&milvuspb.FlushRequest{})
		assert.NoError(t, err)
		assert.Equal(t, 1, size)
		assert.Equal(t, internalpb.RateType_DDLFlush, rt)

		rt, size, err = getRequestInfo(&milvuspb.ManualCompactionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, 1, size)
		assert.Equal(t, internalpb.RateType_DDLCompaction, rt)
	})

	t.Run("test RateLimitInterceptor", func(t *testing.T) {
		limiter := limiterMock{}
		handler := func(ctx context.Context, req interface{}) (interface{}, error) {
			return nil, nil
		}
		serverInfo := &grpc.UnaryServerInfo{FullMethod: "MockFullMethod"}

		limiter.limit = true
		interceptorFun := RateLimitInterceptor(&limiter)
		_, err := interceptorFun(context.Background(), &milvuspb.InsertRequest{}, serverInfo, handler)
		assert.Error(t, err)

		limiter.limit = false
		interceptorFun = RateLimitInterceptor(&limiter)
		_, err = interceptorFun(context.Background(), &milvuspb.InsertRequest{}, serverInfo, handler)
		assert.NoError(t, err)
	})
}
