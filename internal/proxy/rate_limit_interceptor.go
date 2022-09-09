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
	"fmt"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"reflect"

	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/types"
)

// RateLimitInterceptor returns a new unary server interceptors that performs request rate limiting.
func RateLimitInterceptor(limiter types.Limiter) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		rt, n, err := getRequestInfo(req)
		if err == nil {
			limit, rate := limiter.Limit(rt, n)
			if limit {
				return nil, status.Errorf(codes.ResourceExhausted, "%s is rejected by grpc RateLimiter middleware, please retry later.", info.FullMethod)
			}
			if rate == 0 {
				return nil, status.Errorf(codes.ResourceExhausted, commonpb.ErrorCode_ForceDeny.String())
			}
		}
		return handler(ctx, req)
	}
}

// getRequestInfo returns rateType of request and return tokens needed.
func getRequestInfo(req interface{}) (internalpb.RateType, int, error) {
	switch r := req.(type) {
	case *milvuspb.InsertRequest:
		return internalpb.RateType_DMLInsert, proto.Size(r), nil
	case *milvuspb.DeleteRequest:
		return internalpb.RateType_DMLDelete, proto.Size(r), nil
	// TODO: add bulkLoad
	case *milvuspb.SearchRequest:
		return internalpb.RateType_DQLSearch, int(r.GetNq()), nil
	case *milvuspb.QueryRequest:
		return internalpb.RateType_DQLQuery, 1, nil // think of the query request's nq as 1
	case *milvuspb.CreateCollectionRequest, *milvuspb.DropCollectionRequest, *milvuspb.HasCollectionRequest:
		return internalpb.RateType_DDLCollection, 1, nil
	case *milvuspb.LoadCollectionRequest, *milvuspb.ReleaseCollectionRequest, *milvuspb.ShowCollectionsRequest:
		return internalpb.RateType_DDLCollection, 1, nil
	case *milvuspb.CreatePartitionRequest, *milvuspb.DropPartitionRequest, *milvuspb.HasPartitionRequest:
		return internalpb.RateType_DDLPartition, 1, nil
	case *milvuspb.LoadPartitionsRequest, *milvuspb.ReleasePartitionsRequest, *milvuspb.ShowPartitionsRequest:
		return internalpb.RateType_DDLPartition, 1, nil
	case *milvuspb.CreateIndexRequest, *milvuspb.DropIndexRequest, *milvuspb.DescribeIndexRequest:
		return internalpb.RateType_DDLIndex, 1, nil
	case *milvuspb.FlushRequest:
		return internalpb.RateType_DDLFlush, 1, nil
	case *milvuspb.ManualCompactionRequest:
		return internalpb.RateType_DDLCompaction, 1, nil
		// TODO: support more request
	default:
		if req == nil {
			return 0, 0, fmt.Errorf("null request")
		}
		return 0, 0, fmt.Errorf("unsupported request type %s", reflect.TypeOf(req).Name())
	}
}
