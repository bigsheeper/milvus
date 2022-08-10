package proxy

import (
	"context"
	"fmt"
	"reflect"

	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
)

// RateLimitInterceptor returns a new unary server interceptors that performs request rate limiting.
func RateLimitInterceptor(limiter Limiter) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		rt, n, err := estimateRequestSize(req)
		if err == nil {
			if limiter.Limit(rt, n) {
				return nil, status.Errorf(codes.ResourceExhausted, "%s is rejected by grpc RateLimiter middleware, please retry later.", info.FullMethod)
			}
		}
		return handler(ctx, req)
	}
}

func estimateRequestSize(req interface{}) (commonpb.RateType, int, error) {
	switch req.(type) {
	case *milvuspb.InsertRequest:
		return commonpb.RateType_DMLInsert, proto.Size(req.(*milvuspb.InsertRequest)), nil
	case *milvuspb.DeleteRequest:
		return commonpb.RateType_DMLDelete, proto.Size(req.(*milvuspb.DeleteRequest)), nil
	case *milvuspb.SearchRequest:
		return commonpb.RateType_DQLSearch, proto.Size(req.(*milvuspb.SearchRequest)), nil
	case *milvuspb.QueryRequest:
		return commonpb.RateType_DQLQuery, proto.Size(req.(*milvuspb.QueryRequest)), nil
	case *milvuspb.CreateCollectionRequest, *milvuspb.DropCollectionRequest:
		return commonpb.RateType_DDLCollection, 1, nil
	case *milvuspb.CreatePartitionRequest, *milvuspb.DropPartitionRequest:
		return commonpb.RateType_DDLPartition, 1, nil
		// TODO: add more ddl request
	default:
		if req == nil {
			return 0, 0, fmt.Errorf("null request")
		}
		return 0, 0, fmt.Errorf("unsupported request type %s", reflect.TypeOf(req).Name())
	}
}
