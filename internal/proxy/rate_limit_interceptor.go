package proxy

import (
	"context"
	"fmt"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"reflect"
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

func toKilobytes(size int) int {
	return size / 1024
}

func toMegabytes(size int) int {
	return size / 1024 / 1024
}

func estimateRequestSize(req interface{}) (commonpb.RateType, int, error) {
	switch req.(type) {
	case *milvuspb.InsertRequest:
		return commonpb.RateType_DMLInsert, toKilobytes(req.(*milvuspb.InsertRequest).XXX_Size()), nil
	case *milvuspb.DeleteRequest:
		return commonpb.RateType_DMLDelete, toKilobytes(req.(*milvuspb.DeleteRequest).XXX_Size()), nil
	case *milvuspb.SearchRequest:
		return commonpb.RateType_DQLSearch, toKilobytes(req.(*milvuspb.SearchRequest).XXX_Size()), nil
	case *milvuspb.QueryRequest:
		return commonpb.RateType_DQLQuery, toKilobytes(req.(*milvuspb.QueryRequest).XXX_Size()), nil
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
