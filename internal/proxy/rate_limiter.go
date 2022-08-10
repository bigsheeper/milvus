package proxy

import (
	"time"

	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
)

type LimitStrategy int32

const (
	Direct         LimitStrategy = 0
	Warmup         LimitStrategy = 1
	MemoryAdaptive LimitStrategy = 2
	CpuAdaptive    LimitStrategy = 3
)

const (
	DDLBucketSize = 10               // TODO: add to config
	DMLBucketSize = 10 * 1024 * 1024 // bytes
	DQLBucketSize = 10 * 1024 * 1024 // bytes
)

// Limiter defines the interface to perform request rate limiting.
// If Limit function return true, the request will be rejected.
// Otherwise, the request will pass.
type Limiter interface {
	Limit(rt commonpb.RateType, n int) bool
}

type RateLimiter struct {
	limiters map[commonpb.RateType]*rate.Limiter
}

func (rl *RateLimiter) Limit(rt commonpb.RateType, n int) bool {
	return rl.limiters[rt].AllowN(time.Now(), n)
}

func (rl *RateLimiter) setRates(rates []*commonpb.Rate) {
	for _, r := range rates {
		if _, ok := rl.limiters[r.GetRt()]; ok {
			rl.limiters[r.GetRt()].SetLimit(rate.Limit(r.GetR()))
		}
	}
}

func (rl *RateLimiter) registerLimiters() {
	rl.limiters[commonpb.RateType_DDLCollection] = rate.NewLimiter(rate.Limit(Params.QuotaConfig.DDLCollectionRate), DDLBucketSize)
	rl.limiters[commonpb.RateType_DDLPartition] = rate.NewLimiter(rate.Limit(Params.QuotaConfig.DDLPartitionRate), DDLBucketSize)
	rl.limiters[commonpb.RateType_DDLSegments] = rate.NewLimiter(rate.Limit(Params.QuotaConfig.DDLSegmentsRate), DDLBucketSize)
	rl.limiters[commonpb.RateType_DMLInsert] = rate.NewLimiter(rate.Limit(Params.QuotaConfig.DMLInsertRate*1024*1024), DMLBucketSize)
	rl.limiters[commonpb.RateType_DMLDelete] = rate.NewLimiter(rate.Limit(Params.QuotaConfig.DMLDeleteRate*1024*1024), DMLBucketSize)
	rl.limiters[commonpb.RateType_DQLSearch] = rate.NewLimiter(rate.Limit(Params.QuotaConfig.DQLSearchRate*1024*1024), DQLBucketSize)
	rl.limiters[commonpb.RateType_DQLQuery] = rate.NewLimiter(rate.Limit(Params.QuotaConfig.DQLQueryRate*1024*1024), DQLBucketSize)
}

func NewRateLimiter() *RateLimiter {
	rl := &RateLimiter{
		limiters: make(map[commonpb.RateType]*rate.Limiter),
	}
	rl.registerLimiters()
	return rl
}
