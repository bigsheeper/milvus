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
	"fmt"
	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"
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
	DDLBucketSize = 10                // TODO: add to config
	DMLBucketSize = 256 * 1024 * 1024 // bytes
	DQLBucketSize = 32 * 1024 * 1024  // bytes

	warmupSpeed = 0.1
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
	fmt.Println("<<<<<<<<<<<<<<<<<<<<<<<<< n:", n)
	return !rl.limiters[rt].AllowN(time.Now(), n)
}

func (rl *RateLimiter) setRates(rates []*commonpb.Rate) {
	rates = rl.warmup(rates)
	for _, r := range rates {
		if _, ok := rl.limiters[r.GetRt()]; ok {
			//fmt.Println(">>>>>>>>>>>>>>", r.GetR())
			rl.limiters[r.GetRt()].SetLimit(rate.Limit(r.GetR()))
		}
	}
	rl.printRates(rates)
}

func (rl *RateLimiter) printRates(rates []*commonpb.Rate) {
	fmt.Printf("RateLimiter rates:\n---------------------------------\n")
	for _, r := range rates {
		fmt.Printf("%s -> %f\n", r.GetRt().String(), r.GetR())
	}
	fmt.Printf("---------------------------------\n")
}

func (rl *RateLimiter) warmup(originalRates []*commonpb.Rate) []*commonpb.Rate {
	targetRates := make([]*commonpb.Rate, 0, len(originalRates))
	for _, r := range originalRates {
		maxRate, _ := rl.getRateConfigByRateType(r.GetRt())
		newRate := &commonpb.Rate{Rt: r.GetRt(), R: r.GetR() + maxRate*warmupSpeed}
		targetRates = append(targetRates, newRate)
	}
	return targetRates
}

func (rl *RateLimiter) getRateConfigByRateType(rt commonpb.RateType) (float64, int) {
	switch rt {
	case commonpb.RateType_DDLCollection:
		return Params.QuotaConfig.DDLCollectionRate, DDLBucketSize
	case commonpb.RateType_DDLPartition:
		return Params.QuotaConfig.DDLPartitionRate, DDLBucketSize
	case commonpb.RateType_DDLIndex:
		return Params.QuotaConfig.DDLIndexRate, DDLBucketSize
	case commonpb.RateType_DDLSegments:
		return Params.QuotaConfig.DDLSegmentsRate, DDLBucketSize
	case commonpb.RateType_DMLInsert:
		return Params.QuotaConfig.DMLInsertRate * 1024 * 1024, DMLBucketSize
	case commonpb.RateType_DMLDelete:
		return Params.QuotaConfig.DMLDeleteRate * 1024 * 1024, DMLBucketSize
	case commonpb.RateType_DQLSearch:
		return Params.QuotaConfig.DQLSearchRate * 1024 * 1024, DQLBucketSize
	case commonpb.RateType_DQLQuery:
		return Params.QuotaConfig.DQLQueryRate * 1024 * 1024, DQLBucketSize
	}
	panic(fmt.Errorf("invalid rateType in RateLimiter, rt = %d", rt))
}

func (rl *RateLimiter) registerLimiters() {
	for rt := range commonpb.RateType_name {
		r, b := rl.getRateConfigByRateType(commonpb.RateType(rt))
		log.Info("register RateLimiter", zap.String("rateType", commonpb.RateType_name[rt]), zap.Float64("r", r), zap.Int("b", b))
		rl.limiters[commonpb.RateType(rt)] = rate.NewLimiter(rate.Limit(r), b)
	}
}

func NewRateLimiter() *RateLimiter {
	rl := &RateLimiter{
		limiters: make(map[commonpb.RateType]*rate.Limiter),
	}
	rl.registerLimiters()
	return rl
}
