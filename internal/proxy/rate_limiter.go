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
	"time"

	"go.uber.org/zap"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
)

// Limiter defines the interface to perform request rate limiting.
// If Limit function return true, the request will be rejected.
// Otherwise, the request will pass.
type Limiter interface {
	Limit(rt internalpb.RateType, n int) bool
}

// MultiRateLimiter includes multilevel rate limiters, such as global rateLimiter,
// collection level rateLimiter and so on. It also implements Limiter interface.
type MultiRateLimiter struct {
	globalRateLimiter *rateLimiter
	// TODO: add collection level rateLimiter
}

// NewMultiRateLimiter returns a new MultiRateLimiter.
func NewMultiRateLimiter() *MultiRateLimiter {
	m := &MultiRateLimiter{}
	m.globalRateLimiter = newRateLimiter()
	return m
}

// Limit returns true, the request will be rejected.
// Otherwise, the request will pass.
func (m *MultiRateLimiter) Limit(rt internalpb.RateType, n int) bool {
	if !Params.QuotaConfig.EnableQuotaAndLimits {
		return false // never limit
	}
	// TODO: call other rate limiters
	return m.globalRateLimiter.limit(rt, n)
}

// rateLimiter implements Limiter.
type rateLimiter struct {
	limiters map[internalpb.RateType]*rate.Limiter
}

// newRateLimiter returns a new RateLimiter.
func newRateLimiter() *rateLimiter {
	rl := &rateLimiter{
		limiters: make(map[internalpb.RateType]*rate.Limiter),
	}
	rl.registerLimiters()
	return rl
}

// limit returns true, the request will be rejected.
// Otherwise, the request will pass.
func (rl *rateLimiter) limit(rt internalpb.RateType, n int) bool {
	return !rl.limiters[rt].AllowN(time.Now(), n)
}

// setRates sets new rates for the limiters.
func (rl *rateLimiter) setRates(rates []*internalpb.Rate) error {
	for _, r := range rates {
		if _, ok := rl.limiters[r.GetRt()]; ok {
			rl.limiters[r.GetRt()].SetLimit(rate.Limit(r.GetR()))
			metrics.SetRateGaugeByRateType(r.GetRt(), Params.ProxyCfg.GetNodeID(), r.GetR())
		} else {
			return fmt.Errorf("unregister rateLimiter for rateType %s", r.GetRt().String())
		}
	}
	rl.printRates(rates)
	return nil
}

// printRates logs the rate info.
func (rl *rateLimiter) printRates(rates []*internalpb.Rate) {
	//fmt.Printf("RateLimiter set rates:\n---------------------------------\n")
	//for _, r := range rates {
	//	fmt.Printf("%s -> %f\n", r.GetRt().String(), r.GetR())
	//}
	//fmt.Printf("---------------------------------\n")
	log.Debug("RateLimiter setRates", zap.Any("rates", rates))
}

// getRateConfigByRateType returns rate and max size by the specified rateType.
func (rl *rateLimiter) getRateConfigByRateType(rt internalpb.RateType) (float64, int) {
	switch rt {
	case internalpb.RateType_DDLCollection:
		// for all ddl requests, use rate as max size, means max number of ddl requests allowed to happen at once.
		return Params.QuotaConfig.DDLCollectionRate, int(Params.QuotaConfig.DDLCollectionRate)
	case internalpb.RateType_DDLPartition:
		return Params.QuotaConfig.DDLPartitionRate, int(Params.QuotaConfig.DDLPartitionRate)
	case internalpb.RateType_DDLIndex:
		return Params.QuotaConfig.DDLIndexRate, int(Params.QuotaConfig.DDLIndexRate)
	case internalpb.RateType_DDLSegments:
		return Params.QuotaConfig.DDLSegmentsRate, int(Params.QuotaConfig.DDLSegmentsRate)
	case internalpb.RateType_DMLInsert:
		return Params.QuotaConfig.DMLInsertRate, Params.QuotaConfig.MaxInsertSize
	case internalpb.RateType_DMLDelete:
		return Params.QuotaConfig.DMLDeleteRate, Params.QuotaConfig.MaxDeleteSize
	case internalpb.RateType_DQLSearch:
		return Params.QuotaConfig.DQLSearchRate, Params.QuotaConfig.MaxSearchSize
	case internalpb.RateType_DQLQuery:
		return Params.QuotaConfig.DQLQueryRate, Params.QuotaConfig.MaxQuerySize
	}
	panic(fmt.Errorf("invalid rateType in RateLimiter, rt = %d", rt))
}

// registerLimiters register limiter for all rate types.
func (rl *rateLimiter) registerLimiters() {
	for rt := range internalpb.RateType_name {
		r, b := rl.getRateConfigByRateType(internalpb.RateType(rt))
		log.Info("register RateLimiter", zap.String("rateType", internalpb.RateType_name[rt]), zap.Float64("r", r), zap.Int("b", b))
		rl.limiters[internalpb.RateType(rt)] = rate.NewLimiter(rate.Limit(r), b)
	}
}
