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

package ratecollector

import (
	"fmt"
	"math"
	"sync"
	"time"
)

// RateCollector helps to collect and calculate rates (like throughput, QPS, TPS, etc...),
// It implements a sliding window with custom size and granularity to store rates.
type RateCollector struct {
	sync.Mutex

	window      time.Duration
	granularity time.Duration
	position    int
	rates       []float64

	stopOnce sync.Once
	stopC    chan struct{}
}

func NewRateCollector(window time.Duration, granularity time.Duration) (*RateCollector, error) {
	if window == 0 || granularity == 0 {
		return nil, fmt.Errorf("create RateCollector failed, window or granularity cannot be 0, window = %d, granularity = %d", window, granularity)
	}
	if window < granularity || window%granularity != 0 {
		return nil, fmt.Errorf("create RateCollector failed, window has to be a multiplier of the granularity, window = %d, granularity = %d", window, granularity)
	}
	rc := &RateCollector{
		window:      window,
		granularity: granularity,
		position:    0,
		rates:       make([]float64, window/granularity),
	}
	return rc, nil
}

func (r *RateCollector) Start() {
	go r.shift()
}

func (r *RateCollector) Stop() {
	r.stopOnce.Do(func() {
		r.stopC <- struct{}{}
	})
}

func (r *RateCollector) Add(value float64) {
	r.Lock()
	defer r.Unlock()
	r.rates[r.position] += value
}

func (r *RateCollector) Avg() float64 {
	r.Lock()
	defer r.Unlock()
	totalRate := float64(0)
	for _, rate := range r.rates {
		totalRate += rate
	}
	return totalRate / float64(len(r.rates))
}

func (r *RateCollector) Max() float64 {
	r.Lock()
	defer r.Unlock()
	maxRate := float64(0)
	for _, rate := range r.rates {
		if rate > maxRate {
			maxRate = rate
		}
	}
	return maxRate
}

func (r *RateCollector) Min() float64 {
	r.Lock()
	defer r.Unlock()
	minRate := math.MaxFloat64
	for _, rate := range r.rates {
		if rate < minRate {
			minRate = rate
		}
	}
	return minRate
}

func (r *RateCollector) Newest() float64 {
	r.Lock()
	defer r.Unlock()
	return r.rates[r.position]
}

func (r *RateCollector) shift() {
	ticker := time.NewTicker(r.granularity)
	defer ticker.Stop()
	for {
		select {
		case <-r.stopC:
			return
		case <-ticker.C:
			r.Lock()
			if r.position = r.position + 1; r.position >= int(r.window/r.granularity) {
				r.position = 0
			}
			r.rates[r.position] = 0
			r.Unlock()
		}
	}
}
