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

	"github.com/milvus-io/milvus/internal/proto/commonpb"
)

const (
	DefaultWindow      = 10
	DefaultGranularity = 1
)

// RateCollector helps to collect and calculate values (like throughput, QPS, TPS, etc...),
// It implements a sliding window with custom size and granularity to store values.
type RateCollector struct {
	sync.Mutex

	window      time.Duration
	granularity time.Duration
	position    int
	values      map[commonpb.RateType][]float64

	stopOnce sync.Once
	stopChan chan struct{}
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
		values:      make(map[commonpb.RateType][]float64),
	}
	return rc, nil
}

func (r *RateCollector) RegisterForRateType(rt commonpb.RateType) {
	r.Lock()
	defer r.Unlock()
	if _, ok := r.values[rt]; !ok {
		r.values[rt] = make([]float64, int(r.window/r.granularity))
	}
}

func (r *RateCollector) Start() {
	go r.shift()
}

func (r *RateCollector) Stop() {
	r.stopOnce.Do(func() {
		r.stopChan <- struct{}{}
	})
}

func (r *RateCollector) Add(rt commonpb.RateType, value float64) {
	r.Lock()
	defer r.Unlock()
	if _, ok := r.values[rt]; ok {
		r.values[rt][r.position] += value
	}
	//fmt.Printf("Add %f to %s", value, rt.String())
	//fmt.Println("=======", r.values[rt])
}

func (r *RateCollector) Avg(rt commonpb.RateType) (float64, error) {
	r.Lock()
	defer r.Unlock()
	if _, ok := r.values[rt]; ok {
		total := float64(0)
		for _, v := range r.values[rt] {
			total += v
		}
		return total / float64(len(r.values)), nil
	}
	return 0, fmt.Errorf("RateColletor didn't register for rateType %s", rt.String())
}

func (r *RateCollector) Max(rt commonpb.RateType) (float64, error) {
	r.Lock()
	defer r.Unlock()
	if _, ok := r.values[rt]; ok {
		max := float64(0)
		for _, v := range r.values[rt] {
			if v > max {
				max = v
			}
		}
		fmt.Printf("max %f of %s", max, rt.String())
		fmt.Println("------", r.values[rt])
		return max, nil
	}
	return 0, fmt.Errorf("RateColletor didn't register for rateType %s", rt.String())
}

func (r *RateCollector) Min(rt commonpb.RateType) (float64, error) {
	r.Lock()
	defer r.Unlock()
	if _, ok := r.values[rt]; ok {
		min := math.MaxFloat64
		for _, v := range r.values[rt] {
			if v < min {
				min = v
			}
		}
		return min, nil
	}
	return 0, fmt.Errorf("RateColletor didn't register for rateType %s", rt.String())
}

func (r *RateCollector) Newest(rt commonpb.RateType) (float64, error) {
	r.Lock()
	defer r.Unlock()
	fmt.Println("Newest ", r.values[rt][r.position], " of ", rt.String(), r.values[rt])
	//fmt.Println("------", r.values[rt])
	if _, ok := r.values[rt]; ok {
		return r.values[rt][r.position], nil
	}
	return 0, fmt.Errorf("RateColletor didn't register for rateType %s", rt.String())
}

func (r *RateCollector) NewestNonZero(rt commonpb.RateType) (float64, error) {
	r.Lock()
	defer r.Unlock()

	if _, ok := r.values[rt]; ok {
		i := r.position
		for {
			if r.values[rt][i] != 0 {
				fmt.Println("NewestNoneZero ", r.values[rt][i], " of ", rt.String(), r.values[rt])
				return r.values[rt][i], nil
			}
			i--
			if i < 0 {
				i = len(r.values[rt]) - 1
			}
			if i == r.position {
				return r.values[rt][i], nil
			}
		}
	}
	return 0, fmt.Errorf("RateColletor didn't register for rateType %s", rt.String())
}

func (r *RateCollector) shift() {
	ticker := time.NewTicker(r.granularity)
	defer ticker.Stop()
	for {
		select {
		case <-r.stopChan:
			return
		case <-ticker.C:
			r.Lock()
			if r.position = r.position + 1; r.position >= int(r.window/r.granularity) {
				r.position = 0
			}
			for rt := range r.values {
				r.values[rt][r.position] = 0
			}
			fmt.Println(r.values[commonpb.RateType_DMLInsert])
			r.Unlock()
		}
	}
}
