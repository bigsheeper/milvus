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

package ratecollector

import (
	"fmt"
	"math"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
)

const (
	DefaultWindow      = 10 * time.Second
	DefaultGranularity = 1 * time.Second
)

// RateCollector helps to collect and calculate values (like throughput, QPS, TPS, etc...),
// It implements a sliding window with custom size and granularity to store values.
type RateCollector struct {
	sync.Mutex

	window      time.Duration
	granularity time.Duration
	position    int
	values      map[string][]float64

	stopOnce sync.Once
	stopChan chan struct{}
}

// NewRateCollector returns a new RateCollector with given window and granularity.
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
		values:      make(map[string][]float64),
		stopChan:    make(chan struct{}),
	}
	return rc, nil
}

// Register init values of RateCollector for specified label.
func (r *RateCollector) Register(label string) {
	r.Lock()
	defer r.Unlock()
	if _, ok := r.values[label]; !ok {
		r.values[label] = make([]float64, int(r.window/r.granularity))
	}
}

// Start starts RateCollector.
func (r *RateCollector) Start() {
	go r.shift()
}

// Stop stops the RateCollector.
func (r *RateCollector) Stop() {
	r.stopOnce.Do(func() {
		r.stopChan <- struct{}{}
	})
}

// Add increases the current value of specified label.
func (r *RateCollector) Add(label string, value float64) {
	r.Lock()
	defer r.Unlock()
	if _, ok := r.values[label]; ok {
		r.values[label][r.position] += value
	}
}

// Avg returns the mean value in the window of specified label.
func (r *RateCollector) Avg(label string) (float64, error) {
	r.Lock()
	defer r.Unlock()
	if _, ok := r.values[label]; ok {
		total := float64(0)
		for _, v := range r.values[label] {
			total += v
		}
		return total / float64(len(r.values[label])), nil
	}
	return 0, fmt.Errorf("RateColletor didn't register for label %s", label)
}

// Max returns the maximal value in the window of specified label.
func (r *RateCollector) Max(label string) (float64, error) {
	r.Lock()
	defer r.Unlock()
	if _, ok := r.values[label]; ok {
		max := float64(0)
		for _, v := range r.values[label] {
			if v > max {
				max = v
			}
		}
		return max, nil
	}
	return 0, fmt.Errorf("RateColletor didn't register for label %s", label)
}

// Min returns the minimal value in the window of specified label.
func (r *RateCollector) Min(label string) (float64, error) {
	r.Lock()
	defer r.Unlock()
	if _, ok := r.values[label]; ok {
		min := math.MaxFloat64
		for _, v := range r.values[label] {
			if v < min {
				min = v
			}
		}
		return min, nil
	}
	return 0, fmt.Errorf("RateColletor didn't register for label %s", label)
}

// Newest returns the current value in the window of specified label.
func (r *RateCollector) Newest(label string) (float64, error) {
	r.Lock()
	defer r.Unlock()

	if _, ok := r.values[label]; ok {
		log.Debug("RateCollection return newest", zap.String("label", label),
			zap.Float64("newest", r.values[label][r.position]),
			zap.Float64s("values", r.values[label]))
		return r.values[label][r.position], nil
	}
	return 0, fmt.Errorf("RateColletor didn't register for label %s", label)
}

// shift slides the window per granularity.
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
			r.Unlock()
		}
	}
}
