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

// Collector helps to collect and calculate values (like throughput, QPS, TPS, etc...),
// It implements a sliding window with custom size and granularity to store values.
type Collector struct {
	sync.Mutex

	window      time.Duration
	granularity time.Duration
	position    int
	values      []float64

	stopOnce sync.Once
	stopC    chan struct{}
}

func NewCollector(window time.Duration, granularity time.Duration) (*Collector, error) {
	if window == 0 || granularity == 0 {
		return nil, fmt.Errorf("create Collector failed, window or granularity cannot be 0, window = %d, granularity = %d", window, granularity)
	}
	if window < granularity || window%granularity != 0 {
		return nil, fmt.Errorf("create Collector failed, window has to be a multiplier of the granularity, window = %d, granularity = %d", window, granularity)
	}
	rc := &Collector{
		window:      window,
		granularity: granularity,
		position:    0,
		values:      make([]float64, window/granularity),
	}
	return rc, nil
}

func (c *Collector) Start() {
	go c.shift()
}

func (c *Collector) Stop() {
	c.stopOnce.Do(func() {
		c.stopC <- struct{}{}
	})
}

func (c *Collector) Add(value float64) {
	c.Lock()
	defer c.Unlock()
	c.values[c.position] += value
}

func (c *Collector) Avg() float64 {
	c.Lock()
	defer c.Unlock()
	total := float64(0)
	for _, v := range c.values {
		total += v
	}
	return total / float64(len(c.values))
}

func (c *Collector) Max() float64 {
	c.Lock()
	defer c.Unlock()
	max := float64(0)
	for _, v := range c.values {
		if v > max {
			max = v
		}
	}
	return max
}

func (c *Collector) Min() float64 {
	c.Lock()
	defer c.Unlock()
	min := math.MaxFloat64
	for _, v := range c.values {
		if v < min {
			min = v
		}
	}
	return min
}

func (c *Collector) Newest() float64 {
	c.Lock()
	defer c.Unlock()
	return c.values[c.position]
}

func (c *Collector) shift() {
	ticker := time.NewTicker(c.granularity)
	defer ticker.Stop()
	for {
		select {
		case <-c.stopC:
			return
		case <-ticker.C:
			c.Lock()
			if c.position = c.position + 1; c.position >= int(c.window/c.granularity) {
				c.position = 0
			}
			c.values[c.position] = 0
			c.Unlock()
		}
	}
}
