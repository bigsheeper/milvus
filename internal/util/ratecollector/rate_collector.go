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
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
)

const (
	defaultWindow      = 10
	defaultGranularity = 1
)

type RateCollector struct {
	collectorsMu sync.Mutex
	collectors   map[commonpb.RateType]*Collector

	stopOnce sync.Once
}

func (r *RateCollector) AddCollector(rt commonpb.RateType) error {
	r.collectorsMu.Lock()
	defer r.collectorsMu.Unlock()
	if _, ok := r.collectors[rt]; !ok {
		return fmt.Errorf("collector with rateType %s is existed", rt.String())
	}
	collector, err := NewCollector(defaultWindow*time.Second, defaultGranularity*time.Second)
	if err != nil {
		return err
	}
	r.collectors[rt] = collector
	collector.Start()
	return nil
}

func (r *RateCollector) RemoveCollector(rt commonpb.RateType) {
	r.collectorsMu.Lock()
	defer r.collectorsMu.Unlock()
	if _, ok := r.collectors[rt]; ok {
		r.collectors[rt].Stop()
		delete(r.collectors, rt)
	}
}

func (r *RateCollector) Add(rt commonpb.RateType, value float64) {
	
}

func (r *RateCollector) Close() {
	r.stopOnce.Do(func() {
		r.collectorsMu.Lock()
		defer r.collectorsMu.Unlock()
		for _, c := range r.collectors {
			c.Stop()
		}
		r.collectors = nil
	})
}

func NewRateCollector() *RateCollector {
	return &RateCollector{
		collectors: make(map[commonpb.RateType]*Collector),
	}
}
