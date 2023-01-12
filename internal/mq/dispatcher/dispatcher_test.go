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

package dispatcher

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestMergeChance1(t *testing.T) {
	const (
		total = 1000

		timeUnit      = time.Millisecond
		checkDuration = 10
		jitter        = 200
	)
	tmp := total
	hits := 0

	var mu1, mu2 sync.RWMutex
	curNum1 := 0
	curNum2 := 0

	var wg sync.WaitGroup
	check := func() {
		defer wg.Done()
		ticker := time.NewTicker(checkDuration * timeUnit)
		for tmp > 0 {
			select {
			case <-ticker.C:
				mu1.RLock()
				mu2.RLock()
				if curNum1 == curNum2 {
					hits++
				}
				mu1.RUnlock()
				mu2.RUnlock()
				tmp--
			}
		}
	}

	worker1 := func() {
		for {
			mu1.Lock()
			curNum1++
			mu1.Unlock()
			time.Sleep(time.Duration(rand.Int63n(jitter)) * timeUnit)
		}
	}

	worker2 := func() {
		for {
			mu2.Lock()
			curNum2++
			mu2.Unlock()
			time.Sleep(time.Duration(rand.Int63n(jitter)) * timeUnit)
		}
	}

	go worker1()
	go worker2()
	wg.Add(1)
	go check()
	wg.Wait()
	fmt.Printf("total:%d, hits:%d, ratio:%.2f%%\n", total, hits, float64(hits)/float64(total)*100)
}

func TestMergeChance(t *testing.T) {
	const (
		total = 100

		timeUnit      = time.Millisecond
		timeTick      = 200
		checkDuration = 1
		jitter        = 200
	)
	tmp := total
	hits := 0

	var mu1, mu2 sync.RWMutex
	curNum1 := 0
	curNum2 := 0

	chan1 := make(chan int, 10000)
	chan2 := make(chan int, 10000)

	var wg sync.WaitGroup
	check := func() {
		defer wg.Done()
		ticker := time.NewTicker(checkDuration * timeUnit)
		for tmp > 0 {
			select {
			case <-ticker.C:
				mu1.RLock()
				mu2.RLock()
				if curNum1 == curNum2 {
					hits++
				}
				mu1.RUnlock()
				mu2.RUnlock()
				tmp--
			}
		}
	}

	sender := func() {
		num := 0
		ticker := time.NewTicker(timeTick * timeUnit)
		for {
			select {
			case <-ticker.C:
				chan1 <- num
				chan2 <- num
				num++
			}
		}
	}

	worker1 := func() {
		for {
			mu1.Lock()
			curNum1 = <-chan1
			mu1.Unlock()
			time.Sleep(time.Duration(rand.Int63n(jitter)) * timeUnit)
		}
	}

	worker2 := func() {
		for {
			mu2.Lock()
			curNum2 = <-chan2
			mu2.Unlock()
			time.Sleep(time.Duration(rand.Int63n(jitter)) * timeUnit)
		}
	}

	go sender()
	go worker1()
	go worker2()
	wg.Add(1)
	go check()
	wg.Wait()
	fmt.Printf("total:%d, hits:%d, ratio:%.2f%%\n", total, hits, float64(hits)/float64(total)*100)
}
