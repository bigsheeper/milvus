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

package datacoord

import (
	"sync"
	"testing"
	"time"
)

func TestSingleLockUnlock(t *testing.T) {
	lock := NewSegmentKeyLock()
	segmentID := int64(1)

	lock.Lock(segmentID)
	lock.Unlock(segmentID)

	lock.Lock(segmentID)
	lock.Unlock(segmentID)

	lock.RLock(segmentID)
	lock.RUnlock(segmentID)
}

func TestMultipleLocksOrder(t *testing.T) {
	lock := NewSegmentKeyLock()
	ids := []int64{3, 1, 2}

	lock.Lock(ids...)
	lock.Unlock(ids...)

	lock.RLock(ids...)
	lock.RUnlock(ids...)
}

func TestReadWriteLockBehavior(t *testing.T) {
	lock := NewSegmentKeyLock()
	segmentID := int64(1)

	lock.RLock(segmentID)
	defer lock.RUnlock(segmentID)

	writeLock := make(chan struct{})
	go func() {
		lock.Lock(segmentID)
		close(writeLock)
		lock.Unlock(segmentID)
	}()

	select {
	case <-writeLock:
		t.Fatal("Write lock acquired while read lock is held")
	case <-time.After(100 * time.Millisecond):
		// expect get here
	}
}

func TestConcurrentWriters(t *testing.T) {
	lock := NewSegmentKeyLock()
	segmentID := int64(1)
	var wg sync.WaitGroup
	iterations := 100

	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			lock.Lock(segmentID)
			lock.Unlock(segmentID)
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			lock.Lock(segmentID)
			lock.Unlock(segmentID)
		}
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Possible deadlock in concurrent writers")
	}
}

func TestDeadlockAvoidance(t *testing.T) {
	lock := NewSegmentKeyLock()
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		lock.Lock(2, 1)
		defer lock.Unlock(2, 1)
		time.Sleep(100 * time.Millisecond)
	}()

	go func() {
		defer wg.Done()
		time.Sleep(50 * time.Millisecond)
		lock.Lock(1, 2)
		defer lock.Unlock(1, 2)
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("Deadlock detected")
	}
}

func TestHighConcurrency(t *testing.T) {
	lock := NewSegmentKeyLock()
	var wg sync.WaitGroup
	const numGoroutines = 1000

	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			segmentID := int64(id % 50)
			if id%2 == 0 {
				lock.Lock(segmentID)
				defer lock.Unlock(segmentID)
			} else {
				lock.RLock(segmentID)
				defer lock.RUnlock(segmentID)
			}
			time.Sleep(10 * time.Millisecond)
		}(i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("High concurrency test timed out, possible deadlock")
	}
}
