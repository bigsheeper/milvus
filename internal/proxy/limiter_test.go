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
	"context"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func closeEnough(a, b Limit) bool {
	return (math.Abs(float64(a)/float64(b)) - 1.0) < 1e-9
}

const (
	d = 100 * time.Millisecond
)

var (
	t0 = time.Now()
	t1 = t0.Add(time.Duration(1) * d)
	t2 = t0.Add(time.Duration(2) * d)
	t3 = t0.Add(time.Duration(3) * d)
	t4 = t0.Add(time.Duration(4) * d)
	t5 = t0.Add(time.Duration(5) * d)
	t9 = t0.Add(time.Duration(9) * d)
)

type allow struct {
	t  time.Time
	n  int
	ok bool
}

func run(t *testing.T, lim *Limiter, allows []allow) {
	t.Helper()
	for i, allow := range allows {
		ok := lim.AllowN(allow.t, allow.n)
		if ok != allow.ok {
			t.Errorf("step %d: lim.AllowN(%v, %v) = %v want %v",
				i, allow.t, allow.n, ok, allow.ok)
		}
	}
}

func TestLimit(t *testing.T) {
	if Limit(10) == Inf {
		t.Errorf("Limit(10) == Inf should be false")
	}
}

func TestLimiterBurst1(t *testing.T) {
	run(t, NewLimiter(10, 1), []allow{
		{t0, 1, true},
		{t0, 1, false},
		{t0, 1, false},
		{t1, 1, true},
		{t1, 1, false},
		{t1, 1, false},
		{t2, 2, false}, // burst size is 1, so n=2 always fails
		{t2, 1, true},
		{t2, 1, false},
	})
}

func TestLimiterBurst3(t *testing.T) {
	run(t, NewLimiter(10, 3), []allow{
		{t0, 2, true},
		{t0, 2, false},
		{t0, 1, true},
		{t0, 1, false},
		{t1, 4, false},
		{t2, 1, true},
		{t3, 1, true},
		{t4, 1, true},
		{t4, 1, true},
		{t4, 1, false},
		{t4, 1, false},
		{t9, 3, true},
		{t9, 0, true},
	})
}

func TestLimiterJumpBackwards(t *testing.T) {
	run(t, NewLimiter(10, 3), []allow{
		{t1, 1, true}, // start at t1
		{t0, 1, true}, // jump back to t0, two tokens remain
		{t0, 1, true},
		{t0, 1, false},
		{t0, 1, false},
		{t1, 1, true}, // got a token
		{t1, 1, false},
		{t1, 1, false},
		{t2, 1, true}, // got another token
		{t2, 1, false},
		{t2, 1, false},
	})
}

// Ensure that tokensFromDuration doesn't produce
// rounding errors by truncating nanoseconds.
// See golang.org/issues/34861.
func TestLimiter_noTruncationErrors(t *testing.T) {
	if !NewLimiter(0.7692307692307693, 1).AllowN(time.Now(), 1) {
		t.Fatal("expected true")
	}
}

// testTime is a fake time used for testing.
type testTime struct {
	mu     sync.Mutex
	cur    time.Time   // current fake time
	timers []testTimer // fake timers
}

// testTimer is a fake timer.
type testTimer struct {
	when time.Time
	ch   chan<- time.Time
}

// now returns the current fake time.
func (tt *testTime) now() time.Time {
	tt.mu.Lock()
	defer tt.mu.Unlock()
	return tt.cur
}

// newTimer creates a fake timer. It returns the channel,
// a function to stop the timer (which we don't care about),
// and a function to advance to the next timer.
func (tt *testTime) newTimer(dur time.Duration) (<-chan time.Time, func() bool, func()) {
	tt.mu.Lock()
	defer tt.mu.Unlock()
	ch := make(chan time.Time, 1)
	timer := testTimer{
		when: tt.cur.Add(dur),
		ch:   ch,
	}
	tt.timers = append(tt.timers, timer)
	return ch, func() bool { return true }, tt.advanceToTimer
}

// since returns the fake time since the given time.
func (tt *testTime) since(t time.Time) time.Duration {
	tt.mu.Lock()
	defer tt.mu.Unlock()
	return tt.cur.Sub(t)
}

// advance advances the fake time.
func (tt *testTime) advance(dur time.Duration) {
	tt.mu.Lock()
	defer tt.mu.Unlock()
	tt.advanceUnlocked(dur)
}

// advanceUnlock advances the fake time, assuming it is already locked.
func (tt *testTime) advanceUnlocked(dur time.Duration) {
	tt.cur = tt.cur.Add(dur)
	i := 0
	for i < len(tt.timers) {
		if tt.timers[i].when.After(tt.cur) {
			i++
		} else {
			tt.timers[i].ch <- tt.cur
			copy(tt.timers[i:], tt.timers[i+1:])
			tt.timers = tt.timers[:len(tt.timers)-1]
		}
	}
}

// advanceToTimer advances the time to the next timer.
func (tt *testTime) advanceToTimer() {
	tt.mu.Lock()
	defer tt.mu.Unlock()
	if len(tt.timers) == 0 {
		panic("no timer")
	}
	when := tt.timers[0].when
	for _, timer := range tt.timers[1:] {
		if timer.when.Before(when) {
			when = timer.when
		}
	}
	tt.advanceUnlocked(when.Sub(tt.cur))
}

// makeTestTime hooks the testTimer into the package.
func makeTestTime(t *testing.T) *testTime {
	return &testTime{
		cur: time.Now(),
	}
}

func TestSimultaneousRequests(t *testing.T) {
	const (
		limit       = 1
		burst       = 5
		numRequests = 15
	)
	var (
		wg    sync.WaitGroup
		numOK = uint32(0)
	)

	// Very slow replenishing bucket.
	lim := NewLimiter(limit, burst)

	// Tries to take a token, atomically updates the counter and decreases the wait
	// group counter.
	f := func() {
		defer wg.Done()
		if ok := lim.AllowN(time.Now(), 1); ok {
			atomic.AddUint32(&numOK, 1)
		}
	}

	wg.Add(numRequests)
	for i := 0; i < numRequests; i++ {
		go f()
	}
	wg.Wait()
	if numOK != burst {
		t.Errorf("numOK = %d, want %d", numOK, burst)
	}
}

func TestLongRunningQPS(t *testing.T) {
	// The test runs for a few (fake) seconds executing many requests
	// and then checks that overall number of requests is reasonable.
	const (
		limit = 100
		burst = 100
	)
	var (
		numOK = int32(0)
		tt    = makeTestTime(t)
	)

	lim := NewLimiter(limit, burst)

	start := tt.now()
	end := start.Add(5 * time.Second)
	for tt.now().Before(end) {
		if ok := lim.AllowN(tt.now(), 1); ok {
			numOK++
		}

		// This will still offer ~500 requests per second, but won't consume
		// outrageous amount of CPU.
		tt.advance(2 * time.Millisecond)
	}
	elapsed := tt.since(start)
	ideal := burst + (limit * float64(elapsed) / float64(time.Second))

	// We should never get more requests than allowed.
	if want := int32(ideal + 1); numOK > want {
		t.Errorf("numOK = %d, want %d (ideal %f)", numOK, want, ideal)
	}
	// We should get very close to the number of requests allowed.
	if want := int32(0.999 * ideal); numOK < want {
		t.Errorf("numOK = %d, want %d (ideal %f)", numOK, want, ideal)
	}
}

type request struct {
	t   time.Time
	n   int
	act time.Time
	ok  bool
}

// dFromDuration converts a duration to the nearest multiple of the global constant d.
func dFromDuration(dur time.Duration) int {
	// Add d/2 to dur so that integer division will round to
	// the nearest multiple instead of truncating.
	// (We don't care about small inaccuracies.)
	return int((dur + (d / 2)) / d)
}

// dSince returns multiples of d since t0
func dSince(t time.Time) int {
	return dFromDuration(t.Sub(t0))
}

func runReserve(t *testing.T, lim *Limiter, req request) *Reservation {
	t.Helper()
	return runReserveMax(t, lim, req, InfDuration)
}

func runReserveMax(t *testing.T, lim *Limiter, req request, maxReserve time.Duration) *Reservation {
	t.Helper()
	r := lim.reserveN(req.t, req.n, maxReserve)
	if r.ok != req.ok {
		t.Errorf("lim.reserveN(t%d, %v, %v) = (%v) want (t%d, %v)",
			dSince(req.t), req.n, maxReserve, r.ok, dSince(req.act), req.ok)
	}
	return &r
}

func TestSimpleReserve(t *testing.T) {
	lim := NewLimiter(10, 2)

	runReserve(t, lim, request{t0, 2, t0, true})
	runReserve(t, lim, request{t0, 2, t2, true})
	runReserve(t, lim, request{t3, 2, t4, true})
}

func TestMix(t *testing.T) {
	lim := NewLimiter(10, 2)

	runReserve(t, lim, request{t0, 3, t1, false}) // should return false because n > Burst
	runReserve(t, lim, request{t0, 2, t0, true})
	run(t, lim, []allow{{t1, 2, false}}) // not enough tokens - don't allow
	runReserve(t, lim, request{t1, 2, t2, true})
	run(t, lim, []allow{{t1, 1, false}}) // negative tokens - don't allow
	run(t, lim, []allow{{t3, 1, true}})
}

func TestReserveJumpBack(t *testing.T) {
	lim := NewLimiter(10, 2)

	runReserve(t, lim, request{t1, 2, t1, true}) // start at t1
	runReserve(t, lim, request{t0, 1, t1, true}) // should violate Limit,Burst
	runReserve(t, lim, request{t2, 2, t3, true})
}

func TestReserveSetLimit(t *testing.T) {
	lim := NewLimiter(5, 2)

	runReserve(t, lim, request{t0, 2, t0, true})
	runReserve(t, lim, request{t0, 2, t4, true})
	lim.SetLimitAt(t2, 10)
	runReserve(t, lim, request{t2, 1, t4, true}) // violates Limit and Burst
}

func TestReserveMax(t *testing.T) {
	lim := NewLimiter(10, 2)
	maxT := d

	runReserveMax(t, lim, request{t0, 2, t0, true}, maxT)
	runReserveMax(t, lim, request{t0, 1, t1, true}, maxT)  // reserve for close future
	runReserveMax(t, lim, request{t0, 1, t2, false}, maxT) // time to act too far in the future
}

type wait struct {
	name   string
	ctx    context.Context
	n      int
	delay  int // in multiples of d
	nilErr bool
}

func TestZeroLimit(t *testing.T) {
	r := NewLimiter(0, 1)
	if !r.AllowN(time.Now(), 1) {
		t.Errorf("Limit(0, 1) want true when first used")
	}
	if r.AllowN(time.Now(), 1) {
		t.Errorf("Limit(0, 1) want false when already used")
	}
}
