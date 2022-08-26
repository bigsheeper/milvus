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
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func (lim *Limiter) getTokens() float64 {
	lim.mu.Lock()
	defer lim.mu.Unlock()
	return lim.tokens
}

func TestLimiter(t *testing.T) {
	limiter := NewLimiter(100, 100)
	tokens := limiter.getTokens()
	assert.Equal(t, float64(0), tokens)

	now := time.Now()

	ok := limiter.AllowN(now, 200)
	assert.True(t, ok)
	tokens = limiter.getTokens()
	assert.Equal(t, float64(-100), tokens)

	ok = limiter.AllowN(now, 100)
	assert.False(t, ok)
	tokens = limiter.getTokens()
	assert.Equal(t, float64(-100), tokens)

	ok = limiter.AllowN(now.Add(1*time.Second), 300)
	assert.True(t, ok)
	tokens = limiter.getTokens()
	assert.Equal(t, float64(-300), tokens)

	ok = limiter.AllowN(now.Add(2*time.Second), 1)
	assert.False(t, ok)
	tokens = limiter.getTokens()
	assert.Equal(t, float64(-300), tokens)

	ok = limiter.AllowN(now.Add(7*time.Second), 50)
	assert.True(t, ok)
	tokens = limiter.getTokens()
	assert.Equal(t, float64(50), tokens)

	ok = limiter.AllowN(now.Add(7*time.Second), 60)
	assert.True(t, ok)
	tokens = limiter.getTokens()
	assert.Equal(t, float64(-10), tokens)

	ok = limiter.AllowN(now.Add(7*time.Second), 10)
	assert.False(t, ok)
	tokens = limiter.getTokens()
	assert.Equal(t, float64(-10), tokens)
}
