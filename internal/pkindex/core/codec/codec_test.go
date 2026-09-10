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

package codec

import (
	"bytes"
	"math"
	"math/rand"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

var int64Boundaries = []int64{
	math.MinInt64, math.MinInt64 + 1, -1, 0, 1, math.MaxInt64 - 1, math.MaxInt64,
}

func sign(c int) int {
	if c > 0 {
		return 1
	}
	if c < 0 {
		return -1
	}
	return 0
}

func TestInt64PKOrderPreserving(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	pool := append([]int64{}, int64Boundaries...)
	for i := 0; i < 1000; i++ {
		pool = append(pool, r.Int63(), -r.Int63(), r.Int63n(1000)-500)
	}
	for i := 0; i < 100000; i++ {
		a := pool[r.Intn(len(pool))]
		b := pool[r.Intn(len(pool))]
		wantCmp := 0
		if a < b {
			wantCmp = -1
		} else if a > b {
			wantCmp = 1
		}
		got := sign(bytes.Compare(EncodeInt64PK(a), EncodeInt64PK(b)))
		assert.Equalf(t, wantCmp, got, "order mismatch for %d vs %d", a, b)
	}
}

func TestInt64PKRoundtrip(t *testing.T) {
	r := rand.New(rand.NewSource(2))
	vals := append([]int64{}, int64Boundaries...)
	for i := 0; i < 10000; i++ {
		vals = append(vals, r.Int63()-r.Int63())
	}
	for _, v := range vals {
		enc := EncodeInt64PK(v)
		assert.Len(t, enc, Int64PKSize)
		dec, err := DecodeInt64PK(enc)
		assert.NoError(t, err)
		assert.Equal(t, v, dec)
	}
}

func TestDecodeInt64PKInvalidLength(t *testing.T) {
	for _, b := range [][]byte{nil, {}, make([]byte, 7), make([]byte, 9)} {
		_, err := DecodeInt64PK(b)
		assert.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrDataIntegrity)
	}
}

func TestVarcharPKOrderPreserving(t *testing.T) {
	r := rand.New(rand.NewSource(3))
	pool := []string{"", "a", "aa", "ab", "b", "\x00", "\xff", strings.Repeat("z", 300)}
	alphabet := "ab\x00\xffxyz"
	for i := 0; i < 1000; i++ {
		n := r.Intn(20)
		var sb strings.Builder
		for j := 0; j < n; j++ {
			sb.WriteByte(alphabet[r.Intn(len(alphabet))])
		}
		pool = append(pool, sb.String())
	}
	for i := 0; i < 100000; i++ {
		a := pool[r.Intn(len(pool))]
		b := pool[r.Intn(len(pool))]
		got := sign(bytes.Compare(EncodeVarcharPK(a), EncodeVarcharPK(b)))
		assert.Equalf(t, sign(strings.Compare(a, b)), got, "order mismatch for %q vs %q", a, b)
	}
}

func TestVarcharPKRoundtrip(t *testing.T) {
	for _, v := range []string{"", "pk", "\x00\xff", strings.Repeat("x", 1024)} {
		assert.Equal(t, v, DecodeVarcharPK(EncodeVarcharPK(v)))
	}
}

func TestPKEntryRoundtrip(t *testing.T) {
	for _, id := range []int64{0, 1, -1, math.MaxInt64, math.MinInt64, 445566} {
		enc := EncodePKEntry(PKEntry{SegmentID: id})
		dec, err := DecodePKEntry(enc)
		assert.NoError(t, err)
		assert.Equal(t, PKEntry{SegmentID: id}, dec)
	}
}

func TestDecodePKEntryInvalid(t *testing.T) {
	// short buffer
	for _, b := range [][]byte{nil, {}, {pkEntryVersion}, make([]byte, 8)} {
		_, err := DecodePKEntry(b)
		assert.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrDataIntegrity)
	}
	// unknown version
	bad := EncodePKEntry(PKEntry{SegmentID: 1})
	bad[0] = 0x7f
	_, err := DecodePKEntry(bad)
	assert.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrDataIntegrity)
}
