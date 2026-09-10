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

package sst

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/pkindex/core/codec"
	"github.com/milvus-io/milvus/internal/pkindex/pkerr"
)

// writeFixture writes n int64 PKs (0..n-1) with segmentID=pk*10 and returns
// the finalized Info.
func writeFixture(t *testing.T, dir string, n int) Info {
	w, err := NewWriter(dir)
	require.NoError(t, err)
	for i := 0; i < n; i++ {
		err := w.Add(codec.EncodeInt64PK(int64(i)), codec.EncodePKEntry(codec.PKEntry{SegmentID: int64(i * 10)}))
		require.NoError(t, err)
	}
	info, err := w.Close()
	require.NoError(t, err)
	return info
}

func TestWriterReaderRoundtrip(t *testing.T) {
	dir := t.TempDir()
	const n = 10000
	info := writeFixture(t, dir, n)

	assert.Equal(t, int64(n), info.NumEntries)
	assert.Equal(t, codec.EncodeInt64PK(0), info.MinKey)
	assert.Equal(t, codec.EncodeInt64PK(n-1), info.MaxKey)
	assert.Equal(t, filepath.Join(dir, FileName(info.Sha256)), info.Path)
	st, err := os.Stat(info.Path)
	require.NoError(t, err)
	assert.Equal(t, st.Size(), info.Size)

	r, err := OpenReader(info.Path, nil)
	require.NoError(t, err)
	defer r.Close()

	var i int64
	err = r.Iter(func(key, value []byte) error {
		pk, err := codec.DecodeInt64PK(key)
		require.NoError(t, err)
		require.Equal(t, i, pk)
		e, err := codec.DecodePKEntry(value)
		require.NoError(t, err)
		require.Equal(t, pk*10, e.SegmentID)
		i++
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, int64(n), i)
}

func TestReaderProbe(t *testing.T) {
	dir := t.TempDir()
	info := writeFixture(t, dir, 1000)
	r, err := OpenReader(info.Path, nil)
	require.NoError(t, err)
	defer r.Close()

	// hit
	v, ok, err := r.Probe(codec.EncodeInt64PK(233))
	require.NoError(t, err)
	require.True(t, ok)
	e, err := codec.DecodePKEntry(v)
	require.NoError(t, err)
	assert.Equal(t, int64(2330), e.SegmentID)

	// miss inside range is impossible with dense fixture; miss beyond both ends
	_, ok, err = r.Probe(codec.EncodeInt64PK(-1))
	require.NoError(t, err)
	assert.False(t, ok)
	_, ok, err = r.Probe(codec.EncodeInt64PK(1000))
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestContentAddressingDeterministic(t *testing.T) {
	d1, d2 := t.TempDir(), t.TempDir()
	i1 := writeFixture(t, d1, 5000)
	i2 := writeFixture(t, d2, 5000)
	assert.Equal(t, i1.Sha256, i2.Sha256, "same content must produce the same content address")

	d3 := t.TempDir()
	i3 := writeFixture(t, d3, 5001)
	assert.NotEqual(t, i1.Sha256, i3.Sha256)
}

func TestVerify(t *testing.T) {
	dir := t.TempDir()
	info := writeFixture(t, dir, 100)
	require.NoError(t, Verify(info.Path, info.Sha256))

	// flip one byte in the middle
	b, err := os.ReadFile(info.Path)
	require.NoError(t, err)
	b[len(b)/2] ^= 0xff
	tampered := filepath.Join(dir, "tampered.sst")
	require.NoError(t, os.WriteFile(tampered, b, 0o600)) //nolint:gosec // tampered is a fixed name under the test's own temp dir
	err = Verify(tampered, info.Sha256)
	assert.True(t, errors.Is(err, pkerr.ErrCorrupted))
}

func TestEmptyWriter(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(dir)
	require.NoError(t, err)
	info, err := w.Close()
	require.NoError(t, err)
	assert.Zero(t, info.NumEntries)
	assert.Nil(t, info.MinKey)
	assert.Nil(t, info.MaxKey)
}

func TestAbort(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(dir)
	require.NoError(t, err)
	require.NoError(t, w.Add(codec.EncodeInt64PK(1), codec.EncodePKEntry(codec.PKEntry{SegmentID: 1})))
	w.Abort()
	ents, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Empty(t, ents)
}

// TestComparerMatchesEngineDefault guards the producer/consumer contract: the
// engine's pebble DB (default comparer) must agree with the comparer every SST
// is written and read with, or flush output and merge output would not be
// interchangeable.
func TestComparerMatchesEngineDefault(t *testing.T) {
	assert.Equal(t, pebble.DefaultComparer.Name, Comparer.Name)
}

func TestReaderUsesGivenBlockCache(t *testing.T) {
	info := writeFixture(t, t.TempDir(), 1000)
	c := pebble.NewCache(8 << 20)
	defer c.Unref()
	r, err := OpenReader(info.Path, c)
	require.NoError(t, err)
	defer r.Close()

	for i := 0; i < 2; i++ {
		_, ok, err := r.Probe(codec.EncodeInt64PK(233))
		require.NoError(t, err)
		require.True(t, ok)
	}
	m := c.Metrics()
	assert.Positive(t, m.Count, "probed blocks must land in the given cache")
	assert.Positive(t, m.Hits, "the second probe must be served from the given cache")
}

// A probe for a key inside the table's range but absent from it is the common
// case on the write path; the table's bloom filter must answer it without
// reading data blocks.
func TestProbeMissSkipsDataBlocks(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(dir)
	require.NoError(t, err)
	const n = 100000
	for i := int64(0); i < n; i++ { // even keys only: every odd key is an in-range miss
		require.NoError(t, w.Add(codec.EncodeInt64PK(i*2), codec.EncodePKEntry(codec.PKEntry{SegmentID: i})))
	}
	info, err := w.Close()
	require.NoError(t, err)

	c := pebble.NewCache(64 << 20)
	defer c.Unref()
	r, err := OpenReader(info.Path, c)
	require.NoError(t, err)
	defer r.Close()

	// 500 misses spread over the whole key range
	for i := int64(0); i < 500; i++ {
		_, ok, err := r.Probe(codec.EncodeInt64PK(i*(2*n/500) + 1))
		require.NoError(t, err)
		require.False(t, ok)
	}
	// without a filter nearly every probe loads a distinct data block
	assert.Less(t, c.Metrics().Misses, int64(50), "misses must be answered by the bloom filter, not by data blocks")

	v, ok, err := r.Probe(codec.EncodeInt64PK(4242))
	require.NoError(t, err)
	require.True(t, ok)
	e, err := codec.DecodePKEntry(v)
	require.NoError(t, err)
	assert.Equal(t, int64(2121), e.SegmentID)
}

// A flipped byte inside a data block is only discovered when that block is
// read, so the read path must categorize it the same way Verify does.
func TestProbeOnCorruptedDataBlock(t *testing.T) {
	dir := t.TempDir()
	const n = 10000
	info := writeFixture(t, dir, n)

	b, err := os.ReadFile(info.Path)
	require.NoError(t, err)
	b[len(b)/10] ^= 0xff
	corrupted := filepath.Join(dir, "corrupted.sst")
	require.NoError(t, os.WriteFile(corrupted, b, 0o600)) //nolint:gosec // a fixed name under the test's own temp dir

	r, err := OpenReader(corrupted, nil)
	require.NoError(t, err, "the flipped byte is in a data block, so opening must still succeed")
	defer r.Close()

	var probeErr error
	for i := 0; i < n && probeErr == nil; i++ {
		_, _, probeErr = r.Probe(codec.EncodeInt64PK(int64(i)))
	}
	require.Error(t, probeErr, "a flipped byte in a data block must surface on the probe path")
	assert.True(t, errors.Is(probeErr, pkerr.ErrCorrupted), "got %v", probeErr)
}

// Iteration ends on an unreadable block the same way it ends on the last
// entry, so a scan of a damaged file must fail rather than return short.
func TestIterOnCorruptedDataBlock(t *testing.T) {
	dir := t.TempDir()
	const n = 10000
	info := writeFixture(t, dir, n)

	b, err := os.ReadFile(info.Path)
	require.NoError(t, err)
	b[len(b)/10] ^= 0xff
	corrupted := filepath.Join(dir, "corrupted-iter.sst")
	require.NoError(t, os.WriteFile(corrupted, b, 0o600)) //nolint:gosec // a fixed name under the test's own temp dir

	r, err := OpenReader(corrupted, nil)
	require.NoError(t, err)
	defer r.Close()

	err = r.Iter(func(key, value []byte) error { return nil })
	require.Error(t, err)
	assert.True(t, errors.Is(err, pkerr.ErrCorrupted), "got %v", err)
}
