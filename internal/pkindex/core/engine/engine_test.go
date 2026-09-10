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

package engine

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/pkindex/core/codec"
	"github.com/milvus-io/milvus/internal/pkindex/core/sst"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func newTestEngine(t *testing.T, opts ...func(*Config)) Engine {
	shared := NewSharedResources(32 << 20)
	t.Cleanup(shared.Release)
	cfg := Config{
		Dir:      t.TempDir(),
		VChannel: "test-vchannel-v0",
		Shared:   shared,
	}
	for _, o := range opts {
		o(&cfg)
	}
	e, err := Open(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { e.Close() })
	return e
}

func pk(i int64) []byte { return codec.EncodeInt64PK(i) }

func entry(seg int64) []byte { return codec.EncodePKEntry(codec.PKEntry{SegmentID: seg}) }

func probeOne(t *testing.T, e Engine, key []byte) []byte {
	vs, err := e.Probe(context.Background(), [][]byte{key})
	require.NoError(t, err)
	require.Len(t, vs, 1)
	return vs[0]
}

func requireSegment(t *testing.T, e Engine, key []byte, want int64) {
	t.Helper()
	v := probeOne(t, e, key)
	require.NotNil(t, v)
	got, err := codec.DecodePKEntry(v)
	require.NoError(t, err)
	require.Equal(t, want, got.SegmentID)
}

// buildBaseline writes one baseline SST from (pk, value) pairs into dir and
// returns its table descriptor.
func buildBaseline(t *testing.T, dir string, pairs map[int64][]byte) BaselineTable {
	w, err := sst.NewWriter(dir)
	require.NoError(t, err)
	keys := make([]int64, 0, len(pairs))
	for k := range pairs {
		keys = append(keys, k)
	}
	// int64 order == encoded order
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[j] < keys[i] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}
	for _, k := range keys {
		require.NoError(t, w.Add(pk(k), pairs[k]))
	}
	info, err := w.Close()
	require.NoError(t, err)
	return BaselineTable{Path: info.Path, MinKey: info.MinKey, MaxKey: info.MaxKey}
}

// publish copies a drained SST under its content-addressed name. The source
// name cannot be reused: pebble numbers files per DB, so every generation
// starts over at the same numbers and staging by local name would clobber an
// earlier generation's table.
func publish(t *testing.T, info sst.Info, dstDir string) string {
	in, err := os.Open(info.Path)
	require.NoError(t, err)
	defer in.Close()
	dst := filepath.Join(dstDir, sst.FileName(info.Sha256))
	out, err := os.Create(dst)
	require.NoError(t, err)
	_, err = io.Copy(out, in)
	require.NoError(t, err)
	require.NoError(t, out.Close())
	return dst
}

// commitToBaseline runs the caller-side half of one snapshot cycle: publish the
// drained SSTs to a baseline location (standing in for the S3 upload plus the
// manifest commit) and install them.
func commitToBaseline(t *testing.T, e Engine, baseDir string, infos []sst.Info, existing []BaselineTable) []BaselineTable {
	t.Helper()
	fresh := make([]BaselineTable, 0, len(infos))
	for _, info := range infos {
		fresh = append(fresh, BaselineTable{
			Path:   publish(t, info, baseDir),
			MinKey: info.MinKey,
			MaxKey: info.MaxKey,
		})
	}
	tables := append(fresh, existing...) // newest first
	require.NoError(t, e.InstallBaseline(context.Background(), tables))
	return tables
}

func TestApplyProbe(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()

	require.NoError(t, e.Apply(ctx, []Mutation{
		{Key: pk(1), Value: entry(100)},
		{Key: pk(2), Value: entry(200)},
	}))

	requireSegment(t, e, pk(1), 100)
	assert.Nil(t, probeOne(t, e, pk(3)), "never-written key must be absent")

	// delete masks
	require.NoError(t, e.Apply(ctx, []Mutation{{Key: pk(1), Value: nil}}))
	assert.Nil(t, probeOne(t, e, pk(1)), "deleted key must be absent")

	// re-insert after delete
	require.NoError(t, e.Apply(ctx, []Mutation{{Key: pk(1), Value: entry(101)}}))
	requireSegment(t, e, pk(1), 101)
}

func TestBaselineProbeAndIncrementOverride(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()
	baseDir := t.TempDir()

	table := buildBaseline(t, baseDir, map[int64][]byte{
		10: entry(1),
		20: entry(2),
		30: codec.EncodeTombstone(), // deleted in an earlier merge round
	})
	require.NoError(t, e.InstallBaseline(ctx, []BaselineTable{table}))

	requireSegment(t, e, pk(10), 1)

	// baseline tombstone means absent
	assert.Nil(t, probeOne(t, e, pk(30)))

	// outside every table's range: pruned, absent
	assert.Nil(t, probeOne(t, e, pk(5)))
	assert.Nil(t, probeOne(t, e, pk(35)))

	// increment overrides baseline
	require.NoError(t, e.Apply(ctx, []Mutation{{Key: pk(10), Value: entry(11)}}))
	requireSegment(t, e, pk(10), 11)

	// increment delete masks baseline
	require.NoError(t, e.Apply(ctx, []Mutation{{Key: pk(20), Value: nil}}))
	assert.Nil(t, probeOne(t, e, pk(20)))
}

func TestBaselineRecencyOrder(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()

	older := buildBaseline(t, t.TempDir(), map[int64][]byte{7: entry(1), 8: entry(1)})
	newer := buildBaseline(t, t.TempDir(), map[int64][]byte{7: entry(2)})

	// newest-first: overlapping key 7 resolves from the newer table
	require.NoError(t, e.InstallBaseline(ctx, []BaselineTable{newer, older}))
	requireSegment(t, e, pk(7), 2)
	// key 8 only in the older table still resolves
	requireSegment(t, e, pk(8), 1)
}

func TestSnapshotCycle(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()
	baseDir := t.TempDir()

	const n = 1000
	muts := make([]Mutation, 0, n)
	for i := int64(0); i < n; i++ {
		muts = append(muts, Mutation{Key: pk(i), Value: entry(i)})
	}
	require.NoError(t, e.Apply(ctx, muts))

	gen, err := e.RotateIncrement(ctx)
	require.NoError(t, err)
	assert.Equal(t, []Generation{gen}, e.DrainingGenerations())
	// data stays readable while its DB is frozen
	requireSegment(t, e, pk(42), 42)

	infos, err := e.FlushDraining(ctx, gen)
	require.NoError(t, err)
	require.NotEmpty(t, infos)
	var total int64
	for _, info := range infos {
		total += info.NumEntries
		require.NoError(t, sst.Verify(info.Path, info.Sha256))
	}
	assert.Equal(t, int64(n), total, "the frozen DB's SST set must hold everything written to it")

	tables := commitToBaseline(t, e, baseDir, infos, nil)
	require.NoError(t, e.DropDraining(ctx, gen))
	assert.Empty(t, e.DrainingGenerations())

	// after the drop the baseline answers, and the frozen dir is gone
	requireSegment(t, e, pk(42), 42)
	_, err = os.Stat(filepath.Join(e.(*pebbleEngine).cfg.Dir, "increment-1"))
	assert.True(t, os.IsNotExist(err))
	assert.Len(t, tables, len(infos))

	// the fresh active DB keeps taking writes
	require.NoError(t, e.Apply(ctx, []Mutation{{Key: pk(n), Value: entry(n)}}))
	requireSegment(t, e, pk(n), n)
}

// TestFlushDrainingOrdersNewestFirst covers one generation holding several
// overlapping L0 SSTs, which happens whenever a memtable fills up before the
// rotation. The drained set must come back newest first, because that is the
// order InstallBaseline resolves overlapping keys in.
func TestFlushDrainingOrdersNewestFirst(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()
	baseDir := t.TempDir()
	// stands in for a memtable filling up mid-generation
	flushActive := func() { require.NoError(t, e.(*pebbleEngine).active.db.Flush()) }

	require.NoError(t, e.Apply(ctx, []Mutation{
		{Key: pk(1), Value: entry(10)},
		{Key: pk(2), Value: entry(20)},
	}))
	flushActive()
	require.NoError(t, e.Apply(ctx, []Mutation{
		{Key: pk(1), Value: nil},
		{Key: pk(2), Value: entry(21)},
	}))
	flushActive()
	require.NoError(t, e.Apply(ctx, []Mutation{{Key: pk(2), Value: entry(22)}}))

	gen, err := e.RotateIncrement(ctx)
	require.NoError(t, err)
	infos, err := e.FlushDraining(ctx, gen)
	require.NoError(t, err)
	require.Len(t, infos, 3)

	// the newest table holds only pk(2)
	assert.Equal(t, int64(1), infos[0].NumEntries)
	assert.Equal(t, pk(2), infos[0].MinKey)

	commitToBaseline(t, e, baseDir, infos, nil)
	require.NoError(t, e.DropDraining(ctx, gen))

	// answered by the baseline alone now: the delete must still win over the
	// older put, and the latest put over the earlier ones
	assert.Nil(t, probeOne(t, e, pk(1)), "a deleted pk must not resurrect from an older table")
	requireSegment(t, e, pk(2), 22)
}

// TestRotateKeepsConcurrentWrites is the reason rotation freezes instead of
// discarding: writes racing a snapshot cycle land in the new active DB, and a
// write that lands in the frozen DB just before the swap is still drained.
func TestRotateKeepsConcurrentWrites(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()
	baseDir := t.TempDir()

	const writers = 4
	const perWriter = 500
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				key := int64(w*perWriter + i)
				assert.NoError(t, e.Apply(ctx, []Mutation{{Key: pk(key), Value: entry(key)}}))
			}
		}(w)
	}

	// run full snapshot cycles while the writers are running
	var tables []BaselineTable
	for round := 0; round < 3; round++ {
		gen, err := e.RotateIncrement(ctx)
		require.NoError(t, err)
		infos, err := e.FlushDraining(ctx, gen)
		require.NoError(t, err)
		tables = commitToBaseline(t, e, baseDir, infos, tables)
		require.NoError(t, e.DropDraining(ctx, gen))
	}
	wg.Wait()

	// one final cycle to absorb whatever the writers wrote after the last one
	gen, err := e.RotateIncrement(ctx)
	require.NoError(t, err)
	infos, err := e.FlushDraining(ctx, gen)
	require.NoError(t, err)
	commitToBaseline(t, e, baseDir, infos, tables)
	require.NoError(t, e.DropDraining(ctx, gen))

	// every key written during the cycles survived
	for key := int64(0); key < writers*perWriter; key++ {
		requireSegment(t, e, pk(key), key)
	}
}

func TestDropDrainingRequiresRotate(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()

	_, err := e.FlushDraining(ctx, 1)
	assert.Error(t, err, "the active generation is not drainable")
	assert.Error(t, e.DropDraining(ctx, 1))

	gen, err := e.RotateIncrement(ctx)
	require.NoError(t, err)
	require.NoError(t, e.DropDraining(ctx, gen))
	assert.Error(t, e.DropDraining(ctx, gen), "dropping twice must fail")
}

func TestWarmRestartRestoresStack(t *testing.T) {
	shared := NewSharedResources(32 << 20)
	t.Cleanup(shared.Release)
	dir := t.TempDir()
	cfg := Config{Dir: dir, VChannel: "vc", Shared: shared}
	ctx := context.Background()

	e, err := Open(ctx, cfg)
	require.NoError(t, err)
	require.NoError(t, e.Apply(ctx, []Mutation{{Key: pk(1), Value: entry(1)}}))

	// leave a cycle half-finished: frozen, flushed, but never dropped
	gen, err := e.RotateIncrement(ctx)
	require.NoError(t, err)
	_, err = e.FlushDraining(ctx, gen)
	require.NoError(t, err)
	require.NoError(t, e.Apply(ctx, []Mutation{{Key: pk(2), Value: entry(2)}}))
	_, err = e.FlushDraining(ctx, gen) // flushing twice is harmless on a frozen DB
	require.NoError(t, err)
	require.NoError(t, e.Close())

	// reopening restores both DBs, so the interrupted cycle can be resumed
	// instead of leaking a directory
	e2, err := Open(ctx, cfg)
	require.NoError(t, err)
	defer e2.Close()
	assert.Equal(t, []Generation{gen}, e2.DrainingGenerations())
	requireSegment(t, e2, pk(1), 1)

	infos, err := e2.FlushDraining(ctx, gen)
	require.NoError(t, err)
	var total int64
	for _, info := range infos {
		total += info.NumEntries
	}
	assert.Equal(t, int64(1), total)
}

func TestUnflushedWritesDoNotSurviveRestart(t *testing.T) {
	// the engine's own WAL is disabled on purpose: unflushed state is expected
	// to be lost and rebuilt by WAL replay, so the recovery layer must never
	// assume otherwise
	shared := NewSharedResources(32 << 20)
	t.Cleanup(shared.Release)
	dir := t.TempDir()
	cfg := Config{Dir: dir, VChannel: "vc", Shared: shared}
	ctx := context.Background()

	e, err := Open(ctx, cfg)
	require.NoError(t, err)
	require.NoError(t, e.Apply(ctx, []Mutation{{Key: pk(1), Value: entry(1)}}))
	require.NoError(t, e.Close())

	e2, err := Open(ctx, cfg)
	require.NoError(t, err)
	defer e2.Close()
	assert.Nil(t, probeOne(t, e2, pk(1)))
}

func TestStats(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()

	require.NoError(t, e.Apply(ctx, []Mutation{{Key: pk(1), Value: entry(1)}}))
	s := e.Stats()
	assert.Positive(t, s.MemTableBytes)
	assert.Zero(t, s.DrainingDBs)

	gen, err := e.RotateIncrement(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, e.Stats().DrainingDBs)

	infos, err := e.FlushDraining(ctx, gen)
	require.NoError(t, err)
	tables := commitToBaseline(t, e, t.TempDir(), infos, nil)
	assert.Equal(t, len(tables), e.Stats().BaselineTables)
}

func TestDestroy(t *testing.T) {
	shared := NewSharedResources(32 << 20)
	t.Cleanup(shared.Release)
	dir := t.TempDir()
	ctx := context.Background()
	e, err := Open(ctx, Config{Dir: dir, VChannel: "vc", Shared: shared})
	require.NoError(t, err)
	require.NoError(t, e.Apply(ctx, []Mutation{{Key: pk(1), Value: entry(1)}}))
	_, err = e.RotateIncrement(ctx)
	require.NoError(t, err)

	require.NoError(t, e.Destroy())
	_, err = os.Stat(dir)
	assert.True(t, os.IsNotExist(err), "destroy must drop the whole stack")
}

func TestClosedEngineRejects(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()
	require.NoError(t, e.Close())

	assert.Error(t, e.Apply(ctx, []Mutation{{Key: pk(1), Value: entry(1)}}))
	_, err := e.Probe(ctx, [][]byte{pk(1)})
	assert.Error(t, err)
	_, err = e.RotateIncrement(ctx)
	assert.Error(t, err)
	assert.Error(t, e.InstallBaseline(ctx, nil))
	assert.NoError(t, e.Close(), "closing twice is a no-op")
}

func TestLocalMeta(t *testing.T) {
	e := newTestEngine(t)

	_, ok, err := e.GetLocalMeta("checkpoint")
	require.NoError(t, err)
	assert.False(t, ok)

	require.NoError(t, e.PutLocalMeta("checkpoint", []byte("v1")))
	b, ok, err := e.GetLocalMeta("checkpoint")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, []byte("v1"), b)

	// atomic overwrite
	require.NoError(t, e.PutLocalMeta("checkpoint", []byte("v2")))
	b, _, err = e.GetLocalMeta("checkpoint")
	require.NoError(t, err)
	assert.Equal(t, []byte("v2"), b)
}

func TestConcurrentInstallAndProbe(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()

	t1 := buildBaseline(t, t.TempDir(), map[int64][]byte{1: entry(10)})
	t2 := buildBaseline(t, t.TempDir(), map[int64][]byte{1: entry(20)})

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				vs, err := e.Probe(ctx, [][]byte{pk(1)})
				if !assert.NoError(t, err) || len(vs) != 1 || vs[0] == nil {
					continue // vs[0]==nil only before the first install
				}
				got, err := codec.DecodePKEntry(vs[0])
				assert.NoError(t, err)
				assert.Contains(t, []int64{10, 20}, got.SegmentID,
					"probe must always observe one complete baseline")
			}
		}()
	}
	for i := 0; i < 50; i++ {
		require.NoError(t, e.InstallBaseline(ctx, []BaselineTable{t1}))
		require.NoError(t, e.InstallBaseline(ctx, []BaselineTable{t2}))
	}
	close(stop)
	wg.Wait()
}

func TestBaselineReadersShareNodeCache(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()
	table := buildBaseline(t, t.TempDir(), map[int64][]byte{1: entry(10), 2: entry(20)})
	require.NoError(t, e.InstallBaseline(ctx, []BaselineTable{table}))

	c := e.(*pebbleEngine).cfg.Shared.cache
	before := c.Metrics()
	requireSegment(t, e, pk(1), 10)
	requireSegment(t, e, pk(1), 10)
	after := c.Metrics()
	assert.Greater(t, after.Count, before.Count, "baseline blocks must land in the node-level cache")
	assert.Greater(t, after.Hits, before.Hits, "a repeated baseline probe must hit the node-level cache")
}

// requireReadWriteUnblocked asserts Apply and Probe finish while some other
// operation is parked inside its disk IO.
func requireReadWriteUnblocked(t *testing.T, e Engine) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		ctx := context.Background()
		assert.NoError(t, e.Apply(ctx, []Mutation{{Key: pk(7), Value: entry(70)}}))
		_, err := e.Probe(ctx, [][]byte{pk(7)})
		assert.NoError(t, err)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Apply/Probe blocked behind disk IO of a structural operation")
	}
}

func TestRotateOpensNewDBOutsideLock(t *testing.T) {
	e := newTestEngine(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var origin func(*pebbleEngine, Generation) (*incrementDB, error)
	mocker := mockey.Mock((*pebbleEngine).openIncrement).To(func(pe *pebbleEngine, gen Generation) (*incrementDB, error) {
		close(entered)
		<-release
		return origin(pe, gen)
	}).Origin(&origin).Build()
	defer mocker.UnPatch()

	rotated := make(chan error, 1)
	go func() {
		_, err := e.RotateIncrement(context.Background())
		rotated <- err
	}()
	<-entered
	defer func() {
		close(release)
		require.NoError(t, <-rotated)
		requireSegment(t, e, pk(7), 70)
	}()
	requireReadWriteUnblocked(t, e)
}

func TestDropDeletesFrozenDBOutsideLock(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()
	gen, err := e.RotateIncrement(ctx)
	require.NoError(t, err)

	entered, release := make(chan struct{}), make(chan struct{})
	var origin func(*incrementDB) error
	mocker := mockey.Mock((*incrementDB).destroy).To(func(inc *incrementDB) error {
		close(entered)
		<-release
		return origin(inc)
	}).Origin(&origin).Build()
	defer mocker.UnPatch()

	dropped := make(chan error, 1)
	go func() { dropped <- e.DropDraining(ctx, gen) }()
	<-entered
	defer func() {
		close(release)
		require.NoError(t, <-dropped)
	}()
	requireReadWriteUnblocked(t, e)
}

// The tombstone encoding is reserved: a caller deletes with a nil Value, and a
// Value that happens to equal the tombstone is a caller bug, not a delete.
func TestApplyRejectsReservedTombstoneValue(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()
	require.NoError(t, e.Apply(ctx, []Mutation{{Key: pk(1), Value: entry(10)}}))

	err := e.Apply(ctx, []Mutation{
		{Key: pk(2), Value: entry(20)},
		{Key: pk(1), Value: codec.EncodeTombstone()},
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrServiceInternal)

	// the rejected batch leaves no trace
	requireSegment(t, e, pk(1), 10)
	assert.Nil(t, probeOne(t, e, pk(2)))
}

// The SSTs a memtable flush produces become baseline tables as they are, so
// they must carry the bloom filter too.
func TestDrainedTablesCarryBloomFilter(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()
	const n = 100000
	muts := make([]Mutation, 0, n)
	for i := int64(0); i < n; i++ {
		muts = append(muts, Mutation{Key: pk(i * 2), Value: entry(i)})
	}
	require.NoError(t, e.Apply(ctx, muts))
	gen, err := e.RotateIncrement(ctx)
	require.NoError(t, err)
	infos, err := e.FlushDraining(ctx, gen)
	require.NoError(t, err)
	commitToBaseline(t, e, t.TempDir(), infos, nil)
	require.NoError(t, e.DropDraining(ctx, gen))

	c := e.(*pebbleEngine).cfg.Shared.cache
	before := c.Metrics().Misses
	for i := int64(0); i < 500; i++ {
		require.Nil(t, probeOne(t, e, pk(i*(2*n/500)+1)))
	}
	assert.Less(t, c.Metrics().Misses-before, int64(50), "baseline misses must be answered by the bloom filter")
	requireSegment(t, e, pk(4242), 2121)
}

// A missing SharedResources is a wiring bug in Milvus, not bad request content.
func TestOpenWithoutSharedResourcesIsInternalError(t *testing.T) {
	_, err := Open(context.Background(), Config{Dir: t.TempDir(), VChannel: "test-vchannel-v0"})
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrServiceInternal)
}
