// Package harness holds the engine-parameterized UT suite (design §11). Each
// case runs once per registered engine ({pebble} by default, {pebble, slatedb}
// with -tags slatedb). The assertions are engine-agnostic; engine-specific deep
// checks (e.g. Pebble compaction metrics in UT2) are reached via optional
// type-asserted introspection so SlateDB can degrade per R-S2.
package harness

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/pkindex/pkengine"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/pkindex/pkengine/pkenginetest"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/pkindex/pkengine/sstcodec"
)

const collID = int64(1)

func ck(tt uint64) pkengine.WALCheckpoint {
	return pkengine.WALCheckpoint{TimeTick: tt, MessageID: []byte("msg-" + itoa(tt))}
}

func itoa(v uint64) string {
	if v == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	return string(b[i:])
}

func keyInt(pk int64) []byte { return sstcodec.EncodeInt64Key(collID, pk) }

// compactionInspector is the optional Pebble-only introspection for UT2.
type compactionInspector interface {
	BuiltinCompactionCount() int64
	L0FileCount() int64
}

func newEngine(t *testing.T, name string) (pkengine.Engine, pkengine.Compactor, *pkenginetest.MemObjectStore) {
	t.Helper()
	mem := pkenginetest.NewMemObjectStore("demo-bucket")
	eng, comp, err := pkengine.New(name)
	require.NoError(t, err)
	err = eng.Open(context.Background(), pkengine.OpenParam{
		Pchannel:    "pch-0",
		ObjectStore: mem,
		LocalDir:    t.TempDir(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = eng.Close() })
	return eng, comp, mem
}

func forEachEngine(t *testing.T, fn func(t *testing.T, name string)) {
	engines := pkengine.Registered()
	sort.Strings(engines)
	require.NotEmpty(t, engines, "no pkengine registered")
	for _, name := range engines {
		t.Run(name, func(t *testing.T) { fn(t, name) })
	}
}

// fullCompact runs the whole external-compaction + install pipeline once.
func fullCompact(t *testing.T, ctx context.Context, eng pkengine.Engine, comp pkengine.Compactor) bool {
	t.Helper()
	require.NoError(t, eng.Persist(ctx))
	job, ok, err := eng.PlanCompaction(ctx)
	require.NoError(t, err)
	if !ok {
		return false
	}
	result, covered, err := comp.Compact(ctx, job)
	require.NoError(t, err)
	require.NoError(t, eng.InstallCompaction(ctx, result, covered))
	return true
}

// UT1 / C1: lifecycle + point put/delete + delete-then-reinsert.
func TestUT1_LifecyclePutDelete(t *testing.T) {
	forEachEngine(t, func(t *testing.T, name string) {
		ctx := context.Background()
		eng, _, _ := newEngine(t, name)
		k := keyInt(100)

		require.NoError(t, eng.Put(ctx, k, []byte("seg-1"), ck(1)))
		v, found, err := eng.Get(ctx, k)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, []byte("seg-1"), v)

		require.NoError(t, eng.Delete(ctx, k, ck(2)))
		_, found, err = eng.Get(ctx, k)
		require.NoError(t, err)
		assert.False(t, found)

		// delete then re-insert the same PK -> exists again
		require.NoError(t, eng.Put(ctx, k, []byte("seg-2"), ck(3)))
		v, found, err = eng.Get(ctx, k)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, []byte("seg-2"), v)
	})
}

// UT2 / C2: pause builtin compaction; writes do not trigger compaction.
func TestUT2_PauseBuiltinCompaction(t *testing.T) {
	forEachEngine(t, func(t *testing.T, name string) {
		ctx := context.Background()
		eng, _, _ := newEngine(t, name)
		require.NoError(t, eng.PauseBuiltinCompaction(ctx))

		// Many write+persist rounds → multiple L0 flushes with no compaction.
		const rounds = 5
		for r := 0; r < rounds; r++ {
			require.NoError(t, eng.Put(ctx, keyInt(int64(r)), []byte("v"), ck(uint64(r+1))))
			require.NoError(t, eng.Persist(ctx))
		}

		if insp, ok := eng.(compactionInspector); ok {
			assert.Equal(t, int64(0), insp.BuiltinCompactionCount(), "builtin compaction must stay 0")
			assert.GreaterOrEqual(t, insp.L0FileCount(), int64(2), "L0 should accumulate without compaction")
		} else {
			t.Logf("engine %q exposes no compaction introspection: UT2 degraded to 'no error' (R-S2(c))", name)
		}
	})
}

// UT3 / C3: persist materializes SSTs + manifest carrying the WAL checkpoint.
func TestUT3_PersistManifestCheckpoint(t *testing.T) {
	forEachEngine(t, func(t *testing.T, name string) {
		ctx := context.Background()
		eng, _, mem := newEngine(t, name)

		require.NoError(t, eng.Put(ctx, keyInt(1), []byte("a"), ck(10)))
		require.NoError(t, eng.Put(ctx, keyInt(2), []byte("b"), ck(20)))
		require.NoError(t, eng.Persist(ctx))

		// durable checkpoint reflects the latest persisted write position
		dc, err := eng.DurableCheckpoint(ctx)
		require.NoError(t, err)
		assert.Equal(t, uint64(20), dc.TimeTick, "durable checkpoint bound to persisted state")

		// Engines that route persistence through the passed ObjectStore (Pebble)
		// expose SSTs there; engines with a native object store (SlateDB) do not,
		// so the object-presence check degrades to the DurableCheckpoint binding.
		if mem.Len() > 0 {
			assert.NotEmpty(t, mem.KeysWithSuffix(".sst"), "persist must upload SSTs")
		} else {
			t.Logf("engine %q uses a native object store; UT3 verified via DurableCheckpoint only (R-S2)", name)
		}
	})
}

// UT4 / C4: external compaction produces a merged result covering the position.
func TestUT4_ExternalCompaction(t *testing.T) {
	forEachEngine(t, func(t *testing.T, name string) {
		ctx := context.Background()
		eng, comp, _ := newEngine(t, name)

		require.NoError(t, eng.Put(ctx, keyInt(1), []byte("a"), ck(10)))
		require.NoError(t, eng.Put(ctx, keyInt(2), []byte("b"), ck(20)))
		require.NoError(t, eng.Delete(ctx, keyInt(1), ck(30)))
		require.NoError(t, eng.Persist(ctx))

		job, ok, err := eng.PlanCompaction(ctx)
		require.NoError(t, err)
		require.True(t, ok, "compaction should be planned")
		result, covered, err := comp.Compact(ctx, job)
		require.NoError(t, err)
		assert.NotEmpty(t, result, "compaction must produce a result payload")
		assert.Equal(t, uint64(30), covered.TimeTick, "compaction result covers the persisted position")
	})
}

// UT5 / C5: install merged result; data readable; replaced objects recyclable.
func TestUT5_InstallAndRecycle(t *testing.T) {
	forEachEngine(t, func(t *testing.T, name string) {
		ctx := context.Background()
		eng, comp, mem := newEngine(t, name)

		require.NoError(t, eng.Put(ctx, keyInt(1), []byte("a"), ck(10)))
		require.NoError(t, eng.Put(ctx, keyInt(2), []byte("b"), ck(20)))
		require.NoError(t, eng.Persist(ctx))

		persisted := mem.KeysWithSuffix(".sst") // pre-compaction SSTs

		job, ok, err := eng.PlanCompaction(ctx)
		require.NoError(t, err)
		require.True(t, ok)
		result, covered, err := comp.Compact(ctx, job)
		require.NoError(t, err)
		require.NoError(t, eng.InstallCompaction(ctx, result, covered))

		// data still readable after install
		v, found, err := eng.Get(ctx, keyInt(2))
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, []byte("b"), v)
		v, found, err = eng.Get(ctx, keyInt(1))
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, []byte("a"), v)

		// replaced objects recyclable (superseded + removed). SlateDB recycles via
		// its native manifest, not the passed object store, so the check is gated.
		if len(persisted) > 0 {
			for _, k := range persisted {
				exist, err := mem.Exist(ctx, k)
				require.NoError(t, err)
				assert.False(t, exist, "superseded SST %s should be recycled", k)
			}
		} else {
			t.Logf("engine %q recycles via native manifest; UT5 recycle check skipped (R-S2)", name)
		}
	})
}

// UT6 (MUST) / C1–C5: deleted-key resurrection closed loop.
// write A -> delete A -> persist -> compact (drop tombstone) -> install -> A must NOT exist.
func TestUT6_NoResurrection(t *testing.T) {
	forEachEngine(t, func(t *testing.T, name string) {
		ctx := context.Background()
		eng, comp, _ := newEngine(t, name)
		A := keyInt(7)

		require.NoError(t, eng.Put(ctx, A, []byte("seg-A"), ck(1)))
		require.NoError(t, eng.Delete(ctx, A, ck(2)))

		require.True(t, fullCompact(t, ctx, eng, comp), "compaction expected")

		_, found, err := eng.Get(ctx, A)
		require.NoError(t, err)
		assert.False(t, found, "deleted key A must NOT resurrect after compaction+install")
	})
}

// UT6b: resurrection loop with a surviving neighbor (non-empty merged SST).
func TestUT6b_NoResurrectionWithSurvivor(t *testing.T) {
	forEachEngine(t, func(t *testing.T, name string) {
		ctx := context.Background()
		eng, comp, _ := newEngine(t, name)
		A, B := keyInt(7), keyInt(9)

		require.NoError(t, eng.Put(ctx, A, []byte("seg-A"), ck(1)))
		require.NoError(t, eng.Put(ctx, B, []byte("seg-B"), ck(2)))
		require.NoError(t, eng.Delete(ctx, A, ck(3)))

		require.True(t, fullCompact(t, ctx, eng, comp))

		_, found, err := eng.Get(ctx, A)
		require.NoError(t, err)
		assert.False(t, found, "A must not resurrect")
		v, found, err := eng.Get(ctx, B)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, []byte("seg-B"), v)
	})
}

// Checkpoint is off the write path: DurableCheckpoint stays zero until the first
// Persist, then reflects the last applied position. Locks in the "checkpoint only
// persisted at flush" design (both engines).
func TestCheckpointOffWritePath(t *testing.T) {
	forEachEngine(t, func(t *testing.T, name string) {
		ctx := context.Background()
		eng, _, _ := newEngine(t, name)

		require.NoError(t, eng.Put(ctx, keyInt(1), []byte("a"), ck(10)))
		require.NoError(t, eng.Put(ctx, keyInt(2), []byte("b"), ck(20)))

		// no Persist yet → nothing is durable → checkpoint is zero
		dc, err := eng.DurableCheckpoint(ctx)
		require.NoError(t, err)
		assert.Equal(t, uint64(0), dc.TimeTick, "checkpoint must not advance on the write path")

		// Persist binds the applied position into the durable checkpoint
		require.NoError(t, eng.Persist(ctx))
		dc, err = eng.DurableCheckpoint(ctx)
		require.NoError(t, err)
		assert.Equal(t, uint64(20), dc.TimeTick)
	})
}

// UT-CKPT-1 (MUST): durable checkpoint monotonic; stale install (covered<current) rejected.
func TestUTCKPT1_MonotonicAndStaleReject(t *testing.T) {
	forEachEngine(t, func(t *testing.T, name string) {
		ctx := context.Background()
		eng, comp, _ := newEngine(t, name)

		require.NoError(t, eng.Put(ctx, keyInt(1), []byte("a"), ck(5)))
		require.NoError(t, eng.Put(ctx, keyInt(2), []byte("b"), ck(6)))
		require.NoError(t, eng.Persist(ctx))

		dc, err := eng.DurableCheckpoint(ctx)
		require.NoError(t, err)
		require.Equal(t, uint64(6), dc.TimeTick)

		job, ok, err := eng.PlanCompaction(ctx)
		require.NoError(t, err)
		require.True(t, ok)
		result, covered, err := comp.Compact(ctx, job)
		require.NoError(t, err)
		require.Equal(t, uint64(6), covered.TimeTick)

		// valid install at covered=6
		require.NoError(t, eng.InstallCompaction(ctx, result, covered))
		dc, err = eng.DurableCheckpoint(ctx)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, dc.TimeTick, uint64(6))
		current := dc.TimeTick

		// stale install at covered=3 < current must be rejected, position unchanged
		err = eng.InstallCompaction(ctx, result, ck(3))
		assert.Error(t, err, "stale install (covered<current) must be rejected")
		dc, err = eng.DurableCheckpoint(ctx)
		require.NoError(t, err)
		assert.Equal(t, current, dc.TimeTick, "rejected install must not move the position")

		// monotonic across further persists
		require.NoError(t, eng.Put(ctx, keyInt(3), []byte("c"), ck(50)))
		require.NoError(t, eng.Persist(ctx))
		dc, err = eng.DurableCheckpoint(ctx)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, dc.TimeTick, current, "durable checkpoint must be monotonic")
	})
}

// UT-CKPT-2 (MUST): replay idempotency — replaying post-checkpoint writes yields identical state.
func TestUTCKPT2_ReplayIdempotent(t *testing.T) {
	forEachEngine(t, func(t *testing.T, name string) {
		ctx := context.Background()
		eng, _, _ := newEngine(t, name)
		A, B, C := keyInt(1), keyInt(2), keyInt(3)

		// pre-checkpoint state
		require.NoError(t, eng.Put(ctx, A, []byte("a"), ck(1)))
		require.NoError(t, eng.Put(ctx, B, []byte("b"), ck(2)))
		require.NoError(t, eng.Persist(ctx)) // durable checkpoint = 2

		// post-checkpoint batch
		postBatch := func() {
			require.NoError(t, eng.Delete(ctx, A, ck(3)))
			require.NoError(t, eng.Put(ctx, C, []byte("c"), ck(4)))
		}
		postBatch()

		assertState := func() {
			_, found, err := eng.Get(ctx, A)
			require.NoError(t, err)
			assert.False(t, found, "A deleted")
			v, found, err := eng.Get(ctx, B)
			require.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, []byte("b"), v)
			v, found, err = eng.Get(ctx, C)
			require.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, []byte("c"), v)
		}
		assertState()

		// replay the same post-checkpoint batch (simulating WAL replay from durable checkpoint)
		postBatch()
		assertState() // state must be unchanged (idempotent)
	})
}
