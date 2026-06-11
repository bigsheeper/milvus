//go:build slatedb

// Package slatedb is the SlateDB (object-store-native LSM, cgo/uniffi binding)
// implementation of pkengine.Engine / pkengine.Compactor. It is only built with
// `-tags slatedb` because it links the native libslatedb_uniffi (cgo); see
// design §13.2 and the R-S2 capability gaps:
//
//   - C2 (pause builtin compaction): the writer has no in-process compaction →
//     PauseBuiltinCompaction is a confirmation no-op (R-S2(c), degraded).
//   - C3 (manifest position): the binding cannot write manifest custom fields →
//     the WAL checkpoint rides on the reserved meta key, persisted only at
//     Persist/Install (once per flush, off the write path) (R-S2(b), §6.6).
//   - C4 (external compaction): the binding exposes no compactor-control entry →
//     compaction is native (standalone compactor / read-time tombstone
//     application); the Demo Compactor degrades to "trigger + echo covered"
//     (R-S2(a)). Deleted keys never resurrect natively, so UT6 holds via reads.
//   - C5 (install): no explicit ingest → InstallCompaction degrades to
//     "flush + covered>=current check + write back position" (§6.5).
//
// Object store: the Demo resolves "memory:///" (override with
// PKINDEX_SLATEDB_OBJSTORE_URL); the passed pkengine.ObjectStore is not bridged
// because the in-memory test fake is not a resolvable object-store URL — see
// report deviation D-S-OS. Production would resolve the ChunkManager endpoint.
package slatedb

import (
	"context"
	"encoding/json"
	"os"
	"sync"

	slatedbffi "slatedb.io/slatedb-go/uniffi"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/pkindex/pkengine"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/pkindex/pkengine/sstcodec"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func init() {
	pkengine.Register("slatedb", func() (pkengine.Engine, pkengine.Compactor, error) {
		sh := &shared{}
		return &engine{shared: sh}, &compactor{shared: sh}, nil
	})
}

type shared struct {
	pchannel string
}

type engine struct {
	*shared
	store *slatedbffi.ObjectStore
	db    *slatedbffi.Db

	ckmu        sync.Mutex
	lastApplied pkengine.WALCheckpoint // latest applied position (in-memory, off the write path)
}

type checkpointJSON struct {
	TimeTick  uint64 `json:"time_tick"`
	MessageID []byte `json:"message_id"`
}

type compactionJob struct {
	Covered checkpointJSON `json:"covered"`
}

type compactionResult struct {
	Native  bool           `json:"native"`
	Covered checkpointJSON `json:"covered"`
}

// Open resolves the SlateDB-native object store (memory:/// by default) and
// builds the per-pchannel Db. The passed pkengine.ObjectStore is intentionally
// not bridged (D-S-OS): the in-memory test fake is not a resolvable URL, and
// SlateDB manages its SSTs + CAS manifest natively.
func (e *engine) Open(ctx context.Context, p pkengine.OpenParam) error {
	url := os.Getenv("PKINDEX_SLATEDB_OBJSTORE_URL")
	if url == "" {
		url = "memory:///" // Demo default; production resolves the ChunkManager endpoint (D-S-OS)
	}
	store, err := slatedbffi.ObjectStoreResolve(url)
	if err != nil {
		return merr.WrapErrServiceInternal("slatedb ObjectStoreResolve failed: " + err.Error())
	}
	builder := slatedbffi.NewDbBuilder("pkindex/"+p.Pchannel, store)
	defer builder.Destroy()
	db, err := builder.Build()
	if err != nil {
		store.Destroy()
		return merr.WrapErrServiceInternal("slatedb DbBuilder.Build failed: " + err.Error())
	}
	e.pchannel = p.Pchannel
	e.store = store
	e.db = db
	return nil
}

// advance bumps the in-memory lastApplied position (monotonic); the checkpoint
// meta key is NOT touched on the write path — it is persisted only at
// Persist/Install (R-S2(b): meta key replaces the unavailable manifest field).
func (e *engine) advance(at pkengine.WALCheckpoint) {
	e.ckmu.Lock()
	if at.TimeTick > e.lastApplied.TimeTick {
		e.lastApplied = at
	}
	e.ckmu.Unlock()
}

// Put writes the PK->locator entry and records the applied position in memory.
func (e *engine) Put(ctx context.Context, key, value []byte, at pkengine.WALCheckpoint) error {
	if _, err := e.db.Put(key, value); err != nil {
		return err
	}
	e.advance(at)
	return nil
}

// Delete tombstones the PK and records the applied position in memory.
func (e *engine) Delete(ctx context.Context, key []byte, at pkengine.WALCheckpoint) error {
	if _, err := e.db.Delete(key); err != nil {
		return err
	}
	e.advance(at)
	return nil
}

// Get is a point read; SlateDB returns a nil pointer for an absent/tombstoned
// key (tombstones are applied on read, so deleted keys never resurrect).
func (e *engine) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	v, err := e.db.Get(key)
	if err != nil {
		return nil, false, err
	}
	if v == nil {
		return nil, false, nil
	}
	return append([]byte{}, (*v)...), true, nil
}

// Close shuts down and frees the Rust-side Db and ObjectStore handles (R-S4).
func (e *engine) Close() error {
	if e.db != nil {
		if err := e.db.Shutdown(); err != nil {
			return err
		}
		e.db.Destroy()
		e.db = nil
	}
	if e.store != nil {
		e.store.Destroy()
		e.store = nil
	}
	return nil
}

// PauseBuiltinCompaction is a confirmation no-op: the SlateDB writer runs no
// in-process compaction (R-S2(c), degraded vs Pebble's explicit disable).
func (e *engine) PauseBuiltinCompaction(ctx context.Context) error {
	if e.db == nil {
		return merr.WrapErrServiceInternal("slatedb engine not opened")
	}
	return nil
}

// Persist writes the current applied position into the checkpoint meta key, then
// flushes; SlateDB natively materializes SSTs + a versioned CAS manifest to
// object storage (C3) with the meta key persisted in the same flush. The
// checkpoint is written here (once per flush), not on the write path.
func (e *engine) Persist(ctx context.Context) error {
	if e.db == nil {
		return merr.WrapErrServiceInternal("slatedb engine not opened")
	}
	e.ckmu.Lock()
	cur := e.lastApplied
	e.ckmu.Unlock()
	if err := e.writeCheckpoint(cur); err != nil {
		return err
	}
	return e.db.Flush() // native SST + versioned CAS manifest
}

// writeCheckpoint persists the checkpoint meta key (single write, not on the hot path).
func (e *engine) writeCheckpoint(ck pkengine.WALCheckpoint) error {
	wb := slatedbffi.NewWriteBatch()
	defer wb.Destroy()
	if err := wb.Put(sstcodec.CheckpointMetaKey, sstcodec.EncodeCheckpoint(ck.TimeTick, ck.MessageID)); err != nil {
		return err
	}
	_, err := e.db.Write(wb)
	return err
}

// readMetaCheckpoint reads the reserved checkpoint meta key from the latest
// (CAS) state; zero when unset.
func (e *engine) readMetaCheckpoint() pkengine.WALCheckpoint {
	v, err := e.db.Get(sstcodec.CheckpointMetaKey)
	if err != nil || v == nil {
		return pkengine.WALCheckpoint{}
	}
	tt, msgID := sstcodec.DecodeCheckpoint(*v)
	return pkengine.WALCheckpoint{TimeTick: tt, MessageID: msgID}
}

// DurableCheckpoint returns the WAL position covered by the latest CAS state.
func (e *engine) DurableCheckpoint(ctx context.Context) (pkengine.WALCheckpoint, error) {
	if e.db == nil {
		return pkengine.WALCheckpoint{}, merr.WrapErrServiceInternal("slatedb engine not opened")
	}
	return e.readMetaCheckpoint(), nil
}

// PlanCompaction emits a trigger job carrying the current covered position; the
// actual merge is native to SlateDB (R-S2(a)), so there is nothing to plan
// beyond signaling the compactor. ok=false until something has been written.
func (e *engine) PlanCompaction(ctx context.Context) ([]byte, bool, error) {
	cur := e.readMetaCheckpoint()
	if cur.IsZero() {
		return nil, false, nil
	}
	job, err := json.Marshal(compactionJob{Covered: checkpointJSON{TimeTick: cur.TimeTick, MessageID: cur.MessageID}})
	if err != nil {
		return nil, false, err
	}
	return job, true, nil
}

// InstallCompaction degrades to: stale-reject (covered>=current) + flush refresh
// + write back max(covered,current). No explicit ingest/excise — SlateDB applies
// tombstones natively, so deleted keys do not resurrect (§6.5 SlateDB column).
func (e *engine) InstallCompaction(ctx context.Context, result []byte, at pkengine.WALCheckpoint) error {
	if e.db == nil {
		return merr.WrapErrServiceInternal("slatedb engine not opened")
	}
	current := e.readMetaCheckpoint()
	if !at.Covers(current) {
		return merr.WrapErrParameterInvalidMsg(
			"stale pkindex install rejected: covered TimeTick %d < current %d", at.TimeTick, current.TimeTick)
	}
	writeback := at
	if current.TimeTick > writeback.TimeTick {
		writeback = current
	}
	if err := e.writeCheckpoint(writeback); err != nil {
		return err
	}
	return e.db.Flush()
}

// compactor is the indexnode-side worker. With no binding compactor entry it
// degrades to "trigger native compaction + echo covered". If a slatedb-cli
// compactor binary is wired (PKINDEX_SLATEDB_CLI), a production demo could exec
// it here against the same bucket; the in-process UTs rely on native read-time
// tombstone application instead (R-S2(a)).
type compactor struct {
	*shared
}

func (c *compactor) Compact(ctx context.Context, jobBytes []byte) ([]byte, pkengine.WALCheckpoint, error) {
	var job compactionJob
	if err := json.Unmarshal(jobBytes, &job); err != nil {
		return nil, pkengine.WALCheckpoint{}, err
	}
	log.Ctx(ctx).Info("slatedb compaction is native (standalone compactor / read-time tombstone GC); demo compactor echoes covered position (R-S2(a))")
	res, err := json.Marshal(compactionResult{Native: true, Covered: job.Covered})
	if err != nil {
		return nil, pkengine.WALCheckpoint{}, err
	}
	return res, pkengine.WALCheckpoint{TimeTick: job.Covered.TimeTick, MessageID: job.Covered.MessageID}, nil
}
