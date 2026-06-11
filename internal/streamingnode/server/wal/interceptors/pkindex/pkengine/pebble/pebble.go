// Package pebble is the Pebble (pure-Go, local LSM + self-built object-store
// uploader/manifest) implementation of pkengine.Engine / pkengine.Compactor.
//
// C1 build/put/delete: pebble.Open + Batch.Set/Delete (checkpoint meta key in
// the same batch → atomic, §6.6).
// C2 pause builtin compaction: Options.DisableAutomaticCompactions=true at Open.
// C3 persist + manifest: Flush → enumerate on-disk SSTs → upload via ObjectStore
// → manifest.json carrying the WAL checkpoint.
// C4 external compaction (indexnode): download SSTs → sstcodec.MergeSSTs (drop
// tombstones) → upload new SST.
// C5 atomic install: IngestAndExcise (single VersionEdit: excise replaced span +
// ingest merged SST), then write back max(covered,current) to the meta key.
//
// See design doc §4 / §6 / §7.2.
package pebble

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	crdbpebble "github.com/cockroachdb/pebble"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/pkindex/pkengine"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/pkindex/pkengine/sstcodec"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func init() {
	pkengine.Register("pebble", func() (pkengine.Engine, pkengine.Compactor, error) {
		sh := &shared{}
		return &engine{shared: sh}, &compactor{shared: sh}, nil
	})
}

// shared is the in-process state shared between the engine (streamingnode side)
// and its compactor (indexnode side). In production the compactor runs in the
// DataNode process and is configured with its own ObjectStore + scratch dir;
// for the in-process Demo UTs they share this struct so the engine-agnostic
// harness needs no engine-specific wiring. See report deviation D-CMP1.
type shared struct {
	os         pkengine.ObjectStore
	pchannel   string
	prefix     string // object-store key prefix: <root>/pkindex/<pchannel>
	scratchDir string // compactor local scratch
	mergeSeq   int64
}

// engine is the per-pchannel Pebble Engine. mu serializes Persist/Install (which
// mutate the manifest + LSM); Put/Delete/Get rely on Pebble's own concurrency.
//
// Checkpoint is kept OFF the write path: Put/Delete only bump the in-memory
// lastApplied position (no KV write, no extra fsync); the covered WAL position
// is made durable solely at Persist/Install, recorded in manifest.json
// (§6.6) and mirrored in durableCkpt. This keeps the hot path free of redundant
// per-write checkpoint commits while preserving the invariant "durable
// checkpoint never runs ahead of durable data" (it only advances inside the
// flush that makes the data durable). ckmu guards the two checkpoint fields.
type engine struct {
	*shared
	mu          sync.Mutex
	db          *crdbpebble.DB
	dbDir       string // local Pebble data dir
	ingestDir   string // local staging for SSTs about to be IngestAndExcise'd
	manifestVer int    // monotonically increasing manifest version
	ingestSeq   int64  // unique suffix for ingest temp files

	ckmu        sync.Mutex
	lastApplied pkengine.WALCheckpoint // latest position applied to the LSM (in-memory)
	durableCkpt pkengine.WALCheckpoint // latest position made durable (= manifest checkpoint)
}

// --- object-store layout helpers ---

// manifestKey is the object-store key of this pchannel's manifest.json.
func (e *engine) manifestKey() string { return e.prefix + "/manifest.json" }

// sstEntry describes one uploaded SST in the manifest: its object key and the
// [Min,Max] data-key range (used to compute the compaction/excise span).
type sstEntry struct {
	Key     string `json:"key"`
	Min     []byte `json:"min"`
	Max     []byte `json:"max"`
	HasData bool   `json:"has_data"`
}

// checkpointJSON is the JSON form of a WALCheckpoint stored in the manifest.
type checkpointJSON struct {
	TimeTick  uint64 `json:"time_tick"`
	MessageID []byte `json:"message_id"`
}

// manifest is the self-built object-store manifest (A6: simple JSON, single
// writer). Each version carries the covered WAL checkpoint (§6.6).
type manifest struct {
	Version    int            `json:"version"`
	SSTs       []sstEntry     `json:"ssts"`
	Checkpoint checkpointJSON `json:"checkpoint"`
	Superseded []string       `json:"superseded,omitempty"`
}

// compactionJob is the opaque payload PlanCompaction hands to the compactor: the
// SSTs to merge plus the WAL position they cover.
type compactionJob struct {
	InputKeys []string       `json:"input_keys"`
	Covered   checkpointJSON `json:"covered"`
}

// compactionResult is the opaque payload the compactor returns: the merged SST's
// object key, the excise span to remove on install, and the live-key count
// (LiveCount==0 means an empty merged SST → install excises without ingesting).
type compactionResult struct {
	NewSSTKey string `json:"new_sst_key"`
	ExciseMin []byte `json:"excise_min"`
	ExciseMax []byte `json:"excise_max"`
	LiveCount int    `json:"live_count"`
}

// --- Engine implementation ---

// Open creates/opens the per-pchannel Pebble DB under p.LocalDir and remembers
// the object-store prefix (<root>/pkindex/<pchannel>) used for uploads. It pins
// FormatNewest (required by IngestAndExcise, R-P1) and disables builtin
// compaction (C2) so the external indexnode compactor owns merging.
func (e *engine) Open(ctx context.Context, p pkengine.OpenParam) error {
	if p.ObjectStore == nil {
		return merr.WrapErrParameterInvalidMsg("pebble engine requires ObjectStore")
	}
	if p.LocalDir == "" {
		return merr.WrapErrParameterInvalidMsg("pebble engine requires LocalDir")
	}
	e.os = p.ObjectStore
	e.pchannel = p.Pchannel
	e.prefix = path.Join(p.ObjectStore.RootPath(), "pkindex", p.Pchannel)
	e.scratchDir = filepath.Join(p.LocalDir, "compactor-scratch")
	e.dbDir = filepath.Join(p.LocalDir, "db")
	e.ingestDir = filepath.Join(p.LocalDir, "ingest")
	for _, d := range []string{e.scratchDir, e.ingestDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return err
		}
	}
	opts := &crdbpebble.Options{
		Comparer:                    sstcodec.Comparer,
		FormatMajorVersion:          crdbpebble.FormatNewest, // R-P1: required by IngestAndExcise
		DisableAutomaticCompactions: true,                    // C2
	}
	db, err := crdbpebble.Open(e.dbDir, opts)
	if err != nil {
		return err
	}
	e.db = db
	// Resume the durable checkpoint + manifest version from a prior run's manifest.
	if m, err := e.loadManifest(ctx); err == nil {
		e.manifestVer = m.Version
		e.durableCkpt = pkengine.WALCheckpoint{TimeTick: m.Checkpoint.TimeTick, MessageID: m.Checkpoint.MessageID}
		e.lastApplied = e.durableCkpt
	}
	if p.Recover != nil {
		e.advance(*p.Recover)
	}
	return nil
}

// advance bumps the in-memory lastApplied position (monotonic). This is the only
// checkpoint bookkeeping on the write path: no KV write, no fsync.
func (e *engine) advance(at pkengine.WALCheckpoint) {
	e.ckmu.Lock()
	if at.TimeTick > e.lastApplied.TimeTick {
		e.lastApplied = at
	}
	e.ckmu.Unlock()
}

// Put writes one PK->locator entry and records the applied position in memory.
// The covered WAL position is NOT written here — it is persisted only at
// Persist/Install (see the engine type comment). Data still commits with Sync;
// dropping that fsync (the index is WAL-rebuildable) is a separate knob.
func (e *engine) Put(ctx context.Context, key, value []byte, at pkengine.WALCheckpoint) error {
	b := e.db.NewBatch()
	if err := b.Set(key, value, nil); err != nil {
		return err
	}
	if err := e.db.Apply(b, crdbpebble.Sync); err != nil {
		return err
	}
	e.advance(at)
	return nil
}

// Delete tombstones one PK and records the applied position in memory (see Put).
func (e *engine) Delete(ctx context.Context, key []byte, at pkengine.WALCheckpoint) error {
	b := e.db.NewBatch()
	if err := b.Delete(key, nil); err != nil {
		return err
	}
	if err := e.db.Apply(b, crdbpebble.Sync); err != nil {
		return err
	}
	e.advance(at)
	return nil
}

// Get is a point read for UT assertions; found=false on a tombstoned/absent key.
func (e *engine) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	v, closer, err := e.db.Get(key)
	if err == crdbpebble.ErrNotFound {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	out := append([]byte{}, v...)
	_ = closer.Close()
	return out, true, nil
}

// Close closes the underlying Pebble DB.
func (e *engine) Close() error {
	if e.db == nil {
		return nil
	}
	return e.db.Close()
}

// PauseBuiltinCompaction is a confirmation no-op: Pebble's builtin compaction is
// already disabled at Open via DisableAutomaticCompactions (C2).
func (e *engine) PauseBuiltinCompaction(ctx context.Context) error {
	if e.db == nil {
		return merr.WrapErrServiceInternal("pebble engine not opened")
	}
	return nil
}

// setDurable advances the durable checkpoint mirror (monotonic).
func (e *engine) setDurable(ck pkengine.WALCheckpoint) {
	e.ckmu.Lock()
	if ck.TimeTick >= e.durableCkpt.TimeTick {
		e.durableCkpt = ck
	}
	e.ckmu.Unlock()
}

// DurableCheckpoint returns the WAL position covered by the current durable
// state (the last value persisted into manifest.json at Persist/Install), which
// is monotonic by construction. Note it stays zero until the first Persist —
// in-memory writes are not "durable" until flushed.
func (e *engine) DurableCheckpoint(ctx context.Context) (pkengine.WALCheckpoint, error) {
	if e.db == nil {
		return pkengine.WALCheckpoint{}, merr.WrapErrServiceInternal("pebble engine not opened")
	}
	e.ckmu.Lock()
	defer e.ckmu.Unlock()
	return e.durableCkpt, nil
}

// Persist materializes durable state to object storage (C3): it flushes the
// memtable to on-disk SSTs, uploads any not-yet-uploaded SST via the
// ObjectStore, and publishes a new manifest.json version carrying the current
// applied WAL position (which the flush just made durable). Idempotent across
// calls (SSTs are keyed by their stable Pebble filename, re-upload skipped via
// Exist).
func (e *engine) Persist(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.db.Flush(); err != nil {
		return err
	}
	// The flush made everything applied so far durable; bind that position.
	e.ckmu.Lock()
	ck := e.lastApplied
	e.ckmu.Unlock()

	files, err := os.ReadDir(e.dbDir)
	if err != nil {
		return err
	}
	entries := make([]sstEntry, 0, len(files))
	for _, f := range files {
		if f.IsDir() || !strings.HasSuffix(f.Name(), ".sst") {
			continue
		}
		full := filepath.Join(e.dbDir, f.Name())
		min, max, hasData, err := sstcodec.SSTDataRange(full)
		if err != nil {
			return err
		}
		key := e.prefix + "/" + f.Name()
		exist, err := e.os.Exist(ctx, key)
		if err != nil {
			return err
		}
		if !exist {
			b, err := os.ReadFile(full)
			if err != nil {
				return err
			}
			if err := e.os.Write(ctx, key, b); err != nil {
				return err
			}
		}
		entries = append(entries, sstEntry{Key: key, Min: min, Max: max, HasData: hasData})
	}
	e.manifestVer++
	if err := e.writeManifest(ctx, manifest{
		Version:    e.manifestVer,
		SSTs:       entries,
		Checkpoint: checkpointJSON{TimeTick: ck.TimeTick, MessageID: ck.MessageID},
	}); err != nil {
		return err
	}
	e.setDurable(ck)
	return nil
}

// PlanCompaction (C4 planning) reads the current manifest and produces an opaque
// job listing the SSTs to merge plus their union data-key span (the future
// excise span) and the covered checkpoint. ok=false when there is nothing to
// compact (no SSTs, or only meta keys).
func (e *engine) PlanCompaction(ctx context.Context) ([]byte, bool, error) {
	m, err := e.loadManifest(ctx)
	if err != nil {
		return nil, false, err
	}
	if len(m.SSTs) == 0 {
		return nil, false, nil
	}
	inputKeys := make([]string, 0, len(m.SSTs))
	var spanMin, spanMax []byte
	for _, s := range m.SSTs {
		inputKeys = append(inputKeys, s.Key)
		if s.HasData {
			if spanMin == nil || bytes.Compare(s.Min, spanMin) < 0 {
				spanMin = s.Min
			}
			if spanMax == nil || bytes.Compare(s.Max, spanMax) > 0 {
				spanMax = s.Max
			}
		}
	}
	if spanMin == nil {
		return nil, false, nil // nothing but meta keys; nothing to compact
	}
	job, err := json.Marshal(compactionJob{InputKeys: inputKeys, Covered: m.Checkpoint})
	if err != nil {
		return nil, false, err
	}
	return job, true, nil
}

func (e *engine) InstallCompaction(ctx context.Context, result []byte, at pkengine.WALCheckpoint) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	var res compactionResult
	if err := json.Unmarshal(result, &res); err != nil {
		return err
	}

	// (UT-CKPT-1) reject stale install: covered must be >= current durable checkpoint.
	e.ckmu.Lock()
	current := e.durableCkpt
	e.ckmu.Unlock()
	if !at.Covers(current) {
		return merr.WrapErrParameterInvalidMsg(
			"stale pkindex install rejected: covered TimeTick %d < current %d", at.TimeTick, current.TimeTick)
	}
	if res.ExciseMin == nil || res.ExciseMax == nil {
		return merr.WrapErrParameterInvalidMsg("install result missing excise span")
	}

	span := crdbpebble.KeyRange{Start: res.ExciseMin, End: sstcodec.ExciseEndBound(res.ExciseMax)}
	var paths []string
	if res.LiveCount > 0 {
		b, err := e.os.Read(ctx, res.NewSSTKey)
		if err != nil {
			return err
		}
		seq := atomic.AddInt64(&e.ingestSeq, 1)
		localPath := filepath.Join(e.ingestDir, "ingest-"+itoa(seq)+".sst")
		if err := os.WriteFile(localPath, b, 0o644); err != nil {
			return err
		}
		paths = []string{localPath}
	}
	// Single VersionEdit: excise the replaced span (removes old SSTs incl. resurrectable
	// deleted keys) + ingest the merged SST. The covered position rides on the manifest,
	// not the SST, so there is no position regression (§6.6 competition fix).
	if _, err := e.db.IngestAndExcise(paths, nil, span); err != nil {
		return err
	}

	// Advance the durable checkpoint to max(covered, current) in the manifest below.
	writeback := at
	if current.TimeTick > writeback.TimeTick {
		writeback = current
	}

	// Object lifecycle: supersede + remove replaced SSTs, publish new manifest version.
	m, err := e.loadManifest(ctx)
	if err != nil {
		return err
	}
	oldKeys := make([]string, 0, len(m.SSTs))
	for _, s := range m.SSTs {
		oldKeys = append(oldKeys, s.Key)
	}
	for _, k := range oldKeys {
		if k == res.NewSSTKey {
			continue
		}
		_ = e.os.Remove(ctx, k)
	}
	e.manifestVer++
	newSSTs := []sstEntry{}
	if res.LiveCount > 0 {
		newSSTs = append(newSSTs, sstEntry{Key: res.NewSSTKey, Min: res.ExciseMin, Max: res.ExciseMax, HasData: true})
	}
	if err := e.writeManifest(ctx, manifest{
		Version:    e.manifestVer,
		SSTs:       newSSTs,
		Checkpoint: checkpointJSON{TimeTick: writeback.TimeTick, MessageID: writeback.MessageID},
		Superseded: oldKeys,
	}); err != nil {
		return err
	}
	e.setDurable(writeback)
	return nil
}

// writeManifest publishes a manifest version by overwriting manifest.json
// (single writer, A6).
func (e *engine) writeManifest(ctx context.Context, m manifest) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return e.os.Write(ctx, e.manifestKey(), b)
}

// loadManifest reads the current manifest.json, or the empty manifest if none
// has been written yet.
func (e *engine) loadManifest(ctx context.Context) (manifest, error) {
	exist, err := e.os.Exist(ctx, e.manifestKey())
	if err != nil {
		return manifest{}, err
	}
	if !exist {
		return manifest{}, nil
	}
	b, err := e.os.Read(ctx, e.manifestKey())
	if err != nil {
		return manifest{}, err
	}
	var m manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return manifest{}, err
	}
	return m, nil
}

// --- Compactor implementation (indexnode side) ---

// compactor is the Pebble self-built external compactor. It shares the object
// store and scratch dir with its engine via *shared.
type compactor struct {
	*shared
}

// Compact (C4 execution) downloads the job's input SSTs, multi-way merges them
// dropping tombstones (sstcodec.MergeSSTs), uploads the merged SST, and returns
// the result (new SST key + excise span) plus the covered WAL position. Dropping
// tombstones is safe only because install uses IngestAndExcise to remove the
// replaced range (no older SST can resurrect a dropped key).
func (c *compactor) Compact(ctx context.Context, jobBytes []byte) ([]byte, pkengine.WALCheckpoint, error) {
	var job compactionJob
	if err := json.Unmarshal(jobBytes, &job); err != nil {
		return nil, pkengine.WALCheckpoint{}, err
	}
	localPaths := make([]string, 0, len(job.InputKeys))
	for i, key := range job.InputKeys {
		b, err := c.os.Read(ctx, key)
		if err != nil {
			return nil, pkengine.WALCheckpoint{}, err
		}
		lp := filepath.Join(c.scratchDir, "in-"+itoa(int64(i))+".sst")
		if err := os.WriteFile(lp, b, 0o644); err != nil {
			return nil, pkengine.WALCheckpoint{}, err
		}
		localPaths = append(localPaths, lp)
	}
	seq := atomic.AddInt64(&c.mergeSeq, 1)
	outLocal := filepath.Join(c.scratchDir, "merged-"+itoa(seq)+".sst")
	exMin, exMax, n, err := sstcodec.MergeSSTs(localPaths, outLocal)
	if err != nil {
		return nil, pkengine.WALCheckpoint{}, err
	}
	outKey := c.prefix + "/merged-" + itoa(seq) + ".sst"
	b, err := os.ReadFile(outLocal)
	if err != nil {
		return nil, pkengine.WALCheckpoint{}, err
	}
	if err := c.os.Write(ctx, outKey, b); err != nil {
		return nil, pkengine.WALCheckpoint{}, err
	}
	res, err := json.Marshal(compactionResult{NewSSTKey: outKey, ExciseMin: exMin, ExciseMax: exMax, LiveCount: n})
	if err != nil {
		return nil, pkengine.WALCheckpoint{}, err
	}
	return res, pkengine.WALCheckpoint{TimeTick: job.Covered.TimeTick, MessageID: job.Covered.MessageID}, nil
}

// itoa formats an int64 as a decimal string (used for deterministic temp-file
// and object-key suffixes).
func itoa(v int64) string { return strconv.FormatInt(v, 10) }

// --- test-only introspection (not part of pkengine.Engine) ---

// BuiltinCompactionCount returns the number of builtin compactions Pebble has
// run. With DisableAutomaticCompactions it must stay 0 (UT2).
func (e *engine) BuiltinCompactionCount() int64 {
	return e.db.Metrics().Compact.Count
}

// L0FileCount returns the number of files currently in L0 (UT2 accumulation).
func (e *engine) L0FileCount() int64 {
	return e.db.Metrics().Levels[0].NumFiles
}
