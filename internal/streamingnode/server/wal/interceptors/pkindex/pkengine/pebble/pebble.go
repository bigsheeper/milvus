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
type engine struct {
	*shared
	mu          sync.Mutex
	db          *crdbpebble.DB
	dbDir       string // local Pebble data dir
	ingestDir   string // local staging for SSTs about to be IngestAndExcise'd
	manifestVer int    // monotonically increasing manifest version
	ingestSeq   int64  // unique suffix for ingest temp files
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
	return nil
}

// Put writes one PK->locator entry and advances the checkpoint meta key in the
// SAME batch, so data and its covered WAL position commit atomically (§6.6).
//
// Note: the checkpoint meta Set is a cheap memtable write, but Apply(..., Sync)
// fsyncs the WAL once per call. Since the abstraction is per-key (§3) and
// DoAppend loops Put per PK, a single insert message of N PKs writes the same
// checkpoint N times and fsyncs N times. That is redundant-but-idempotent and
// acceptable for the Demo (perf is a non-goal, A10); a production version would
// batch a whole message's PKs + one checkpoint into a single commit (and could
// relax Sync, since the index is rebuildable from the WAL via DurableCheckpoint).
func (e *engine) Put(ctx context.Context, key, value []byte, at pkengine.WALCheckpoint) error {
	b := e.db.NewBatch()
	if err := b.Set(key, value, nil); err != nil {
		return err
	}
	if err := b.Set(sstcodec.CheckpointMetaKey, sstcodec.EncodeCheckpoint(at.TimeTick, at.MessageID), nil); err != nil {
		return err
	}
	return e.db.Apply(b, crdbpebble.Sync)
}

// Delete tombstones one PK and advances the checkpoint meta key in the same
// atomic batch (see Put for the per-key checkpoint rationale).
func (e *engine) Delete(ctx context.Context, key []byte, at pkengine.WALCheckpoint) error {
	b := e.db.NewBatch()
	if err := b.Delete(key, nil); err != nil {
		return err
	}
	if err := b.Set(sstcodec.CheckpointMetaKey, sstcodec.EncodeCheckpoint(at.TimeTick, at.MessageID), nil); err != nil {
		return err
	}
	return e.db.Apply(b, crdbpebble.Sync)
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

// readMetaCheckpoint reads the reserved checkpoint meta key from the DB; returns
// the zero checkpoint when unset. This is the single source of truth for the
// covered WAL position (advanced by Put/Delete and the install write-back).
func (e *engine) readMetaCheckpoint() pkengine.WALCheckpoint {
	v, closer, err := e.db.Get(sstcodec.CheckpointMetaKey)
	if err != nil {
		return pkengine.WALCheckpoint{}
	}
	tt, msgID := sstcodec.DecodeCheckpoint(v)
	_ = closer.Close()
	return pkengine.WALCheckpoint{TimeTick: tt, MessageID: msgID}
}

// DurableCheckpoint returns the WAL position covered by the current durable
// state (the meta key), which is monotonic by construction.
func (e *engine) DurableCheckpoint(ctx context.Context) (pkengine.WALCheckpoint, error) {
	if e.db == nil {
		return pkengine.WALCheckpoint{}, merr.WrapErrServiceInternal("pebble engine not opened")
	}
	return e.readMetaCheckpoint(), nil
}

// Persist materializes durable state to object storage (C3): it flushes the
// memtable to on-disk SSTs, uploads any not-yet-uploaded SST via the
// ObjectStore, and publishes a new manifest.json version carrying the current
// WAL checkpoint. Idempotent across calls (SSTs are keyed by their stable Pebble
// filename, re-upload skipped via Exist).
func (e *engine) Persist(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.db.Flush(); err != nil {
		return err
	}
	ck := e.readMetaCheckpoint()

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
	return e.writeManifest(ctx, manifest{
		Version:    e.manifestVer,
		SSTs:       entries,
		Checkpoint: checkpointJSON{TimeTick: ck.TimeTick, MessageID: ck.MessageID},
	})
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
	current := e.readMetaCheckpoint()
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
	// deleted keys) + ingest the merged SST. The merged SST carries NO checkpoint meta
	// key, so no position regression (§6.6 competition fix).
	if _, err := e.db.IngestAndExcise(paths, nil, span); err != nil {
		return err
	}

	// Write back max(covered, current). at.Covers(current) holds, so writeback == at.
	writeback := at
	if current.TimeTick > writeback.TimeTick {
		writeback = current
	}
	b := e.db.NewBatch()
	if err := b.Set(sstcodec.CheckpointMetaKey, sstcodec.EncodeCheckpoint(writeback.TimeTick, writeback.MessageID), nil); err != nil {
		return err
	}
	if err := e.db.Apply(b, crdbpebble.Sync); err != nil {
		return err
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
	return e.writeManifest(ctx, manifest{
		Version:    e.manifestVer,
		SSTs:       newSSTs,
		Checkpoint: checkpointJSON{TimeTick: writeback.TimeTick, MessageID: writeback.MessageID},
		Superseded: oldKeys,
	})
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
