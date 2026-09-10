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

// Package engine implements the per-vchannel pkindex storage engine.
//
// State is layered. The increment side is a stack of pebble DBs: one active
// DB taking writes, plus zero or more frozen DBs being drained. Every DB has
// its own WAL disabled (durability comes from the Milvus WAL) and automatic
// compactions disabled (merging is offloaded to datanode). The baseline side
// is a read-only set of SST files, locally cached with the authoritative copy
// in object storage, replaced only as a whole.
//
// A probe walks active, then the frozen DBs newest-first, then the baseline
// tables in the order they were installed. Deletes are stored as codec
// tombstone values, so a recent delete masks an older entry below it.
//
// The snapshot cycle a caller runs is:
//
//	gen := RotateIncrement()   // freeze the active DB, start a fresh one
//	infos := FlushDraining(gen) // the frozen DB's complete SST set
//	... upload the SSTs, commit them to the manifest ...
//	InstallBaseline(...)        // now the baseline covers them
//	DropDraining(gen)           // only now is the frozen DB removable
//
// Rotating first is what makes the cycle safe without pausing writes: writes
// arriving mid-cycle land in the new active DB, never in the DB being drained,
// and coverage is only ever removed after it has been added somewhere else.
//
// The engine is semantics-free: keys and values are opaque bytes (encoded by
// the codec package); when to rotate, flush, install or drop is the caller's
// policy.
package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/cockroachdb/pebble"

	"github.com/milvus-io/milvus/internal/pkindex/core/codec"
	"github.com/milvus-io/milvus/internal/pkindex/core/sst"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	incrementDirPrefix = "increment-"
	metaDirName        = "meta"
)

// Generation identifies one increment DB within an engine. It increases
// monotonically and names that DB's directory.
type Generation int64

// Engine is one per-vchannel pkindex engine. All methods are safe for
// concurrent use.
type Engine interface {
	// Apply commits a batch of mutations to the active increment DB.
	Apply(ctx context.Context, muts []Mutation) error

	// Probe resolves each key to its stored value, or nil when the key is
	// absent or deleted. Results align with keys.
	Probe(ctx context.Context, keys [][]byte) ([][]byte, error)

	// RotateIncrement freezes the active increment DB and starts a fresh one.
	// The frozen DB stays in the read path until dropped, so no state becomes
	// unreachable and writes are never paused. It returns the frozen DB's
	// generation.
	RotateIncrement(ctx context.Context) (Generation, error)

	// FlushDraining flushes the frozen DB's memtable and returns its complete
	// SST set, newest first. Because the DB is frozen the result is stable:
	// uploading these files captures everything that DB will ever hold. The
	// tables may overlap in key range, so the order is part of the result: it
	// is the order InstallBaseline expects, ahead of every older generation's
	// tables.
	FlushDraining(ctx context.Context, gen Generation) ([]sst.Info, error)

	// DropDraining removes a frozen DB from the read path and deletes it. The
	// caller must have installed a baseline covering its contents first, or
	// that state is lost. On error the generation is out of the read path
	// regardless and must not be retried: whatever is left on disk is picked
	// up as draining by the next Open, which can drop it again.
	DropDraining(ctx context.Context, gen Generation) error

	// DrainingGenerations lists the frozen generations, newest first.
	DrainingGenerations() []Generation

	// InstallBaseline atomically replaces the baseline table set. Tables must
	// be ordered newest-first: on overlapping key ranges the earlier table
	// wins a probe. Passing nil clears the baseline.
	InstallBaseline(ctx context.Context, tables []BaselineTable) error

	// PutLocalMeta durably stores a small engine-local metadata entry, keeping
	// the pkindex keyspace free of meta keys.
	PutLocalMeta(key string, val []byte) error

	// GetLocalMeta reads a metadata entry; ok is false when it does not exist.
	GetLocalMeta(key string) ([]byte, bool, error)

	// Stats snapshots the engine's shape, for triggering policies and metrics.
	Stats() Stats

	// Close releases all handles, keeping on-disk state for a warm restart.
	Close() error

	// Destroy closes the engine and deletes its whole data directory.
	Destroy() error
}

// SharedResources are the node-level pebble resources every engine instance on
// the node attaches to: one block cache and one table (file handle) cache.
type SharedResources struct {
	cache      *pebble.Cache
	tableCache *pebble.TableCache
}

// NewSharedResources creates the node-level caches. cacheBytes is the total
// block cache budget shared by all engines on the node.
func NewSharedResources(cacheBytes int64) *SharedResources {
	c := pebble.NewCache(cacheBytes)
	return &SharedResources{
		cache:      c,
		tableCache: pebble.NewTableCache(c, 8, 4096),
	}
}

// Release drops the references held by SharedResources; call it after every
// engine using it is closed.
func (s *SharedResources) Release() {
	s.tableCache.Unref()
	s.cache.Unref()
}

// Config configures one engine instance.
type Config struct {
	// Dir is the engine's private data directory (increment DBs + local meta).
	Dir string
	// VChannel is the owning vchannel, for logging only.
	VChannel string
	// Shared are the node-level caches; required.
	Shared *SharedResources
	// MemTableSize caps one memtable's size in bytes; 0 keeps pebble's default.
	MemTableSize uint64
}

// Mutation is one write: Value nil marks the key deleted (stored as a codec
// tombstone), any other Value is stored as-is. The tombstone encoding itself
// is reserved and rejected as a Value.
type Mutation struct {
	Key   []byte
	Value []byte
}

// BaselineTable is one read-only baseline SST to install: its local path and
// its key range for pruning (from the manifest).
type BaselineTable struct {
	Path   string
	MinKey []byte
	MaxKey []byte
}

// Stats is a point-in-time snapshot of one engine's shape.
type Stats struct {
	// MemTableBytes is the active DB's current memtable size.
	MemTableBytes uint64
	// L0Tables is the number of L0 SSTs in the active DB.
	L0Tables int64
	// DrainingDBs is the number of frozen increment DBs awaiting a drop.
	DrainingDBs int
	// BaselineTables is the number of installed baseline SSTs.
	BaselineTables int
}

type incrementDB struct {
	gen Generation
	dir string
	db  *pebble.DB
}

// destroy closes the DB and deletes its directory.
func (inc *incrementDB) destroy() error {
	if err := inc.db.Close(); err != nil {
		return merr.WrapErrIoFailed(inc.dir, err)
	}
	if err := os.RemoveAll(inc.dir); err != nil {
		return merr.WrapErrIoFailed(inc.dir, err)
	}
	return nil
}

type baselineReader struct {
	table  BaselineTable
	reader *sst.Reader
}

type pebbleEngine struct {
	cfg    Config
	logger *mlog.Logger

	// cycleMu serializes the structural operations (RotateIncrement,
	// FlushDraining, InstallBaseline, DropDraining, Close). They do their disk
	// IO holding only cycleMu, which Apply and Probe never take, and hold mu or
	// baseMu just long enough to swap pointers. Lock order: cycleMu, mu, baseMu.
	cycleMu sync.Mutex

	// mu guards the increment stack and closed. They are written holding both
	// cycleMu and mu.Lock, so either lock suffices to read them. Apply and
	// Probe hold RLock for their whole call, so once a structural operation
	// has taken Lock to unlink a DB, no reader can still be using it.
	mu       sync.RWMutex
	active   *incrementDB
	draining []*incrementDB // newest first
	closed   bool

	// baseMu guards the baseline reader set, separately from mu so installing
	// a baseline does not block writes.
	baseMu   sync.RWMutex
	baseline []baselineReader
}

// Open opens (or creates) the engine under cfg.Dir. Every increment DB left on
// disk is reopened: the newest becomes active, the rest are restored as
// draining, so a cycle interrupted by a crash can be resumed rather than
// leaking its directory.
func Open(ctx context.Context, cfg Config) (Engine, error) {
	if cfg.Shared == nil {
		return nil, merr.WrapErrServiceInternalMsg("pkindex engine opened without shared resources")
	}
	if err := os.MkdirAll(filepath.Join(cfg.Dir, metaDirName), 0o755); err != nil {
		return nil, merr.WrapErrIoFailed(cfg.Dir, err)
	}
	e := &pebbleEngine{
		cfg:    cfg,
		logger: mlog.With(mlog.FieldModule("pkindex"), mlog.FieldVChannel(cfg.VChannel)),
	}
	gens, err := existingGenerations(cfg.Dir)
	if err != nil {
		return nil, err
	}
	if len(gens) == 0 {
		gens = []Generation{1}
	}
	// descending: the newest generation is the active one
	sort.Slice(gens, func(i, j int) bool { return gens[i] > gens[j] })
	opened := make([]*incrementDB, 0, len(gens))
	for _, gen := range gens {
		inc, err := e.openIncrement(gen)
		if err != nil {
			for _, o := range opened {
				o.db.Close()
			}
			return nil, err
		}
		opened = append(opened, inc)
	}
	e.active, e.draining = opened[0], opened[1:]
	e.logger.Info(ctx, "pkindex engine opened",
		mlog.String("dir", cfg.Dir),
		mlog.Int64("activeGen", int64(e.active.gen)),
		mlog.Int("drainingDBs", len(e.draining)))
	return e, nil
}

func existingGenerations(dir string) ([]Generation, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, merr.WrapErrIoFailed(dir, err)
	}
	gens := make([]Generation, 0, len(ents))
	for _, ent := range ents {
		if !ent.IsDir() || !strings.HasPrefix(ent.Name(), incrementDirPrefix) {
			continue
		}
		n, err := strconv.ParseInt(strings.TrimPrefix(ent.Name(), incrementDirPrefix), 10, 64)
		if err != nil {
			continue
		}
		gens = append(gens, Generation(n))
	}
	return gens, nil
}

func (e *pebbleEngine) incrementDir(gen Generation) string {
	return filepath.Join(e.cfg.Dir, fmt.Sprintf("%s%d", incrementDirPrefix, gen))
}

func (e *pebbleEngine) openIncrement(gen Generation) (*incrementDB, error) {
	dir := e.incrementDir(gen)
	opts := &pebble.Options{
		DisableWAL:                  true,
		DisableAutomaticCompactions: true,
		// L0 is bounded by external compaction on datanode and by rotation, not by
		// stalling writers: a stall here would block the WAL append path.
		L0StopWritesThreshold: 1 << 20,
		// flush output lands in L0 and later serves as baseline tables as is
		Levels:             []pebble.LevelOptions{{FilterPolicy: sst.FilterPolicy}},
		Cache:              e.cfg.Shared.cache,
		TableCache:         e.cfg.Shared.tableCache,
		FormatMajorVersion: pebble.FormatNewest,
	}
	if e.cfg.MemTableSize > 0 {
		opts.MemTableSize = e.cfg.MemTableSize
	}
	db, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, merr.WrapErrIoFailed(dir, err)
	}
	return &incrementDB{gen: gen, dir: dir, db: db}, nil
}

func (e *pebbleEngine) Apply(ctx context.Context, muts []Mutation) error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closed {
		return merr.WrapErrServiceInternalMsg("pkindex engine closed")
	}
	b := e.active.db.NewBatch()
	defer b.Close()
	for _, m := range muts {
		v := m.Value
		if v == nil {
			// a codec tombstone, not a pebble native tombstone: it must stay
			// visible to Get so it masks entries in the layers below
			v = codec.EncodeTombstone()
		} else if codec.IsTombstone(v) {
			return merr.WrapErrServiceInternalMsg("pkindex value of key %x is the reserved tombstone encoding", m.Key)
		}
		if err := b.Set(m.Key, v, nil); err != nil {
			return merr.WrapErrIoFailed(e.cfg.Dir, err)
		}
	}
	if err := b.Commit(pebble.NoSync); err != nil {
		return merr.WrapErrIoFailed(e.cfg.Dir, err)
	}
	return nil
}

func (e *pebbleEngine) Probe(ctx context.Context, keys [][]byte) ([][]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closed {
		return nil, merr.WrapErrServiceInternalMsg("pkindex engine closed")
	}
	e.baseMu.RLock()
	defer e.baseMu.RUnlock()

	out := make([][]byte, len(keys))
	for i, key := range keys {
		v, err := e.probeOne(key)
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}

func (e *pebbleEngine) probeOne(key []byte) ([]byte, error) {
	// increment stack newest-first: active, then the frozen DBs
	if v, found, err := e.probeIncrement(e.active, key); err != nil || found {
		return v, err
	}
	for _, inc := range e.draining {
		if v, found, err := e.probeIncrement(inc, key); err != nil || found {
			return v, err
		}
	}
	// baseline tables in installed (recency) order, range-pruned
	for _, br := range e.baseline {
		if br.table.MinKey != nil && sst.Comparer.Compare(key, br.table.MinKey) < 0 {
			continue
		}
		if br.table.MaxKey != nil && sst.Comparer.Compare(key, br.table.MaxKey) > 0 {
			continue
		}
		bv, ok, err := br.reader.Probe(key)
		if err != nil {
			return nil, err
		}
		if ok {
			if codec.IsTombstone(bv) {
				return nil, nil
			}
			return bv, nil
		}
	}
	return nil, nil
}

// probeIncrement reports found=true once a layer holds the key, whether as a
// value or as a tombstone; a tombstone resolves to a nil value and stops the
// walk so lower layers cannot resurrect it.
func (e *pebbleEngine) probeIncrement(inc *incrementDB, key []byte) (value []byte, found bool, err error) {
	v, closer, err := inc.db.Get(key)
	if err == pebble.ErrNotFound {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, merr.WrapErrIoFailed(inc.dir, err)
	}
	defer closer.Close()
	if codec.IsTombstone(v) {
		return nil, true, nil
	}
	// the value is only valid until closer.Close, so hand back a copy
	return append([]byte{}, v...), true, nil
}

func (e *pebbleEngine) RotateIncrement(ctx context.Context) (Generation, error) {
	e.cycleMu.Lock()
	defer e.cycleMu.Unlock()
	if e.closed {
		return 0, merr.WrapErrServiceInternalMsg("pkindex engine closed")
	}
	// only structural operations replace e.active, and cycleMu excludes them
	frozen := e.active
	inc, err := e.openIncrement(frozen.gen + 1)
	if err != nil {
		return 0, err
	}
	// the frozen DB stays in the read path; only its writability is given up
	e.mu.Lock()
	e.active = inc
	e.draining = append([]*incrementDB{frozen}, e.draining...)
	drainingDBs := len(e.draining)
	e.mu.Unlock()
	e.logger.Info(ctx, "pkindex increment rotated",
		mlog.Int64("activeGen", int64(inc.gen)),
		mlog.Int64("frozenGen", int64(frozen.gen)),
		mlog.Int("drainingDBs", drainingDBs))
	return frozen.gen, nil
}

func (e *pebbleEngine) FlushDraining(ctx context.Context, gen Generation) ([]sst.Info, error) {
	e.cycleMu.Lock()
	defer e.cycleMu.Unlock()
	if e.closed {
		return nil, merr.WrapErrServiceInternalMsg("pkindex engine closed")
	}
	inc := e.findDraining(gen)
	if inc == nil {
		return nil, merr.WrapErrServiceInternalMsg("generation %d is not draining", gen)
	}
	if err := inc.db.Flush(); err != nil {
		return nil, merr.WrapErrIoFailed(inc.dir, err)
	}
	levels, err := inc.db.SSTables()
	if err != nil {
		return nil, merr.WrapErrIoFailed(inc.dir, err)
	}
	var tables []pebble.SSTableInfo
	for _, level := range levels {
		tables = append(tables, level...)
	}
	// Flushes of one DB are serial and nothing compacts or ingests, so the
	// tables' sequence number ranges are disjoint and ordering by them is
	// ordering by recency.
	sort.Slice(tables, func(i, j int) bool { return tables[i].LargestSeqNum > tables[j].LargestSeqNum })
	infos := make([]sst.Info, 0, len(tables))
	for _, t := range tables {
		info, err := sst.ComputeInfo(filepath.Join(inc.dir, fmt.Sprintf("%s.sst", t.FileNum)))
		if err != nil {
			return nil, err
		}
		infos = append(infos, info)
	}
	return infos, nil
}

func (e *pebbleEngine) DropDraining(ctx context.Context, gen Generation) error {
	e.cycleMu.Lock()
	defer e.cycleMu.Unlock()
	if e.closed {
		return merr.WrapErrServiceInternalMsg("pkindex engine closed")
	}
	idx := -1
	for i, inc := range e.draining {
		if inc.gen == gen {
			idx = i
			break
		}
	}
	if idx < 0 {
		return merr.WrapErrServiceInternalMsg("generation %d is not draining", gen)
	}
	inc := e.draining[idx]
	e.mu.Lock()
	e.draining = append(e.draining[:idx:idx], e.draining[idx+1:]...)
	e.mu.Unlock()
	if err := inc.destroy(); err != nil {
		return err
	}
	e.logger.Info(ctx, "pkindex draining increment dropped", mlog.Int64("gen", int64(gen)))
	return nil
}

func (e *pebbleEngine) DrainingGenerations() []Generation {
	e.mu.RLock()
	defer e.mu.RUnlock()
	gens := make([]Generation, 0, len(e.draining))
	for _, inc := range e.draining {
		gens = append(gens, inc.gen)
	}
	return gens
}

func (e *pebbleEngine) findDraining(gen Generation) *incrementDB {
	for _, inc := range e.draining {
		if inc.gen == gen {
			return inc
		}
	}
	return nil
}

func (e *pebbleEngine) InstallBaseline(ctx context.Context, tables []BaselineTable) error {
	e.cycleMu.Lock()
	defer e.cycleMu.Unlock()
	if e.closed {
		return merr.WrapErrServiceInternalMsg("pkindex engine closed")
	}
	readers := make([]baselineReader, 0, len(tables))
	for _, t := range tables {
		r, err := sst.OpenReader(t.Path, e.cfg.Shared.cache)
		if err != nil {
			for _, br := range readers {
				br.reader.Close()
			}
			return err
		}
		readers = append(readers, baselineReader{table: t, reader: r})
	}
	e.baseMu.Lock()
	old := e.baseline
	e.baseline = readers
	e.baseMu.Unlock()
	// holding the write lock proved no probe still references the old set
	for _, br := range old {
		br.reader.Close()
	}
	e.logger.Info(ctx, "pkindex baseline installed", mlog.Int("tables", len(tables)))
	return nil
}

func (e *pebbleEngine) PutLocalMeta(key string, val []byte) error {
	dir := filepath.Join(e.cfg.Dir, metaDirName)
	tmp, err := os.CreateTemp(dir, key+"-*")
	if err != nil {
		return merr.WrapErrIoFailed(dir, err)
	}
	if _, err := tmp.Write(val); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return merr.WrapErrIoFailed(tmp.Name(), err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return merr.WrapErrIoFailed(tmp.Name(), err)
	}
	tmp.Close()
	if err := os.Rename(tmp.Name(), filepath.Join(dir, key)); err != nil {
		os.Remove(tmp.Name())
		return merr.WrapErrIoFailed(dir, err)
	}
	return nil
}

func (e *pebbleEngine) GetLocalMeta(key string) ([]byte, bool, error) {
	b, err := os.ReadFile(filepath.Join(e.cfg.Dir, metaDirName, key))
	if os.IsNotExist(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, merr.WrapErrIoFailed(key, err)
	}
	return b, true, nil
}

func (e *pebbleEngine) Stats() Stats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	s := Stats{}
	if !e.closed {
		m := e.active.db.Metrics()
		s.MemTableBytes = m.MemTable.Size
		s.L0Tables = m.Levels[0].NumFiles
		s.DrainingDBs = len(e.draining)
	}
	e.baseMu.RLock()
	s.BaselineTables = len(e.baseline)
	e.baseMu.RUnlock()
	return s
}

func (e *pebbleEngine) Close() error {
	e.cycleMu.Lock()
	defer e.cycleMu.Unlock()
	if e.closed {
		return nil
	}
	// once Lock is granted every in-flight Apply and Probe has returned, and
	// later ones see closed, so the handles can be released outside mu
	e.mu.Lock()
	e.closed = true
	e.mu.Unlock()
	err := e.active.db.Close()
	for _, inc := range e.draining {
		if cerr := inc.db.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}
	e.baseMu.Lock()
	old := e.baseline
	e.baseline = nil
	e.baseMu.Unlock()
	for _, br := range old {
		br.reader.Close()
	}
	if err != nil {
		return merr.WrapErrIoFailed(e.cfg.Dir, err)
	}
	return nil
}

func (e *pebbleEngine) Destroy() error {
	if err := e.Close(); err != nil {
		return err
	}
	if err := os.RemoveAll(e.cfg.Dir); err != nil {
		return merr.WrapErrIoFailed(e.cfg.Dir, err)
	}
	return nil
}
