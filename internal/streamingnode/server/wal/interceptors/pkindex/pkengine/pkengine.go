// Package pkengine defines the engine-agnostic capability abstraction for the
// Primary Key Index KV-store feasibility demo.
//
// The abstraction is defined by capability *semantics* only. It deliberately
// does NOT leak any concrete engine's implementation shape (Pebble's SST key
// lists / manifest.json / IngestAndExcise, SlateDB's CAS-manifest / compaction
// signals). Engine-specific payloads ride on opaque []byte (see Compactor /
// PlanCompaction / InstallCompaction). The WAL checkpoint is a first-class
// parameter: both writes and installs carry it.
//
// See design doc "Milvus 主键索引 KV 引擎接入 —— 可行性验证 Demo 设计文档（双引擎版）" §3.
package pkengine

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// WALCheckpoint is the Milvus WAL position covered by some "durable state".
//
// TimeTick is PChannel-level monotonically increasing and is the canonical
// ordering key (see streaming-system message model). MessageID is the
// serialized message.MessageID kept for traceability / tie-breaking.
type WALCheckpoint struct {
	TimeTick  uint64 // PChannel-level monotonically increasing
	MessageID []byte // serialized message.MessageID
}

// Covers reports whether c is at or beyond o, i.e. c covers everything o does.
// Ordering is by TimeTick, which is monotonic per pchannel. A zero checkpoint
// (TimeTick==0) is treated as "covers nothing".
func (c WALCheckpoint) Covers(o WALCheckpoint) bool {
	return c.TimeTick >= o.TimeTick
}

// IsZero reports whether the checkpoint is unset.
func (c WALCheckpoint) IsZero() bool {
	return c.TimeTick == 0 && len(c.MessageID) == 0
}

// ObjectStore is the minimal object-storage capability the engines need.
//
// Its method set is an exact subset of internal/storage.ChunkManager, so a real
// *storage.LocalChunkManager / MinioChunkManager structurally satisfies it and
// the production interceptor can pass resource.Resource().ChunkManager()
// directly. Typing OpenParam against this minimal interface (rather than
// importing internal/storage) keeps the engine layer and its UTs free of the
// cgo/C++ transitive dependencies that internal/storage pulls in (gorocksdb,
// storagev2/packed). See verification report deviation D-OS1.
type ObjectStore interface {
	RootPath() string
	Write(ctx context.Context, filePath string, content []byte) error
	Read(ctx context.Context, filePath string) ([]byte, error)
	Exist(ctx context.Context, filePath string) (bool, error)
	Remove(ctx context.Context, filePath string) error
}

// OpenParam is the engine-agnostic open input.
type OpenParam struct {
	Pchannel    string         // per-pchannel isolation unit
	ObjectStore ObjectStore    // shared object-storage handle (SlateDB resolves its object store URL/creds from it)
	LocalDir    string         // local staging dir (required by Pebble; ignored by SlateDB)
	Recover     *WALCheckpoint // optional: recover up to this checkpoint
}

// Engine is a per-pchannel KV engine, defined by capability semantics only
// (never bound to any engine's implementation shape).
type Engine interface {
	// C1 lifecycle + point writes. Put/Delete carry their WAL checkpoint.
	Open(ctx context.Context, p OpenParam) error
	Put(ctx context.Context, key, value []byte, at WALCheckpoint) error
	Delete(ctx context.Context, key []byte, at WALCheckpoint) error
	// Get is existence / point read, for UT assertions only (Demo does not validate point queries).
	Get(ctx context.Context, key []byte) (value []byte, found bool, err error)
	Close() error

	// C2 hand compaction control to the outside (engine decides: disable builtin / or has none).
	PauseBuiltinCompaction(ctx context.Context) error

	// C3 persist current state to object storage (engine-decided mechanism).
	Persist(ctx context.Context) error
	// DurableCheckpoint is the WAL position covered by the current durable state. Must be monotonic.
	DurableCheckpoint(ctx context.Context) (WALCheckpoint, error)

	// C4 produce an opaque job describing what the external compactor should merge.
	PlanCompaction(ctx context.Context) (job []byte, ok bool, err error)

	// C5 atomically "install external compaction result + advance WAL position".
	// Engine-decided mechanism (Pebble: IngestAndExcise single VersionEdit;
	// SlateDB: CAS-manifest refresh).
	InstallCompaction(ctx context.Context, result []byte, at WALCheckpoint) error
}

// Compactor runs the heavy work on indexnode; engine-specific implementation.
type Compactor interface {
	Compact(ctx context.Context, job []byte) (result []byte, covered WALCheckpoint, err error)
}

// Factory constructs a fresh (Engine, Compactor) pair for one engine kind.
type Factory func() (Engine, Compactor, error)

var registry = map[string]Factory{}

// Register wires an engine kind into the factory. Engine implementation packages
// call this from init(); SlateDB's registration is guarded by the `slatedb`
// build tag so the default (pure-Go) build never compiles cgo.
func Register(name string, f Factory) {
	registry[name] = f
}

// Registered returns the engine kinds available in the current build.
func Registered() []string {
	names := make([]string, 0, len(registry))
	for name := range registry {
		names = append(names, name)
	}
	return names
}

// New constructs the engine selected by name (config key pkindex.engine = pebble | slatedb).
func New(name string) (Engine, Compactor, error) {
	f, ok := registry[name]
	if !ok {
		return nil, nil, merr.WrapErrParameterInvalidMsg(
			"unknown pkindex engine %q; registered engines: %v (slatedb requires building with -tags slatedb)",
			name, Registered())
	}
	return f()
}
