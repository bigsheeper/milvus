# Primary Key Index Core Library (codec, SST, engine)

- **Created:** 2026-09-17
- **Status:** Draft
- **Component:** `internal/pkindex/core`
- **Related work:** LSM-based Primary Key Index, issue #52305

## Summary

The LSM-based primary key index (#52305) keeps, per vchannel, a map from
primary key to the segment holding the live row, and consults it on the write
path to deduplicate inserts. This document covers only the storage library
underneath that feature: three packages with no dependency on any node, RPC, or
WAL code.

| Package | Responsibility |
|---|---|
| `core/codec` | Order-preserving primary key encoding, value encoding, the tombstone value |
| `core/sst` | SST write, read, iteration, content-addressed naming, checksum verification |
| `core/engine` | Per-vchannel engine: a stack of writable increment DBs plus a read-only baseline SST set, the cycle that moves data from one to the other, node-level shared caches |

The library provides mechanism only. When to rotate, flush, upload, install or
drop is decided by its callers, which are not part of this change. Nothing in
Milvus calls the library yet.

## Context

The feature design in #52305 fixes the constraints this library works under:

- One index instance per vchannel, so that dropping a channel is a directory
  delete and instances have independent lifecycles.
- The storage engine's own WAL is off. Durability comes from the Milvus WAL;
  the index is rebuilt by replaying it from a checkpoint.
- The storage engine's automatic compaction is off. Merging is done elsewhere,
  so the streaming node never pays for it on the write path.
- Baseline data is a set of immutable SST files whose authoritative copy is in
  object storage. It is only ever replaced as a whole, by a new set.
- The streaming node keeps writing at all times. No step of moving data from
  the writable side to the baseline may pause writes.

## Goals

- A point lookup that sees, for any key, the most recent write or delete across
  the writable side and the baseline.
- Replacing the baseline set atomically with respect to concurrent lookups.
- Handing the writable side's data over to the baseline without pausing writes
  and without a window in which acknowledged state is unreachable.
- SST files that are interchangeable between the producer on the streaming node
  (memtable flush) and any other producer using `core/sst`.
- Resuming an interrupted hand-over after a crash without leaking directories.

## Non-goals

- The manifest that records which SST files form the baseline.
- Merging SST files.
- Uploading, downloading, or garbage-collecting files in object storage.
- The write-path decision logic and its integration into the WAL.
- Configuration parameters. The engine takes a `Config` struct; wiring it to
  `paramtable` belongs with the first caller.

## Encoding

A key is the order-preserving encoding of one primary key and carries no
prefix: an engine instance belongs to one vchannel, whose collection has one
primary key field of a fixed type.

- `int64`: 8 bytes big-endian with the sign bit flipped, so bytewise order
  equals numeric order.
- `varchar`: the raw bytes.

Order preservation is what lets an SST's min/max key prune point lookups
without decoding.

A value starts with a kind byte that selects its layout: `0x00` is a tombstone
and carries nothing else, `0x01` is a PK entry and carries the 8-byte
big-endian segment ID.

Later optional fields, redundant field data and a custom timestamp, go into a
protobuf message `PKEntryExt` appended after the fixed part, as a new kind
`0x02` = `[SegmentID 8B BE][PKEntryExt]`. The message runs to the end of the
value, so it needs no length prefix, and a node that does not know a field
skips it instead of failing. SegmentID keeps the same offset as in `0x01`, so
the hot path that only reads SegmentID never unmarshals protobuf; only a
caller that wants the optional fields pays for decoding. This follows
CockroachDB's `MVCCValue`, which likewise pairs a hand-encoded simple form
with an extended one.

The key and value kinds `0x00` and `0x01` are hand-encoded. Protobuf cannot
encode the key at all, because varints and field tags do not preserve order,
and writing a protobuf definition that merely describes a format the code does
not actually use would put a second, diverging source of truth next to the
real one. The layouts live in the codec package's comments, and golden tests
pin the exact bytes.

A delete is stored as the tombstone value, not as the storage engine's native
tombstone. A delete on the writable side must mask an older entry in the
baseline, and must survive a memtable flush into an SST that later becomes part
of the baseline; a native tombstone does neither once it leaves its DB.
Physically dropping tombstoned keys is the job of whatever merges SSTs.

The tombstone encoding is reserved. `Engine.Apply` takes a nil value to mean
delete and rejects a non-nil value equal to the tombstone, so the rule is
enforced at the single write entry rather than left to callers.

## SST files

SSTs use Pebble's sstable format (`TableFormatPebblev4`) with
`pebble.DefaultComparer` and a whole-key bloom filter of 10 bits per key. The
comparer, table format and filter policy are exported from `core/sst` and used
by every producer and consumer, including the engine's increment DBs, so a
memtable flush output can serve as a baseline table as is.

Shared files are content-addressed: the published name is the hex sha256 of the
file bytes plus `.sst`. Uploads are idempotent and a download can be verified
against the recorded checksum (`sst.Verify`).

One trap is documented on `sst.Info.Path`: Pebble numbers files per DB, so
flush outputs of different increment DBs share local file names. A consumer
must stage or upload them under the content-addressed name.

### Bloom filter

Deduplicating an insert mostly probes keys that do not exist, and an absent key
is the most expensive point lookup: every baseline table whose range covers the
key must be consulted. Range pruning handles monotonically increasing keys, but
not user-supplied random or varchar keys, where every table spans the whole key
space.

Pebble's built-in filter works here without a comparer that defines `Split`.
The sstable writer adds the whole user key when there is no `Split`; the
sstable-level `SeekPrefixGE(prefix, key)` only uses `prefix` to query the
filter; `DB.Get` passes the whole key as the prefix when there is no `Split`.
Only the DB-level `Iterator.SeekPrefixGE`, which the library does not use,
requires `Split`.

Measured once, with a warm cache: 20,000 in-range misses against a table of
200,000 keys went from 60,111 block accesses and 37 ms to 41,707 and 14 ms, for
a 14% larger file. Cold-read benefit has not been measured.

Cost to be aware of: Pebble's table filter is one block per table, about 1.25
bytes per key, and only helps while it stays in the block cache. A table of 2.5
million keys has a filter block of about 3 MB; once evicted, each probe reads it
back whole, which is worse than having no filter. Callers sizing the node-level
cache must budget for it. A memory-resident filter outside the block cache
remains possible later; the filter is not part of any interface.

## Engine

### Two parts

- **Increment**: a stack of Pebble DBs. Exactly one is active and takes writes;
  zero or more are frozen and being drained. WAL and automatic compaction are
  disabled, and `L0StopWritesThreshold` is raised out of reach because a stall
  here would block the Milvus WAL append path.
- **Baseline**: a set of read-only SST readers over locally cached files. They
  are not ingested into any Pebble DB.

A lookup walks the active DB, then the frozen DBs newest first, then the
baseline tables in the order they were installed, skipping tables whose key
range excludes the key. The first layer holding the key decides; a tombstone
resolves to "absent" and stops the walk.

Replacing the baseline swaps the reader set under a lock of its own and never
touches the increment side.

**Rejected: one Pebble DB holding both parts, with `IngestAndExcise` swapping in
merge results.** Excise removes the key span from every level, including writes
that arrived after the merge's input was fixed; and a single ingest batch must
be non-overlapping, so "merge output plus newer L0 tables" cannot go in
atomically. With a writer that never stops, this either loses new writes or
requires pausing them.

### Draining cycle

```
gen := RotateIncrement()      // freeze the active DB, open a new one
infos := FlushDraining(gen)   // the frozen DB's complete SST set, newest first
...upload the SSTs, record them...
InstallBaseline(...)          // the baseline now covers this data
DropDraining(gen)             // only now may the frozen DB be deleted
```

Rotating first is the fence: once it returns, writes land in the new DB and can
never reach the DB being drained, so its SST set is stable. The frozen DB stays
in the read path until dropped, so the cycle needs neither a write pause nor
atomicity across steps.

**Rejected: rotate as "open a new DB and delete the old one".** Writes that
arrive after the flush and before the rotation sit in the old DB's memtable,
neither uploaded nor in the baseline; deleting the DB loses them.

Invariants:

1. Only a frozen DB is ever flushed for upload. There is no in-place flush of
   the active DB.
2. Coverage is added before it is removed: `InstallBaseline` precedes
   `DropDraining`. The reverse order loses state.
3. `Open` reopens every increment directory left on disk: the newest generation
   becomes active and the rest are restored as draining. A cycle interrupted by
   a crash can be resumed, and no directory leaks. Writes that were only in a
   memtable are lost by design and come back through Milvus WAL replay.

### Recency order of SSTs

A memtable flushes whenever it fills, so one frozen DB commonly holds several L0
tables with overlapping key ranges. If they reach the baseline in the wrong
order, a deleted key comes back to life.

`FlushDraining` returns the tables newest first. Flushes of one DB are serial
and nothing compacts or ingests, so the tables' sequence number ranges are
disjoint and descending `LargestSeqNum` is recency order. `sst.Info` carries no
ordering field: list order is the contract. Whoever records the baseline must
keep a newer generation's tables ahead of an older generation's, and keep the
order `FlushDraining` returned within a generation.

### Locking

Structural operations (`RotateIncrement`, `FlushDraining`, `InstallBaseline`,
`DropDraining`, `Close`) serialize on one mutex and do their disk IO (opening a
DB, flushing and hashing, opening readers, closing a DB, deleting a directory)
while holding only that mutex. `Apply` and `Probe` never take it. The
read-write lock guarding the increment stack is write-locked only to swap
pointers.

`FlushDraining` and `InstallBaseline` deliberately do not do IO under the read
lock either: Go's `RWMutex` blocks new readers once a writer is waiting, so a
long read-locked flush would stall every `Apply` and `Probe` as soon as a
rotation queued behind it. The price is that structural operations wait for one
another; writes are unaffected.

### Shared caches

`SharedResources` holds one block cache and one table cache per node. Increment
DBs and baseline readers all attach to it. `sst.ComputeInfo`, a one-off full
scan, reads without caching.

Each baseline reader gets its own cache ID. Pebble offers no per-file eviction
for a standalone reader, so after a baseline replacement the old readers' blocks
linger until evicted by use.

### Local metadata

`PutLocalMeta`/`GetLocalMeta` store small entries as sidecar files written by
atomic rename, keeping the key space free of metadata keys.

## Interface

```go
type Engine interface {
    Apply(ctx, muts []Mutation) error                  // nil Value deletes
    Probe(ctx, keys [][]byte) ([][]byte, error)        // nil for absent or deleted
    RotateIncrement(ctx) (Generation, error)
    FlushDraining(ctx, gen) ([]sst.Info, error)        // newest first
    DropDraining(ctx, gen) error
    DrainingGenerations() []Generation                 // newest first
    InstallBaseline(ctx, tables []BaselineTable) error // newest first; nil clears
    PutLocalMeta(key string, val []byte) error
    GetLocalMeta(key string) ([]byte, bool, error)
    Stats() Stats
    Close() error                                      // keeps disk state
    Destroy() error                                    // deletes the data directory
}
```

The engine moves opaque bytes; encoding belongs to the caller. Its one piece of
value knowledge is the tombstone. Errors are returned as they are; what to do
about a failing engine is the caller's policy. A mockery mock is generated for
the interface.

## Failure semantics

- A failed `RotateIncrement` leaves the engine unchanged.
- A failed `DropDraining` has already removed the generation from the read
  path and must not be retried; whatever is left on disk is restored as
  draining by the next `Open` and can be dropped then.
- A corrupt file is not a missing key. Pebble reports a block it cannot read by
  ending iteration, the same way it reports the end of the data, and carries
  the reason on the iterator instead. A read path that ignores it answers
  "absent" for every key in that block, which for this index means silently
  skipping deduplication, so both the point lookup and the scan check the
  iterator before concluding that a key is not there.

### Errors

The library does not return `merr`. A caller inside the primary key index
branches on what happened, and `merr.Is` matches on the numeric code alone, so
distinct situations that share a code become indistinguishable: an engine that
is closed and a generation that is not draining would both arrive as
`ServiceInternal`, although one is worth retrying against a reopened engine and
the other is a caller mistake. Errors are therefore shaped in two layers:

- A signal a caller branches on is a sentinel declared by the package that
  returns it: `engine.ErrClosed`, `engine.ErrNotDraining`.
- How a failure should be reported outward is a category from the leaf package
  `pkerr`, attached with `errors.Mark` so the original error and the context
  each call site added both survive: `ErrIO` for a local disk failure,
  `ErrCorrupted` for stored bytes that do not match the expected layout, and
  `ErrUnavailable` for a condition a retry may clear. A closed engine carries
  both `ErrClosed` and `ErrUnavailable`.
- A failure that is a bug in Milvus rather than a condition to act on — no
  shared resources, a value equal to the reserved tombstone — carries no
  category at all, so nothing downstream retries it or reads it as bad data.

Translation happens once, at the boundary that leaves the primary key index
tree: `pkerr.ToMerr` for a coordinator's gRPC service or a worker's task
result, and the streamingnode interceptor's own mapping to streaming status
errors. `ToMerr` maps `ErrCorrupted` to `ErrDataIntegrity`, `ErrIO` to
`ErrIoFailed`, `ErrUnavailable` to `ErrServiceUnavailable`, and anything
uncategorized to `ErrServiceInternal`; a `merr` passes through untouched. None
of these is caused by request content, so none is an input error. This mirrors
how the streaming package keeps `walimpls.ErrFenced` internal and translates at
its interceptor boundary. An error that escapes a boundary untranslated crosses
gRPC as code 65535, so each boundary carries a test covering every sentinel and
category it can produce.

A mark is only visible to `github.com/cockroachdb/errors.Is`, which is what
`errors` resolves to across this repository; the standard library's `errors.Is`
does not see it and reports false.

## Feasibility: many DBs per node

One Pebble DB per vchannel was checked before building on it, with 1 to 1024
DBs on one node: idle memory 87 to 125 KB per DB, 1024 DBs opened in parallel in
2.42 s, and no order-of-magnitude loss of aggregate write throughput from 64 to
1024 DBs (single run, variance not estimated). Steady state is about 200 KB and
7 file descriptors per DB. The read path was not part of that measurement.

## Verification

Unit tests, run with `-race`:

- codec: order preservation over 1e5 random pairs for both key types plus the
  boundary values; round trips; the value kind byte; golden bytes for both key
  types and both value kinds.
- pkerr: every category and an uncategorized error translate to the expected
  merr code with the call site's context still in the message; a merr passes
  through.
- sst: round trip; deterministic content addressing; a flipped byte fails
  `Verify`, and a flipped byte inside a data block fails the point lookup and
  the scan rather than reading as a missing key; min/max keys; a reader uses
  the cache it is given; in-range misses are answered by the filter, not by
  data blocks.
- engine: write then read; delete masks; increment overrides baseline; baseline
  recency order; the full draining cycle; concurrent writes during a cycle are
  all retained; several overlapping L0 tables in one generation keep deletes and
  latest values after the cycle; flush outputs carry the filter; baseline
  lookups hit the node-level cache; `Apply` and `Probe` proceed while a
  rotation's or a drop's disk IO is held up; concurrent install and probe see no
  intermediate state; warm restart restores the stack; unflushed writes do not
  survive a restart; reserved value rejected with no partial write; every method
  rejects work after `Close` with both `ErrClosed` and `ErrUnavailable`; an
  unknown generation reports `ErrNotDraining`; `Destroy`.

`make static-check` passes.

## Follow-ups

- A caller-driven iterator on `sst.Reader`. The callback `Iter(fn)` cannot
  drive a k-way merge.
- Reporting which Milvus WAL position a frozen generation covers. Writes are
  applied concurrently, so apply order is not timetick order; whether the engine
  or its caller tracks this is undecided.
- Read-path measurements: baseline lookup cost, cache hit benefit, cold-read
  benefit of the filter, and the cache budget the filter needs.
- `paramtable` wiring for the data directory, cache size and memtable size.
