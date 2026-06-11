// Package sstcodec holds the key/value encoding, the shared Pebble Comparer and
// the sstable multi-way merge helper. It is reused by both the Pebble engine
// (streamingnode side, install via IngestAndExcise) and the Pebble compactor
// (indexnode side), so both ends produce/consume SSTs with the SAME comparer
// name and key encoding — see design doc §12 (sstcodec) and risk R-P2.
//
// Key layout (design §8, refined for excise safety):
//
//	data key = 0x01 domain ‖ 8B BE(collectionID) ‖ 1B pk-type ‖ pk bytes
//	meta key = 0x00 domain ‖ "wal_ckpt"
//
// int64 PKs are encoded big-endian with the sign bit flipped so the bytewise
// order matches numeric order (negatives sort before positives); varchar PKs
// use raw bytes. The leading domain byte guarantees the checkpoint meta key
// (0x00…) sorts strictly before every data key (0x01…), so it is never covered
// by an excise span over the PK keyspace — satisfying §6.6's requirement that
// the position meta key lives outside the excised range.
package sstcodec

import (
	"bytes"
	"encoding/binary"
	"os"
	"sort"

	crdbpebble "github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

const (
	domainMeta byte = 0x00
	domainData byte = 0x01

	pkTypeInt64   byte = 0x01
	pkTypeVarChar byte = 0x02
)

// Comparer is the shared comparer used on both ends. The key encoding is
// order-preserving under plain bytewise comparison, so the default bytewise
// comparer suffices; using the SAME named comparer on both ends satisfies R-P2.
var Comparer = crdbpebble.DefaultComparer

// TableFormat is the SST table format used for merged SSTs; must be ingestible
// by a FormatNewest DB.
const TableFormat = sstable.TableFormatPebblev4

// CheckpointMetaKey is the reserved key carrying the WAL checkpoint. It lives in
// the meta domain (0x00) which sorts before all data-domain (0x01) keys.
var CheckpointMetaKey = append([]byte{domainMeta}, []byte("wal_ckpt")...)

// EncodeInt64Key encodes an int64 primary key into a data-domain key.
func EncodeInt64Key(collID, pk int64) []byte {
	b := make([]byte, 1+8+1+8)
	b[0] = domainData
	binary.BigEndian.PutUint64(b[1:9], uint64(collID))
	b[9] = pkTypeInt64
	binary.BigEndian.PutUint64(b[10:18], uint64(pk)^(uint64(1)<<63))
	return b
}

// EncodeVarCharKey encodes a varchar primary key into a data-domain key.
func EncodeVarCharKey(collID int64, pk string) []byte {
	b := make([]byte, 0, 1+8+1+len(pk))
	b = append(b, domainData)
	var c [8]byte
	binary.BigEndian.PutUint64(c[:], uint64(collID))
	b = append(b, c[:]...)
	b = append(b, pkTypeVarChar)
	b = append(b, pk...)
	return b
}

// IsMetaKey reports whether a key is in the reserved meta domain.
func IsMetaKey(k []byte) bool { return len(k) > 0 && k[0] == domainMeta }

// EncodeCheckpoint serializes a WAL checkpoint into the meta value.
func EncodeCheckpoint(timeTick uint64, msgID []byte) []byte {
	b := make([]byte, 8+len(msgID))
	binary.BigEndian.PutUint64(b[:8], timeTick)
	copy(b[8:], msgID)
	return b
}

// DecodeCheckpoint deserializes a meta value into (timeTick, messageID).
func DecodeCheckpoint(b []byte) (timeTick uint64, msgID []byte) {
	if len(b) < 8 {
		return 0, nil
	}
	timeTick = binary.BigEndian.Uint64(b[:8])
	if len(b) > 8 {
		msgID = append([]byte{}, b[8:]...)
	}
	return timeTick, msgID
}

// SSTDataRange opens an on-disk SST and returns the [min,max] user-key range
// over its DATA-domain keys (meta keys excluded). hasData is false for an SST
// with no data keys.
func SSTDataRange(path string) (min, max []byte, hasData bool, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, false, err
	}
	readable, err := sstable.NewSimpleReadable(f)
	if err != nil {
		f.Close()
		return nil, nil, false, err
	}
	r, err := sstable.NewReader(readable, sstable.ReaderOptions{Comparer: Comparer})
	if err != nil {
		readable.Close()
		return nil, nil, false, err
	}
	defer r.Close()
	it, err := r.NewIter(nil, nil)
	if err != nil {
		return nil, nil, false, err
	}
	defer it.Close()
	for k, _ := it.First(); k != nil; k, _ = it.Next() {
		if IsMetaKey(k.UserKey) {
			continue
		}
		uk := append([]byte{}, k.UserKey...)
		if min == nil || bytes.Compare(uk, min) < 0 {
			min = uk
		}
		if max == nil || bytes.Compare(uk, max) > 0 {
			max = uk
		}
		hasData = true
	}
	return min, max, hasData, nil
}

type mergeEntry struct {
	seq  uint64
	kind sstable.InternalKeyKind
	val  []byte
}

// MergeSSTs performs a multi-way merge over the given local SST files: per user
// key it keeps the highest-sequence internal entry and DROPS tombstones
// (deleted keys are omitted entirely from the output). Dropping tombstones is
// safe ONLY because install uses IngestAndExcise to remove the replaced range
// (so no older SST can resurrect a dropped key). Meta-domain keys are skipped
// (the merged SST never carries the checkpoint meta key — §6.6).
//
// It writes the surviving live keys to outPath as a fresh SST and returns the
// union [min,max] data-key range over ALL input keys (set and delete) — this is
// the excise span needed to remove all replaced data on install.
func MergeSSTs(inputPaths []string, outPath string) (exciseMin, exciseMax []byte, liveCount int, err error) {
	merged := map[string]mergeEntry{}
	for _, p := range inputPaths {
		if err := mergeOne(p, merged, &exciseMin, &exciseMax); err != nil {
			return nil, nil, 0, err
		}
	}

	keys := make([]string, 0, len(merged))
	for k, e := range merged {
		if e.kind == sstable.InternalKeyKindSet {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys) // bytewise sort matches Comparer for data keys

	of, err := vfs.Default.Create(outPath)
	if err != nil {
		return nil, nil, 0, err
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(of), sstable.WriterOptions{
		Comparer:    Comparer,
		TableFormat: TableFormat,
	})
	for _, k := range keys {
		if err := w.Set([]byte(k), merged[k].val); err != nil {
			_ = w.Close()
			return nil, nil, 0, err
		}
		liveCount++
	}
	if err := w.Close(); err != nil {
		return nil, nil, 0, err
	}
	return exciseMin, exciseMax, liveCount, nil
}

// mergeOne folds one input SST into the running merge: it iterates the SST's
// internal keys (including tombstones, which the raw sstable iterator exposes),
// skips meta-domain keys, expands the union [exciseMin,exciseMax] data range, and
// keeps the highest-sequence entry per user key in merged.
func mergeOne(path string, merged map[string]mergeEntry, exciseMin, exciseMax *[]byte) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	readable, err := sstable.NewSimpleReadable(f)
	if err != nil {
		f.Close()
		return err
	}
	r, err := sstable.NewReader(readable, sstable.ReaderOptions{Comparer: Comparer})
	if err != nil {
		readable.Close()
		return err
	}
	defer r.Close()
	it, err := r.NewIter(nil, nil)
	if err != nil {
		return err
	}
	defer it.Close()
	for k, lv := it.First(); k != nil; k, lv = it.Next() {
		if IsMetaKey(k.UserKey) {
			continue
		}
		uk := append([]byte{}, k.UserKey...)
		if *exciseMin == nil || bytes.Compare(uk, *exciseMin) < 0 {
			*exciseMin = uk
		}
		if *exciseMax == nil || bytes.Compare(uk, *exciseMax) > 0 {
			*exciseMax = uk
		}
		v, _, verr := lv.Value(nil)
		if verr != nil {
			return verr
		}
		val := append([]byte{}, v...)
		cur, ok := merged[string(uk)]
		if !ok || k.SeqNum() >= cur.seq {
			merged[string(uk)] = mergeEntry{seq: k.SeqNum(), kind: k.Kind(), val: val}
		}
	}
	return nil
}

// ExciseEndBound returns an exclusive upper bound that includes max (the excise
// span End in IngestAndExcise is exclusive).
func ExciseEndBound(max []byte) []byte {
	return append(append([]byte{}, max...), 0x00)
}
