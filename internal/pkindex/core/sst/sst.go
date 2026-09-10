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

// Package sst reads and writes the pkindex SST files.
//
// An SST is a pebble sstable holding order-preserving encoded primary keys
// (see the codec package) mapped to PKEntry values. The same comparer and
// table format are used by every producer (streamingnode memtable flush,
// datanode merge) and consumer (baseline readers, merge input), so files are
// interchangeable across roles.
//
// Shared files are content-addressed: the published name is the hex sha256 of
// the file bytes plus the ".sst" extension, which makes uploads idempotent and
// lets downloads be verified against the manifest.
package sst

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"

	"github.com/milvus-io/milvus/internal/pkindex/pkerr"
)

// Extension is the file extension of a pkindex SST file.
const Extension = ".sst"

// Comparer is the comparer shared by every SST producer and consumer, and by
// the engine's pebble DB. Keys are order-preserving encoded (codec package),
// so plain bytewise comparison is correct.
var Comparer = pebble.DefaultComparer

// TableFormat is the sstable format every producer writes. It is the format a
// memtable flush of the engine's increment DB produces.
const TableFormat = sstable.TableFormatPebblev4

// FilterPolicy is the bloom filter every producer writes into its tables, over
// whole keys (Comparer has no Split). A probe for an absent key, the common
// case when deduplicating inserts, is then answered from the table's filter
// block instead of its data blocks.
var FilterPolicy pebble.FilterPolicy = bloom.FilterPolicy(10)

// MarkPebbleErr categorizes a failure reported by pebble's sstable reader or
// writer, adding no context of its own: pebble reports a file whose bytes do
// not parse as ErrCorruption, and everything else these calls can fail with is
// a disk failure. The engine package classifies its own pebble errors through
// this, so that one rule covers both.
func MarkPebbleErr(err error) error {
	if errors.Is(err, pebble.ErrCorruption) {
		return errors.Mark(err, pkerr.ErrCorrupted)
	}
	return errors.Mark(err, pkerr.ErrIO)
}

// Info describes one finalized SST file.
type Info struct {
	// Sha256 is the hex sha256 of the file bytes; FileName derives the
	// content-addressed name from it.
	Sha256 string
	// Path is where the file is. A Writer names it by content (FileName);
	// ComputeInfo keeps the path it was given, which for a pebble flush output
	// is a per-DB file number that repeats across DBs, so a consumer must not
	// stage or upload by this base name.
	Path string
	// MinKey and MaxKey are the smallest and largest user keys in the file,
	// used for range pruning. Both are nil for an empty file.
	MinKey []byte
	MaxKey []byte
	// NumEntries is the number of key/value pairs.
	NumEntries int64
	// Size is the file size in bytes.
	Size int64
}

// FileName returns the content-addressed file name for a sha256 hex digest.
func FileName(sha256Hex string) string {
	return sha256Hex + Extension
}

// Verify streams the file and checks its sha256 against want.
func Verify(path string, want string) error {
	f, err := os.Open(path)
	if err != nil {
		return pkerr.MarkIO(err, "open sst %s", path)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return pkerr.MarkIO(err, "read sst %s", path)
	}
	got := hex.EncodeToString(h.Sum(nil))
	if got != want {
		return errors.Mark(
			errors.Newf("sst checksum mismatch at %s: got %s, want %s", path, got, want),
			pkerr.ErrCorrupted)
	}
	return nil
}

// ComputeInfo scans an existing SST file (e.g. one produced by a pebble
// memtable flush) and returns its Info. Path is kept as given; the file is not
// renamed.
func ComputeInfo(path string) (Info, error) {
	f, err := os.Open(path)
	if err != nil {
		return Info{}, pkerr.MarkIO(err, "open sst %s", path)
	}
	h := sha256.New()
	size, err := io.Copy(h, f)
	f.Close()
	if err != nil {
		return Info{}, pkerr.MarkIO(err, "read sst %s", path)
	}
	info := Info{
		Sha256: hex.EncodeToString(h.Sum(nil)),
		Path:   path,
		Size:   size,
	}
	// a one-off full scan: keep its blocks out of any shared cache
	r, err := OpenReader(path, nil)
	if err != nil {
		return Info{}, err
	}
	defer r.Close()
	err = r.Iter(func(key, _ []byte) error {
		if info.MinKey == nil {
			info.MinKey = append([]byte{}, key...)
		}
		info.MaxKey = append(info.MaxKey[:0], key...)
		info.NumEntries++
		return nil
	})
	if err != nil {
		return Info{}, err
	}
	return info, nil
}

// Writer builds one SST file in a directory. Keys must be added in strictly
// increasing order. Close finalizes the file under its content-addressed name;
// Abort discards it.
type Writer struct {
	dir     string
	tmpPath string
	w       *sstable.Writer

	minKey  []byte
	maxKey  []byte
	entries int64
}

// NewWriter creates a Writer staging its output in dir.
func NewWriter(dir string) (*Writer, error) {
	// reserve a unique temp name, then reopen it through pebble's vfs which
	// provides the Writable the sstable writer needs
	tmp, err := os.CreateTemp(dir, "pkindex-*.sst.tmp")
	if err != nil {
		return nil, pkerr.MarkIO(err, "create temp sst in %s", dir)
	}
	tmpPath := tmp.Name()
	tmp.Close()
	f, err := vfs.Default.Create(tmpPath)
	if err != nil {
		os.Remove(tmpPath)
		return nil, pkerr.MarkIO(err, "open temp sst %s", tmpPath)
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{
		Comparer:     Comparer,
		TableFormat:  TableFormat,
		FilterPolicy: FilterPolicy,
	})
	return &Writer{dir: dir, tmpPath: tmpPath, w: w}, nil
}

// Add appends one key/value pair; keys must arrive in strictly increasing
// order under Comparer.
func (w *Writer) Add(key, value []byte) error {
	if err := w.w.Set(key, value); err != nil {
		return MarkPebbleErr(errors.Wrapf(err, "write sst %s", w.tmpPath))
	}
	if w.minKey == nil {
		w.minKey = append([]byte{}, key...)
	}
	w.maxKey = append(w.maxKey[:0], key...)
	w.entries++
	return nil
}

// Close finalizes the SST, computes its sha256, renames it to the
// content-addressed name and returns its Info.
func (w *Writer) Close() (Info, error) {
	if err := w.w.Close(); err != nil {
		os.Remove(w.tmpPath)
		return Info{}, MarkPebbleErr(errors.Wrapf(err, "finalize sst %s", w.tmpPath))
	}
	f, err := os.Open(w.tmpPath)
	if err != nil {
		return Info{}, pkerr.MarkIO(err, "open sst %s", w.tmpPath)
	}
	h := sha256.New()
	size, err := io.Copy(h, f)
	f.Close()
	if err != nil {
		return Info{}, pkerr.MarkIO(err, "read sst %s", w.tmpPath)
	}
	sum := hex.EncodeToString(h.Sum(nil))
	path := filepath.Join(w.dir, FileName(sum))
	if err := os.Rename(w.tmpPath, path); err != nil {
		return Info{}, pkerr.MarkIO(err, "publish sst as %s", path)
	}
	return Info{
		Sha256:     sum,
		Path:       path,
		MinKey:     w.minKey,
		MaxKey:     w.maxKey,
		NumEntries: w.entries,
		Size:       size,
	}, nil
}

// Abort discards the staged file.
func (w *Writer) Abort() {
	w.w.Close()
	os.Remove(w.tmpPath)
}

// Reader reads one SST file for point lookups and ordered iteration.
type Reader struct {
	file *os.File
	r    *sstable.Reader
}

// OpenReader opens an SST file. Blocks it reads are cached in cache, which the
// reader references until Close; nil reads without caching.
func OpenReader(path string, cache *pebble.Cache) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, pkerr.MarkIO(err, "open sst %s", path)
	}
	readable, err := sstable.NewSimpleReadable(f)
	if err != nil {
		f.Close()
		return nil, MarkPebbleErr(errors.Wrapf(err, "read sst %s", path))
	}
	r, err := sstable.NewReader(readable, sstable.ReaderOptions{
		Comparer: Comparer,
		Cache:    cache,
		Filters:  map[string]sstable.FilterPolicy{FilterPolicy.Name(): FilterPolicy},
	})
	if err != nil {
		readable.Close()
		return nil, MarkPebbleErr(errors.Wrapf(err, "open sst reader %s", path))
	}
	return &Reader{file: f, r: r}, nil
}

// Probe returns the value stored for key, or ok=false if the key is absent.
func (r *Reader) Probe(key []byte) (value []byte, ok bool, err error) {
	it, err := r.r.NewIter(nil, nil)
	if err != nil {
		return nil, false, MarkPebbleErr(errors.Wrapf(err, "scan sst %s", r.file.Name()))
	}
	defer it.Close()
	// the prefix argument is what gets checked against the bloom filter; the
	// filter holds whole keys, so the prefix is the key itself
	ik, lv := it.SeekPrefixGE(key, key, 0)
	if ik == nil {
		// a nil key is also how the iterator reports a block it could not read,
		// so skipping Error here would report a corrupt file as a missing key
		// and silently defeat deduplication
		if err := it.Error(); err != nil {
			return nil, false, MarkPebbleErr(errors.Wrapf(err, "probe sst %s", r.file.Name()))
		}
		return nil, false, nil
	}
	if Comparer.Compare(ik.UserKey, key) != 0 {
		return nil, false, nil
	}
	v, callerOwned, err := lv.Value(nil)
	if err != nil {
		return nil, false, MarkPebbleErr(errors.Wrapf(err, "read sst value in %s", r.file.Name()))
	}
	if !callerOwned {
		v = append([]byte{}, v...)
	}
	return v, true, nil
}

// Iter iterates all entries in key order, invoking fn per entry; iteration
// stops at the first error. The key/value slices are only valid within fn.
func (r *Reader) Iter(fn func(key, value []byte) error) error {
	it, err := r.r.NewIter(nil, nil)
	if err != nil {
		return MarkPebbleErr(errors.Wrapf(err, "scan sst %s", r.file.Name()))
	}
	defer it.Close()
	for ik, lv := it.First(); ik != nil; ik, lv = it.Next() {
		v, _, err := lv.Value(nil)
		if err != nil {
			return MarkPebbleErr(errors.Wrapf(err, "read sst value in %s", r.file.Name()))
		}
		if err := fn(ik.UserKey, v); err != nil {
			return err
		}
	}
	// the loop also ends on a block the iterator could not read
	if err := it.Error(); err != nil {
		return MarkPebbleErr(errors.Wrapf(err, "scan sst %s", r.file.Name()))
	}
	return nil
}

// Close releases the reader and the underlying file.
func (r *Reader) Close() error {
	if err := r.r.Close(); err != nil {
		return MarkPebbleErr(errors.Wrapf(err, "close sst %s", r.file.Name()))
	}
	return nil
}
