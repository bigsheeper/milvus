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

// Package codec defines the key/value encoding of the primary key index.
//
// A pkindex engine instance is scoped to one vchannel, whose collection has a
// single primary key field of a fixed type. The keyspace therefore carries no
// prefix: a key is exactly the order-preserving encoding of one primary key,
// and the PK type is engine-level metadata rather than a per-key tag.
//
// Order preservation means plain bytewise comparison of encoded keys matches
// the natural order of the original values, so SST min/max ranges prune point
// lookups without decoding.
package codec

import (
	"encoding/binary"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	// Int64PKSize is the encoded size of an int64 primary key.
	Int64PKSize = 8

	// tombstoneVersion marks a deleted PK. Deletes are stored as this sentinel
	// value rather than as engine-native tombstones, so that a recent delete in
	// the increment layer masks an older entry in the read-only baseline layer,
	// and survives a memtable flush into an SST. Merge (LWW) physically drops
	// tombstoned keys.
	tombstoneVersion byte = 0x00

	// pkEntryVersion is the current PKEntry encoding version.
	pkEntryVersion byte = 0x01

	// pkEntrySize is the encoded size of a version-0x01 PKEntry.
	pkEntrySize = 1 + 8
)

// EncodeInt64PK encodes an int64 primary key as 8 big-endian bytes with the
// sign bit flipped, so that bytewise order equals numeric order (negative
// values sort before non-negative ones).
func EncodeInt64PK(pk int64) []byte {
	b := make([]byte, Int64PKSize)
	binary.BigEndian.PutUint64(b, uint64(pk)^(1<<63))
	return b
}

// DecodeInt64PK decodes a key produced by EncodeInt64PK.
func DecodeInt64PK(b []byte) (int64, error) {
	if len(b) != Int64PKSize {
		return 0, merr.WrapErrDataIntegrityMsg("invalid int64 pk key length %d, want %d", len(b), Int64PKSize)
	}
	return int64(binary.BigEndian.Uint64(b) ^ (1 << 63)), nil
}

// EncodeVarcharPK encodes a varchar primary key as its raw bytes; bytewise
// order over raw bytes is already the lexicographic order of the string.
func EncodeVarcharPK(pk string) []byte {
	return []byte(pk)
}

// DecodeVarcharPK decodes a key produced by EncodeVarcharPK.
func DecodeVarcharPK(b []byte) string {
	return string(b)
}

// PKEntry is the value stored per primary key: the segment currently holding
// the live row for that PK.
type PKEntry struct {
	SegmentID int64
}

// EncodePKEntry encodes a PKEntry as a 1-byte version followed by the 8-byte
// big-endian segment ID. The version byte reserves room for future value
// extensions (field redundancy, custom timestamps).
func EncodePKEntry(e PKEntry) []byte {
	b := make([]byte, pkEntrySize)
	b[0] = pkEntryVersion
	binary.BigEndian.PutUint64(b[1:], uint64(e.SegmentID))
	return b
}

// EncodeTombstone returns the value marking a deleted PK.
func EncodeTombstone() []byte {
	return []byte{tombstoneVersion}
}

// IsTombstone reports whether a stored value is the delete marker.
func IsTombstone(b []byte) bool {
	return len(b) == 1 && b[0] == tombstoneVersion
}

// DecodePKEntry decodes a value produced by EncodePKEntry.
func DecodePKEntry(b []byte) (PKEntry, error) {
	if len(b) < pkEntrySize {
		return PKEntry{}, merr.WrapErrDataIntegrityMsg("invalid pk entry length %d, want at least %d", len(b), pkEntrySize)
	}
	if b[0] != pkEntryVersion {
		return PKEntry{}, merr.WrapErrDataIntegrityMsg("unknown pk entry version %d", b[0])
	}
	return PKEntry{SegmentID: int64(binary.BigEndian.Uint64(b[1:pkEntrySize]))}, nil
}
