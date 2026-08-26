// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package typeutil

import (
	"unsafe"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

// scalarFieldAccessor bundles the per-oneof-variant slice accessors of
// schemapb.ScalarField so the FieldData operations in schema.go (append /
// delete / update / merge / prepare) are written once, generically, instead
// of once per variant. Each operation keeps its own switch over the variants
// it supports; only the case bodies go through the accessor, so the
// per-operation variant membership (e.g. MergeFieldData rejecting
// GeometryWktData, DeleteFieldData skipping it) stays where it was.
type scalarFieldAccessor[T any] struct {
	// data returns the payload slice, nil-safe on a missing wrapper.
	data func(sf *schemapb.ScalarField) []T
	// has reports whether the variant's typed wrapper is present.
	has func(sf *schemapb.ScalarField) bool
	// setData writes the slice back into the existing wrapper.
	setData func(sf *schemapb.ScalarField, data []T)
	// install creates the typed wrapper on dst holding data, copying
	// variant metadata (ArrayData.ElementType) from meta.
	install func(dst *schemapb.ScalarField, meta *schemapb.ScalarField, data []T)
}

func sizeOfScalarElem[T any]() int64 {
	var v T
	/* #nosec G103 */
	return int64(unsafe.Sizeof(v))
}

// appendOne appends src's row idx to dst, creating the wrapper if missing,
// and returns the size accounted for the appended element.
func (a scalarFieldAccessor[T]) appendOne(dst, src *schemapb.ScalarField, idx int64) int64 {
	v := a.data(src)[idx]
	if !a.has(dst) {
		a.install(dst, src, []T{v})
	} else {
		a.setData(dst, append(a.data(dst), v))
	}
	return sizeOfScalarElem[T]()
}

// appendMany appends src's rows at indices to dst, creating the wrapper
// with capacity len(indices) if missing.
func (a scalarFieldAccessor[T]) appendMany(dst, src *schemapb.ScalarField, indices []int64) {
	if !a.has(dst) {
		a.install(dst, src, make([]T, 0, len(indices)))
	}
	srcData := a.data(src)
	dstData := a.data(dst)
	for _, idx := range indices {
		dstData = append(dstData, srcData[idx])
	}
	a.setData(dst, dstData)
}

// deleteLast drops the last row.
func (a scalarFieldAccessor[T]) deleteLast(sf *schemapb.ScalarField) {
	data := a.data(sf)
	a.setData(sf, data[:len(data)-1])
}

// updateAtGuarded replaces base row baseIdx with update row updateIdx when
// both wrappers are present and both indices are in range; otherwise no-op.
func (a scalarFieldAccessor[T]) updateAtGuarded(base, update *schemapb.ScalarField, baseIdx, updateIdx int64) {
	if !a.has(base) || !a.has(update) {
		return
	}
	baseData, updateData := a.data(base), a.data(update)
	if int(updateIdx) < len(updateData) && int(baseIdx) < len(baseData) {
		baseData[baseIdx] = updateData[updateIdx]
	}
}

// updateByIndices writes update rows into base at paired indices, unguarded.
func (a scalarFieldAccessor[T]) updateByIndices(base, update *schemapb.ScalarField, baseIndices, updateIndices []int64) {
	baseData, updateData := a.data(base), a.data(update)
	for i, baseIdx := range baseIndices {
		baseData[baseIdx] = updateData[updateIndices[i]]
	}
}

// mergeFrom appends all of src's rows to dst, adopting src's slice when dst
// has no wrapper yet.
func (a scalarFieldAccessor[T]) mergeFrom(dst, src *schemapb.ScalarField) {
	if !a.has(dst) {
		a.install(dst, src, a.data(src))
	} else {
		a.setData(dst, append(a.data(dst), a.data(src)...))
	}
}

// prepare installs an empty wrapper with the given capacity on dst, copying
// variant metadata from sample.
func (a scalarFieldAccessor[T]) prepare(dst, sample *schemapb.ScalarField, capacity int64) {
	a.install(dst, sample, make([]T, 0, capacity))
}

var boolFieldAccessor = scalarFieldAccessor[bool]{
	data:    func(sf *schemapb.ScalarField) []bool { return sf.GetBoolData().GetData() },
	has:     func(sf *schemapb.ScalarField) bool { return sf.GetBoolData() != nil },
	setData: func(sf *schemapb.ScalarField, data []bool) { sf.GetBoolData().Data = data },
	install: func(dst, _ *schemapb.ScalarField, data []bool) {
		dst.Data = &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: data}}
	},
}

var intFieldAccessor = scalarFieldAccessor[int32]{
	data:    func(sf *schemapb.ScalarField) []int32 { return sf.GetIntData().GetData() },
	has:     func(sf *schemapb.ScalarField) bool { return sf.GetIntData() != nil },
	setData: func(sf *schemapb.ScalarField, data []int32) { sf.GetIntData().Data = data },
	install: func(dst, _ *schemapb.ScalarField, data []int32) {
		dst.Data = &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: data}}
	},
}

var longFieldAccessor = scalarFieldAccessor[int64]{
	data:    func(sf *schemapb.ScalarField) []int64 { return sf.GetLongData().GetData() },
	has:     func(sf *schemapb.ScalarField) bool { return sf.GetLongData() != nil },
	setData: func(sf *schemapb.ScalarField, data []int64) { sf.GetLongData().Data = data },
	install: func(dst, _ *schemapb.ScalarField, data []int64) {
		dst.Data = &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: data}}
	},
}

var floatFieldAccessor = scalarFieldAccessor[float32]{
	data:    func(sf *schemapb.ScalarField) []float32 { return sf.GetFloatData().GetData() },
	has:     func(sf *schemapb.ScalarField) bool { return sf.GetFloatData() != nil },
	setData: func(sf *schemapb.ScalarField, data []float32) { sf.GetFloatData().Data = data },
	install: func(dst, _ *schemapb.ScalarField, data []float32) {
		dst.Data = &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: data}}
	},
}

var doubleFieldAccessor = scalarFieldAccessor[float64]{
	data:    func(sf *schemapb.ScalarField) []float64 { return sf.GetDoubleData().GetData() },
	has:     func(sf *schemapb.ScalarField) bool { return sf.GetDoubleData() != nil },
	setData: func(sf *schemapb.ScalarField, data []float64) { sf.GetDoubleData().Data = data },
	install: func(dst, _ *schemapb.ScalarField, data []float64) {
		dst.Data = &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: data}}
	},
}

var stringFieldAccessor = scalarFieldAccessor[string]{
	data:    func(sf *schemapb.ScalarField) []string { return sf.GetStringData().GetData() },
	has:     func(sf *schemapb.ScalarField) bool { return sf.GetStringData() != nil },
	setData: func(sf *schemapb.ScalarField, data []string) { sf.GetStringData().Data = data },
	install: func(dst, _ *schemapb.ScalarField, data []string) {
		dst.Data = &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: data}}
	},
}

var arrayFieldAccessor = scalarFieldAccessor[*schemapb.ScalarField]{
	data:    func(sf *schemapb.ScalarField) []*schemapb.ScalarField { return sf.GetArrayData().GetData() },
	has:     func(sf *schemapb.ScalarField) bool { return sf.GetArrayData() != nil },
	setData: func(sf *schemapb.ScalarField, data []*schemapb.ScalarField) { sf.GetArrayData().Data = data },
	install: func(dst, meta *schemapb.ScalarField, data []*schemapb.ScalarField) {
		dst.Data = &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
			Data:        data,
			ElementType: meta.GetArrayData().GetElementType(),
		}}
	},
}

var jsonFieldAccessor = scalarFieldAccessor[[]byte]{
	data:    func(sf *schemapb.ScalarField) [][]byte { return sf.GetJsonData().GetData() },
	has:     func(sf *schemapb.ScalarField) bool { return sf.GetJsonData() != nil },
	setData: func(sf *schemapb.ScalarField, data [][]byte) { sf.GetJsonData().Data = data },
	install: func(dst, _ *schemapb.ScalarField, data [][]byte) {
		dst.Data = &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: data}}
	},
}

var timestamptzFieldAccessor = scalarFieldAccessor[int64]{
	data:    func(sf *schemapb.ScalarField) []int64 { return sf.GetTimestamptzData().GetData() },
	has:     func(sf *schemapb.ScalarField) bool { return sf.GetTimestamptzData() != nil },
	setData: func(sf *schemapb.ScalarField, data []int64) { sf.GetTimestamptzData().Data = data },
	install: func(dst, _ *schemapb.ScalarField, data []int64) {
		dst.Data = &schemapb.ScalarField_TimestamptzData{TimestamptzData: &schemapb.TimestamptzArray{Data: data}}
	},
}

var geometryFieldAccessor = scalarFieldAccessor[[]byte]{
	data:    func(sf *schemapb.ScalarField) [][]byte { return sf.GetGeometryData().GetData() },
	has:     func(sf *schemapb.ScalarField) bool { return sf.GetGeometryData() != nil },
	setData: func(sf *schemapb.ScalarField, data [][]byte) { sf.GetGeometryData().Data = data },
	install: func(dst, _ *schemapb.ScalarField, data [][]byte) {
		dst.Data = &schemapb.ScalarField_GeometryData{GeometryData: &schemapb.GeometryArray{Data: data}}
	},
}

var geometryWktFieldAccessor = scalarFieldAccessor[string]{
	data:    func(sf *schemapb.ScalarField) []string { return sf.GetGeometryWktData().GetData() },
	has:     func(sf *schemapb.ScalarField) bool { return sf.GetGeometryWktData() != nil },
	setData: func(sf *schemapb.ScalarField, data []string) { sf.GetGeometryWktData().Data = data },
	install: func(dst, _ *schemapb.ScalarField, data []string) {
		dst.Data = &schemapb.ScalarField_GeometryWktData{GeometryWktData: &schemapb.GeometryWktArray{Data: data}}
	},
}

var bytesFieldAccessor = scalarFieldAccessor[[]byte]{
	data:    func(sf *schemapb.ScalarField) [][]byte { return sf.GetBytesData().GetData() },
	has:     func(sf *schemapb.ScalarField) bool { return sf.GetBytesData() != nil },
	setData: func(sf *schemapb.ScalarField, data [][]byte) { sf.GetBytesData().Data = data },
	install: func(dst, _ *schemapb.ScalarField, data [][]byte) {
		dst.Data = &schemapb.ScalarField_BytesData{BytesData: &schemapb.BytesArray{Data: data}}
	},
}
