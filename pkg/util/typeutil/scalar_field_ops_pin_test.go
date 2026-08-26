package typeutil

import (
	"fmt"
	"reflect"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

// Pinning tests for the scalar-variant arms of the FieldData operations in
// schema.go: AppendFieldData, AppendFieldDataByColumn, DeleteFieldData,
// UpdateFieldData, UpdateFieldDataByColumn, MergeFieldData and
// PrepareResultFieldData. They assert the behavior of the per-variant case
// bodies, including the asymmetries between operations (GeometryWktData is
// skipped by DeleteFieldData, rejected by MergeFieldData and contributes no
// appendSize; BytesData is only handled by MergeFieldData; JsonData forks on
// IsDynamic; ArrayData copies ElementType on wrapper creation), so a
// structural change to those bodies can be verified output-identical.

func pinScalarFieldData(dt schemapb.DataType, fieldID int64, scalars *schemapb.ScalarField) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      dt,
		FieldName: fmt.Sprintf("f%d", fieldID),
		FieldId:   fieldID,
		Field:     &schemapb.FieldData_Scalars{Scalars: scalars},
	}
}

type scalarPinVariant struct {
	name     string
	dataType schemapb.DataType
	// twoRows returns a FieldData holding rows [row(0), row(1)].
	twoRows func(fieldID int64) *schemapb.FieldData
	// dataOf extracts the payload slice.
	dataOf func(fd *schemapb.FieldData) interface{}
	// rows builds the expected payload slice for the given row indices.
	rows     func(idxs ...int) interface{}
	elemSize int64 // AppendFieldData size contribution per appended row
	inDelete bool  // DeleteFieldData has a case for this variant
	inMerge  bool  // MergeFieldData has a case for this variant
}

func pinArrayRow(i int) *schemapb.ScalarField {
	return &schemapb.ScalarField{
		Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{int64(i)}}},
	}
}

var scalarPinVariants = []scalarPinVariant{
	{
		name: "bool", dataType: schemapb.DataType_Bool,
		twoRows: func(id int64) *schemapb.FieldData {
			return pinScalarFieldData(schemapb.DataType_Bool, id, &schemapb.ScalarField{
				Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: []bool{false, true}}},
			})
		},
		dataOf: func(fd *schemapb.FieldData) interface{} { return fd.GetScalars().GetBoolData().GetData() },
		rows: func(idxs ...int) interface{} {
			out := make([]bool, 0, len(idxs))
			for _, i := range idxs {
				out = append(out, i%2 == 1)
			}
			return out
		},
		elemSize: int64(unsafe.Sizeof(false)), inDelete: true, inMerge: true,
	},
	{
		name: "int", dataType: schemapb.DataType_Int32,
		twoRows: func(id int64) *schemapb.FieldData {
			return pinScalarFieldData(schemapb.DataType_Int32, id, &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{10, 11}}},
			})
		},
		dataOf: func(fd *schemapb.FieldData) interface{} { return fd.GetScalars().GetIntData().GetData() },
		rows: func(idxs ...int) interface{} {
			out := make([]int32, 0, len(idxs))
			for _, i := range idxs {
				out = append(out, int32(10+i))
			}
			return out
		},
		elemSize: int64(unsafe.Sizeof(int32(0))), inDelete: true, inMerge: true,
	},
	{
		name: "long", dataType: schemapb.DataType_Int64,
		twoRows: func(id int64) *schemapb.FieldData {
			return pinScalarFieldData(schemapb.DataType_Int64, id, &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{100, 101}}},
			})
		},
		dataOf: func(fd *schemapb.FieldData) interface{} { return fd.GetScalars().GetLongData().GetData() },
		rows: func(idxs ...int) interface{} {
			out := make([]int64, 0, len(idxs))
			for _, i := range idxs {
				out = append(out, int64(100+i))
			}
			return out
		},
		elemSize: int64(unsafe.Sizeof(int64(0))), inDelete: true, inMerge: true,
	},
	{
		name: "timestamptz", dataType: schemapb.DataType_Timestamptz,
		twoRows: func(id int64) *schemapb.FieldData {
			return pinScalarFieldData(schemapb.DataType_Timestamptz, id, &schemapb.ScalarField{
				Data: &schemapb.ScalarField_TimestamptzData{TimestamptzData: &schemapb.TimestamptzArray{Data: []int64{1000, 1001}}},
			})
		},
		dataOf: func(fd *schemapb.FieldData) interface{} { return fd.GetScalars().GetTimestamptzData().GetData() },
		rows: func(idxs ...int) interface{} {
			out := make([]int64, 0, len(idxs))
			for _, i := range idxs {
				out = append(out, int64(1000+i))
			}
			return out
		},
		elemSize: int64(unsafe.Sizeof(int64(0))), inDelete: true, inMerge: true,
	},
	{
		name: "float", dataType: schemapb.DataType_Float,
		twoRows: func(id int64) *schemapb.FieldData {
			return pinScalarFieldData(schemapb.DataType_Float, id, &schemapb.ScalarField{
				Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: []float32{0.5, 1.5}}},
			})
		},
		dataOf: func(fd *schemapb.FieldData) interface{} { return fd.GetScalars().GetFloatData().GetData() },
		rows: func(idxs ...int) interface{} {
			out := make([]float32, 0, len(idxs))
			for _, i := range idxs {
				out = append(out, float32(i)+0.5)
			}
			return out
		},
		elemSize: int64(unsafe.Sizeof(float32(0))), inDelete: true, inMerge: true,
	},
	{
		name: "double", dataType: schemapb.DataType_Double,
		twoRows: func(id int64) *schemapb.FieldData {
			return pinScalarFieldData(schemapb.DataType_Double, id, &schemapb.ScalarField{
				Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: []float64{0.25, 1.25}}},
			})
		},
		dataOf: func(fd *schemapb.FieldData) interface{} { return fd.GetScalars().GetDoubleData().GetData() },
		rows: func(idxs ...int) interface{} {
			out := make([]float64, 0, len(idxs))
			for _, i := range idxs {
				out = append(out, float64(i)+0.25)
			}
			return out
		},
		elemSize: int64(unsafe.Sizeof(float64(0))), inDelete: true, inMerge: true,
	},
	{
		name: "string", dataType: schemapb.DataType_VarChar,
		twoRows: func(id int64) *schemapb.FieldData {
			return pinScalarFieldData(schemapb.DataType_VarChar, id, &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"s0", "s1"}}},
			})
		},
		dataOf: func(fd *schemapb.FieldData) interface{} { return fd.GetScalars().GetStringData().GetData() },
		rows: func(idxs ...int) interface{} {
			out := make([]string, 0, len(idxs))
			for _, i := range idxs {
				out = append(out, fmt.Sprintf("s%d", i))
			}
			return out
		},
		elemSize: int64(unsafe.Sizeof("")), inDelete: true, inMerge: true,
	},
	{
		name: "array", dataType: schemapb.DataType_Array,
		twoRows: func(id int64) *schemapb.FieldData {
			return pinScalarFieldData(schemapb.DataType_Array, id, &schemapb.ScalarField{
				Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
					Data:        []*schemapb.ScalarField{pinArrayRow(0), pinArrayRow(1)},
					ElementType: schemapb.DataType_Int64,
				}},
			})
		},
		dataOf: func(fd *schemapb.FieldData) interface{} { return fd.GetScalars().GetArrayData().GetData() },
		rows: func(idxs ...int) interface{} {
			out := make([]*schemapb.ScalarField, 0, len(idxs))
			for _, i := range idxs {
				out = append(out, pinArrayRow(i))
			}
			return out
		},
		elemSize: int64(unsafe.Sizeof((*schemapb.ScalarField)(nil))), inDelete: false, inMerge: true,
	},
	{
		name: "json", dataType: schemapb.DataType_JSON,
		twoRows: func(id int64) *schemapb.FieldData {
			return pinScalarFieldData(schemapb.DataType_JSON, id, &schemapb.ScalarField{
				Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{
					Data: [][]byte{[]byte(`{"k":0}`), []byte(`{"k":1}`)},
				}},
			})
		},
		dataOf: func(fd *schemapb.FieldData) interface{} { return fd.GetScalars().GetJsonData().GetData() },
		rows: func(idxs ...int) interface{} {
			out := make([][]byte, 0, len(idxs))
			for _, i := range idxs {
				out = append(out, []byte(fmt.Sprintf(`{"k":%d}`, i)))
			}
			return out
		},
		elemSize: int64(unsafe.Sizeof([]byte(nil))), inDelete: true, inMerge: true,
	},
	{
		name: "geometry", dataType: schemapb.DataType_Geometry,
		twoRows: func(id int64) *schemapb.FieldData {
			return pinScalarFieldData(schemapb.DataType_Geometry, id, &schemapb.ScalarField{
				Data: &schemapb.ScalarField_GeometryData{GeometryData: &schemapb.GeometryArray{
					Data: [][]byte{{0}, {1}},
				}},
			})
		},
		dataOf: func(fd *schemapb.FieldData) interface{} { return fd.GetScalars().GetGeometryData().GetData() },
		rows: func(idxs ...int) interface{} {
			out := make([][]byte, 0, len(idxs))
			for _, i := range idxs {
				out = append(out, []byte{byte(i)})
			}
			return out
		},
		elemSize: int64(unsafe.Sizeof([]byte(nil))), inDelete: true, inMerge: true,
	},
	{
		name: "geometry_wkt", dataType: schemapb.DataType_Geometry,
		twoRows: func(id int64) *schemapb.FieldData {
			return pinScalarFieldData(schemapb.DataType_Geometry, id, &schemapb.ScalarField{
				Data: &schemapb.ScalarField_GeometryWktData{GeometryWktData: &schemapb.GeometryWktArray{
					Data: []string{"w0", "w1"},
				}},
			})
		},
		dataOf: func(fd *schemapb.FieldData) interface{} { return fd.GetScalars().GetGeometryWktData().GetData() },
		rows: func(idxs ...int) interface{} {
			out := make([]string, 0, len(idxs))
			for _, i := range idxs {
				out = append(out, fmt.Sprintf("w%d", i))
			}
			return out
		},
		elemSize: 0, inDelete: false, inMerge: false,
	},
}

func TestPinScalarAppendFieldData(t *testing.T) {
	for _, v := range scalarPinVariants {
		t.Run(v.name, func(t *testing.T) {
			src := v.twoRows(100)
			dst := make([]*schemapb.FieldData, 1)

			// creation path: dst column does not exist yet
			size := AppendFieldData(dst, []*schemapb.FieldData{src}, 1)
			require.NotNil(t, dst[0])
			assert.Equal(t, src.GetType(), dst[0].GetType())
			assert.Equal(t, src.GetFieldName(), dst[0].GetFieldName())
			assert.Equal(t, src.GetFieldId(), dst[0].GetFieldId())
			assert.Equal(t, v.rows(1), v.dataOf(dst[0]))
			assert.Equal(t, v.elemSize, size)

			// append path: dst wrapper already exists
			size = AppendFieldData(dst, []*schemapb.FieldData{src}, 0)
			assert.Equal(t, v.rows(1, 0), v.dataOf(dst[0]))
			assert.Equal(t, v.elemSize, size)
		})
	}

	t.Run("array element type copied on creation", func(t *testing.T) {
		src := scalarPinVariants[7].twoRows(100)
		dst := make([]*schemapb.FieldData, 1)
		AppendFieldData(dst, []*schemapb.FieldData{src}, 0)
		assert.Equal(t, schemapb.DataType_Int64, dst[0].GetScalars().GetArrayData().GetElementType())
	})
}

func TestPinScalarAppendFieldDataByColumn(t *testing.T) {
	for _, v := range scalarPinVariants {
		t.Run(v.name, func(t *testing.T) {
			src := v.twoRows(100)
			dst := &schemapb.FieldData{
				Type:      src.GetType(),
				FieldName: src.GetFieldName(),
				FieldId:   src.GetFieldId(),
				Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{}},
			}

			// creation path, out-of-order indices
			AppendFieldDataByColumn(dst, src, []int64{1, 0})
			assert.Equal(t, v.rows(1, 0), v.dataOf(dst))

			// append path
			AppendFieldDataByColumn(dst, src, []int64{1})
			assert.Equal(t, v.rows(1, 0, 1), v.dataOf(dst))
		})
	}

	t.Run("array element type copied on creation", func(t *testing.T) {
		src := scalarPinVariants[7].twoRows(100)
		dst := &schemapb.FieldData{
			Type:    src.GetType(),
			FieldId: src.GetFieldId(),
			Field:   &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{}},
		}
		AppendFieldDataByColumn(dst, src, []int64{0})
		assert.Equal(t, schemapb.DataType_Int64, dst.GetScalars().GetArrayData().GetElementType())
	})
}

func TestPinScalarDeleteFieldData(t *testing.T) {
	for _, v := range scalarPinVariants {
		t.Run(v.name, func(t *testing.T) {
			dst := v.twoRows(100)
			DeleteFieldData([]*schemapb.FieldData{dst})
			if v.inDelete {
				assert.Equal(t, v.rows(0), v.dataOf(dst))
			} else {
				// variants without a DeleteFieldData case are left untouched
				assert.Equal(t, v.rows(0, 1), v.dataOf(dst))
			}
		})
	}
}

func TestPinScalarUpdateFieldData(t *testing.T) {
	for _, v := range scalarPinVariants {
		t.Run(v.name, func(t *testing.T) {
			base := v.twoRows(100)
			update := v.twoRows(100)
			err := UpdateFieldData([]*schemapb.FieldData{base}, []*schemapb.FieldData{update}, 1, 0)
			require.NoError(t, err)
			// base row 1 replaced by update row 0
			assert.Equal(t, v.rows(0, 0), v.dataOf(base))
			// update side untouched
			assert.Equal(t, v.rows(0, 1), v.dataOf(update))
		})
	}

	t.Run("update wrapper of different variant leaves base untouched", func(t *testing.T) {
		long := scalarPinVariants[2]
		base := long.twoRows(100)
		update := pinScalarFieldData(schemapb.DataType_Int64, 100, &schemapb.ScalarField{
			Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{7}}},
		})
		err := UpdateFieldData([]*schemapb.FieldData{base}, []*schemapb.FieldData{update}, 1, 0)
		require.NoError(t, err)
		assert.Equal(t, long.rows(0, 1), long.dataOf(base))
	})

	t.Run("out of bounds update index leaves base untouched", func(t *testing.T) {
		long := scalarPinVariants[2]
		base := long.twoRows(100)
		update := long.twoRows(100)
		err := UpdateFieldData([]*schemapb.FieldData{base}, []*schemapb.FieldData{update}, 1, 5)
		require.NoError(t, err)
		assert.Equal(t, long.rows(0, 1), long.dataOf(base))
	})

	t.Run("unsupported scalar variant returns error", func(t *testing.T) {
		base := pinScalarFieldData(schemapb.DataType_None, 100, &schemapb.ScalarField{
			Data: &schemapb.ScalarField_BytesData{BytesData: &schemapb.BytesArray{Data: [][]byte{{1}}}},
		})
		update := pinScalarFieldData(schemapb.DataType_None, 100, &schemapb.ScalarField{
			Data: &schemapb.ScalarField_BytesData{BytesData: &schemapb.BytesArray{Data: [][]byte{{2}}}},
		})
		err := UpdateFieldData([]*schemapb.FieldData{base}, []*schemapb.FieldData{update}, 0, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported scalar field type: None")
	})

	t.Run("dynamic json merges keys", func(t *testing.T) {
		base := pinScalarFieldData(schemapb.DataType_JSON, 100, &schemapb.ScalarField{
			Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{
				Data: [][]byte{[]byte(`{"a":1,"b":2}`)},
			}},
		})
		base.IsDynamic = true
		update := pinScalarFieldData(schemapb.DataType_JSON, 100, &schemapb.ScalarField{
			Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{
				Data: [][]byte{[]byte(`{"b":3}`)},
			}},
		})
		update.IsDynamic = true
		err := UpdateFieldData([]*schemapb.FieldData{base}, []*schemapb.FieldData{update}, 0, 0)
		require.NoError(t, err)
		assert.Equal(t, [][]byte{[]byte(`{"a":1,"b":3}`)}, base.GetScalars().GetJsonData().GetData())
	})
}

func TestPinScalarUpdateFieldDataByColumn(t *testing.T) {
	for _, v := range scalarPinVariants {
		t.Run(v.name, func(t *testing.T) {
			base := v.twoRows(100)
			update := v.twoRows(100)
			err := UpdateFieldDataByColumn(base, update, []int64{0, 1}, []int64{1, 0})
			require.NoError(t, err)
			assert.Equal(t, v.rows(1, 0), v.dataOf(base))
		})
	}

	t.Run("dynamic json merges keys", func(t *testing.T) {
		base := pinScalarFieldData(schemapb.DataType_JSON, 100, &schemapb.ScalarField{
			Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{
				Data: [][]byte{[]byte(`{"a":1,"b":2}`)},
			}},
		})
		base.IsDynamic = true
		update := pinScalarFieldData(schemapb.DataType_JSON, 100, &schemapb.ScalarField{
			Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{
				Data: [][]byte{[]byte(`{"b":3}`)},
			}},
		})
		update.IsDynamic = true
		err := UpdateFieldDataByColumn(base, update, []int64{0}, []int64{0})
		require.NoError(t, err)
		assert.Equal(t, [][]byte{[]byte(`{"a":1,"b":3}`)}, base.GetScalars().GetJsonData().GetData())
	})
}

func TestPinScalarMergeFieldData(t *testing.T) {
	for _, v := range scalarPinVariants {
		if !v.inMerge {
			continue
		}
		t.Run(v.name, func(t *testing.T) {
			src := v.twoRows(100)
			dst := &schemapb.FieldData{
				Type:      src.GetType(),
				FieldName: src.GetFieldName(),
				FieldId:   src.GetFieldId(),
				Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{}},
			}

			// nil dst wrapper adopts src payload
			err := MergeFieldData([]*schemapb.FieldData{dst}, []*schemapb.FieldData{v.twoRows(100)})
			require.NoError(t, err)
			assert.Equal(t, v.rows(0, 1), v.dataOf(dst))

			// existing dst wrapper appends
			err = MergeFieldData([]*schemapb.FieldData{dst}, []*schemapb.FieldData{src})
			require.NoError(t, err)
			assert.Equal(t, v.rows(0, 1, 0, 1), v.dataOf(dst))
		})
	}

	t.Run("array element type copied on adoption", func(t *testing.T) {
		src := scalarPinVariants[7].twoRows(100)
		dst := &schemapb.FieldData{
			Type:    src.GetType(),
			FieldId: src.GetFieldId(),
			Field:   &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{}},
		}
		err := MergeFieldData([]*schemapb.FieldData{dst}, []*schemapb.FieldData{src})
		require.NoError(t, err)
		assert.Equal(t, schemapb.DataType_Int64, dst.GetScalars().GetArrayData().GetElementType())
	})

	t.Run("bytes data is mergeable", func(t *testing.T) {
		mkBytes := func() *schemapb.FieldData {
			return pinScalarFieldData(schemapb.DataType_None, 100, &schemapb.ScalarField{
				Data: &schemapb.ScalarField_BytesData{BytesData: &schemapb.BytesArray{Data: [][]byte{{1}, {2}}}},
			})
		}
		dst := &schemapb.FieldData{
			FieldId: 100,
			Field:   &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{}},
		}
		err := MergeFieldData([]*schemapb.FieldData{dst}, []*schemapb.FieldData{mkBytes()})
		require.NoError(t, err)
		assert.Equal(t, [][]byte{{1}, {2}}, dst.GetScalars().GetBytesData().GetData())
		err = MergeFieldData([]*schemapb.FieldData{dst}, []*schemapb.FieldData{mkBytes()})
		require.NoError(t, err)
		assert.Equal(t, [][]byte{{1}, {2}, {1}, {2}}, dst.GetScalars().GetBytesData().GetData())
	})

	t.Run("geometry wkt variant is rejected", func(t *testing.T) {
		src := scalarPinVariants[10].twoRows(100)
		dst := &schemapb.FieldData{
			Type:    src.GetType(),
			FieldId: src.GetFieldId(),
			Field:   &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{}},
		}
		err := MergeFieldData([]*schemapb.FieldData{dst}, []*schemapb.FieldData{src})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported data type: Geometry")
	})

	t.Run("missing dst field returns error", func(t *testing.T) {
		src := scalarPinVariants[2].twoRows(100)
		err := MergeFieldData([]*schemapb.FieldData{}, []*schemapb.FieldData{src})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "fields in src but not in dst: Int64")
	})
}

func TestPinScalarPrepareResultFieldData(t *testing.T) {
	for _, v := range scalarPinVariants {
		t.Run(v.name, func(t *testing.T) {
			sample := v.twoRows(100)
			out := PrepareResultFieldData([]*schemapb.FieldData{sample}, 7)
			require.Len(t, out, 1)
			assert.Equal(t, sample.GetType(), out[0].GetType())
			assert.Equal(t, sample.GetFieldName(), out[0].GetFieldName())
			assert.Equal(t, sample.GetFieldId(), out[0].GetFieldId())

			data := reflect.ValueOf(v.dataOf(out[0]))
			require.Equal(t, reflect.Slice, data.Kind())
			assert.Equal(t, 0, data.Len())
			assert.Equal(t, 7, data.Cap())
		})
	}

	t.Run("array element type copied", func(t *testing.T) {
		sample := scalarPinVariants[7].twoRows(100)
		out := PrepareResultFieldData([]*schemapb.FieldData{sample}, 7)
		require.Len(t, out, 1)
		assert.Equal(t, schemapb.DataType_Int64, out[0].GetScalars().GetArrayData().GetElementType())
	})
}
