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

package importutilv2

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"github.com/sbinet/npyio"
	"github.com/sbinet/npyio/npy"
	"golang.org/x/text/encoding/unicode"
	"io"
	"reflect"
	"regexp"
	"strconv"
	"unicode/utf8"
)

var (
	reStrPre  = regexp.MustCompile(`^[|]*?(\d.*)[Sa]$`)
	reStrPost = regexp.MustCompile(`^[|]*?[Sa](\d.*)$`)
	reUniPre  = regexp.MustCompile(`^[<|>]*?(\d.*)U$`)
	reUniPost = regexp.MustCompile(`^[<|>]*?U(\d.*)$`)
)

type NumpyReader struct {
	schema  *schemapb.CollectionSchema
	readers map[int64]ColumnReader // fieldID -> ColumnReader
}

func (n *NumpyReader) Next(count int64) (*storage.InsertData, error) {
	insertData, err := storage.NewInsertData(n.schema)
	if err != nil {
		return nil, err
	}
	for fieldID, reader := range n.readers {
		fieldData, err := reader.Next(count)
		if err != nil {
			return nil, err
		}
		insertData.Data[fieldID] = fieldData
	}
	return insertData, nil
}

type NumpyColumnReader struct {
	reader      io.Reader
	npyReader   *npy.Reader
	order       binary.ByteOrder
	dim         int64
	fieldSchema *schemapb.FieldSchema
}

func ReadData[T any](reader io.Reader, order binary.ByteOrder, count int64) ([]T, error) {
	data := make([]T, count)
	err := binary.Read(reader, order, &data) // TODO: dyh, handle EOF
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (n *NumpyColumnReader) Next(count int64) (storage.FieldData, error) {
	dt := n.fieldSchema.GetDataType()
	switch dt {
	case schemapb.DataType_Bool:
		data, err := ReadData[bool](n.reader, n.order, count)
		if err != nil {
			return nil, err
		}
		return &storage.BoolFieldData{
			Data: data,
		}, nil
	case schemapb.DataType_Int8:
		data, err := ReadData[int8](n.reader, n.order, count)
		if err != nil {
			return nil, err
		}
		return &storage.Int8FieldData{
			Data: data,
		}, nil
	case schemapb.DataType_Int16:
		data, err := ReadData[int16](n.reader, n.order, count)
		if err != nil {
			return nil, err
		}
		return &storage.Int16FieldData{
			Data: data,
		}, nil
	case schemapb.DataType_Int32:
		data, err := ReadData[int32](n.reader, n.order, count)
		if err != nil {
			return nil, err
		}
		return &storage.Int32FieldData{
			Data: data,
		}, nil
	case schemapb.DataType_Int64:
		data, err := ReadData[int64](n.reader, n.order, count)
		if err != nil {
			return nil, err
		}
		return &storage.Int64FieldData{
			Data: data,
		}, nil
	case schemapb.DataType_Float:
		data, err := ReadData[float32](n.reader, n.order, count)
		if err != nil {
			return nil, err
		}
		return &storage.FloatFieldData{
			Data: data,
		}, nil
	case schemapb.DataType_Double:
		data, err := ReadData[float64](n.reader, n.order, count)
		if err != nil {
			return nil, err
		}
		return &storage.DoubleFieldData{
			Data: data,
		}, nil
	case schemapb.DataType_VarChar:
		data, err := n.ReadString(count)
		if err != nil {
			return nil, err
		}
		return &storage.StringFieldData{
			Data: data,
		}, nil
	case schemapb.DataType_JSON:
		// JSON field read data from string array numpy
		data, err := n.ReadString(count)
		if err != nil {
			return nil, err
		}
		byteArr := make([][]byte, 0)
		for _, str := range data {
			var dummy interface{}
			err = json.Unmarshal([]byte(str), &dummy)
			if err != nil {
				return nil, merr.WrapErrImportFailed(
					fmt.Sprintf("failed to parse value '%v' for JSON field '%s', error: %v",
						str, n.fieldSchema.GetName(), err))
			}
			byteArr = append(byteArr, []byte(str))
		}
		return &storage.JSONFieldData{
			Data: byteArr,
		}, nil
	case schemapb.DataType_BinaryVector:
		data, err := ReadData[uint8](n.reader, n.order, count*(n.dim/8))
		if err != nil {
			return nil, err
		}
		return &storage.BinaryVectorFieldData{
			Data: data,
			Dim:  int(n.dim),
		}, nil
	case schemapb.DataType_FloatVector:
		// float32/float64 numpy file can be used for float vector file, 2 reasons:
		// 1. for float vector, we support float32 and float64 numpy file because python float value is 64 bit
		// 2. for float64 numpy file, the performance is worse than float32 numpy file
		elementType, err := convertNumpyType(n.npyReader.Header.Descr.Type)
		if err != nil {
			return nil, err
		}
		var data []float32
		if elementType == schemapb.DataType_Float {
			data, err = ReadData[float32](n.reader, n.order, count*n.dim)
			if err != nil {
				return nil, err
			}
			err = typeutil.VerifyFloats32(data)
			if err != nil {
				return nil, nil
			}
		} else if elementType == schemapb.DataType_Double {
			var data64 []float64
			data = make([]float32, 0, count*n.dim)
			data64, err = ReadData[float64](n.reader, n.order, count*n.dim)
			if err != nil {
				return nil, err
			}
			for _, f64 := range data64 {
				err = typeutil.VerifyFloat(f64)
				if err != nil {
					return nil, err
				}
				data = append(data, float32(f64))
			}
		}
		return &storage.FloatVectorFieldData{
			Data: data,
			Dim:  int(n.dim),
		}, nil
	default:
		return nil, fmt.Errorf("unexpected data type: %s", dt.String())
	}
}

// convertNumpyType gets data type converted from numpy header description, for vector field, the type is int8(binary vector) or float32(float vector)
func convertNumpyType(typeStr string) (schemapb.DataType, error) {
	switch typeStr {
	case "b1", "<b1", "|b1", "bool":
		return schemapb.DataType_Bool, nil
	case "u1", "<u1", "|u1", "uint8": // binary vector data type is uint8
		return schemapb.DataType_BinaryVector, nil
	case "i1", "<i1", "|i1", ">i1", "int8":
		return schemapb.DataType_Int8, nil
	case "i2", "<i2", "|i2", ">i2", "int16":
		return schemapb.DataType_Int16, nil
	case "i4", "<i4", "|i4", ">i4", "int32":
		return schemapb.DataType_Int32, nil
	case "i8", "<i8", "|i8", ">i8", "int64":
		return schemapb.DataType_Int64, nil
	case "f4", "<f4", "|f4", ">f4", "float32":
		return schemapb.DataType_Float, nil
	case "f8", "<f8", "|f8", ">f8", "float64":
		return schemapb.DataType_Double, nil
	default:
		if isStringType(typeStr) {
			// Note: JSON field and VARCHAR field are using string type numpy
			return schemapb.DataType_VarChar, nil
		}
		return schemapb.DataType_None, merr.WrapErrImportFailed(fmt.Sprintf("the numpy file dtype '%s' is not supported", typeStr))
	}
}

func stringLen(dtype string) (int, bool, error) {
	var utf bool
	switch {
	case reStrPre.MatchString(dtype), reStrPost.MatchString(dtype):
		utf = false
	case reUniPre.MatchString(dtype), reUniPost.MatchString(dtype):
		utf = true
	}

	if m := reStrPre.FindStringSubmatch(dtype); m != nil {
		v, err := strconv.Atoi(m[1])
		if err != nil {
			return 0, false, err
		}
		return v, utf, nil
	}
	if m := reStrPost.FindStringSubmatch(dtype); m != nil {
		v, err := strconv.Atoi(m[1])
		if err != nil {
			return 0, false, err
		}
		return v, utf, nil
	}
	if m := reUniPre.FindStringSubmatch(dtype); m != nil {
		v, err := strconv.Atoi(m[1])
		if err != nil {
			return 0, false, err
		}
		return v, utf, nil
	}
	if m := reUniPost.FindStringSubmatch(dtype); m != nil {
		v, err := strconv.Atoi(m[1])
		if err != nil {
			return 0, false, err
		}
		return v, utf, nil
	}

	return 0, false, merr.WrapErrImportFailed(fmt.Sprintf("dtype '%s' of numpy file is not varchar data type", dtype))
}

func isStringType(typeStr string) bool {
	rt := npyio.TypeFrom(typeStr)
	return rt == reflect.TypeOf((*string)(nil)).Elem()
}

// setByteOrder sets BigEndian/LittleEndian, the logic of this method is copied from npyio lib
func (n *NumpyColumnReader) setByteOrder() {
	var nativeEndian binary.ByteOrder
	v := uint16(1)
	switch byte(v >> 8) {
	case 0:
		nativeEndian = binary.LittleEndian
	case 1:
		nativeEndian = binary.BigEndian
	}

	switch n.npyReader.Header.Descr.Type[0] {
	case '<':
		n.order = binary.LittleEndian
	case '>':
		n.order = binary.BigEndian
	default:
		n.order = nativeEndian
	}
}

func (n *NumpyColumnReader) GetShape() []int {
	return n.npyReader.Header.Descr.Shape
}

func (n *NumpyColumnReader) ReadString(count int64) ([]string, error) {
	// varchar length, this is the max length, some item is shorter than this length, but they also occupy bytes of max length
	maxLen, utf, err := stringLen(n.npyReader.Header.Descr.Type)
	if err != nil || maxLen <= 0 {
		return nil, merr.WrapErrImportFailed(
			fmt.Sprintf("failed to get max length %d of varchar from numpy file header, error: %v", maxLen, err))
	}

	// read data
	data := make([]string, 0, count)
	for len(data) < int(count) {
		if utf {
			// in the numpy file with utf32 encoding, the dType could be like "<U2",
			// "<" is byteorder(LittleEndian), "U" means it is utf32 encoding, "2" means the max length of strings is 2(characters)
			// each character occupy 4 bytes, each string occupys 4*maxLen bytes
			// for example, a numpy file has two strings: "a" and "bb", the maxLen is 2, byte order is LittleEndian
			// the character "a" occupys 2*4=8 bytes(0x97,0x00,0x00,0x00,0x00,0x00,0x00,0x00),
			// the "bb" occupys 8 bytes(0x97,0x00,0x00,0x00,0x98,0x00,0x00,0x00)
			// for non-ascii characters, the unicode could be 1 ~ 4 bytes, each character occupys 4 bytes, too
			raw, err := io.ReadAll(io.LimitReader(n.reader, utf8.UTFMax*int64(maxLen)))
			if err != nil {
				return nil, merr.WrapErrImportFailed(fmt.Sprintf("failed to read utf32 bytes from numpy file, error: %v", err))
			}
			str, err := decodeUtf32(raw, n.order)
			if err != nil {
				return nil, merr.WrapErrImportFailed(fmt.Sprintf("failed to decode utf32 bytes, error: %v", err))
			}
			data = append(data, str)
		} else {
			// in the numpy file with ansi encoding, the dType could be like "S2", maxLen is 2, each string occupys 2 bytes
			// bytes.Index(buf, []byte{0}) tell us which position is the end of the string
			buf, err := io.ReadAll(io.LimitReader(n.reader, int64(maxLen)))
			if err != nil {
				return nil, merr.WrapErrImportFailed(fmt.Sprintf("failed to read ascii bytes from numpy file, error: %v", err))
			}
			n := bytes.Index(buf, []byte{0})
			if n > 0 {
				buf = buf[:n]
			}
			data = append(data, string(buf))
		}
	}
	return data, nil
}

func decodeUtf32(src []byte, order binary.ByteOrder) (string, error) {
	if len(src)%4 != 0 {
		return "", merr.WrapErrImportFailed(fmt.Sprintf("invalid utf32 bytes length %d, the byte array length should be multiple of 4", len(src)))
	}

	var str string
	for len(src) > 0 {
		// check the high bytes, if high bytes are 0, the UNICODE is less than U+FFFF, we can use unicode.UTF16 to decode
		isUtf16 := false
		var lowbytesPosition int
		uOrder := unicode.LittleEndian
		if order == binary.LittleEndian {
			if src[2] == 0 && src[3] == 0 {
				isUtf16 = true
			}
			lowbytesPosition = 0
		} else {
			if src[0] == 0 && src[1] == 0 {
				isUtf16 = true
			}
			lowbytesPosition = 2
			uOrder = unicode.BigEndian
		}

		if isUtf16 {
			// use unicode.UTF16 to decode the low bytes to utf8
			// utf32 and utf16 is same if the unicode code is less than 65535
			if src[lowbytesPosition] != 0 || src[lowbytesPosition+1] != 0 {
				decoder := unicode.UTF16(uOrder, unicode.IgnoreBOM).NewDecoder()
				res, err := decoder.Bytes(src[lowbytesPosition : lowbytesPosition+2])
				if err != nil {
					return "", merr.WrapErrImportFailed(fmt.Sprintf("failed to decode utf32 binary bytes, error: %v", err))
				}
				str += string(res)
			}
		} else {
			// convert the 4 bytes to a unicode and encode to utf8
			// Golang strongly opposes utf32 coding, this kind of encoding has been excluded from standard lib
			var x uint32
			if order == binary.LittleEndian {
				x = uint32(src[3])<<24 | uint32(src[2])<<16 | uint32(src[1])<<8 | uint32(src[0])
			} else {
				x = uint32(src[0])<<24 | uint32(src[1])<<16 | uint32(src[2])<<8 | uint32(src[3])
			}
			r := rune(x)
			utf8Code := make([]byte, 4)
			utf8.EncodeRune(utf8Code, r)
			if r == utf8.RuneError {
				return "", merr.WrapErrImportFailed(fmt.Sprintf("failed to convert 4 bytes unicode %d to utf8 rune", x))
			}
			str += string(utf8Code)
		}

		src = src[4:]
	}
	return str, nil
}
