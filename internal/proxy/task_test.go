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

package proxy

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/testutils"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
	"github.com/milvus-io/milvus/pkg/v2/util/uniquegenerator"
)

// TODO(dragondriver): add more test cases

const (
	maxTestStringLen     = 100
	testBoolField        = "bool"
	testInt32Field       = "int32"
	testInt64Field       = "int64"
	testFloatField       = "float"
	testDoubleField      = "double"
	testVarCharField     = "varChar"
	testFloatVecField    = "fvec"
	testBinaryVecField   = "bvec"
	testFloat16VecField  = "f16vec"
	testBFloat16VecField = "bf16vec"
	testStructArrayField = "structArray"
	testVecDim           = 128
	testMaxVarCharLength = 100
)

func genCollectionSchema(collectionName string) *schemapb.CollectionSchema {
	return constructCollectionSchemaWithAllType(
		testBoolField,
		testInt32Field,
		testInt64Field,
		testFloatField,
		testDoubleField,
		testFloatVecField,
		testBinaryVecField,
		testFloat16VecField,
		testBFloat16VecField,
		testStructArrayField,
		testVecDim,
		collectionName)
}

func constructCollectionSchema(
	int64Field, floatVecField string,
	dim int,
	collectionName string,
) *schemapb.CollectionSchema {
	pk := &schemapb.FieldSchema{
		FieldID:      100,
		Name:         int64Field,
		IsPrimaryKey: true,
		Description:  "",
		DataType:     schemapb.DataType_Int64,
		TypeParams:   nil,
		IndexParams:  nil,
		AutoID:       true,
	}
	fVec := &schemapb.FieldSchema{
		FieldID:      101,
		Name:         floatVecField,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: strconv.Itoa(dim),
			},
		},
		IndexParams: nil,
		AutoID:      false,
	}
	return &schemapb.CollectionSchema{
		Name:        collectionName,
		Description: "",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			pk,
			fVec,
		},
		Properties: []*commonpb.KeyValuePair{
			{
				Key:   common.CollectionTTLConfigKey,
				Value: "15",
			},
		},
	}
}

func constructCollectionSchemaEnableDynamicSchema(
	int64Field, floatVecField string,
	dim int,
	collectionName string,
) *schemapb.CollectionSchema {
	pk := &schemapb.FieldSchema{
		FieldID:      0,
		Name:         int64Field,
		IsPrimaryKey: true,
		Description:  "",
		DataType:     schemapb.DataType_Int64,
		TypeParams:   nil,
		IndexParams:  nil,
		AutoID:       true,
	}
	fVec := &schemapb.FieldSchema{
		FieldID:      0,
		Name:         floatVecField,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: strconv.Itoa(dim),
			},
		},
		IndexParams: nil,
		AutoID:      false,
	}
	return &schemapb.CollectionSchema{
		Name:               collectionName,
		Description:        "",
		AutoID:             false,
		EnableDynamicField: true,
		Fields: []*schemapb.FieldSchema{
			pk,
			fVec,
		},
	}
}

func ConstructCollectionSchemaWithPartitionKey(collectionName string, fieldName2DataType map[string]schemapb.DataType, primaryFieldName string, partitionKeyFieldName string, autoID bool) *schemapb.CollectionSchema {
	schema := constructCollectionSchemaByDataType(collectionName, fieldName2DataType, primaryFieldName, autoID)
	for _, field := range schema.Fields {
		if field.Name == partitionKeyFieldName {
			field.IsPartitionKey = true
		}
	}

	return schema
}

func constructCollectionSchemaByDataType(collectionName string, fieldName2DataType map[string]schemapb.DataType, primaryFieldName string, autoID bool) *schemapb.CollectionSchema {
	fieldsSchema := make([]*schemapb.FieldSchema, 0)

	idx := int64(100)
	for fieldName, dataType := range fieldName2DataType {
		fieldSchema := &schemapb.FieldSchema{
			FieldID:  idx,
			Name:     fieldName,
			DataType: dataType,
		}
		idx++
		if typeutil.IsVectorType(dataType) {
			fieldSchema.TypeParams = []*commonpb.KeyValuePair{
				{
					Key:   common.DimKey,
					Value: strconv.Itoa(testVecDim),
				},
			}
		}
		if dataType == schemapb.DataType_VarChar {
			fieldSchema.TypeParams = []*commonpb.KeyValuePair{
				{
					Key:   common.MaxLengthKey,
					Value: strconv.Itoa(testMaxVarCharLength),
				},
			}
		}
		if fieldName == primaryFieldName {
			fieldSchema.IsPrimaryKey = true
			fieldSchema.AutoID = autoID
		}

		fieldsSchema = append(fieldsSchema, fieldSchema)
	}

	return &schemapb.CollectionSchema{
		Name:   collectionName,
		Fields: fieldsSchema,
	}
}

func constructCollectionSchemaWithAllType(
	boolField, int32Field, int64Field, floatField, doubleField string,
	floatVecField, binaryVecField, float16VecField, bfloat16VecField, structArrayField string,
	dim int,
	collectionName string,
) *schemapb.CollectionSchema {
	b := &schemapb.FieldSchema{
		FieldID:      0,
		Name:         boolField,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_Bool,
		TypeParams:   nil,
		IndexParams:  nil,
		AutoID:       false,
	}
	i32 := &schemapb.FieldSchema{
		FieldID:      0,
		Name:         int32Field,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_Int32,
		TypeParams:   nil,
		IndexParams:  nil,
		AutoID:       false,
	}
	i64 := &schemapb.FieldSchema{
		FieldID:      0,
		Name:         int64Field,
		IsPrimaryKey: true,
		Description:  "",
		DataType:     schemapb.DataType_Int64,
		TypeParams:   nil,
		IndexParams:  nil,
		AutoID:       false,
	}
	f := &schemapb.FieldSchema{
		FieldID:      0,
		Name:         floatField,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_Float,
		TypeParams:   nil,
		IndexParams:  nil,
		AutoID:       false,
	}
	d := &schemapb.FieldSchema{
		FieldID:      0,
		Name:         doubleField,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_Double,
		TypeParams:   nil,
		IndexParams:  nil,
		AutoID:       false,
	}
	fVec := &schemapb.FieldSchema{
		FieldID:      0,
		Name:         floatVecField,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: strconv.Itoa(dim),
			},
		},
		IndexParams: nil,
		AutoID:      false,
	}
	bVec := &schemapb.FieldSchema{
		FieldID:      0,
		Name:         binaryVecField,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_BinaryVector,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: strconv.Itoa(dim),
			},
		},
		IndexParams: nil,
		AutoID:      false,
	}
	f16Vec := &schemapb.FieldSchema{
		FieldID:      0,
		Name:         float16VecField,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_Float16Vector,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: strconv.Itoa(dim),
			},
		},
		IndexParams: nil,
		AutoID:      false,
	}
	bf16Vec := &schemapb.FieldSchema{
		FieldID:      0,
		Name:         bfloat16VecField,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_BFloat16Vector,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: strconv.Itoa(dim),
			},
		},
		IndexParams: nil,
		AutoID:      false,
	}

	// StructArrayField schema for testing
	structArrayFields := []*schemapb.StructArrayFieldSchema{
		{
			FieldID:     0,
			Name:        structArrayField,
			Description: "test struct array field",
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:     0,
					Name:        "sub_varchar_array",
					DataType:    schemapb.DataType_Array,
					ElementType: schemapb.DataType_VarChar,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.MaxLengthKey,
							Value: strconv.Itoa(testMaxVarCharLength),
						},
						{
							Key:   common.MaxCapacityKey,
							Value: "100",
						},
					},
				},
				{
					FieldID:     0,
					Name:        "sub_vector_array",
					DataType:    schemapb.DataType_ArrayOfVector,
					ElementType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: strconv.Itoa(dim),
						},
						{
							Key:   common.MaxCapacityKey,
							Value: "10",
						},
					},
				},
			},
		},
	}

	schema := &schemapb.CollectionSchema{
		Name:              collectionName,
		Description:       "",
		AutoID:            false,
		StructArrayFields: structArrayFields,
	}

	if enableMultipleVectorFields {
		schema.Fields = []*schemapb.FieldSchema{
			b,
			i32,
			i64,
			f,
			d,
			fVec,
			bVec,
			f16Vec,
			bf16Vec,
		}
	} else {
		schema.Fields = []*schemapb.FieldSchema{
			b,
			i32,
			i64,
			f,
			d,
			fVec,
			// bVec,
		}
	}

	return schema
}

func constructPlaceholderGroup(
	nq, dim int,
) *commonpb.PlaceholderGroup {
	values := make([][]byte, 0, nq)
	for i := 0; i < nq; i++ {
		bs := make([]byte, 0, dim*4)
		for j := 0; j < dim; j++ {
			var buffer bytes.Buffer
			f := rand.Float32()
			err := binary.Write(&buffer, common.Endian, f)
			if err != nil {
				panic(err)
			}
			bs = append(bs, buffer.Bytes()...)
		}
		values = append(values, bs)
	}

	return &commonpb.PlaceholderGroup{
		Placeholders: []*commonpb.PlaceholderValue{
			{
				Tag:    "$0",
				Type:   commonpb.PlaceholderType_FloatVector,
				Values: values,
			},
		},
	}
}

func constructSearchRequest(
	dbName, collectionName string,
	expr string,
	floatVecField string,
	nq, dim, nprobe, topk, roundDecimal int,
) *milvuspb.SearchRequest {
	params := make(map[string]string)
	params["nprobe"] = strconv.Itoa(nprobe)
	b, err := json.Marshal(params)
	if err != nil {
		panic(err)
	}
	plg := constructPlaceholderGroup(nq, dim)
	plgBs, err := proto.Marshal(plg)
	if err != nil {
		panic(err)
	}

	return &milvuspb.SearchRequest{
		Base:             nil,
		DbName:           dbName,
		CollectionName:   collectionName,
		PartitionNames:   nil,
		Dsl:              expr,
		PlaceholderGroup: plgBs,
		DslType:          commonpb.DslType_BoolExprV1,
		OutputFields:     nil,
		SearchParams: []*commonpb.KeyValuePair{
			{
				Key:   common.MetricTypeKey,
				Value: metric.L2,
			},
			{
				Key:   ParamsKey,
				Value: string(b),
			},
			{
				Key:   AnnsFieldKey,
				Value: floatVecField,
			},
			{
				Key:   TopKKey,
				Value: strconv.Itoa(topk),
			},
			{
				Key:   RoundDecimalKey,
				Value: strconv.Itoa(roundDecimal),
			},
		},
		TravelTimestamp:    0,
		GuaranteeTimestamp: 0,
	}
}

func TestTranslateOutputFields(t *testing.T) {
	const (
		idFieldName                = "id"
		tsFieldName                = "timestamp"
		floatVectorFieldName       = "float_vector"
		binaryVectorFieldName      = "binary_vector"
		float16VectorFieldName     = "float16_vector"
		bfloat16VectorFieldName    = "bfloat16_vector"
		sparseFloatVectorFieldName = "sparse_float_vector"
	)
	var outputFields []string
	var userOutputFields []string
	var userDynamicFields []string
	var requestedPK bool
	var err error

	collSchema := &schemapb.CollectionSchema{
		Name:        "TestTranslateOutputFields",
		Description: "TestTranslateOutputFields",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{Name: idFieldName, FieldID: 0, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{Name: tsFieldName, FieldID: 1, DataType: schemapb.DataType_Int64},
			{Name: floatVectorFieldName, FieldID: 100, DataType: schemapb.DataType_FloatVector},
			{Name: binaryVectorFieldName, FieldID: 101, DataType: schemapb.DataType_BinaryVector},
			{Name: float16VectorFieldName, FieldID: 102, DataType: schemapb.DataType_Float16Vector},
			{Name: bfloat16VectorFieldName, FieldID: 103, DataType: schemapb.DataType_BFloat16Vector},
			{Name: sparseFloatVectorFieldName, FieldID: 104, DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
		},
		Functions: []*schemapb.FunctionSchema{
			{
				Name:             "bm25",
				Type:             schemapb.FunctionType_BM25,
				OutputFieldNames: []string{sparseFloatVectorFieldName},
				// omit other fields for brevity
			},
		},
	}
	schema := newSchemaInfo(collSchema)

	// Test empty output fields
	outputFields, userOutputFields, userDynamicFields, requestedPK, err = translateOutputFields([]string{}, schema, false)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{}, outputFields)
	assert.ElementsMatch(t, []string{}, userOutputFields)
	assert.ElementsMatch(t, []string{}, userDynamicFields)
	assert.False(t, requestedPK)

	// Test single field
	outputFields, userOutputFields, userDynamicFields, requestedPK, err = translateOutputFields([]string{idFieldName}, schema, false)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{idFieldName}, outputFields)
	assert.ElementsMatch(t, []string{idFieldName}, userOutputFields)
	assert.ElementsMatch(t, []string{}, userDynamicFields)
	assert.True(t, requestedPK)

	// Test multiple fields
	outputFields, userOutputFields, userDynamicFields, requestedPK, err = translateOutputFields([]string{idFieldName, tsFieldName}, schema, false)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName}, outputFields)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName}, userOutputFields)
	assert.ElementsMatch(t, []string{}, userDynamicFields)
	assert.True(t, requestedPK)

	// Test with vector field
	outputFields, userOutputFields, userDynamicFields, requestedPK, err = translateOutputFields([]string{idFieldName, tsFieldName, floatVectorFieldName}, schema, false)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName}, outputFields)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName}, userOutputFields)
	assert.ElementsMatch(t, []string{}, userDynamicFields)
	assert.True(t, requestedPK)

	// Test without id field
	outputFields, userOutputFields, userDynamicFields, requestedPK, err = translateOutputFields([]string{tsFieldName, floatVectorFieldName}, schema, false)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{tsFieldName, floatVectorFieldName}, outputFields)
	assert.ElementsMatch(t, []string{tsFieldName, floatVectorFieldName}, userOutputFields)
	assert.ElementsMatch(t, []string{}, userDynamicFields)
	assert.False(t, requestedPK)

	// Test wildcard - should not include function output fields
	outputFields, userOutputFields, userDynamicFields, requestedPK, err = translateOutputFields([]string{"*"}, schema, false)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName, binaryVectorFieldName, float16VectorFieldName, bfloat16VectorFieldName}, outputFields)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName, binaryVectorFieldName, float16VectorFieldName, bfloat16VectorFieldName}, userOutputFields)
	assert.ElementsMatch(t, []string{}, userDynamicFields)
	assert.True(t, requestedPK)

	// Test wildcard with spaces
	outputFields, userOutputFields, userDynamicFields, requestedPK, err = translateOutputFields([]string{" * "}, schema, false)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName, binaryVectorFieldName, float16VectorFieldName, bfloat16VectorFieldName}, outputFields)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName, binaryVectorFieldName, float16VectorFieldName, bfloat16VectorFieldName}, userOutputFields)
	assert.ElementsMatch(t, []string{}, userDynamicFields)
	assert.True(t, requestedPK)

	// Test function output field - should error
	_, _, _, _, err = translateOutputFields([]string{"*", sparseFloatVectorFieldName}, schema, false)
	assert.Error(t, err)
	_, _, _, _, err = translateOutputFields([]string{sparseFloatVectorFieldName}, schema, false)
	assert.Error(t, err)
	_, _, _, _, err = translateOutputFields([]string{sparseFloatVectorFieldName}, schema, true)
	assert.Error(t, err)

	// Test with removePkField=true
	outputFields, userOutputFields, userDynamicFields, requestedPK, err = translateOutputFields([]string{}, schema, true)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{}, outputFields)
	assert.ElementsMatch(t, []string{}, userOutputFields)
	assert.ElementsMatch(t, []string{}, userDynamicFields)
	assert.False(t, requestedPK)

	// if removePkField is true, pk field should be removed from output fields

	outputFields, userOutputFields, userDynamicFields, requestedPK, err = translateOutputFields([]string{"*"}, schema, true)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{tsFieldName, floatVectorFieldName, binaryVectorFieldName, float16VectorFieldName, bfloat16VectorFieldName}, outputFields)
	assert.ElementsMatch(t, []string{tsFieldName, floatVectorFieldName, binaryVectorFieldName, float16VectorFieldName, bfloat16VectorFieldName}, userOutputFields)
	assert.ElementsMatch(t, []string{}, userDynamicFields)
	assert.True(t, requestedPK)

	outputFields, userOutputFields, userDynamicFields, requestedPK, err = translateOutputFields([]string{idFieldName, tsFieldName}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{tsFieldName}, outputFields)
	assert.ElementsMatch(t, []string{tsFieldName}, userOutputFields)
	assert.ElementsMatch(t, []string{}, userDynamicFields)
	assert.True(t, requestedPK)

	outputFields, userOutputFields, userDynamicFields, requestedPK, err = translateOutputFields([]string{idFieldName, tsFieldName, floatVectorFieldName}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{tsFieldName, floatVectorFieldName}, outputFields)
	assert.ElementsMatch(t, []string{tsFieldName, floatVectorFieldName}, userOutputFields)
	assert.ElementsMatch(t, []string{}, userDynamicFields)
	assert.True(t, requestedPK)

	outputFields, userOutputFields, userDynamicFields, requestedPK, err = translateOutputFields([]string{"*"}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{tsFieldName, floatVectorFieldName, binaryVectorFieldName, float16VectorFieldName, bfloat16VectorFieldName}, outputFields)
	assert.ElementsMatch(t, []string{tsFieldName, floatVectorFieldName, binaryVectorFieldName, float16VectorFieldName, bfloat16VectorFieldName}, userOutputFields)
	assert.ElementsMatch(t, []string{}, userDynamicFields)
	assert.True(t, requestedPK)

	outputFields, userOutputFields, userDynamicFields, requestedPK, err = translateOutputFields([]string{"*", tsFieldName}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{tsFieldName, floatVectorFieldName, binaryVectorFieldName, float16VectorFieldName, bfloat16VectorFieldName}, outputFields)
	assert.ElementsMatch(t, []string{tsFieldName, floatVectorFieldName, binaryVectorFieldName, float16VectorFieldName, bfloat16VectorFieldName}, userOutputFields)
	assert.ElementsMatch(t, []string{}, userDynamicFields)
	assert.True(t, requestedPK)

	outputFields, userOutputFields, userDynamicFields, requestedPK, err = translateOutputFields([]string{"*", floatVectorFieldName}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{tsFieldName, floatVectorFieldName, binaryVectorFieldName, float16VectorFieldName, bfloat16VectorFieldName}, outputFields)
	assert.ElementsMatch(t, []string{tsFieldName, floatVectorFieldName, binaryVectorFieldName, float16VectorFieldName, bfloat16VectorFieldName}, userOutputFields)
	assert.ElementsMatch(t, []string{}, userDynamicFields)
	assert.True(t, requestedPK)

	// Test non-existent field, dynamic field not enabled
	_, _, _, _, err = translateOutputFields([]string{"A"}, schema, true)
	assert.Error(t, err)

	t.Run("enable dynamic schema", func(t *testing.T) {
		collSchema := &schemapb.CollectionSchema{
			Name:               "TestTranslateOutputFields",
			Description:        "TestTranslateOutputFields",
			AutoID:             false,
			EnableDynamicField: true,
			Fields: []*schemapb.FieldSchema{
				{Name: idFieldName, FieldID: 1, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{Name: tsFieldName, FieldID: 2, DataType: schemapb.DataType_Int64},
				{Name: floatVectorFieldName, FieldID: 100, DataType: schemapb.DataType_FloatVector},
				{Name: binaryVectorFieldName, FieldID: 101, DataType: schemapb.DataType_BinaryVector},
				{Name: common.MetaFieldName, FieldID: 102, DataType: schemapb.DataType_JSON, IsDynamic: true},
			},
		}
		schema := newSchemaInfo(collSchema)

		outputFields, userOutputFields, userDynamicFields, requestedPK, err = translateOutputFields([]string{"A", idFieldName}, schema, true)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{common.MetaFieldName}, outputFields)
		assert.ElementsMatch(t, []string{"A"}, userOutputFields)
		assert.ElementsMatch(t, []string{"A"}, userDynamicFields)
		assert.True(t, requestedPK)

		outputFields, userOutputFields, userDynamicFields, requestedPK, err = translateOutputFields([]string{"$meta[\"A\"]", idFieldName}, schema, true)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{common.MetaFieldName}, outputFields)
		assert.ElementsMatch(t, []string{"$meta[\"A\"]"}, userOutputFields)
		assert.ElementsMatch(t, []string{"A"}, userDynamicFields)
		assert.True(t, requestedPK)

		// Test invalid dynamic field expressions
		_, _, _, _, err = translateOutputFields([]string{idFieldName, floatVectorFieldName, `$meta["A"]["B"]`}, schema, true)
		assert.Error(t, err)

		_, _, _, _, err = translateOutputFields([]string{idFieldName, floatVectorFieldName, "$meta[\"\"]"}, schema, true)
		assert.Error(t, err)

		_, _, _, _, err = translateOutputFields([]string{idFieldName, floatVectorFieldName, "$meta[]"}, schema, true)
		assert.Error(t, err)

		_, _, _, _, err = translateOutputFields([]string{idFieldName, floatVectorFieldName, "$meta["}, schema, true)
		assert.Error(t, err)

		_, _, _, _, err = translateOutputFields([]string{idFieldName, floatVectorFieldName, "[]"}, schema, true)
		assert.Error(t, err)

		_, _, _, _, err = translateOutputFields([]string{idFieldName, floatVectorFieldName, "A > 1"}, schema, true)
		assert.Error(t, err)

		_, _, _, _, err = translateOutputFields([]string{idFieldName, floatVectorFieldName, ""}, schema, true)
		assert.Error(t, err)
	})
}

func TestAddFieldTask(t *testing.T) {
	rc := NewMixCoordMock()
	ctx := context.Background()
	prefix := "TestAddFieldTask"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	collectionID := int64(1)
	int64Field := "int64"
	floatVecField := "fvec"
	varCharField := "varChar"

	rc.collName2ID[collectionName] = collectionID
	rc.collID2Meta[collectionID] = collectionMeta{
		name: collectionName,
		id:   collectionID,
		schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, DataType: schemapb.DataType_Int64, AutoID: true, Name: "ID"},
				{
					FieldID: 101, DataType: schemapb.DataType_FloatVector, Name: "vector",
					TypeParams: []*commonpb.KeyValuePair{
						{Key: "dim", Value: "128"},
					},
				},
			},
		},
	}
	fieldName2Type := make(map[string]schemapb.DataType)
	fieldName2Type[int64Field] = schemapb.DataType_Int64
	fieldName2Type[varCharField] = schemapb.DataType_VarChar
	fieldName2Type[floatVecField] = schemapb.DataType_FloatVector
	schema := constructCollectionSchemaByDataType(collectionName, fieldName2Type, int64Field, false)

	fSchema := &schemapb.FieldSchema{
		Name:     "add",
		DataType: schemapb.DataType_Bool,
		Nullable: true,
	}
	bytes, err := proto.Marshal(fSchema)
	assert.NoError(t, err)
	task := &addCollectionFieldTask{
		Condition: NewTaskCondition(ctx),
		AddCollectionFieldRequest: &milvuspb.AddCollectionFieldRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			Schema:         bytes,
		},
		ctx:       ctx,
		mixCoord:  rc,
		result:    nil,
		oldSchema: schema,
	}

	t.Run("on enqueue", func(t *testing.T) {
		err := task.OnEnqueue()
		assert.NoError(t, err)
		assert.Equal(t, commonpb.MsgType_AddCollectionField, task.Type())
	})

	t.Run("ctx", func(t *testing.T) {
		traceCtx := task.TraceCtx()
		assert.NotNil(t, traceCtx)
	})

	t.Run("id", func(t *testing.T) {
		id := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
		task.SetID(id)
		assert.Equal(t, id, task.ID())
	})

	t.Run("name", func(t *testing.T) {
		assert.Equal(t, AddFieldTaskName, task.Name())
	})

	t.Run("ts", func(t *testing.T) {
		ts := Timestamp(time.Now().UnixNano())
		task.SetTs(ts)
		assert.Equal(t, ts, task.BeginTs())
		assert.Equal(t, ts, task.EndTs())
	})

	t.Run("process task", func(t *testing.T) {
		var err error
		// nil collection schema
		task.oldSchema = nil
		err = task.PreExecute(ctx)
		assert.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)

		bytes, err := proto.Marshal(fSchema)
		assert.NoError(t, err)
		task.Schema = bytes

		task.oldSchema = schema

		err = task.PreExecute(ctx)
		assert.NoError(t, err)

		err = task.Execute(ctx)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, task.result.ErrorCode)

		err = task.PostExecute(ctx)
		assert.NoError(t, err)
	})

	t.Run("PreExecute", func(t *testing.T) {
		var err error

		err = task.PreExecute(ctx)
		assert.NoError(t, err)

		// nil schema
		task.Schema = nil
		err = task.PreExecute(ctx)
		assert.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)

		// too many fields
		Params.Save(Params.ProxyCfg.MaxFieldNum.Key, fmt.Sprint(task.oldSchema.Fields))
		fSchema := &schemapb.FieldSchema{
			Name: "add_field",
		}
		bytes, err := proto.Marshal(fSchema)
		assert.NoError(t, err)
		task.Schema = bytes
		err = task.PreExecute(ctx)
		assert.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		Params.Reset(Params.ProxyCfg.MaxFieldNum.Key)

		// invalid field type
		fSchema = &schemapb.FieldSchema{
			DataType: schemapb.DataType_None,
		}
		bytes, err = proto.Marshal(fSchema)
		assert.NoError(t, err)
		task.Schema = bytes
		err = task.PreExecute(ctx)
		assert.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)

		// not support vector field
		fSchema = &schemapb.FieldSchema{
			DataType: schemapb.DataType_FloatVector,
		}
		bytes, err = proto.Marshal(fSchema)
		assert.NoError(t, err)
		task.Schema = bytes
		err = task.PreExecute(ctx)
		assert.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)

		// not support system field
		fSchema = &schemapb.FieldSchema{
			Name: common.TimeStampFieldName,
		}
		bytes, err = proto.Marshal(fSchema)
		assert.NoError(t, err)
		task.Schema = bytes
		err = task.PreExecute(ctx)
		assert.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)

		// must be nullable
		fSchema = &schemapb.FieldSchema{
			Nullable: false,
		}
		bytes, err = proto.Marshal(fSchema)
		assert.NoError(t, err)
		task.Schema = bytes
		err = task.PreExecute(ctx)
		assert.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)

		// not support pk field
		fSchema = &schemapb.FieldSchema{
			IsPrimaryKey: true,
		}
		bytes, err = proto.Marshal(fSchema)
		assert.NoError(t, err)
		task.Schema = bytes
		err = task.PreExecute(ctx)
		assert.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)

		// not support add partition key
		fSchema = &schemapb.FieldSchema{
			IsPartitionKey: true,
			DataType:       schemapb.DataType_Bool,
			Nullable:       true,
			Name:           "new field",
		}
		bytes, err = proto.Marshal(fSchema)
		assert.NoError(t, err)
		task.Schema = bytes
		err = task.PreExecute(ctx)
		assert.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)

		Params.Save(Params.ProxyCfg.MustUsePartitionKey.Key, "true")
		err = task.PreExecute(ctx)
		assert.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		Params.Reset(Params.ProxyCfg.MustUsePartitionKey.Key)

		// not support autoID
		fSchema = &schemapb.FieldSchema{
			AutoID: true,
		}
		bytes, err = proto.Marshal(fSchema)
		assert.NoError(t, err)
		task.Schema = bytes
		err = task.PreExecute(ctx)
		assert.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)

		// more ClusteringKey field
		fSchema = &schemapb.FieldSchema{
			IsClusteringKey: true,
		}
		bytes, err = proto.Marshal(fSchema)
		assert.NoError(t, err)
		task.Schema = bytes
		task.oldSchema = schema
		task.oldSchema.Fields = append(task.oldSchema.Fields, &schemapb.FieldSchema{
			IsClusteringKey: true,
		})
		err = task.PreExecute(ctx)
		assert.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)

		// fieldName invalid
		fSchema = &schemapb.FieldSchema{
			Name: "",
		}
		bytes, err = proto.Marshal(fSchema)
		assert.NoError(t, err)
		task.Schema = bytes
		err = task.PreExecute(ctx)
		assert.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)

		// duplicated FieldName
		fSchema = &schemapb.FieldSchema{
			Name: varCharField,
		}
		bytes, err = proto.Marshal(fSchema)
		assert.NoError(t, err)
		task.Schema = bytes
		err = task.PreExecute(ctx)
		assert.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})
}

func TestCreateCollectionTask(t *testing.T) {
	mix := NewMixCoordMock()
	ctx := context.Background()
	shardsNum := common.DefaultShardsNum
	prefix := "TestCreateCollectionTask"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	int64Field := "int64"
	floatVecField := "fvec"
	varCharField := "varChar"

	fieldName2Type := make(map[string]schemapb.DataType)
	fieldName2Type[int64Field] = schemapb.DataType_Int64
	fieldName2Type[varCharField] = schemapb.DataType_VarChar
	fieldName2Type[floatVecField] = schemapb.DataType_FloatVector
	schema := constructCollectionSchemaByDataType(collectionName, fieldName2Type, int64Field, false)
	var marshaledSchema []byte
	marshaledSchema, err := proto.Marshal(schema)
	assert.NoError(t, err)

	task := &createCollectionTask{
		Condition: NewTaskCondition(ctx),
		CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			Schema:         marshaledSchema,
			ShardsNum:      shardsNum,
		},
		ctx:      ctx,
		mixCoord: mix,
		result:   nil,
		schema:   nil,
	}

	t.Run("on enqueue", func(t *testing.T) {
		err := task.OnEnqueue()
		assert.NoError(t, err)
		assert.Equal(t, commonpb.MsgType_CreateCollection, task.Type())
	})

	t.Run("ctx", func(t *testing.T) {
		traceCtx := task.TraceCtx()
		assert.NotNil(t, traceCtx)
	})

	t.Run("id", func(t *testing.T) {
		id := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
		task.SetID(id)
		assert.Equal(t, id, task.ID())
	})

	t.Run("name", func(t *testing.T) {
		assert.Equal(t, CreateCollectionTaskName, task.Name())
	})

	t.Run("ts", func(t *testing.T) {
		ts := Timestamp(time.Now().UnixNano())
		task.SetTs(ts)
		assert.Equal(t, ts, task.BeginTs())
		assert.Equal(t, ts, task.EndTs())
	})

	t.Run("process task", func(t *testing.T) {
		var err error

		err = task.PreExecute(ctx)
		assert.NoError(t, err)

		err = task.Execute(ctx)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, task.result.ErrorCode)

		// recreate -> fail
		err = task.Execute(ctx)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, task.result.ErrorCode)

		err = task.PostExecute(ctx)
		assert.NoError(t, err)
	})

	t.Run("PreExecute", func(t *testing.T) {
		var err error

		err = task.PreExecute(ctx)
		assert.NoError(t, err)

		Params.Save(Params.ProxyCfg.MustUsePartitionKey.Key, "true")
		err = task.PreExecute(ctx)
		assert.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		Params.Reset(Params.ProxyCfg.MustUsePartitionKey.Key)

		task.Schema = []byte{0x1, 0x2, 0x3, 0x4}
		err = task.PreExecute(ctx)
		assert.Error(t, err)
		task.Schema = marshaledSchema

		task.ShardsNum = Params.ProxyCfg.MaxShardNum.GetAsInt32() + 1
		err = task.PreExecute(ctx)
		assert.Error(t, err)
		task.ShardsNum = shardsNum

		reqBackup := proto.Clone(task.CreateCollectionRequest).(*milvuspb.CreateCollectionRequest)
		schemaBackup := proto.Clone(schema).(*schemapb.CollectionSchema)

		schemaWithTooManyFields := &schemapb.CollectionSchema{
			Name:        collectionName,
			Description: "",
			AutoID:      false,
			Fields:      make([]*schemapb.FieldSchema, Params.ProxyCfg.MaxFieldNum.GetAsInt32()+1),
		}
		marshaledSchemaWithTooManyFields, err := proto.Marshal(schemaWithTooManyFields)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = marshaledSchemaWithTooManyFields
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		// too many vector fields
		schema = proto.Clone(schemaBackup).(*schemapb.CollectionSchema)
		schema.Fields = append(schema.Fields, schema.Fields[0])
		for i := 0; i < Params.ProxyCfg.MaxVectorFieldNum.GetAsInt(); i++ {
			schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
				FieldID:      101,
				Name:         floatVecField + "_" + strconv.Itoa(i),
				IsPrimaryKey: false,
				Description:  "",
				DataType:     schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: strconv.Itoa(testVecDim),
					},
				},
				IndexParams: nil,
				AutoID:      false,
			})
		}
		tooManyVectorFieldsSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = tooManyVectorFieldsSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		// without vector field
		schema = &schemapb.CollectionSchema{
			Name:        collectionName,
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					Name:         "id",
					DataType:     schemapb.DataType_Int64,
					IsPrimaryKey: true,
				},
			},
		}
		noVectorSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = noVectorSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		task.CreateCollectionRequest = reqBackup

		// validateCollectionName
		schema = proto.Clone(schemaBackup).(*schemapb.CollectionSchema)
		schema.Name = " " // empty
		emptyNameSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = emptyNameSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		schema.Name = prefix
		for i := 0; i < Params.ProxyCfg.MaxNameLength.GetAsInt(); i++ {
			schema.Name += strconv.Itoa(i % 10)
		}
		tooLongNameSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = tooLongNameSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		schema.Name = "$" // invalid first char
		invalidFirstCharSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = invalidFirstCharSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		// validateDuplicatedFieldName
		schema = proto.Clone(schemaBackup).(*schemapb.CollectionSchema)
		schema.Fields = append(schema.Fields, schema.Fields[0])
		duplicatedFieldsSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = duplicatedFieldsSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		// validatePrimaryKey
		schema = proto.Clone(schemaBackup).(*schemapb.CollectionSchema)
		for idx := range schema.Fields {
			schema.Fields[idx].IsPrimaryKey = false
		}
		noPrimaryFieldsSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = noPrimaryFieldsSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		// validateFieldName
		schema = proto.Clone(schemaBackup).(*schemapb.CollectionSchema)
		for idx := range schema.Fields {
			schema.Fields[idx].Name = "$"
		}
		invalidFieldNameSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = invalidFieldNameSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		// validateMaxLengthPerRow
		schema = proto.Clone(schemaBackup).(*schemapb.CollectionSchema)
		for idx := range schema.Fields {
			if schema.Fields[idx].DataType == schemapb.DataType_VarChar {
				schema.Fields[idx].TypeParams = nil
			}
		}
		noTypeParamsSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = noTypeParamsSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		// ValidateVectorField
		schema = proto.Clone(schemaBackup).(*schemapb.CollectionSchema)
		for idx := range schema.Fields {
			if schema.Fields[idx].DataType == schemapb.DataType_FloatVector ||
				schema.Fields[idx].DataType == schemapb.DataType_BinaryVector {
				schema.Fields[idx].TypeParams = nil
			}
		}
		noDimSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = noDimSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		// ValidateVectorField
		schema = proto.Clone(schemaBackup).(*schemapb.CollectionSchema)
		for _, field := range schema.Fields {
			field.TypeParams = append(field.TypeParams, &commonpb.KeyValuePair{
				Key:   common.FieldSkipLoadKey,
				Value: "true",
			})
		}

		// Validate default load list
		skipLoadSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = skipLoadSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		schema = proto.Clone(schemaBackup).(*schemapb.CollectionSchema)
		for idx := range schema.Fields {
			if schema.Fields[idx].DataType == schemapb.DataType_FloatVector ||
				schema.Fields[idx].DataType == schemapb.DataType_BinaryVector {
				schema.Fields[idx].TypeParams = []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: "not int",
					},
				}
			}
		}
		dimNotIntSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = dimNotIntSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		schema = proto.Clone(schemaBackup).(*schemapb.CollectionSchema)
		for idx := range schema.Fields {
			if schema.Fields[idx].DataType == schemapb.DataType_FloatVector ||
				schema.Fields[idx].DataType == schemapb.DataType_BinaryVector {
				schema.Fields[idx].TypeParams = []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: strconv.Itoa(Params.ProxyCfg.MaxDimension.GetAsInt() + 1),
					},
				}
			}
		}
		tooLargeDimSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = tooLargeDimSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		schema = proto.Clone(schemaBackup).(*schemapb.CollectionSchema)
		schema.Fields[1].DataType = schemapb.DataType_BinaryVector
		schema.Fields[1].TypeParams = []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: strconv.Itoa(Params.ProxyCfg.MaxDimension.GetAsInt() + 1),
			},
		}
		binaryTooLargeDimSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = binaryTooLargeDimSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		schema = proto.Clone(schemaBackup).(*schemapb.CollectionSchema)
		lastFieldID := schema.Fields[len(schema.Fields)-1].FieldID
		schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
			FieldID:      lastFieldID + 1,
			Name:         "second_vector",
			IsPrimaryKey: false,
			Description:  "",
			DataType:     schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   common.DimKey,
					Value: strconv.Itoa(128),
				},
			},
			IndexParams: nil,
			AutoID:      false,
		})
		twoVecFieldsSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = twoVecFieldsSchema
		err = task.PreExecute(ctx)
		if enableMultipleVectorFields {
			assert.NoError(t, err)
		} else {
			assert.Error(t, err)
		}

		task.CreateCollectionRequest = reqBackup

		// Test StructArrayField validation
		structArrayFieldSchema := constructCollectionSchemaWithStructArrayField(collectionName+"_struct", testStructArrayField, false)

		// Test valid StructArrayField
		validStructSchema, err := proto.Marshal(structArrayFieldSchema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = validStructSchema
		err = task.PreExecute(ctx)
		assert.NoError(t, err)

		// Test invalid StructArrayField - empty fields
		invalidStructSchema := proto.Clone(structArrayFieldSchema).(*schemapb.CollectionSchema)
		invalidStructSchema.StructArrayFields[0].Fields = []*schemapb.FieldSchema{} // Empty fields
		invalidStructSchemaBytes, err := proto.Marshal(invalidStructSchema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = invalidStructSchemaBytes
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		// Test invalid StructArrayField - invalid field name
		invalidStructSchema2 := proto.Clone(structArrayFieldSchema).(*schemapb.CollectionSchema)
		invalidStructSchema2.StructArrayFields[0].Fields[0].Name = "$invalid" // Invalid field name
		invalidStructSchema2Bytes, err := proto.Marshal(invalidStructSchema2)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = invalidStructSchema2Bytes
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		// Test invalid StructArrayField - non-array/non-vector-array sub-field
		invalidStructSchema3 := proto.Clone(structArrayFieldSchema).(*schemapb.CollectionSchema)
		invalidStructSchema3.StructArrayFields[0].Fields[0].DataType = schemapb.DataType_Int64 // Should be Array or ArrayOfVector
		invalidStructSchema3Bytes, err := proto.Marshal(invalidStructSchema3)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = invalidStructSchema3Bytes
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		// Restore original schema for remaining tests
		task.CreateCollectionRequest = reqBackup
	})

	t.Run("specify dynamic field", func(t *testing.T) {
		dynamicField := &schemapb.FieldSchema{
			Name:      "json",
			IsDynamic: true,
		}
		var marshaledSchema []byte
		schema2 := &schemapb.CollectionSchema{
			Name:   collectionName,
			Fields: append(schema.Fields, dynamicField),
		}
		marshaledSchema, err := proto.Marshal(schema2)
		assert.NoError(t, err)

		task2 := &createCollectionTask{
			Condition: NewTaskCondition(ctx),
			CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
				Base:           nil,
				DbName:         dbName,
				CollectionName: collectionName,
				Schema:         marshaledSchema,
				ShardsNum:      shardsNum,
			},
			ctx:      ctx,
			mixCoord: mix,
			result:   nil,
			schema:   nil,
		}

		err = task2.OnEnqueue()
		assert.NoError(t, err)

		err = task2.PreExecute(ctx)
		assert.Error(t, err)
	})

	t.Run("collection with embedding function ", func(t *testing.T) {
		paramtable.Init()
		paramtable.Get().CredentialCfg.Credential.GetFunc = func() map[string]string {
			return map[string]string{
				"mock.apikey": "mock",
			}
		}
		ts := function.CreateOpenAIEmbeddingServer()
		defer ts.Close()
		paramtable.Get().FunctionCfg.TextEmbeddingProviders.GetFunc = func() map[string]string {
			return map[string]string{
				"openai.url": ts.URL,
			}
		}
		schema.Functions = []*schemapb.FunctionSchema{
			{
				Name:             "test",
				Type:             schemapb.FunctionType_TextEmbedding,
				InputFieldNames:  []string{varCharField},
				OutputFieldNames: []string{floatVecField},
				Params: []*commonpb.KeyValuePair{
					{Key: "provider", Value: "openai"},
					{Key: "model_name", Value: "text-embedding-ada-002"},
					{Key: "credential", Value: "mock"},
					{Key: "dim", Value: "128"},
				},
			},
		}

		marshaledSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)

		task2 := &createCollectionTask{
			Condition: NewTaskCondition(ctx),
			CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
				Base:           nil,
				DbName:         dbName,
				CollectionName: collectionName,
				Schema:         marshaledSchema,
				ShardsNum:      shardsNum,
			},
			ctx:      ctx,
			mixCoord: mix,
			result:   nil,
			schema:   nil,
		}

		err = task2.OnEnqueue()
		assert.NoError(t, err)

		err = task2.PreExecute(ctx)
		assert.NoError(t, err)
	})
}

func TestHasCollectionTask(t *testing.T) {
	mixc := NewMixCoordMock()
	defer mixc.Close()
	ctx := context.Background()
	mgr := newShardClientMgr()
	InitMetaCache(ctx, mixc, mgr)
	prefix := "TestHasCollectionTask"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()

	shardsNum := int32(2)
	int64Field := "int64"
	floatVecField := "fvec"
	dim := 128

	schema := constructCollectionSchema(int64Field, floatVecField, dim, collectionName)
	marshaledSchema, err := proto.Marshal(schema)
	assert.NoError(t, err)

	createColReq := &milvuspb.CreateCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_DropCollection,
			MsgID:     100,
			Timestamp: 100,
		},
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      shardsNum,
	}

	// CreateCollection
	task := &hasCollectionTask{
		Condition: NewTaskCondition(ctx),
		HasCollectionRequest: &milvuspb.HasCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_HasCollection,
				MsgID:     100,
				Timestamp: 100,
			},
			DbName:         dbName,
			CollectionName: collectionName,
		},
		ctx:      ctx,
		mixCoord: mixc,
		result:   nil,
	}
	task.OnEnqueue()
	task.PreExecute(ctx)

	assert.Equal(t, commonpb.MsgType_HasCollection, task.Type())
	assert.Equal(t, UniqueID(100), task.ID())
	assert.Equal(t, Timestamp(100), task.BeginTs())
	assert.Equal(t, Timestamp(100), task.EndTs())
	assert.Equal(t, paramtable.GetNodeID(), task.GetBase().GetSourceID())
	// missing collectionID in globalMetaCache
	err = task.Execute(ctx)
	assert.NoError(t, err)
	assert.Equal(t, false, task.result.Value)
	// createIsoCollection in RootCood and fill GlobalMetaCache
	mixc.CreateCollection(ctx, createColReq)
	globalMetaCache.GetCollectionID(ctx, GetCurDBNameFromContextOrDefault(ctx), collectionName)

	// success to drop collection
	err = task.Execute(ctx)
	assert.NoError(t, err)
	assert.Equal(t, true, task.result.Value)

	// illegal name
	task.CollectionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.Error(t, err)

	task.CollectionName = collectionName

	// invalidate collection cache, trigger rootcoord rpc
	globalMetaCache.RemoveCollection(ctx, dbName, collectionName)

	// rc return collection not found error
	mixc.describeCollectionFunc = func(ctx context.Context, request *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
		return nil, merr.WrapErrCollectionNotFoundWithDB(dbName, collectionName)
	}
	err = task.PreExecute(ctx)
	assert.NoError(t, err)
	err = task.Execute(ctx)
	assert.NoError(t, err)
	assert.False(t, task.result.GetValue())
	assert.NotNil(t, task.result.GetStatus())

	// rootcoord failed to get response
	mixc.updateState(commonpb.StateCode_Abnormal)
	err = task.PreExecute(ctx)
	assert.NoError(t, err)
	err = task.Execute(ctx)
	assert.Error(t, err)
}

func TestDescribeCollectionTask(t *testing.T) {
	mixc := NewMixCoordMock()
	defer mixc.Close()
	ctx := context.Background()
	mgr := newShardClientMgr()
	InitMetaCache(ctx, mixc, mgr)
	prefix := "TestDescribeCollectionTask"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()

	// CreateCollection
	task := &describeCollectionTask{
		Condition: NewTaskCondition(ctx),
		DescribeCollectionRequest: &milvuspb.DescribeCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DescribeCollection,
				MsgID:     100,
				Timestamp: 100,
			},
			DbName:         dbName,
			CollectionName: collectionName,
		},
		ctx:      ctx,
		mixCoord: mixc,
		result:   nil,
	}
	task.OnEnqueue()
	task.PreExecute(ctx)

	assert.Equal(t, commonpb.MsgType_DescribeCollection, task.Type())
	assert.Equal(t, UniqueID(100), task.ID())
	assert.Equal(t, Timestamp(100), task.BeginTs())
	assert.Equal(t, Timestamp(100), task.EndTs())
	assert.Equal(t, paramtable.GetNodeID(), task.GetBase().GetSourceID())
	// missing collectionID in globalMetaCache
	err := task.Execute(ctx)
	assert.NoError(t, err)

	// illegal name
	task.CollectionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.Error(t, err)

	// describe collection with id
	task.CollectionID = 1
	task.CollectionName = ""
	err = task.PreExecute(ctx)
	assert.NoError(t, err)

	task.CollectionID = 0
	task.CollectionName = collectionName
	err = task.PreExecute(ctx)
	assert.NoError(t, err)
	err = task.Execute(ctx)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, task.result.GetStatus().GetErrorCode())
}

func TestDescribeCollectionTask_ShardsNum1(t *testing.T) {
	mix := NewMixCoordMock()

	ctx := context.Background()
	mgr := newShardClientMgr()
	InitMetaCache(ctx, mix, mgr)
	prefix := "TestDescribeCollectionTask"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()

	shardsNum := common.DefaultShardsNum
	int64Field := "int64"
	floatVecField := "fvec"
	dim := 128

	schema := constructCollectionSchema(int64Field, floatVecField, dim, collectionName)
	marshaledSchema, err := proto.Marshal(schema)
	assert.NoError(t, err)

	createColReq := &milvuspb.CreateCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_DropCollection,
			MsgID:     100,
			Timestamp: 100,
		},
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      shardsNum,
	}

	mix.CreateCollection(ctx, createColReq)
	globalMetaCache.GetCollectionID(ctx, GetCurDBNameFromContextOrDefault(ctx), collectionName)

	// CreateCollection
	task := &describeCollectionTask{
		Condition: NewTaskCondition(ctx),
		DescribeCollectionRequest: &milvuspb.DescribeCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DescribeCollection,
				MsgID:     100,
				Timestamp: 100,
			},
			DbName:         dbName,
			CollectionName: collectionName,
		},
		ctx:      ctx,
		mixCoord: mix,
		result:   nil,
	}
	err = task.PreExecute(ctx)
	assert.NoError(t, err)

	err = task.Execute(ctx)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, task.result.GetStatus().GetErrorCode())
	assert.Equal(t, shardsNum, task.result.ShardsNum)
	assert.Equal(t, collectionName, task.result.GetCollectionName())
}

func TestDescribeCollectionTask_EnableDynamicSchema(t *testing.T) {
	mix := NewMixCoordMock()
	ctx := context.Background()
	mgr := newShardClientMgr()
	InitMetaCache(ctx, mix, mgr)
	prefix := "TestDescribeCollectionTask"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()

	shardsNum := common.DefaultShardsNum
	int64Field := "int64"
	floatVecField := "fvec"
	dim := 128

	schema := constructCollectionSchemaEnableDynamicSchema(int64Field, floatVecField, dim, collectionName)
	marshaledSchema, err := proto.Marshal(schema)
	assert.NoError(t, err)

	createColReq := &milvuspb.CreateCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_DropCollection,
			MsgID:     100,
			Timestamp: 100,
		},
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      shardsNum,
	}

	mix.CreateCollection(ctx, createColReq)
	globalMetaCache.GetCollectionID(ctx, dbName, collectionName)

	// CreateCollection
	task := &describeCollectionTask{
		Condition: NewTaskCondition(ctx),
		DescribeCollectionRequest: &milvuspb.DescribeCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DescribeCollection,
				MsgID:     100,
				Timestamp: 100,
			},
			DbName:         dbName,
			CollectionName: collectionName,
		},
		ctx:      ctx,
		mixCoord: mix,
		result:   nil,
	}
	err = task.PreExecute(ctx)
	assert.NoError(t, err)

	err = task.Execute(ctx)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, task.result.GetStatus().GetErrorCode())
	assert.Equal(t, shardsNum, task.result.ShardsNum)
	assert.Equal(t, collectionName, task.result.GetCollectionName())
	assert.Equal(t, 2, len(task.result.Schema.Fields))
}

func TestDescribeCollectionTask_ShardsNum2(t *testing.T) {
	mix := NewMixCoordMock()
	ctx := context.Background()
	mgr := newShardClientMgr()
	InitMetaCache(ctx, mix, mgr)
	prefix := "TestDescribeCollectionTask"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()

	int64Field := "int64"
	floatVecField := "fvec"
	dim := 128

	schema := constructCollectionSchema(int64Field, floatVecField, dim, collectionName)
	marshaledSchema, err := proto.Marshal(schema)
	assert.NoError(t, err)

	createColReq := &milvuspb.CreateCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_DropCollection,
			MsgID:     100,
			Timestamp: 100,
		},
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
	}

	mix.CreateCollection(ctx, createColReq)
	globalMetaCache.GetCollectionID(ctx, GetCurDBNameFromContextOrDefault(ctx), collectionName)

	// CreateCollection
	task := &describeCollectionTask{
		Condition: NewTaskCondition(ctx),
		DescribeCollectionRequest: &milvuspb.DescribeCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DescribeCollection,
				MsgID:     100,
				Timestamp: 100,
			},
			DbName:         dbName,
			CollectionName: collectionName,
		},
		ctx:      ctx,
		mixCoord: mix,
		result:   nil,
	}
	task.PreExecute(ctx)

	// missing collectionID in globalMetaCache
	err = task.Execute(ctx)
	assert.NoError(t, err)

	err = task.Execute(ctx)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, task.result.GetStatus().GetErrorCode())
	assert.Equal(t, common.DefaultShardsNum, task.result.ShardsNum)
	assert.Equal(t, collectionName, task.result.GetCollectionName())
}

func TestCreatePartitionTask(t *testing.T) {
	rc := mocks.NewMockMixCoordClient(t)

	mockCache := NewMockCache(t)
	mockCache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).Return(newSchemaInfo(&schemapb.CollectionSchema{
		EnableDynamicField: false,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "ID", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "Vector", DataType: schemapb.DataType_FloatVector},
		},
	}), nil)
	globalMetaCache = mockCache

	ctx := context.Background()
	prefix := "TestCreatePartitionTask"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	partitionName := prefix + funcutil.GenRandomStr()

	task := &createPartitionTask{
		Condition: NewTaskCondition(ctx),
		CreatePartitionRequest: &milvuspb.CreatePartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_CreatePartition,
				MsgID:     100,
				Timestamp: 100,
			},
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  partitionName,
		},
		ctx:      ctx,
		mixCoord: rc,
		result:   nil,
	}
	task.OnEnqueue()
	task.PreExecute(ctx)

	assert.Equal(t, commonpb.MsgType_CreatePartition, task.Type())
	assert.Equal(t, UniqueID(100), task.ID())
	assert.Equal(t, Timestamp(100), task.BeginTs())
	assert.Equal(t, Timestamp(100), task.EndTs())
	assert.Equal(t, paramtable.GetNodeID(), task.GetBase().GetSourceID())

	// setup global meta cache
	mockCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(100, nil).Once()
	mockCache.EXPECT().GetPartitionID(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(1000, nil).Once()
	rc.EXPECT().CreatePartition(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()
	rc.EXPECT().SyncNewCreatedPartition(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()
	err := task.Execute(ctx)
	assert.NoError(t, err)

	task.CollectionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.Error(t, err)

	task.CollectionName = collectionName
	task.PartitionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.Error(t, err)
}

func TestDropPartitionTask(t *testing.T) {
	ctx := context.Background()
	prefix := "TestDropPartitionTask"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	partitionName := prefix + funcutil.GenRandomStr()
	mixc := NewMixCoordMock()
	mockCache := NewMockCache(t)
	mockCache.On("GetCollectionID",
		mock.Anything, // context.Context
		mock.AnythingOfType("string"),
		mock.AnythingOfType("string"),
	).Return(UniqueID(1), nil)
	mockCache.On("GetPartitionID",
		mock.Anything, // context.Context
		mock.AnythingOfType("string"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("string"),
	).Return(UniqueID(1), nil)
	mockCache.On("GetCollectionSchema",
		mock.Anything, // context.Context
		mock.AnythingOfType("string"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("string"),
	).Return(newSchemaInfo(&schemapb.CollectionSchema{}), nil)
	globalMetaCache = mockCache

	task := &dropPartitionTask{
		Condition: NewTaskCondition(ctx),
		DropPartitionRequest: &milvuspb.DropPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DropPartition,
				MsgID:     100,
				Timestamp: 100,
			},
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  partitionName,
		},
		ctx:      ctx,
		mixCoord: mixc,
		result:   nil,
	}
	task.OnEnqueue()
	task.PreExecute(ctx)

	assert.Equal(t, commonpb.MsgType_DropPartition, task.Type())
	assert.Equal(t, UniqueID(100), task.ID())
	assert.Equal(t, Timestamp(100), task.BeginTs())
	assert.Equal(t, Timestamp(100), task.EndTs())
	assert.Equal(t, paramtable.GetNodeID(), task.GetBase().GetSourceID())
	err := task.Execute(ctx)
	assert.Error(t, err)

	task.CollectionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.Error(t, err)

	task.CollectionName = collectionName
	task.PartitionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.Error(t, err)

	t.Run("get collectionID error", func(t *testing.T) {
		mockCache := NewMockCache(t)
		mockCache.On("GetCollectionID",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(UniqueID(1), errors.New("error"))
		mockCache.On("GetCollectionSchema",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(newSchemaInfo(&schemapb.CollectionSchema{}), nil)
		globalMetaCache = mockCache
		task.PartitionName = "partition1"
		err = task.PreExecute(ctx)
		assert.Error(t, err)
	})

	t.Run("partition not exist", func(t *testing.T) {
		task.PartitionName = "partition2"

		mockCache := NewMockCache(t)
		mockCache.On("GetPartitionID",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(UniqueID(0), merr.WrapErrPartitionNotFound(partitionName))
		mockCache.On("GetCollectionID",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(UniqueID(1), nil)
		mockCache.On("GetCollectionSchema",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(newSchemaInfo(&schemapb.CollectionSchema{}), nil)
		globalMetaCache = mockCache
		err = task.PreExecute(ctx)
		assert.NoError(t, err)
	})

	t.Run("get partition error", func(t *testing.T) {
		task.PartitionName = "partition3"

		mockCache := NewMockCache(t)
		mockCache.On("GetPartitionID",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(UniqueID(0), errors.New("error"))
		mockCache.On("GetCollectionID",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(UniqueID(1), nil)
		mockCache.On("GetCollectionSchema",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(newSchemaInfo(&schemapb.CollectionSchema{}), nil)
		globalMetaCache = mockCache
		err = task.PreExecute(ctx)
		assert.Error(t, err)
	})
}

func TestHasPartitionTask(t *testing.T) {
	rc := NewMixCoordMock()

	defer rc.Close()
	ctx := context.Background()
	prefix := "TestHasPartitionTask"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	partitionName := prefix + funcutil.GenRandomStr()

	task := &hasPartitionTask{
		Condition: NewTaskCondition(ctx),
		HasPartitionRequest: &milvuspb.HasPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_HasPartition,
				MsgID:     100,
				Timestamp: 100,
			},
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  partitionName,
		},
		ctx:      ctx,
		mixCoord: rc,
		result:   nil,
	}
	task.OnEnqueue()
	task.PreExecute(ctx)

	assert.Equal(t, commonpb.MsgType_HasPartition, task.Type())
	assert.Equal(t, UniqueID(100), task.ID())
	assert.Equal(t, Timestamp(100), task.BeginTs())
	assert.Equal(t, Timestamp(100), task.EndTs())
	assert.Equal(t, paramtable.GetNodeID(), task.GetBase().GetSourceID())
	err := task.Execute(ctx)
	assert.Error(t, err)

	task.CollectionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.Error(t, err)

	task.CollectionName = collectionName
	task.PartitionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.Error(t, err)
}

func TestShowPartitionsTask(t *testing.T) {
	mixc := NewMixCoordMock()

	defer mixc.Close()
	ctx := context.Background()
	prefix := "TestShowPartitionsTask"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	partitionName := prefix + funcutil.GenRandomStr()

	task := &showPartitionsTask{
		Condition: NewTaskCondition(ctx),
		ShowPartitionsRequest: &milvuspb.ShowPartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ShowPartitions,
				MsgID:     100,
				Timestamp: 100,
			},
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionNames: []string{partitionName},
			Type:           milvuspb.ShowType_All,
		},
		ctx:      ctx,
		mixCoord: mixc,
		result:   nil,
	}
	task.OnEnqueue()
	task.PreExecute(ctx)

	assert.Equal(t, commonpb.MsgType_ShowPartitions, task.Type())
	assert.Equal(t, UniqueID(100), task.ID())
	assert.Equal(t, Timestamp(100), task.BeginTs())
	assert.Equal(t, Timestamp(100), task.EndTs())
	assert.Equal(t, paramtable.GetNodeID(), task.GetBase().GetSourceID())
	err := task.Execute(ctx)
	assert.Error(t, err)

	task.CollectionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.Error(t, err)

	task.CollectionName = collectionName
	task.ShowPartitionsRequest.Type = milvuspb.ShowType_InMemory
	task.PartitionNames = []string{"#0xc0de"}
	err = task.PreExecute(ctx)
	assert.Error(t, err)

	task.CollectionName = collectionName
	task.PartitionNames = []string{partitionName}
	task.ShowPartitionsRequest.Type = milvuspb.ShowType_InMemory
	err = task.Execute(ctx)
	assert.Error(t, err)
}

func TestTask_Int64PrimaryKey(t *testing.T) {
	var err error

	qc := NewMixCoordMock()
	ctx := context.Background()

	mgr := newShardClientMgr()
	err = InitMetaCache(ctx, qc, mgr)
	assert.NoError(t, err)

	shardsNum := int32(2)
	prefix := "TestTask_int64pk"
	dbName := "int64PK"
	collectionName := prefix + funcutil.GenRandomStr()
	partitionName := prefix + funcutil.GenRandomStr()

	fieldName2Types := map[string]schemapb.DataType{
		testBoolField:     schemapb.DataType_Bool,
		testInt32Field:    schemapb.DataType_Int32,
		testInt64Field:    schemapb.DataType_Int64,
		testFloatField:    schemapb.DataType_Float,
		testDoubleField:   schemapb.DataType_Double,
		testFloatVecField: schemapb.DataType_FloatVector,
	}
	if enableMultipleVectorFields {
		fieldName2Types[testBinaryVecField] = schemapb.DataType_BinaryVector
	}
	nb := 10

	schema := constructCollectionSchemaByDataType(collectionName, fieldName2Types, testInt64Field, false)
	marshaledSchema, err := proto.Marshal(schema)
	assert.NoError(t, err)

	createColT := &createCollectionTask{
		Condition: NewTaskCondition(ctx),
		CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			Schema:         marshaledSchema,
			ShardsNum:      shardsNum,
		},
		ctx:      ctx,
		mixCoord: qc,
		result:   nil,
		schema:   nil,
	}

	assert.NoError(t, createColT.OnEnqueue())
	assert.NoError(t, createColT.PreExecute(ctx))
	assert.NoError(t, createColT.Execute(ctx))
	assert.NoError(t, createColT.PostExecute(ctx))

	_, _ = qc.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_CreatePartition,
			MsgID:     0,
			Timestamp: 0,
			SourceID:  paramtable.GetNodeID(),
		},
		DbName:         dbName,
		CollectionName: collectionName,
		PartitionName:  partitionName,
	})

	collectionID, err := globalMetaCache.GetCollectionID(ctx, dbName, collectionName)
	assert.NoError(t, err)

	dmlChannelsFunc := getDmlChannelsFunc(ctx, qc)
	factory := newSimpleMockMsgStreamFactory()
	chMgr := newChannelsMgrImpl(dmlChannelsFunc, nil, factory)
	defer chMgr.removeAllDMLStream()

	_, err = chMgr.getOrCreateDmlStream(ctx, collectionID)
	assert.NoError(t, err)
	pchans, err := chMgr.getChannels(collectionID)
	assert.NoError(t, err)

	interval := time.Millisecond * 10
	tso := newMockTsoAllocator()

	ticker := newChannelsTimeTicker(ctx, interval, []string{}, newGetStatisticsFunc(pchans), tso)
	_ = ticker.start()
	defer ticker.close()

	idAllocator, err := allocator.NewIDAllocator(ctx, qc, paramtable.GetNodeID())
	assert.NoError(t, err)
	_ = idAllocator.Start()
	defer idAllocator.Close()

	segAllocator, err := newSegIDAssigner(ctx, &mockDataCoord{expireTime: Timestamp(2500)}, getLastTick1)
	assert.NoError(t, err)
	_ = segAllocator.Start()
	defer segAllocator.Close()

	t.Run("insert", func(t *testing.T) {
		hash := testutils.GenerateHashKeys(nb)
		task := &insertTask{
			insertMsg: &BaseInsertTask{
				BaseMsg: msgstream.BaseMsg{
					HashValues: hash,
				},
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType:  commonpb.MsgType_Insert,
						MsgID:    0,
						SourceID: paramtable.GetNodeID(),
					},
					DbName:         dbName,
					CollectionName: collectionName,
					PartitionName:  partitionName,
					NumRows:        uint64(nb),
					Version:        msgpb.InsertDataVersion_ColumnBased,
				},
			},

			Condition: NewTaskCondition(ctx),
			ctx:       ctx,
			result: &milvuspb.MutationResult{
				Status:       merr.Success(),
				IDs:          nil,
				SuccIndex:    nil,
				ErrIndex:     nil,
				Acknowledged: false,
				InsertCnt:    0,
				DeleteCnt:    0,
				UpsertCnt:    0,
				Timestamp:    0,
			},
			idAllocator:   idAllocator,
			segIDAssigner: segAllocator,
			chMgr:         chMgr,
			chTicker:      ticker,
			vChannels:     nil,
			pChannels:     nil,
			schema:        nil,
		}

		for fieldName, dataType := range fieldName2Types {
			task.insertMsg.FieldsData = append(task.insertMsg.FieldsData, generateFieldData(dataType, fieldName, nb))
		}

		assert.NoError(t, task.OnEnqueue())
		assert.NoError(t, task.PreExecute(ctx))
		assert.NoError(t, task.Execute(ctx))
		assert.NoError(t, task.PostExecute(ctx))
	})

	t.Run("simple delete", func(t *testing.T) {
		task := &deleteTask{
			Condition: NewTaskCondition(ctx),
			req: &milvuspb.DeleteRequest{
				CollectionName: collectionName,
				PartitionName:  partitionName,
				Expr:           "int64 in [0, 1]",
			},
			idAllocator: idAllocator,
			ctx:         ctx,
			primaryKeys: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{0, 1}}},
			},
			chMgr:        chMgr,
			chTicker:     ticker,
			collectionID: collectionID,
			vChannels:    []string{"test-ch"},
		}

		assert.NoError(t, task.OnEnqueue())
		assert.NotNil(t, task.TraceCtx())

		id := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
		task.SetID(id)
		assert.Equal(t, id, task.ID())
		assert.Equal(t, commonpb.MsgType_Delete, task.Type())

		ts := Timestamp(time.Now().UnixNano())
		task.SetTs(ts)
		assert.Equal(t, ts, task.BeginTs())
		assert.Equal(t, ts, task.EndTs())

		assert.NoError(t, task.PreExecute(ctx))
		assert.NoError(t, task.Execute(ctx))
		assert.NoError(t, task.PostExecute(ctx))
	})
}

func TestIndexType(t *testing.T) {
	rc := NewMixCoordMock()
	defer rc.Close()

	ctx := context.Background()
	shardsNum := int32(2)
	prefix := "TestTask_all"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()

	fieldName2Types := map[string]schemapb.DataType{
		testBoolField:     schemapb.DataType_Bool,
		testInt32Field:    schemapb.DataType_Int32,
		testInt64Field:    schemapb.DataType_Int64,
		testFloatField:    schemapb.DataType_Float,
		testDoubleField:   schemapb.DataType_Double,
		testFloatVecField: schemapb.DataType_FloatVector,
	}

	t.Run("invalid type param", func(t *testing.T) {
		paramtable.Init()
		Params.Save(Params.AutoIndexConfig.Enable.Key, "true")
		defer Params.Reset(Params.AutoIndexConfig.Enable.Key)

		schema := constructCollectionSchemaByDataType(collectionName, fieldName2Types, testInt64Field, false)
		for _, field := range schema.Fields {
			dataType := field.GetDataType()
			if typeutil.IsVectorType(dataType) {
				field.IndexParams = append(field.IndexParams, &commonpb.KeyValuePair{
					Key:   common.MmapEnabledKey,
					Value: "true",
				})
				break
			}
		}
		marshaledSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)

		createColT := &createCollectionTask{
			Condition: NewTaskCondition(ctx),
			CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
				Base:           nil,
				DbName:         dbName,
				CollectionName: collectionName,
				Schema:         marshaledSchema,
				ShardsNum:      shardsNum,
			},
			ctx:      ctx,
			mixCoord: rc,
		}
		assert.NoError(t, createColT.OnEnqueue())
		assert.Error(t, createColT.PreExecute(ctx))
	})
}

func TestTask_VarCharPrimaryKey(t *testing.T) {
	var err error
	mixc := NewMixCoordMock()

	ctx := context.Background()

	mgr := newShardClientMgr()
	err = InitMetaCache(ctx, mixc, mgr)
	assert.NoError(t, err)

	shardsNum := int32(2)
	prefix := "TestTask_all"
	dbName := "testvarchar"
	collectionName := prefix + funcutil.GenRandomStr()
	partitionName := prefix + funcutil.GenRandomStr()

	fieldName2Types := map[string]schemapb.DataType{
		testBoolField:     schemapb.DataType_Bool,
		testInt32Field:    schemapb.DataType_Int32,
		testInt64Field:    schemapb.DataType_Int64,
		testFloatField:    schemapb.DataType_Float,
		testDoubleField:   schemapb.DataType_Double,
		testVarCharField:  schemapb.DataType_VarChar,
		testFloatVecField: schemapb.DataType_FloatVector,
	}
	if enableMultipleVectorFields {
		fieldName2Types[testBinaryVecField] = schemapb.DataType_BinaryVector
	}
	nb := 10

	schema := constructCollectionSchemaByDataType(collectionName, fieldName2Types, testVarCharField, false)
	marshaledSchema, err := proto.Marshal(schema)
	assert.NoError(t, err)

	createColT := &createCollectionTask{
		Condition: NewTaskCondition(ctx),
		CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			Schema:         marshaledSchema,
			ShardsNum:      shardsNum,
		},
		ctx:      ctx,
		mixCoord: mixc,
		result:   nil,
		schema:   nil,
	}

	assert.NoError(t, createColT.OnEnqueue())
	assert.NoError(t, createColT.PreExecute(ctx))
	assert.NoError(t, createColT.Execute(ctx))
	assert.NoError(t, createColT.PostExecute(ctx))

	_, _ = mixc.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_CreatePartition,
			MsgID:     0,
			Timestamp: 0,
			SourceID:  paramtable.GetNodeID(),
		},
		DbName:         dbName,
		CollectionName: collectionName,
		PartitionName:  partitionName,
	})

	collectionID, err := globalMetaCache.GetCollectionID(ctx, dbName, collectionName)
	assert.NoError(t, err)

	dmlChannelsFunc := getDmlChannelsFunc(ctx, mixc)
	factory := newSimpleMockMsgStreamFactory()
	chMgr := newChannelsMgrImpl(dmlChannelsFunc, nil, factory)
	defer chMgr.removeAllDMLStream()

	_, err = chMgr.getOrCreateDmlStream(ctx, collectionID)
	assert.NoError(t, err)
	pchans, err := chMgr.getChannels(collectionID)
	assert.NoError(t, err)

	interval := time.Millisecond * 10
	tso := newMockTsoAllocator()

	ticker := newChannelsTimeTicker(ctx, interval, []string{}, newGetStatisticsFunc(pchans), tso)
	_ = ticker.start()
	defer ticker.close()

	idAllocator, err := allocator.NewIDAllocator(ctx, mixc, paramtable.GetNodeID())
	assert.NoError(t, err)
	_ = idAllocator.Start()
	defer idAllocator.Close()

	segAllocator, err := newSegIDAssigner(ctx, &mockDataCoord{expireTime: Timestamp(2500)}, getLastTick1)
	assert.NoError(t, err)
	segAllocator.Init()
	_ = segAllocator.Start()
	defer segAllocator.Close()

	t.Run("insert", func(t *testing.T) {
		hash := testutils.GenerateHashKeys(nb)
		task := &insertTask{
			insertMsg: &BaseInsertTask{
				BaseMsg: msgstream.BaseMsg{
					HashValues: hash,
				},
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType:  commonpb.MsgType_Insert,
						MsgID:    0,
						SourceID: paramtable.GetNodeID(),
					},
					DbName:         dbName,
					CollectionName: collectionName,
					PartitionName:  partitionName,
					NumRows:        uint64(nb),
					Version:        msgpb.InsertDataVersion_ColumnBased,
				},
			},

			Condition: NewTaskCondition(ctx),
			ctx:       ctx,
			result: &milvuspb.MutationResult{
				Status:       merr.Success(),
				IDs:          nil,
				SuccIndex:    nil,
				ErrIndex:     nil,
				Acknowledged: false,
				InsertCnt:    0,
				DeleteCnt:    0,
				UpsertCnt:    0,
				Timestamp:    0,
			},
			idAllocator:   idAllocator,
			segIDAssigner: segAllocator,
			chMgr:         chMgr,
			chTicker:      ticker,
			vChannels:     nil,
			pChannels:     nil,
			schema:        nil,
		}

		fieldID := common.StartOfUserFieldID
		for fieldName, dataType := range fieldName2Types {
			task.insertMsg.FieldsData = append(task.insertMsg.FieldsData, generateFieldData(dataType, fieldName, nb))
			fieldID++
		}

		assert.NoError(t, task.OnEnqueue())
		assert.NoError(t, task.PreExecute(ctx))
		assert.NoError(t, task.Execute(ctx))
		assert.NoError(t, task.PostExecute(ctx))
	})

	t.Run("upsert", func(t *testing.T) {
		hash := testutils.GenerateHashKeys(nb)
		task := &upsertTask{
			upsertMsg: &msgstream.UpsertMsg{
				InsertMsg: &BaseInsertTask{
					BaseMsg: msgstream.BaseMsg{
						HashValues: hash,
					},
					InsertRequest: &msgpb.InsertRequest{
						Base: &commonpb.MsgBase{
							MsgType:  commonpb.MsgType_Insert,
							MsgID:    0,
							SourceID: paramtable.GetNodeID(),
						},
						DbName:         dbName,
						CollectionName: collectionName,
						PartitionName:  partitionName,
						NumRows:        uint64(nb),
						Version:        msgpb.InsertDataVersion_ColumnBased,
					},
				},
				DeleteMsg: &msgstream.DeleteMsg{
					BaseMsg: msgstream.BaseMsg{
						HashValues: hash,
					},
					DeleteRequest: &msgpb.DeleteRequest{
						Base: &commonpb.MsgBase{
							MsgType:   commonpb.MsgType_Delete,
							MsgID:     0,
							Timestamp: 0,
							SourceID:  paramtable.GetNodeID(),
						},
						DbName:         dbName,
						CollectionName: collectionName,
						PartitionName:  partitionName,
					},
				},
			},

			Condition: NewTaskCondition(ctx),
			req: &milvuspb.UpsertRequest{
				Base: &commonpb.MsgBase{
					MsgType:  commonpb.MsgType_Insert,
					MsgID:    0,
					SourceID: paramtable.GetNodeID(),
				},
				DbName:         dbName,
				CollectionName: collectionName,
				PartitionName:  partitionName,
				HashKeys:       hash,
				NumRows:        uint32(nb),
			},
			ctx: ctx,
			result: &milvuspb.MutationResult{
				Status:       merr.Success(),
				IDs:          nil,
				SuccIndex:    nil,
				ErrIndex:     nil,
				Acknowledged: false,
				InsertCnt:    0,
				DeleteCnt:    0,
				UpsertCnt:    0,
				Timestamp:    0,
			},
			idAllocator:   idAllocator,
			segIDAssigner: segAllocator,
			chMgr:         chMgr,
			chTicker:      ticker,
			vChannels:     nil,
			pChannels:     nil,
			schema:        nil,
		}

		fieldID := common.StartOfUserFieldID
		for fieldName, dataType := range fieldName2Types {
			task.req.FieldsData = append(task.req.FieldsData, generateFieldData(dataType, fieldName, nb))
			fieldID++
		}

		assert.NoError(t, task.OnEnqueue())
		assert.NoError(t, task.PreExecute(ctx))
		assert.NoError(t, task.Execute(ctx))
		assert.NoError(t, task.PostExecute(ctx))
	})

	t.Run("simple delete", func(t *testing.T) {
		task := &deleteTask{
			Condition: NewTaskCondition(ctx),
			req: &milvuspb.DeleteRequest{
				CollectionName: collectionName,
				PartitionName:  partitionName,
				Expr:           "varChar in [\"milvus\", \"test\"]",
			},
			idAllocator: idAllocator,
			ctx:         ctx,
			chMgr:       chMgr,
			chTicker:    ticker,
			vChannels:   []string{"test-channel"},
			primaryKeys: &schemapb.IDs{
				IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"milvus", "test"}}},
			},
			collectionID: collectionID,
		}

		assert.NoError(t, task.OnEnqueue())
		assert.NotNil(t, task.TraceCtx())

		id := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
		task.SetID(id)
		assert.Equal(t, id, task.ID())
		assert.Equal(t, commonpb.MsgType_Delete, task.Type())

		ts := Timestamp(time.Now().UnixNano())
		task.SetTs(ts)
		assert.Equal(t, ts, task.BeginTs())
		assert.Equal(t, ts, task.EndTs())

		assert.NoError(t, task.PreExecute(ctx))
		assert.NoError(t, task.Execute(ctx))
		assert.NoError(t, task.PostExecute(ctx))
	})
}

func Test_createIndexTask_getIndexedFieldAndFunction(t *testing.T) {
	collectionName := "test"
	fieldName := "test"

	cit := &createIndexTask{
		req: &milvuspb.CreateIndexRequest{
			CollectionName: collectionName,
			FieldName:      fieldName,
		},
	}

	idField := &schemapb.FieldSchema{
		FieldID:      100,
		Name:         "id",
		IsPrimaryKey: false,
		DataType:     schemapb.DataType_FloatVector,
		TypeParams:   nil,
		IndexParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "128",
			},
		},
		AutoID: false,
	}
	vectorField := &schemapb.FieldSchema{
		FieldID:      101,
		Name:         fieldName,
		IsPrimaryKey: false,
		DataType:     schemapb.DataType_FloatVector,
		TypeParams:   nil,
		IndexParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "128",
			},
		},
		AutoID: false,
	}

	t.Run("normal", func(t *testing.T) {
		cache := NewMockCache(t)
		cache.On("GetCollectionSchema",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(newSchemaInfo(&schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				idField,
				vectorField,
			},
		}), nil)

		globalMetaCache = cache
		err := cit.getIndexedFieldAndFunction(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, fieldName, cit.fieldSchema.GetName())
	})

	t.Run("schema not found", func(t *testing.T) {
		cache := NewMockCache(t)
		cache.On("GetCollectionSchema",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(nil, errors.New("mock"))
		globalMetaCache = cache
		err := cit.getIndexedFieldAndFunction(context.Background())
		assert.Error(t, err)
	})

	t.Run("field not found", func(t *testing.T) {
		otherField := typeutil.Clone(vectorField)
		otherField.Name = otherField.Name + "_other"
		cache := NewMockCache(t)
		cache.On("GetCollectionSchema",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(newSchemaInfo(&schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				idField,
				otherField,
			},
		}), nil)
		globalMetaCache = cache
		err := cit.getIndexedFieldAndFunction(context.Background())
		assert.Error(t, err)
	})
}

func Test_fillDimension(t *testing.T) {
	t.Run("scalar", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_Int64,
		}
		assert.NoError(t, fillDimension(f, nil))
	})

	t.Run("no dim in schema", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_FloatVector,
		}
		assert.Error(t, fillDimension(f, nil))
	})

	t.Run("dimension mismatch", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_FloatVector,
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   common.DimKey,
					Value: "128",
				},
			},
		}
		assert.Error(t, fillDimension(f, map[string]string{common.DimKey: "8"}))
	})

	t.Run("normal", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_FloatVector,
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   common.DimKey,
					Value: "128",
				},
			},
		}
		m := map[string]string{}
		assert.NoError(t, fillDimension(f, m))
		assert.Equal(t, "128", m[common.DimKey])
	})
}

func Test_checkTrain(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_FloatVector,
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   common.DimKey,
					Value: "128",
				},
			},
		}
		m := map[string]string{
			common.IndexTypeKey:  "IVF_FLAT",
			"nlist":              "1024",
			common.MetricTypeKey: "L2",
		}
		assert.NoError(t, checkTrain(context.TODO(), f, m))
	})

	t.Run("scalar", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_Int64,
		}
		m := map[string]string{
			common.IndexTypeKey: "scalar",
		}
		assert.Error(t, checkTrain(context.TODO(), f, m))
	})

	t.Run("dimension mismatch", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_FloatVector,
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   common.DimKey,
					Value: "128",
				},
			},
		}
		m := map[string]string{
			common.IndexTypeKey:  "IVF_FLAT",
			"nlist":              "1024",
			common.MetricTypeKey: "L2",
			common.DimKey:        "8",
		}
		assert.Error(t, checkTrain(context.TODO(), f, m))
	})

	t.Run("nlist test", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_FloatVector,
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   common.DimKey,
					Value: "128",
				},
			},
		}
		m := map[string]string{
			common.IndexTypeKey:  "IVF_FLAT",
			common.MetricTypeKey: "L2",
		}
		assert.NoError(t, checkTrain(context.TODO(), f, m))
	})
}

func Test_createIndexTask_PreExecute(t *testing.T) {
	collectionName := "test"
	fieldName := "test"

	cit := &createIndexTask{
		req: &milvuspb.CreateIndexRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_CreateIndex,
			},
			CollectionName: collectionName,
			FieldName:      fieldName,
		},
	}

	t.Run("normal", func(t *testing.T) {
		cache := NewMockCache(t)
		cache.On("GetCollectionID",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(UniqueID(100), nil)
		cache.On("GetCollectionSchema",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(newSchemaInfo(&schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      100,
					Name:         fieldName,
					IsPrimaryKey: false,
					DataType:     schemapb.DataType_FloatVector,
					TypeParams:   nil,
					IndexParams: []*commonpb.KeyValuePair{
						{
							Key:   "dim",
							Value: "128",
						},
					},
					AutoID: false,
				},
			},
		}), nil)
		globalMetaCache = cache
		cit.req.ExtraParams = []*commonpb.KeyValuePair{
			{
				Key:   common.IndexTypeKey,
				Value: "IVF_FLAT",
			},
			{
				Key:   "nlist",
				Value: "1024",
			},
			{
				Key:   common.MetricTypeKey,
				Value: "L2",
			},
		}
		assert.NoError(t, cit.PreExecute(context.Background()))
	})

	t.Run("collection not found", func(t *testing.T) {
		cache := NewMockCache(t)
		cache.On("GetCollectionID",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(UniqueID(0), errors.New("mock"))
		globalMetaCache = cache
		assert.Error(t, cit.PreExecute(context.Background()))
	})

	t.Run("index name length exceed 255", func(t *testing.T) {
		cache := NewMockCache(t)
		cache.On("GetCollectionID",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(UniqueID(100), nil)
		globalMetaCache = cache

		for i := 0; i < 256; i++ {
			cit.req.IndexName += "a"
		}
		err := cit.PreExecute(context.Background())

		assert.Error(t, err)
	})

	t.Run("index name start with number", func(t *testing.T) {
		cit.req.IndexName = "12a"
		err := cit.PreExecute(context.Background())

		assert.Error(t, err)
	})

	t.Run("index name include special characters", func(t *testing.T) {
		cit.req.IndexName = "ac#1"
		err := cit.PreExecute(context.Background())

		assert.Error(t, err)
	})
}

func Test_dropCollectionTask_PreExecute(t *testing.T) {
	dct := &dropCollectionTask{DropCollectionRequest: &milvuspb.DropCollectionRequest{
		Base:           &commonpb.MsgBase{},
		CollectionName: "valid", // invalid
	}}
	ctx := context.Background()
	err := dct.PreExecute(ctx)
	assert.NoError(t, err)
}

func Test_dropCollectionTask_Execute(t *testing.T) {
	mockRC := mocks.NewMockMixCoordClient(t)
	mockRC.On("DropCollection",
		mock.Anything, // context.Context
		mock.Anything, // *milvuspb.DropCollectionRequest
		mock.Anything,
	).Return(&commonpb.Status{}, func(ctx context.Context, request *milvuspb.DropCollectionRequest, opts ...grpc.CallOption) error {
		switch request.GetCollectionName() {
		case "c1":
			return errors.New("error mock DropCollection")
		case "c2":
			return merr.WrapErrCollectionNotFound("mock")
		default:
			return nil
		}
	})

	ctx := context.Background()

	dct := &dropCollectionTask{mixCoord: mockRC, DropCollectionRequest: &milvuspb.DropCollectionRequest{CollectionName: "normal"}}
	err := dct.Execute(ctx)
	assert.NoError(t, err)

	dct.DropCollectionRequest.CollectionName = "c1"
	err = dct.Execute(ctx)
	assert.Error(t, err)

	dct.DropCollectionRequest.CollectionName = "c2"
	err = dct.Execute(ctx)
	assert.Error(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, dct.result.GetErrorCode())
}

func Test_dropCollectionTask_PostExecute(t *testing.T) {
	dct := &dropCollectionTask{}
	assert.NoError(t, dct.PostExecute(context.Background()))
}

func Test_loadCollectionTask_Execute(t *testing.T) {
	rc := NewMixCoordMock()

	dbName := funcutil.GenRandomStr()
	collectionName := funcutil.GenRandomStr()
	collectionID := UniqueID(1)
	// fieldName := funcutil.GenRandomStr()
	indexName := funcutil.GenRandomStr()
	ctx := context.Background()
	indexID := int64(1000)

	shardMgr := newShardClientMgr()
	// failed to get collection id.
	_ = InitMetaCache(ctx, rc, shardMgr)

	rc.DescribeCollectionFunc = func(ctx context.Context, request *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
		return &milvuspb.DescribeCollectionResponse{
			Status:         merr.Success(),
			Schema:         newTestSchema(),
			CollectionID:   collectionID,
			CollectionName: request.CollectionName,
		}, nil
	}

	lct := &loadCollectionTask{
		LoadCollectionRequest: &milvuspb.LoadCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_LoadCollection,
				MsgID:     1,
				Timestamp: 1,
				SourceID:  1,
				TargetID:  1,
			},
			DbName:         dbName,
			CollectionName: collectionName,
			ReplicaNumber:  1,
		},
		ctx:          ctx,
		mixCoord:     rc,
		result:       nil,
		collectionID: 0,
	}

	t.Run("indexcoord describe index error", func(t *testing.T) {
		err := lct.Execute(ctx)
		assert.Error(t, err)
	})

	t.Run("indexcoord describe index not success", func(t *testing.T) {
		rc.DescribeIndexFunc = func(ctx context.Context, request *indexpb.DescribeIndexRequest, opts ...grpc.CallOption) (*indexpb.DescribeIndexResponse, error) {
			return &indexpb.DescribeIndexResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    "fail reason",
				},
			}, nil
		}

		err := lct.Execute(ctx)
		assert.Error(t, err)
	})

	t.Run("no vector index", func(t *testing.T) {
		rc.DescribeIndexFunc = func(ctx context.Context, request *indexpb.DescribeIndexRequest, opts ...grpc.CallOption) (*indexpb.DescribeIndexResponse, error) {
			return &indexpb.DescribeIndexResponse{
				Status: merr.Success(),
				IndexInfos: []*indexpb.IndexInfo{
					{
						CollectionID:         collectionID,
						FieldID:              100,
						IndexName:            indexName,
						IndexID:              indexID,
						TypeParams:           nil,
						IndexParams:          nil,
						IndexedRows:          1025,
						TotalRows:            1025,
						State:                commonpb.IndexState_Finished,
						IndexStateFailReason: "",
						IsAutoIndex:          false,
						UserIndexParams:      nil,
					},
				},
			}, nil
		}

		err := lct.Execute(ctx)
		assert.Error(t, err)
	})

	t.Run("not all vector fields with index", func(t *testing.T) {
		vecFields := make([]*schemapb.FieldSchema, 0)
		for _, field := range newTestSchema().GetFields() {
			if typeutil.IsVectorType(field.GetDataType()) {
				vecFields = append(vecFields, field)
			}
		}

		assert.GreaterOrEqual(t, len(vecFields), 2)

		rc.DescribeIndexFunc = func(ctx context.Context, request *indexpb.DescribeIndexRequest, opts ...grpc.CallOption) (*indexpb.DescribeIndexResponse, error) {
			return &indexpb.DescribeIndexResponse{
				Status: merr.Success(),
				IndexInfos: []*indexpb.IndexInfo{
					{
						CollectionID:         collectionID,
						FieldID:              vecFields[0].FieldID,
						IndexName:            indexName,
						IndexID:              indexID,
						TypeParams:           nil,
						IndexParams:          nil,
						IndexedRows:          1025,
						TotalRows:            1025,
						State:                commonpb.IndexState_Finished,
						IndexStateFailReason: "",
						IsAutoIndex:          false,
						UserIndexParams:      nil,
					},
				},
			}, nil
		}

		err := lct.Execute(ctx)
		assert.Error(t, err)
	})
}

func Test_loadPartitionTask_Execute(t *testing.T) {
	qc := NewMixCoordMock()

	dbName := funcutil.GenRandomStr()
	collectionName := funcutil.GenRandomStr()
	collectionID := UniqueID(1)
	// fieldName := funcutil.GenRandomStr()
	indexName := funcutil.GenRandomStr()
	ctx := context.Background()
	indexID := int64(1000)

	shardMgr := newShardClientMgr()
	// failed to get collection id.
	_ = InitMetaCache(ctx, qc, shardMgr)

	qc.DescribeCollectionFunc = func(ctx context.Context, request *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
		return &milvuspb.DescribeCollectionResponse{
			Status:         merr.Success(),
			Schema:         newTestSchema(),
			CollectionID:   collectionID,
			CollectionName: request.CollectionName,
		}, nil
	}

	lpt := &loadPartitionsTask{
		LoadPartitionsRequest: &milvuspb.LoadPartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_LoadCollection,
				MsgID:     1,
				Timestamp: 1,
				SourceID:  1,
				TargetID:  1,
			},
			DbName:         dbName,
			CollectionName: collectionName,
			ReplicaNumber:  1,
		},
		ctx:          ctx,
		mixCoord:     qc,
		result:       nil,
		collectionID: 0,
	}

	t.Run("indexcoord describe index error", func(t *testing.T) {
		err := lpt.Execute(ctx)
		assert.Error(t, err)
	})

	t.Run("indexcoord describe index not success", func(t *testing.T) {
		qc.DescribeIndexFunc = func(ctx context.Context, request *indexpb.DescribeIndexRequest, opts ...grpc.CallOption) (*indexpb.DescribeIndexResponse, error) {
			return &indexpb.DescribeIndexResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    "fail reason",
				},
			}, nil
		}

		err := lpt.Execute(ctx)
		assert.Error(t, err)
	})

	t.Run("no vector index", func(t *testing.T) {
		qc.DescribeIndexFunc = func(ctx context.Context, request *indexpb.DescribeIndexRequest, opts ...grpc.CallOption) (*indexpb.DescribeIndexResponse, error) {
			return &indexpb.DescribeIndexResponse{
				Status: merr.Success(),
				IndexInfos: []*indexpb.IndexInfo{
					{
						CollectionID:         collectionID,
						FieldID:              100,
						IndexName:            indexName,
						IndexID:              indexID,
						TypeParams:           nil,
						IndexParams:          nil,
						IndexedRows:          1025,
						TotalRows:            1025,
						State:                commonpb.IndexState_Finished,
						IndexStateFailReason: "",
						IsAutoIndex:          false,
						UserIndexParams:      nil,
					},
				},
			}, nil
		}

		err := lpt.Execute(ctx)
		assert.Error(t, err)
	})
}

func TestCreateResourceGroupTask(t *testing.T) {
	mixc := NewMixCoordMock()

	defer mixc.Close()

	ctx := context.Background()
	mgr := newShardClientMgr()
	InitMetaCache(ctx, mixc, mgr)

	createRGReq := &milvuspb.CreateResourceGroupRequest{
		Base: &commonpb.MsgBase{
			MsgID:     1,
			Timestamp: 2,
			TargetID:  3,
		},
		ResourceGroup: "rg",
	}

	task := &CreateResourceGroupTask{
		CreateResourceGroupRequest: createRGReq,
		ctx:                        ctx,
		mixCoord:                   mixc,
	}
	task.OnEnqueue()
	task.PreExecute(ctx)

	assert.Equal(t, commonpb.MsgType_CreateResourceGroup, task.Type())
	assert.Equal(t, UniqueID(1), task.ID())
	assert.Equal(t, Timestamp(2), task.BeginTs())
	assert.Equal(t, Timestamp(2), task.EndTs())
	assert.Equal(t, paramtable.GetNodeID(), task.Base.GetSourceID())
	assert.Equal(t, UniqueID(3), task.Base.GetTargetID())

	err := task.Execute(ctx)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, task.result.ErrorCode)
}

func TestDropResourceGroupTask(t *testing.T) {
	mixc := NewMixCoordMock()

	defer mixc.Close()

	ctx := context.Background()
	mgr := newShardClientMgr()
	InitMetaCache(ctx, mixc, mgr)

	dropRGReq := &milvuspb.DropResourceGroupRequest{
		Base: &commonpb.MsgBase{
			MsgID:     1,
			Timestamp: 2,
			TargetID:  3,
		},
		ResourceGroup: "rg",
	}

	task := &DropResourceGroupTask{
		DropResourceGroupRequest: dropRGReq,
		ctx:                      ctx,
		mixCoord:                 mixc,
	}
	task.OnEnqueue()
	task.PreExecute(ctx)

	assert.Equal(t, commonpb.MsgType_DropResourceGroup, task.Type())
	assert.Equal(t, UniqueID(1), task.ID())
	assert.Equal(t, Timestamp(2), task.BeginTs())
	assert.Equal(t, Timestamp(2), task.EndTs())
	assert.Equal(t, paramtable.GetNodeID(), task.Base.GetSourceID())
	assert.Equal(t, UniqueID(3), task.Base.GetTargetID())

	err := task.Execute(ctx)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, task.result.ErrorCode)
}

func TestTransferNodeTask(t *testing.T) {
	mixc := NewMixCoordMock()

	defer mixc.Close()
	ctx := context.Background()
	mgr := newShardClientMgr()
	InitMetaCache(ctx, mixc, mgr)

	req := &milvuspb.TransferNodeRequest{
		Base: &commonpb.MsgBase{
			MsgID:     1,
			Timestamp: 2,
			TargetID:  3,
		},
		SourceResourceGroup: "rg1",
		TargetResourceGroup: "rg2",
		NumNode:             1,
	}

	task := &TransferNodeTask{
		TransferNodeRequest: req,
		ctx:                 ctx,
		mixCoord:            mixc,
	}
	task.OnEnqueue()
	task.PreExecute(ctx)

	assert.Equal(t, commonpb.MsgType_TransferNode, task.Type())
	assert.Equal(t, UniqueID(1), task.ID())
	assert.Equal(t, Timestamp(2), task.BeginTs())
	assert.Equal(t, Timestamp(2), task.EndTs())
	assert.Equal(t, paramtable.GetNodeID(), task.Base.GetSourceID())
	assert.Equal(t, UniqueID(3), task.Base.GetTargetID())

	err := task.Execute(ctx)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, task.result.ErrorCode)
}

func TestTransferReplicaTask(t *testing.T) {
	rc := &MockMixCoordClientInterface{}

	ctx := context.Background()
	mgr := newShardClientMgr()
	InitMetaCache(ctx, rc, mgr)
	// make it avoid remote call on rc
	globalMetaCache.GetCollectionSchema(context.Background(), GetCurDBNameFromContextOrDefault(ctx), "collection1")

	req := &milvuspb.TransferReplicaRequest{
		Base: &commonpb.MsgBase{
			MsgID:     1,
			Timestamp: 2,
			TargetID:  3,
		},
		CollectionName:      "collection1",
		SourceResourceGroup: "rg1",
		TargetResourceGroup: "rg2",
		NumReplica:          1,
	}

	task := &TransferReplicaTask{
		TransferReplicaRequest: req,
		ctx:                    ctx,
		mixCoord:               rc,
	}
	task.OnEnqueue()
	task.PreExecute(ctx)

	assert.Equal(t, commonpb.MsgType_TransferReplica, task.Type())
	assert.Equal(t, UniqueID(1), task.ID())
	assert.Equal(t, Timestamp(2), task.BeginTs())
	assert.Equal(t, Timestamp(2), task.EndTs())
	assert.Equal(t, paramtable.GetNodeID(), task.Base.GetSourceID())
	assert.Equal(t, UniqueID(3), task.Base.GetTargetID())

	err := task.Execute(ctx)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, task.result.ErrorCode)
}

func TestListResourceGroupsTask(t *testing.T) {
	rc := &MockMixCoordClientInterface{}

	rc.listResourceGroups = func(ctx context.Context, request *milvuspb.ListResourceGroupsRequest) (*milvuspb.ListResourceGroupsResponse, error) {
		return &milvuspb.ListResourceGroupsResponse{
			Status:         merr.Success(),
			ResourceGroups: []string{meta.DefaultResourceGroupName, "rg"},
		}, nil
	}

	ctx := context.Background()
	mgr := newShardClientMgr()
	InitMetaCache(ctx, rc, mgr)

	req := &milvuspb.ListResourceGroupsRequest{
		Base: &commonpb.MsgBase{
			MsgID:     1,
			Timestamp: 2,
			TargetID:  3,
		},
	}

	task := &ListResourceGroupsTask{
		ListResourceGroupsRequest: req,
		ctx:                       ctx,
		mixCoord:                  rc,
	}
	task.OnEnqueue()
	task.PreExecute(ctx)

	assert.Equal(t, commonpb.MsgType_ListResourceGroups, task.Type())
	assert.Equal(t, UniqueID(1), task.ID())
	assert.Equal(t, Timestamp(2), task.BeginTs())
	assert.Equal(t, Timestamp(2), task.EndTs())
	assert.Equal(t, paramtable.GetNodeID(), task.Base.GetSourceID())
	assert.Equal(t, UniqueID(3), task.Base.GetTargetID())

	err := task.Execute(ctx)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, task.result.GetStatus().GetErrorCode())
	groups := task.result.GetResourceGroups()
	assert.Contains(t, groups, meta.DefaultResourceGroupName)
	assert.Contains(t, groups, "rg")
}

func TestDescribeResourceGroupTask(t *testing.T) {
	rc := &MockMixCoordClientInterface{}
	rc.describeResourceGroup = func(ctx context.Context, request *querypb.DescribeResourceGroupRequest) (*querypb.DescribeResourceGroupResponse, error) {
		return &querypb.DescribeResourceGroupResponse{
			Status: merr.Success(),
			ResourceGroup: &querypb.ResourceGroupInfo{
				Name:             "rg",
				Capacity:         2,
				NumAvailableNode: 1,
				NumOutgoingNode:  map[int64]int32{1: 1},
				NumIncomingNode:  map[int64]int32{2: 2},
			},
		}, nil
	}

	ctx := context.Background()
	mgr := newShardClientMgr()
	InitMetaCache(ctx, rc, mgr)
	// make it avoid remote call on rc
	globalMetaCache.GetCollectionSchema(context.Background(), GetCurDBNameFromContextOrDefault(ctx), "collection1")
	globalMetaCache.GetCollectionSchema(context.Background(), GetCurDBNameFromContextOrDefault(ctx), "collection2")

	req := &milvuspb.DescribeResourceGroupRequest{
		Base: &commonpb.MsgBase{
			MsgID:     1,
			Timestamp: 2,
			TargetID:  3,
		},
		ResourceGroup: "rg",
	}

	task := &DescribeResourceGroupTask{
		DescribeResourceGroupRequest: req,
		ctx:                          ctx,
		mixCoord:                     rc,
	}
	task.OnEnqueue()
	task.PreExecute(ctx)

	assert.Equal(t, commonpb.MsgType_DescribeResourceGroup, task.Type())
	assert.Equal(t, UniqueID(1), task.ID())
	assert.Equal(t, Timestamp(2), task.BeginTs())
	assert.Equal(t, Timestamp(2), task.EndTs())
	assert.Equal(t, paramtable.GetNodeID(), task.Base.GetSourceID())
	assert.Equal(t, UniqueID(3), task.Base.GetTargetID())

	err := task.Execute(ctx)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, task.result.GetStatus().GetErrorCode())
	groupInfo := task.result.GetResourceGroup()
	outgoingNodeNum := groupInfo.GetNumOutgoingNode()
	incomingNodeNum := groupInfo.GetNumIncomingNode()
	assert.NotNil(t, outgoingNodeNum["collection1"])
	assert.NotNil(t, incomingNodeNum["collection2"])
}

func TestDescribeResourceGroupTaskFailed(t *testing.T) {
	rc := &MockMixCoordClientInterface{}

	rc.describeResourceGroup = func(ctx context.Context, request *querypb.DescribeResourceGroupRequest) (*querypb.DescribeResourceGroupResponse, error) {
		return &querypb.DescribeResourceGroupResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError},
		}, nil
	}

	ctx := context.Background()
	mgr := newShardClientMgr()
	InitMetaCache(ctx, rc, mgr)
	// make it avoid remote call on rc
	globalMetaCache.GetCollectionSchema(context.Background(), GetCurDBNameFromContextOrDefault(ctx), "collection1")
	globalMetaCache.GetCollectionSchema(context.Background(), GetCurDBNameFromContextOrDefault(ctx), "collection2")

	req := &milvuspb.DescribeResourceGroupRequest{
		Base: &commonpb.MsgBase{
			MsgID:     1,
			Timestamp: 2,
			TargetID:  3,
		},
		ResourceGroup: "rgggg",
	}

	task := &DescribeResourceGroupTask{
		DescribeResourceGroupRequest: req,
		ctx:                          ctx,
		mixCoord:                     rc,
	}
	task.OnEnqueue()
	task.PreExecute(ctx)

	assert.Equal(t, commonpb.MsgType_DescribeResourceGroup, task.Type())
	assert.Equal(t, UniqueID(1), task.ID())
	assert.Equal(t, Timestamp(2), task.BeginTs())
	assert.Equal(t, Timestamp(2), task.EndTs())
	assert.Equal(t, paramtable.GetNodeID(), task.Base.GetSourceID())
	assert.Equal(t, UniqueID(3), task.Base.GetTargetID())

	err := task.Execute(ctx)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, task.result.GetStatus().GetErrorCode())

	rc.describeResourceGroup = func(ctx context.Context, request *querypb.DescribeResourceGroupRequest) (*querypb.DescribeResourceGroupResponse, error) {
		return &querypb.DescribeResourceGroupResponse{
			Status: merr.Success(),
			ResourceGroup: &querypb.ResourceGroupInfo{
				Name:             "rg",
				Capacity:         2,
				NumAvailableNode: 1,
				NumOutgoingNode:  map[int64]int32{3: 1},
				NumIncomingNode:  map[int64]int32{4: 2},
			},
		}, nil
	}

	err = task.Execute(ctx)
	assert.NoError(t, err)
	assert.Len(t, task.result.ResourceGroup.NumOutgoingNode, 0)
	assert.Len(t, task.result.ResourceGroup.NumIncomingNode, 0)
}

func TestCreateCollectionTaskWithPartitionKey(t *testing.T) {
	rc := NewMixCoordMock()
	paramtable.Init()

	defer rc.Close()
	ctx := context.Background()
	shardsNum := common.DefaultShardsNum
	prefix := "TestCreateCollectionTaskWithPartitionKey"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()

	int64Field := &schemapb.FieldSchema{
		Name:         "int64",
		DataType:     schemapb.DataType_Int64,
		IsPrimaryKey: true,
	}
	varCharField := &schemapb.FieldSchema{
		Name:     "varChar",
		DataType: schemapb.DataType_VarChar,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "max_length",
				Value: strconv.Itoa(testMaxVarCharLength),
			},
			{
				Key:   "enable_analyzer",
				Value: "true",
			},
		},
	}
	floatVecField := &schemapb.FieldSchema{
		Name:     "fvec",
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: strconv.Itoa(testVecDim),
			},
		},
	}
	sparseVecField := &schemapb.FieldSchema{
		Name:     "sparse",
		DataType: schemapb.DataType_SparseFloatVector,
	}
	partitionKeyField := &schemapb.FieldSchema{
		Name:           "partition_key",
		DataType:       schemapb.DataType_Int64,
		IsPartitionKey: true,
	}
	schema := &schemapb.CollectionSchema{
		Name:   collectionName,
		Fields: []*schemapb.FieldSchema{int64Field, varCharField, partitionKeyField, floatVecField},
	}

	marshaledSchema, err := proto.Marshal(schema)
	assert.NoError(t, err)

	task := &createCollectionTask{
		Condition: NewTaskCondition(ctx),
		CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgID:     UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()),
				Timestamp: Timestamp(time.Now().UnixNano()),
			},
			DbName:         dbName,
			CollectionName: collectionName,
			Schema:         marshaledSchema,
			ShardsNum:      shardsNum,
		},
		ctx:      ctx,
		mixCoord: rc,
		result:   nil,
		schema:   nil,
	}

	t.Run("PreExecute", func(t *testing.T) {
		defer Params.Reset(Params.RootCoordCfg.MaxPartitionNum.Key)
		var err error

		// test default num partitions
		err = task.PreExecute(ctx)
		assert.NoError(t, err)
		assert.Equal(t, common.DefaultPartitionsWithPartitionKey, task.GetNumPartitions())

		Params.Save(Params.RootCoordCfg.MaxPartitionNum.Key, "16")
		task.NumPartitions = 0
		err = task.PreExecute(ctx)
		assert.NoError(t, err)
		assert.Equal(t, int64(16), task.GetNumPartitions())
		Params.Reset(Params.RootCoordCfg.MaxPartitionNum.Key)

		// test specify num partition without partition key field
		partitionKeyField.IsPartitionKey = false
		task.NumPartitions = common.DefaultPartitionsWithPartitionKey * 2
		marshaledSchema, err = proto.Marshal(schema)
		assert.NoError(t, err)
		task.Schema = marshaledSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)
		partitionKeyField.IsPartitionKey = true

		// test multi partition key field
		varCharField.IsPartitionKey = true
		marshaledSchema, err = proto.Marshal(schema)
		assert.NoError(t, err)
		task.Schema = marshaledSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)
		varCharField.IsPartitionKey = false

		// test partitions < 0
		task.NumPartitions = -2
		marshaledSchema, err = proto.Marshal(schema)
		assert.NoError(t, err)
		task.Schema = marshaledSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)
		task.NumPartitions = 1000

		// test partition key type not in [int64, varChar]
		partitionKeyField.DataType = schemapb.DataType_FloatVector
		marshaledSchema, err = proto.Marshal(schema)
		assert.NoError(t, err)
		task.Schema = marshaledSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)
		partitionKeyField.DataType = schemapb.DataType_Int64

		// test partition key set nullable == true
		partitionKeyField.Nullable = true
		marshaledSchema, err = proto.Marshal(schema)
		assert.NoError(t, err)
		task.Schema = marshaledSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)
		partitionKeyField.DataType = schemapb.DataType_Int64
		partitionKeyField.Nullable = false

		// test partition key field not primary key field
		primaryField, _ := typeutil.GetPrimaryFieldSchema(schema)
		primaryField.IsPartitionKey = true
		marshaledSchema, err = proto.Marshal(schema)
		assert.NoError(t, err)
		task.Schema = marshaledSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)
		primaryField.IsPartitionKey = false

		// test partition num too large
		Params.Save(Params.RootCoordCfg.MaxPartitionNum.Key, "16")
		marshaledSchema, err = proto.Marshal(schema)
		assert.NoError(t, err)
		task.Schema = marshaledSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)
		Params.Reset(Params.RootCoordCfg.MaxPartitionNum.Key)

		marshaledSchema, err = proto.Marshal(schema)
		assert.NoError(t, err)
		task.Schema = marshaledSchema
		err = task.PreExecute(ctx)
		assert.NoError(t, err)

		// test schema with function
		//	 invalid function
		schema.Functions = []*schemapb.FunctionSchema{
			{Name: "test", Type: schemapb.FunctionType_BM25, InputFieldNames: []string{"invalid name"}},
		}
		marshaledSchema, err = proto.Marshal(schema)
		assert.NoError(t, err)
		task.Schema = marshaledSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		//   normal case
		schema.Fields = append(schema.Fields, sparseVecField)
		schema.Functions = []*schemapb.FunctionSchema{
			{Name: "test", Type: schemapb.FunctionType_BM25, InputFieldNames: []string{varCharField.Name}, OutputFieldNames: []string{sparseVecField.Name}},
		}
		marshaledSchema, err = proto.Marshal(schema)
		assert.NoError(t, err)
		task.Schema = marshaledSchema
		err = task.PreExecute(ctx)
		assert.NoError(t, err)
	})

	t.Run("Execute", func(t *testing.T) {
		err = task.Execute(ctx)
		assert.NoError(t, err)

		// check default partitions
		err = InitMetaCache(ctx, rc, nil)
		assert.NoError(t, err)
		partitionNames, err := getDefaultPartitionsInPartitionKeyMode(ctx, "", task.CollectionName)
		assert.NoError(t, err)
		assert.Equal(t, task.GetNumPartitions(), int64(len(partitionNames)))

		createPartitionTask := &createPartitionTask{
			Condition: NewTaskCondition(ctx),
			CreatePartitionRequest: &milvuspb.CreatePartitionRequest{
				Base: &commonpb.MsgBase{
					MsgID:     UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()),
					Timestamp: Timestamp(time.Now().UnixNano()),
				},
				DbName:         dbName,
				CollectionName: collectionName,
				PartitionName:  "new_partition",
			},
			ctx:      ctx,
			mixCoord: rc,
		}
		err = createPartitionTask.PreExecute(ctx)
		assert.Error(t, err)

		dropPartitionTask := &dropPartitionTask{
			Condition: NewTaskCondition(ctx),
			DropPartitionRequest: &milvuspb.DropPartitionRequest{
				Base: &commonpb.MsgBase{
					MsgID:     UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()),
					Timestamp: Timestamp(time.Now().UnixNano()),
				},
				DbName:         dbName,
				CollectionName: collectionName,
				PartitionName:  "new_partition",
			},
			ctx:      ctx,
			mixCoord: rc,
		}
		err = dropPartitionTask.PreExecute(ctx)
		assert.Error(t, err)

		loadPartitionTask := &loadPartitionsTask{
			Condition: NewTaskCondition(ctx),
			LoadPartitionsRequest: &milvuspb.LoadPartitionsRequest{
				Base: &commonpb.MsgBase{
					MsgID:     UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()),
					Timestamp: Timestamp(time.Now().UnixNano()),
				},
				DbName:         dbName,
				CollectionName: collectionName,
				PartitionNames: []string{"_default_0"},
			},
			ctx: ctx,
		}
		err = loadPartitionTask.PreExecute(ctx)
		assert.Error(t, err)

		releasePartitionsTask := &releasePartitionsTask{
			Condition: NewTaskCondition(ctx),
			ReleasePartitionsRequest: &milvuspb.ReleasePartitionsRequest{
				Base: &commonpb.MsgBase{
					MsgID:     UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()),
					Timestamp: Timestamp(time.Now().UnixNano()),
				},
				DbName:         dbName,
				CollectionName: collectionName,
				PartitionNames: []string{"_default_0"},
			},
			ctx: ctx,
		}
		err = releasePartitionsTask.PreExecute(ctx)
		assert.Error(t, err)
	})
}

func TestPartitionKey(t *testing.T) {
	qc := NewMixCoordMock()
	defer qc.Close()
	ctx := context.Background()

	mgr := newShardClientMgr()
	err := InitMetaCache(ctx, qc, mgr)
	assert.NoError(t, err)

	shardsNum := common.DefaultShardsNum
	prefix := "TestInsertTaskWithPartitionKey"
	collectionName := prefix + funcutil.GenRandomStr()

	fieldName2Type := make(map[string]schemapb.DataType)
	fieldName2Type["int64_field"] = schemapb.DataType_Int64
	fieldName2Type["varChar_field"] = schemapb.DataType_VarChar
	fieldName2Type["fvec_field"] = schemapb.DataType_FloatVector
	schema := constructCollectionSchemaByDataType(collectionName, fieldName2Type, "int64_field", false)
	partitionKeyField := &schemapb.FieldSchema{
		Name:           "partition_key_field",
		DataType:       schemapb.DataType_Int64,
		IsPartitionKey: true,
	}
	fieldName2Type["partition_key_field"] = schemapb.DataType_Int64
	schema.Fields = append(schema.Fields, partitionKeyField)
	marshaledSchema, err := proto.Marshal(schema)
	assert.NoError(t, err)

	createCollectionTask := &createCollectionTask{
		Condition: NewTaskCondition(ctx),
		CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgID:     UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()),
				Timestamp: Timestamp(time.Now().UnixNano()),
			},
			DbName:         "",
			CollectionName: collectionName,
			Schema:         marshaledSchema,
			ShardsNum:      shardsNum,
			NumPartitions:  common.DefaultPartitionsWithPartitionKey,
		},
		ctx:      ctx,
		mixCoord: qc,
		result:   nil,
		schema:   nil,
	}
	err = createCollectionTask.PreExecute(ctx)
	assert.NoError(t, err)
	err = createCollectionTask.Execute(ctx)
	assert.NoError(t, err)

	collectionID, err := globalMetaCache.GetCollectionID(ctx, GetCurDBNameFromContextOrDefault(ctx), collectionName)
	assert.NoError(t, err)

	dmlChannelsFunc := getDmlChannelsFunc(ctx, qc)
	factory := newSimpleMockMsgStreamFactory()
	chMgr := newChannelsMgrImpl(dmlChannelsFunc, nil, factory)
	defer chMgr.removeAllDMLStream()

	_, err = chMgr.getOrCreateDmlStream(ctx, collectionID)
	assert.NoError(t, err)
	pchans, err := chMgr.getChannels(collectionID)
	assert.NoError(t, err)

	interval := time.Millisecond * 10
	tso := newMockTsoAllocator()

	ticker := newChannelsTimeTicker(ctx, interval, []string{}, newGetStatisticsFunc(pchans), tso)
	_ = ticker.start()
	defer ticker.close()

	idAllocator, err := allocator.NewIDAllocator(ctx, qc, paramtable.GetNodeID())
	assert.NoError(t, err)
	_ = idAllocator.Start()
	defer idAllocator.Close()

	segAllocator, err := newSegIDAssigner(ctx, &mockDataCoord{expireTime: Timestamp(2500)}, getLastTick1)
	assert.NoError(t, err)
	segAllocator.Init()
	_ = segAllocator.Start()
	defer segAllocator.Close()

	partitionNames, err := getDefaultPartitionsInPartitionKeyMode(ctx, "", collectionName)
	assert.NoError(t, err)
	assert.Equal(t, common.DefaultPartitionsWithPartitionKey, int64(len(partitionNames)))

	nb := 10
	fieldID := common.StartOfUserFieldID
	fieldDatas := make([]*schemapb.FieldData, 0)
	for fieldName, dataType := range fieldName2Type {
		fieldData := generateFieldData(dataType, fieldName, nb)
		fieldData.FieldId = int64(fieldID)
		fieldDatas = append(fieldDatas, generateFieldData(dataType, fieldName, nb))
		fieldID++
	}

	t.Run("Insert", func(t *testing.T) {
		it := &insertTask{
			insertMsg: &BaseInsertTask{
				BaseMsg: msgstream.BaseMsg{},
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType:  commonpb.MsgType_Insert,
						MsgID:    0,
						SourceID: paramtable.GetNodeID(),
					},
					CollectionName: collectionName,
					FieldsData:     fieldDatas,
					NumRows:        uint64(nb),
					Version:        msgpb.InsertDataVersion_ColumnBased,
				},
			},

			Condition: NewTaskCondition(ctx),
			ctx:       ctx,
			result: &milvuspb.MutationResult{
				Status:       merr.Success(),
				IDs:          nil,
				SuccIndex:    nil,
				ErrIndex:     nil,
				Acknowledged: false,
				InsertCnt:    0,
				DeleteCnt:    0,
				UpsertCnt:    0,
				Timestamp:    0,
			},
			idAllocator:   idAllocator,
			segIDAssigner: segAllocator,
			chMgr:         chMgr,
			chTicker:      ticker,
			vChannels:     nil,
			pChannels:     nil,
			schema:        nil,
		}

		// don't support specify partition name if use partition key
		it.insertMsg.PartitionName = partitionNames[0]
		assert.Error(t, it.PreExecute(ctx))

		it.insertMsg.PartitionName = ""
		assert.NoError(t, it.OnEnqueue())
		assert.NoError(t, it.PreExecute(ctx))
		assert.NoError(t, it.Execute(ctx))
		assert.NoError(t, it.PostExecute(ctx))
	})

	t.Run("Upsert", func(t *testing.T) {
		hash := testutils.GenerateHashKeys(nb)
		ut := &upsertTask{
			ctx:       ctx,
			Condition: NewTaskCondition(ctx),
			baseMsg: msgstream.BaseMsg{
				HashValues: hash,
			},
			req: &milvuspb.UpsertRequest{
				Base: commonpbutil.NewMsgBase(
					commonpbutil.WithMsgType(commonpb.MsgType_Upsert),
					commonpbutil.WithSourceID(paramtable.GetNodeID()),
				),
				CollectionName: collectionName,
				FieldsData:     fieldDatas,
				NumRows:        uint32(nb),
			},

			result: &milvuspb.MutationResult{
				Status: merr.Success(),
				IDs: &schemapb.IDs{
					IdField: nil,
				},
			},
			idAllocator:   idAllocator,
			segIDAssigner: segAllocator,
			chMgr:         chMgr,
			chTicker:      ticker,
		}

		// don't support specify partition name if use partition key
		ut.req.PartitionName = partitionNames[0]
		assert.Error(t, ut.PreExecute(ctx))

		ut.req.PartitionName = ""
		assert.NoError(t, ut.OnEnqueue())
		assert.NoError(t, ut.PreExecute(ctx))
		assert.NoError(t, ut.Execute(ctx))
		assert.NoError(t, ut.PostExecute(ctx))
	})

	t.Run("delete", func(t *testing.T) {
		dt := &deleteTask{
			Condition: NewTaskCondition(ctx),
			req: &milvuspb.DeleteRequest{
				CollectionName: collectionName,
				Expr:           "int64_field in [0, 1]",
			},
			ctx: ctx,
			primaryKeys: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{0, 1}}},
			},
			idAllocator:  idAllocator,
			chMgr:        chMgr,
			chTicker:     ticker,
			collectionID: collectionID,
			vChannels:    []string{"test-channel"},
		}

		dt.req.PartitionName = ""
		assert.NoError(t, dt.PreExecute(ctx))
		assert.NoError(t, dt.Execute(ctx))
		assert.NoError(t, dt.PostExecute(ctx))
	})

	t.Run("search", func(t *testing.T) {
		searchTask := &searchTask{
			ctx: ctx,
			SearchRequest: &internalpb.SearchRequest{
				Base: &commonpb.MsgBase{},
			},
			request: &milvuspb.SearchRequest{
				CollectionName: collectionName,
				Nq:             1,
			},
			mixCoord: qc,
			tr:       timerecord.NewTimeRecorder("test-search"),
		}

		// don't support specify partition name if use partition key
		searchTask.request.PartitionNames = partitionNames
		err = searchTask.PreExecute(ctx)
		assert.Error(t, err)
	})

	t.Run("query", func(t *testing.T) {
		queryTask := &queryTask{
			ctx: ctx,
			RetrieveRequest: &internalpb.RetrieveRequest{
				Base: &commonpb.MsgBase{},
			},
			request: &milvuspb.QueryRequest{
				CollectionName: collectionName,
			},
			mixCoord: qc,
		}

		// don't support specify partition name if use partition key
		queryTask.request.PartitionNames = partitionNames
		err = queryTask.PreExecute(ctx)
		assert.Error(t, err)
	})
}

func TestDefaultPartition(t *testing.T) {
	qc := NewMixCoordMock()
	ctx := context.Background()

	mgr := newShardClientMgr()
	err := InitMetaCache(ctx, qc, mgr)
	assert.NoError(t, err)

	shardsNum := common.DefaultShardsNum
	prefix := "TestInsertTaskWithPartitionKey"
	collectionName := prefix + funcutil.GenRandomStr()

	fieldName2Type := make(map[string]schemapb.DataType)
	fieldName2Type["int64_field"] = schemapb.DataType_Int64
	fieldName2Type["varChar_field"] = schemapb.DataType_VarChar
	fieldName2Type["fvec_field"] = schemapb.DataType_FloatVector
	schema := constructCollectionSchemaByDataType(collectionName, fieldName2Type, "int64_field", false)
	marshaledSchema, err := proto.Marshal(schema)
	assert.NoError(t, err)

	t.Run("create collection", func(t *testing.T) {
		createCollectionTask := &createCollectionTask{
			Condition: NewTaskCondition(ctx),
			CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgID:     UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()),
					Timestamp: Timestamp(time.Now().UnixNano()),
				},
				DbName:         "",
				CollectionName: collectionName,
				Schema:         marshaledSchema,
				ShardsNum:      shardsNum,
			},
			ctx:      ctx,
			mixCoord: qc,
			result:   nil,
			schema:   nil,
		}
		err = createCollectionTask.PreExecute(ctx)
		assert.NoError(t, err)
		err = createCollectionTask.Execute(ctx)
		assert.NoError(t, err)
	})

	collectionID, err := globalMetaCache.GetCollectionID(ctx, GetCurDBNameFromContextOrDefault(ctx), collectionName)
	assert.NoError(t, err)

	dmlChannelsFunc := getDmlChannelsFunc(ctx, qc)
	factory := newSimpleMockMsgStreamFactory()
	chMgr := newChannelsMgrImpl(dmlChannelsFunc, nil, factory)
	defer chMgr.removeAllDMLStream()

	_, err = chMgr.getOrCreateDmlStream(ctx, collectionID)
	assert.NoError(t, err)
	pchans, err := chMgr.getChannels(collectionID)
	assert.NoError(t, err)

	interval := time.Millisecond * 10
	tso := newMockTsoAllocator()

	ticker := newChannelsTimeTicker(ctx, interval, []string{}, newGetStatisticsFunc(pchans), tso)
	_ = ticker.start()
	defer ticker.close()

	idAllocator, err := allocator.NewIDAllocator(ctx, qc, paramtable.GetNodeID())
	assert.NoError(t, err)
	_ = idAllocator.Start()
	defer idAllocator.Close()

	segAllocator, err := newSegIDAssigner(ctx, &mockDataCoord{expireTime: Timestamp(2500)}, getLastTick1)
	assert.NoError(t, err)
	segAllocator.Init()
	_ = segAllocator.Start()
	defer segAllocator.Close()

	nb := 10
	fieldID := common.StartOfUserFieldID
	fieldDatas := make([]*schemapb.FieldData, 0)
	for fieldName, dataType := range fieldName2Type {
		fieldData := generateFieldData(dataType, fieldName, nb)
		fieldData.FieldId = int64(fieldID)
		fieldDatas = append(fieldDatas, generateFieldData(dataType, fieldName, nb))
		fieldID++
	}

	t.Run("Insert", func(t *testing.T) {
		it := &insertTask{
			insertMsg: &BaseInsertTask{
				BaseMsg: msgstream.BaseMsg{},
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType:  commonpb.MsgType_Insert,
						MsgID:    0,
						SourceID: paramtable.GetNodeID(),
					},
					CollectionName: collectionName,
					FieldsData:     fieldDatas,
					NumRows:        uint64(nb),
					Version:        msgpb.InsertDataVersion_ColumnBased,
				},
			},

			Condition: NewTaskCondition(ctx),
			ctx:       ctx,
			result: &milvuspb.MutationResult{
				Status:       merr.Success(),
				IDs:          nil,
				SuccIndex:    nil,
				ErrIndex:     nil,
				Acknowledged: false,
				InsertCnt:    0,
				DeleteCnt:    0,
				UpsertCnt:    0,
				Timestamp:    0,
			},
			idAllocator:   idAllocator,
			segIDAssigner: segAllocator,
			chMgr:         chMgr,
			chTicker:      ticker,
			vChannels:     nil,
			pChannels:     nil,
			schema:        nil,
		}

		it.insertMsg.PartitionName = ""
		assert.NoError(t, it.OnEnqueue())
		assert.NoError(t, it.PreExecute(ctx))
		assert.NoError(t, it.Execute(ctx))
		assert.NoError(t, it.PostExecute(ctx))
	})

	t.Run("Upsert", func(t *testing.T) {
		hash := testutils.GenerateHashKeys(nb)
		ut := &upsertTask{
			ctx:       ctx,
			Condition: NewTaskCondition(ctx),
			baseMsg: msgstream.BaseMsg{
				HashValues: hash,
			},
			req: &milvuspb.UpsertRequest{
				Base: commonpbutil.NewMsgBase(
					commonpbutil.WithMsgType(commonpb.MsgType_Upsert),
					commonpbutil.WithSourceID(paramtable.GetNodeID()),
				),
				CollectionName: collectionName,
				FieldsData:     fieldDatas,
				NumRows:        uint32(nb),
			},

			result: &milvuspb.MutationResult{
				Status: merr.Success(),
				IDs: &schemapb.IDs{
					IdField: nil,
				},
			},
			idAllocator:   idAllocator,
			segIDAssigner: segAllocator,
			chMgr:         chMgr,
			chTicker:      ticker,
		}

		ut.req.PartitionName = ""
		assert.NoError(t, ut.OnEnqueue())
		assert.NoError(t, ut.PreExecute(ctx))
		assert.NoError(t, ut.Execute(ctx))
		assert.NoError(t, ut.PostExecute(ctx))
	})

	t.Run("delete", func(t *testing.T) {
		dt := &deleteTask{
			Condition: NewTaskCondition(ctx),
			req: &milvuspb.DeleteRequest{
				CollectionName: collectionName,
				Expr:           "int64_field in [0, 1]",
			},
			ctx: ctx,
			primaryKeys: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{0, 1}}},
			},
			idAllocator:  idAllocator,
			chMgr:        chMgr,
			chTicker:     ticker,
			collectionID: collectionID,
			vChannels:    []string{"test-channel"},
		}

		dt.req.PartitionName = ""
		assert.NoError(t, dt.PreExecute(ctx))
		assert.NoError(t, dt.Execute(ctx))
		assert.NoError(t, dt.PostExecute(ctx))
	})
}

func TestClusteringKey(t *testing.T) {
	qc := NewMixCoordMock()

	ctx := context.Background()

	mgr := newShardClientMgr()
	err := InitMetaCache(ctx, qc, mgr)
	assert.NoError(t, err)

	shardsNum := common.DefaultShardsNum
	prefix := "TestClusteringKey"
	collectionName := prefix + funcutil.GenRandomStr()

	t.Run("create collection normal", func(t *testing.T) {
		fieldName2Type := make(map[string]schemapb.DataType)
		fieldName2Type["int64_field"] = schemapb.DataType_Int64
		fieldName2Type["varChar_field"] = schemapb.DataType_VarChar
		schema := constructCollectionSchemaByDataType(collectionName, fieldName2Type, "int64_field", false)
		fieldName2Type["cluster_key_field"] = schemapb.DataType_Int64
		clusterKeyField := &schemapb.FieldSchema{
			Name:            "cluster_key_field",
			DataType:        schemapb.DataType_Int64,
			IsClusteringKey: true,
		}
		schema.Fields = append(schema.Fields, clusterKeyField)
		vecField := &schemapb.FieldSchema{
			Name:     "fvec_field",
			DataType: schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   common.DimKey,
					Value: strconv.Itoa(testVecDim),
				},
			},
		}
		schema.Fields = append(schema.Fields, vecField)
		marshaledSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)

		createCollectionTask := &createCollectionTask{
			Condition: NewTaskCondition(ctx),
			CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgID:     UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()),
					Timestamp: Timestamp(time.Now().UnixNano()),
				},
				DbName:         "",
				CollectionName: collectionName,
				Schema:         marshaledSchema,
				ShardsNum:      shardsNum,
			},
			ctx:      ctx,
			mixCoord: qc,
			result:   nil,
			schema:   nil,
		}
		err = createCollectionTask.PreExecute(ctx)
		assert.NoError(t, err)
		err = createCollectionTask.Execute(ctx)
		assert.NoError(t, err)
	})

	t.Run("create collection not support more than one clustering key", func(t *testing.T) {
		fieldName2Type := make(map[string]schemapb.DataType)
		fieldName2Type["int64_field"] = schemapb.DataType_Int64
		fieldName2Type["varChar_field"] = schemapb.DataType_VarChar
		schema := constructCollectionSchemaByDataType(collectionName, fieldName2Type, "int64_field", false)
		fieldName2Type["cluster_key_field"] = schemapb.DataType_Int64
		clusterKeyField := &schemapb.FieldSchema{
			Name:            "cluster_key_field",
			DataType:        schemapb.DataType_Int64,
			IsClusteringKey: true,
		}
		schema.Fields = append(schema.Fields, clusterKeyField)
		clusterKeyField2 := &schemapb.FieldSchema{
			Name:            "cluster_key_field2",
			DataType:        schemapb.DataType_Int64,
			IsClusteringKey: true,
		}
		schema.Fields = append(schema.Fields, clusterKeyField2)
		marshaledSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)

		createCollectionTask := &createCollectionTask{
			Condition: NewTaskCondition(ctx),
			CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgID:     UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()),
					Timestamp: Timestamp(time.Now().UnixNano()),
				},
				DbName:         "",
				CollectionName: collectionName,
				Schema:         marshaledSchema,
				ShardsNum:      shardsNum,
			},
			ctx:      ctx,
			mixCoord: qc,
			result:   nil,
			schema:   nil,
		}
		err = createCollectionTask.PreExecute(ctx)
		assert.Error(t, err)
	})

	t.Run("create collection with vector clustering key", func(t *testing.T) {
		fieldName2Type := make(map[string]schemapb.DataType)
		fieldName2Type["int64_field"] = schemapb.DataType_Int64
		fieldName2Type["varChar_field"] = schemapb.DataType_VarChar
		schema := constructCollectionSchemaByDataType(collectionName, fieldName2Type, "int64_field", false)
		clusterKeyField := &schemapb.FieldSchema{
			Name:            "vec_field",
			DataType:        schemapb.DataType_FloatVector,
			IsClusteringKey: true,
		}
		schema.Fields = append(schema.Fields, clusterKeyField)
		marshaledSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)

		createCollectionTask := &createCollectionTask{
			Condition: NewTaskCondition(ctx),
			CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgID:     UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()),
					Timestamp: Timestamp(time.Now().UnixNano()),
				},
				DbName:         "",
				CollectionName: collectionName,
				Schema:         marshaledSchema,
				ShardsNum:      shardsNum,
			},
			ctx:      ctx,
			mixCoord: qc,
			result:   nil,
			schema:   nil,
		}
		err = createCollectionTask.PreExecute(ctx)
		assert.Error(t, err)
	})
}

func TestAlterCollectionCheckLoaded(t *testing.T) {
	qc := NewMixCoordMock()
	InitMetaCache(context.Background(), qc, nil)
	collectionName := "test_alter_collection_check_loaded"
	createColReq := &milvuspb.CreateCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_DropCollection,
			MsgID:     100,
			Timestamp: 100,
		},
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         nil,
		ShardsNum:      1,
	}
	qc.CreateCollection(context.Background(), createColReq)
	resp, err := qc.DescribeCollection(context.Background(), &milvuspb.DescribeCollectionRequest{CollectionName: collectionName})
	assert.NoError(t, err)

	qc.ShowLoadCollectionsFunc = func(ctx context.Context, req *querypb.ShowCollectionsRequest, opts ...grpc.CallOption) (*querypb.ShowCollectionsResponse, error) {
		return &querypb.ShowCollectionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			CollectionIDs:       []int64{resp.CollectionID},
			InMemoryPercentages: []int64{100},
		}, nil
	}

	task := &alterCollectionTask{
		AlterCollectionRequest: &milvuspb.AlterCollectionRequest{
			Base:           &commonpb.MsgBase{},
			CollectionName: collectionName,
			Properties:     []*commonpb.KeyValuePair{{Key: common.MmapEnabledKey, Value: "true"}},
		},
		mixCoord: qc,
	}
	err = task.PreExecute(context.Background())
	assert.Equal(t, merr.Code(merr.ErrCollectionLoaded), merr.Code(err))
}

func TestTaskPartitionKeyIsolation(t *testing.T) {
	qc := NewMixCoordMock()
	ctx := context.Background()
	mgr := newShardClientMgr()
	err := InitMetaCache(ctx, qc, mgr)
	assert.NoError(t, err)
	shardsNum := common.DefaultShardsNum
	prefix := "TestPartitionKeyIsolation"
	collectionName := prefix + funcutil.GenRandomStr()

	getSchema := func(colName string, hasPartitionKey bool) *schemapb.CollectionSchema {
		fieldName2Type := make(map[string]schemapb.DataType)
		fieldName2Type["fvec_field"] = schemapb.DataType_FloatVector
		fieldName2Type["varChar_field"] = schemapb.DataType_VarChar
		fieldName2Type["int64_field"] = schemapb.DataType_Int64
		schema := constructCollectionSchemaByDataType(colName, fieldName2Type, "int64_field", false)
		if hasPartitionKey {
			partitionKeyField := &schemapb.FieldSchema{
				Name:           "partition_key_field",
				DataType:       schemapb.DataType_Int64,
				IsPartitionKey: true,
			}
			fieldName2Type["partition_key_field"] = schemapb.DataType_Int64
			schema.Fields = append(schema.Fields, partitionKeyField)
		}
		return schema
	}

	getCollectionTask := func(colName string, isIso bool, marshaledSchema []byte) *createCollectionTask {
		isoStr := "false"
		if isIso {
			isoStr = "true"
		}

		return &createCollectionTask{
			Condition: NewTaskCondition(ctx),
			CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgID:     UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()),
					Timestamp: Timestamp(time.Now().UnixNano()),
				},
				DbName:         "",
				CollectionName: colName,
				Schema:         marshaledSchema,
				ShardsNum:      shardsNum,
				Properties:     []*commonpb.KeyValuePair{{Key: common.PartitionKeyIsolationKey, Value: isoStr}},
			},
			ctx:      ctx,
			mixCoord: qc,
			result:   nil,
			schema:   nil,
		}
	}

	createIsoCollection := func(colName string, hasPartitionKey bool, isIsolation bool, isIsoNil bool) {
		isoStr := "false"
		if isIsolation {
			isoStr = "true"
		}
		schema := getSchema(colName, hasPartitionKey)
		marshaledSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		createColReq := &milvuspb.CreateCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DropCollection,
				MsgID:     100,
				Timestamp: 100,
			},
			DbName:         dbName,
			CollectionName: colName,
			Schema:         marshaledSchema,
			ShardsNum:      1,
			Properties:     []*commonpb.KeyValuePair{{Key: common.PartitionKeyIsolationKey, Value: isoStr}},
		}
		if isIsoNil {
			createColReq.Properties = nil
		}

		stats, err := qc.CreateCollection(ctx, createColReq)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, stats.ErrorCode)
	}

	getAlterCollectionTask := func(colName string, isIsolation bool) *alterCollectionTask {
		isoStr := "false"
		if isIsolation {
			isoStr = "true"
		}

		return &alterCollectionTask{
			AlterCollectionRequest: &milvuspb.AlterCollectionRequest{
				Base:           &commonpb.MsgBase{},
				CollectionName: colName,
				Properties:     []*commonpb.KeyValuePair{{Key: common.PartitionKeyIsolationKey, Value: isoStr}},
			},
			mixCoord: qc,
		}
	}

	t.Run("create collection valid", func(t *testing.T) {
		paramtable.Get().CommonCfg.EnableMaterializedView.SwapTempValue("true")
		defer paramtable.Get().CommonCfg.EnableMaterializedView.SwapTempValue("false")
		schema := getSchema(collectionName, true)
		marshaledSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)

		createCollectionTask := getCollectionTask(collectionName, true, marshaledSchema)
		err = createCollectionTask.PreExecute(ctx)
		assert.NoError(t, err)
		err = createCollectionTask.Execute(ctx)
		assert.NoError(t, err)
	})

	t.Run("create collection without isolation", func(t *testing.T) {
		schema := getSchema(collectionName, true)
		marshaledSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)

		createCollectionTask := getCollectionTask(collectionName, false, marshaledSchema)
		err = createCollectionTask.PreExecute(ctx)
		assert.NoError(t, err)
		err = createCollectionTask.Execute(ctx)
		assert.NoError(t, err)
	})

	t.Run("create collection isolation but no partition key", func(t *testing.T) {
		schema := getSchema(collectionName, false)
		marshaledSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)

		createCollectionTask := getCollectionTask(collectionName, true, marshaledSchema)
		assert.ErrorContains(t, createCollectionTask.PreExecute(ctx), "partition key isolation mode is enabled but no partition key field is set")
	})

	t.Run("create collection with isolation and partition key but MV is not enabled", func(t *testing.T) {
		paramtable.Get().CommonCfg.EnableMaterializedView.SwapTempValue("false")
		schema := getSchema(collectionName, true)
		marshaledSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)

		createCollectionTask := getCollectionTask(collectionName, true, marshaledSchema)
		assert.ErrorContains(t, createCollectionTask.PreExecute(ctx), "partition key isolation mode is enabled but current Milvus does not support it")
	})

	t.Run("alter collection from valid", func(t *testing.T) {
		paramtable.Get().CommonCfg.EnableMaterializedView.SwapTempValue("true")
		defer paramtable.Get().CommonCfg.EnableMaterializedView.SwapTempValue("false")
		colName := collectionName + "AlterValid"
		createIsoCollection(colName, true, false, false)
		alterTask := getAlterCollectionTask(colName, true)
		err := alterTask.PreExecute(ctx)
		assert.NoError(t, err)
	})

	t.Run("alter collection without isolation", func(t *testing.T) {
		colName := collectionName + "AlterNoIso"
		createIsoCollection(colName, true, false, true)
		alterTask := alterCollectionTask{
			AlterCollectionRequest: &milvuspb.AlterCollectionRequest{
				Base:           &commonpb.MsgBase{},
				CollectionName: colName,
				Properties:     nil,
			},
			mixCoord: qc,
		}
		err := alterTask.PreExecute(ctx)
		assert.NoError(t, err)
	})

	t.Run("alter collection isolation but no partition key", func(t *testing.T) {
		colName := collectionName + "AlterNoPartkey"
		createIsoCollection(colName, false, false, false)
		alterTask := getAlterCollectionTask(colName, true)
		assert.ErrorContains(t, alterTask.PreExecute(ctx), "partition key isolation mode is enabled but no partition key field is set")
	})

	t.Run("alter collection with isolation and partition key but MV is not enabled", func(t *testing.T) {
		paramtable.Get().CommonCfg.EnableMaterializedView.SwapTempValue("false")
		colName := collectionName + "AlterNoMv"
		createIsoCollection(colName, true, false, false)
		alterTask := getAlterCollectionTask(colName, true)
		assert.ErrorContains(t, alterTask.PreExecute(ctx), "partition key isolation mode is enabled but current Milvus does not support it")
	})

	t.Run("alter collection with vec index and isolation", func(t *testing.T) {
		paramtable.Get().CommonCfg.EnableMaterializedView.SwapTempValue("true")
		defer paramtable.Get().CommonCfg.EnableMaterializedView.SwapTempValue("false")
		colName := collectionName + "AlterVecIndex"
		createIsoCollection(colName, true, true, false)
		resp, err := qc.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{DbName: dbName, CollectionName: colName})
		assert.NoError(t, err)
		var vecFieldID int64 = 0
		for _, field := range resp.Schema.Fields {
			if field.DataType == schemapb.DataType_FloatVector {
				vecFieldID = field.FieldID
				break
			}
		}
		assert.NotEqual(t, vecFieldID, int64(0))
		qc.DescribeIndexFunc = func(ctx context.Context, request *indexpb.DescribeIndexRequest, opts ...grpc.CallOption) (*indexpb.DescribeIndexResponse, error) {
			return &indexpb.DescribeIndexResponse{
				Status: merr.Success(),
				IndexInfos: []*indexpb.IndexInfo{
					{
						FieldID: vecFieldID,
					},
				},
			}, nil
		}
		alterTask := getAlterCollectionTask(colName, false)
		assert.ErrorContains(t, alterTask.PreExecute(ctx),
			"can not alter partition key isolation mode if the collection already has a vector index. Please drop the index first")
	})
}

func TestAlterCollectionForReplicateProperty(t *testing.T) {
	cache := globalMetaCache
	defer func() { globalMetaCache = cache }()
	mockCache := NewMockCache(t)
	mockCache.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&collectionInfo{
		replicateID: "local-mac-1",
	}, nil).Maybe()
	mockCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(1, nil).Maybe()
	mockCache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).Return(&schemaInfo{}, nil).Maybe()
	globalMetaCache = mockCache
	ctx := context.Background()
	mockRootcoord := mocks.NewMockMixCoordClient(t)
	t.Run("invalid replicate id", func(t *testing.T) {
		task := &alterCollectionTask{
			AlterCollectionRequest: &milvuspb.AlterCollectionRequest{
				Properties: []*commonpb.KeyValuePair{
					{
						Key:   common.ReplicateIDKey,
						Value: "xxxxx",
					},
				},
			},
			mixCoord: mockRootcoord,
		}

		err := task.PreExecute(ctx)
		assert.Error(t, err)
	})

	t.Run("empty replicate id", func(t *testing.T) {
		task := &alterCollectionTask{
			AlterCollectionRequest: &milvuspb.AlterCollectionRequest{
				CollectionName: "test",
				Properties: []*commonpb.KeyValuePair{
					{
						Key:   common.ReplicateIDKey,
						Value: "",
					},
				},
			},
			mixCoord: mockRootcoord,
		}

		err := task.PreExecute(ctx)
		assert.Error(t, err)
	})

	t.Run("fail to alloc ts", func(t *testing.T) {
		task := &alterCollectionTask{
			AlterCollectionRequest: &milvuspb.AlterCollectionRequest{
				CollectionName: "test",
				Properties: []*commonpb.KeyValuePair{
					{
						Key:   common.ReplicateEndTSKey,
						Value: "100",
					},
				},
			},
			mixCoord: mockRootcoord,
		}

		mockRootcoord.EXPECT().AllocTimestamp(mock.Anything, mock.Anything).Return(nil, errors.New("err")).Once()
		err := task.PreExecute(ctx)
		assert.Error(t, err)
	})

	t.Run("alloc wrong ts", func(t *testing.T) {
		task := &alterCollectionTask{
			AlterCollectionRequest: &milvuspb.AlterCollectionRequest{
				CollectionName: "test",
				Properties: []*commonpb.KeyValuePair{
					{
						Key:   common.ReplicateEndTSKey,
						Value: "100",
					},
				},
			},
			mixCoord: mockRootcoord,
		}

		mockRootcoord.EXPECT().AllocTimestamp(mock.Anything, mock.Anything).Return(&rootcoordpb.AllocTimestampResponse{
			Status:    merr.Success(),
			Timestamp: 99,
		}, nil).Once()
		err := task.PreExecute(ctx)
		assert.Error(t, err)
	})
}

func TestInsertForReplicate(t *testing.T) {
	cache := globalMetaCache
	defer func() { globalMetaCache = cache }()
	mockCache := NewMockCache(t)
	globalMetaCache = mockCache

	t.Run("get replicate id fail", func(t *testing.T) {
		mockCache.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("err")).Once()
		task := &insertTask{
			insertMsg: &msgstream.InsertMsg{
				InsertRequest: &msgpb.InsertRequest{
					CollectionName: "foo",
				},
			},
		}
		err := task.PreExecute(context.Background())
		assert.Error(t, err)
	})
	t.Run("insert with replicate id", func(t *testing.T) {
		mockCache.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&collectionInfo{
			schema: &schemaInfo{
				CollectionSchema: &schemapb.CollectionSchema{
					Properties: []*commonpb.KeyValuePair{
						{
							Key:   common.ReplicateIDKey,
							Value: "local-mac",
						},
					},
				},
			},
			replicateID: "local-mac",
		}, nil).Once()
		task := &insertTask{
			insertMsg: &msgstream.InsertMsg{
				InsertRequest: &msgpb.InsertRequest{
					CollectionName: "foo",
				},
			},
		}
		err := task.PreExecute(context.Background())
		assert.Error(t, err)
	})
}

func TestAlterCollectionFieldCheckLoaded(t *testing.T) {
	qc := NewMixCoordMock()
	InitMetaCache(context.Background(), qc, nil)
	collectionName := "test_alter_collection_field_check_loaded"
	createColReq := &milvuspb.CreateCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_DropCollection,
			MsgID:     100,
			Timestamp: 100,
		},
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         nil,
		ShardsNum:      1,
	}
	qc.CreateCollection(context.Background(), createColReq)
	resp, err := qc.DescribeCollection(context.Background(), &milvuspb.DescribeCollectionRequest{CollectionName: collectionName})
	assert.NoError(t, err)

	qc.ShowLoadCollectionsFunc = func(ctx context.Context, req *querypb.ShowCollectionsRequest, opts ...grpc.CallOption) (*querypb.ShowCollectionsResponse, error) {
		return &querypb.ShowCollectionsResponse{
			Status:              &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			CollectionIDs:       []int64{resp.CollectionID},
			InMemoryPercentages: []int64{100},
		}, nil
	}

	// update property "mmap.enabled" but the collection is loaded
	task := &alterCollectionFieldTask{
		AlterCollectionFieldRequest: &milvuspb.AlterCollectionFieldRequest{
			Base:           &commonpb.MsgBase{},
			CollectionName: collectionName,
			Properties:     []*commonpb.KeyValuePair{{Key: common.MmapEnabledKey, Value: "true"}},
		},
		mixCoord: qc,
	}
	err = task.PreExecute(context.Background())
	assert.Equal(t, merr.Code(merr.ErrCollectionLoaded), merr.Code(err))

	// delete property "mmap.enabled" but the collection is loaded
	task = &alterCollectionFieldTask{
		AlterCollectionFieldRequest: &milvuspb.AlterCollectionFieldRequest{
			Base:           &commonpb.MsgBase{},
			CollectionName: collectionName,
			DeleteKeys:     []string{common.MmapEnabledKey},
		},
		mixCoord: qc,
	}
	err = task.PreExecute(context.Background())
	assert.Equal(t, merr.Code(merr.ErrCollectionLoaded), merr.Code(err))
}

func TestAlterCollectionField(t *testing.T) {
	qc := NewMixCoordMock()
	InitMetaCache(context.Background(), qc, nil)
	collectionName := "test_alter_collection_field"

	// Create collection with string and array fields
	schema := &schemapb.CollectionSchema{
		Name: collectionName,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:    100,
				Name:       "string_field",
				DataType:   schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "100"}},
			},
			{
				FieldID:     101,
				Name:        "array_field",
				DataType:    schemapb.DataType_Array,
				ElementType: schemapb.DataType_Int64,
				TypeParams:  []*commonpb.KeyValuePair{{Key: "max_capacity", Value: "100"}},
			},
		},
	}
	schemaBytes, err := proto.Marshal(schema)
	assert.NoError(t, err)

	createColReq := &milvuspb.CreateCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_CreateCollection,
			MsgID:     100,
			Timestamp: 100,
		},
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         schemaBytes,
		ShardsNum:      1,
	}
	qc.CreateCollection(context.Background(), createColReq)

	// The collection is not loaded
	qc.ShowLoadCollectionsFunc = func(ctx context.Context, req *querypb.ShowCollectionsRequest, opts ...grpc.CallOption) (*querypb.ShowCollectionsResponse, error) {
		return &querypb.ShowCollectionsResponse{
			Status:              &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			CollectionIDs:       []int64{},
			InMemoryPercentages: []int64{},
		}, nil
	}

	// Test cases
	// 1. the collection is not loaded, updating properties is allowed, deleting mmap.enabled/mmap_enabled is allowed
	// 2. max_length can only be updated on varchar field or arrar field with varchar element
	// 3. max_capacity can only be updated on array field
	// 4. invalid number for max_length/max_capacity is not allowed
	// 5. not allow to delete max_length/max_capacity
	tests := []struct {
		name        string
		fieldName   string
		properties  []*commonpb.KeyValuePair
		deleteKeys  []string
		expectError bool
		errCode     int32
	}{
		{
			name:      "update string field max_length",
			fieldName: "string_field",
			properties: []*commonpb.KeyValuePair{
				{Key: common.MaxLengthKey, Value: "200"},
			},
			expectError: false,
		},
		{
			name:      "update array field max_capacity",
			fieldName: "array_field",
			properties: []*commonpb.KeyValuePair{
				{Key: common.MaxCapacityKey, Value: "200"},
			},
			expectError: false,
		},
		{
			name:      "update mmap_enabled",
			fieldName: "string_field",
			properties: []*commonpb.KeyValuePair{
				{Key: common.MmapEnabledKey, Value: "true"},
			},
			expectError: false,
		},
		{
			name:      "invalid property key",
			fieldName: "string_field",
			properties: []*commonpb.KeyValuePair{
				{Key: "invalid_key", Value: "value"},
			},
			expectError: true,
			errCode:     merr.Code(merr.ErrParameterInvalid),
		},
		{
			name:      "invalid max_length value type",
			fieldName: "string_field",
			properties: []*commonpb.KeyValuePair{
				{Key: common.MaxLengthKey, Value: "not_a_number"},
			},
			expectError: true,
			errCode:     merr.Code(merr.ErrParameterInvalid),
		},
		{
			name:      "invalid max_capacity value type",
			fieldName: "array_field",
			properties: []*commonpb.KeyValuePair{
				{Key: common.MaxCapacityKey, Value: "not_a_number"},
			},
			expectError: true,
			errCode:     merr.Code(merr.ErrParameterInvalid),
		},
		{
			name:      "max_length exceeds limit",
			fieldName: "string_field",
			properties: []*commonpb.KeyValuePair{
				{Key: common.MaxLengthKey, Value: "65536"},
			},
			expectError: true,
			errCode:     merr.Code(merr.ErrParameterInvalid),
		},
		{
			name:      "max_capacity exceeds limit",
			fieldName: "array_field",
			properties: []*commonpb.KeyValuePair{
				{Key: common.MaxCapacityKey, Value: "5000"},
			},
			expectError: true,
			errCode:     merr.Code(merr.ErrParameterInvalid),
		},
		{
			name:      "max_capacity invalid range",
			fieldName: "array_field",
			properties: []*commonpb.KeyValuePair{
				{Key: common.MaxCapacityKey, Value: "0"},
			},
			expectError: true,
			errCode:     merr.Code(merr.ErrParameterInvalid),
		},
		{
			name:      "max_capacity invalid type",
			fieldName: "string_field",
			properties: []*commonpb.KeyValuePair{
				{Key: common.MaxCapacityKey, Value: "3"},
			},
			expectError: true,
			errCode:     merr.Code(merr.ErrParameterInvalid),
		},
		{
			name:      "not allow to update max_length on non-varchar field",
			fieldName: "array_field",
			properties: []*commonpb.KeyValuePair{
				{Key: common.MaxLengthKey, Value: "10"},
			},
			expectError: true,
			errCode:     merr.Code(merr.ErrParameterInvalid),
		},
		{
			name:        "delete mmap.enabled is allowed",
			fieldName:   "string_field",
			deleteKeys:  []string{common.MmapEnabledKey},
			expectError: false,
		},
		{
			name:        "delete mmap_enabled is allowed",
			fieldName:   "string_field",
			deleteKeys:  []string{MmapEnabledKey},
			expectError: false,
		},
		{
			name:        "delete max_length is not allowed",
			fieldName:   "string_field",
			deleteKeys:  []string{common.MaxLengthKey},
			expectError: true,
			errCode:     merr.Code(merr.ErrParameterInvalid),
		},
		{
			name:        "delete max_capacity is not allowed",
			fieldName:   "array_field",
			deleteKeys:  []string{common.MaxCapacityKey},
			expectError: true,
			errCode:     merr.Code(merr.ErrParameterInvalid),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			task := &alterCollectionFieldTask{
				AlterCollectionFieldRequest: &milvuspb.AlterCollectionFieldRequest{
					Base:           &commonpb.MsgBase{},
					CollectionName: collectionName,
					FieldName:      test.fieldName,
					Properties:     test.properties,
					DeleteKeys:     test.deleteKeys,
				},
				mixCoord: qc,
			}

			err := task.PreExecute(context.Background())
			if test.expectError {
				assert.Error(t, err)
				if test.errCode != 0 {
					assert.Equal(t, test.errCode, merr.Code(err))
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// constructCollectionSchemaWithStructArrayField constructs a collection schema specifically for testing StructArrayField
func constructCollectionSchemaWithStructArrayField(collectionName string, structArrayFieldName string, autoID bool) *schemapb.CollectionSchema {
	// Primary key field
	pkField := &schemapb.FieldSchema{
		FieldID:      100,
		Name:         testInt64Field,
		IsPrimaryKey: true,
		Description:  "primary key field",
		DataType:     schemapb.DataType_Int64,
		AutoID:       autoID,
	}

	// Vector field
	vecField := &schemapb.FieldSchema{
		FieldID:      101,
		Name:         testFloatVecField,
		IsPrimaryKey: false,
		Description:  "float vector field",
		DataType:     schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: strconv.Itoa(testVecDim),
			},
		},
	}

	// StructArrayField with various sub-fields for comprehensive testing
	structArrayField := &schemapb.StructArrayFieldSchema{
		FieldID:     102,
		Name:        structArrayFieldName,
		Description: "struct array field for testing",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:     1021,
				Name:        "sub_text_array",
				DataType:    schemapb.DataType_Array,
				ElementType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.MaxLengthKey,
						Value: strconv.Itoa(testMaxVarCharLength),
					},
					{
						Key:   common.MaxCapacityKey,
						Value: "50",
					},
				},
			},
			{
				FieldID:     1022,
				Name:        "sub_int_array",
				DataType:    schemapb.DataType_Array,
				ElementType: schemapb.DataType_Int32,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.MaxCapacityKey,
						Value: "20",
					},
				},
			},
			{
				FieldID:     1023,
				Name:        "sub_float_vector_array",
				DataType:    schemapb.DataType_ArrayOfVector,
				ElementType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: strconv.Itoa(testVecDim),
					},
					{
						Key:   common.MaxCapacityKey,
						Value: "5",
					},
				},
			},
		},
	}

	return &schemapb.CollectionSchema{
		Name:              collectionName,
		Description:       "test collection with struct array field",
		AutoID:            autoID,
		Fields:            []*schemapb.FieldSchema{pkField, vecField},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{structArrayField},
	}
}

// TestCreateCollectionTaskWithStructArrayField tests creating collections with StructArrayField
func TestCreateCollectionTaskWithStructArrayField(t *testing.T) {
	mix := NewMixCoordMock()
	ctx := context.Background()
	shardsNum := common.DefaultShardsNum
	prefix := "TestCreateCollectionTaskWithStructArrayField"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()

	// Test with StructArrayField
	schema := constructCollectionSchemaWithStructArrayField(collectionName, testStructArrayField, false)
	marshaledSchema, err := proto.Marshal(schema)
	assert.NoError(t, err)

	task := &createCollectionTask{
		Condition: NewTaskCondition(ctx),
		CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			Schema:         marshaledSchema,
			ShardsNum:      shardsNum,
		},
		ctx:      ctx,
		mixCoord: mix,
		result:   nil,
		schema:   nil,
	}

	t.Run("create collection with struct array field", func(t *testing.T) {
		err := task.OnEnqueue()
		assert.NoError(t, err)

		err = task.PreExecute(ctx)
		assert.NoError(t, err)

		// Verify schema contains StructArrayFields
		assert.NotNil(t, task.schema)
		assert.Len(t, task.schema.StructArrayFields, 1)

		structArrayField := task.schema.StructArrayFields[0]
		assert.Equal(t, testStructArrayField, structArrayField.Name)
		assert.Len(t, structArrayField.Fields, 3)

		// Verify sub-fields in StructArrayField
		subFields := structArrayField.Fields

		// sub_text_array
		assert.Equal(t, "sub_text_array", subFields[0].Name)
		assert.Equal(t, schemapb.DataType_Array, subFields[0].DataType)
		assert.Equal(t, schemapb.DataType_VarChar, subFields[0].ElementType)

		// sub_int_array
		assert.Equal(t, "sub_int_array", subFields[1].Name)
		assert.Equal(t, schemapb.DataType_Array, subFields[1].DataType)
		assert.Equal(t, schemapb.DataType_Int32, subFields[1].ElementType)

		// sub_float_vector_array
		assert.Equal(t, "sub_float_vector_array", subFields[2].Name)
		assert.Equal(t, schemapb.DataType_ArrayOfVector, subFields[2].DataType)
		assert.Equal(t, schemapb.DataType_FloatVector, subFields[2].ElementType)

		err = task.Execute(ctx)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, task.result.ErrorCode)

		err = task.PostExecute(ctx)
		assert.NoError(t, err)
	})

	t.Run("validate struct array field constraints", func(t *testing.T) {
		// Test invalid sub-field in StructArrayField
		invalidSchema := constructCollectionSchemaWithStructArrayField(collectionName+"_invalid", testStructArrayField, false)

		// Add an invalid sub-field (nested array)
		invalidSubField := &schemapb.FieldSchema{
			FieldID:     1024,
			Name:        "invalid_nested_array",
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_Array, // This should be invalid - nested arrays
		}
		invalidSchema.StructArrayFields[0].Fields = append(invalidSchema.StructArrayFields[0].Fields, invalidSubField)

		invalidMarshaledSchema, err := proto.Marshal(invalidSchema)
		assert.NoError(t, err)

		invalidTask := &createCollectionTask{
			Condition: NewTaskCondition(ctx),
			CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
				Base:           nil,
				DbName:         dbName,
				CollectionName: collectionName + "_invalid",
				Schema:         invalidMarshaledSchema,
				ShardsNum:      shardsNum,
			},
			ctx:      ctx,
			mixCoord: mix,
			result:   nil,
			schema:   nil,
		}

		err = invalidTask.OnEnqueue()
		assert.NoError(t, err)

		err = invalidTask.PreExecute(ctx)
		assert.Error(t, err) // Should fail due to nested array validation
	})
}

// TestDescribeCollectionTaskWithStructArrayField tests describing collections with StructArrayField
func TestDescribeCollectionTaskWithStructArrayField(t *testing.T) {
	mix := NewMixCoordMock()
	ctx := context.Background()
	prefix := "TestDescribeCollectionTaskWithStructArrayField"
	collectionName := prefix + funcutil.GenRandomStr()

	// Create collection with StructArrayField first
	schema := constructCollectionSchemaWithStructArrayField(collectionName, testStructArrayField, true)
	marshaledSchema, err := proto.Marshal(schema)
	assert.NoError(t, err)

	createTask := &createCollectionTask{
		Condition: NewTaskCondition(ctx),
		CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
			CollectionName: collectionName,
			Schema:         marshaledSchema,
		},
		ctx:      ctx,
		mixCoord: mix,
	}

	err = createTask.OnEnqueue()
	assert.NoError(t, err)
	err = createTask.PreExecute(ctx)
	assert.NoError(t, err)
	err = createTask.Execute(ctx)
	assert.NoError(t, err)

	// Now test describe collection
	describeTask := &describeCollectionTask{
		Condition: NewTaskCondition(ctx),
		DescribeCollectionRequest: &milvuspb.DescribeCollectionRequest{
			CollectionName: collectionName,
		},
		ctx:      ctx,
		mixCoord: mix,
	}

	t.Run("describe collection with struct array field", func(t *testing.T) {
		err := describeTask.PreExecute(ctx)
		assert.NoError(t, err)

		err = describeTask.Execute(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, describeTask.result)
		assert.Equal(t, commonpb.ErrorCode_Success, describeTask.result.Status.ErrorCode)

		// Verify StructArrayFields are returned in describe response
		resultSchema := describeTask.result.Schema
		assert.NotNil(t, resultSchema)
		assert.Len(t, resultSchema.StructArrayFields, 1)

		structArrayField := resultSchema.StructArrayFields[0]
		assert.Equal(t, testStructArrayField, structArrayField.Name)
		assert.Len(t, structArrayField.Fields, 3)

		err = describeTask.PostExecute(ctx)
		assert.NoError(t, err)
	})
}
