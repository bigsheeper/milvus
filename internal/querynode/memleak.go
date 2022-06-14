package querynode

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	//"runtime/debug"
	//"runtime"
	"net/http"
	_ "net/http/pprof"

	//"sync"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/internal/storage"
	"math"
)

func genCollectionMeta(collectionID UniqueID, schema *schemapb.CollectionSchema) *etcdpb.CollectionMeta {
	colInfo := &etcdpb.CollectionMeta{
		ID:           collectionID,
		Schema:       schema,
		PartitionIDs: []UniqueID{defaultPartitionID},
	}
	return colInfo
}

var uidField = constFieldParam{
	id:        rowIDFieldID,
	dataType:  schemapb.DataType_Int64,
	fieldName: "RowID",
}

var timestampField = constFieldParam{
	id:        timestampFieldID,
	dataType:  schemapb.DataType_Int64,
	fieldName: "Timestamp",
}

var globalCnt int64

func genMsgStreamBaseMsg() msgstream.BaseMsg {
	return msgstream.BaseMsg{
		BeginTimestamp: 0,
		EndTimestamp:   0,
		HashValues:     []uint32{0},
		MsgPosition: &internalpb.MsgPosition{
			ChannelName: "",
			MsgID:       []byte{},
			MsgGroup:    "",
			Timestamp:   10,
		},
	}
}

func genCommonMsgBase(msgType commonpb.MsgType) *commonpb.MsgBase {
	return &commonpb.MsgBase{
		MsgType: msgType,
		MsgID:   rand.Int63(),
	}
}

func newFloatVectorFieldData(fieldName string, numRows, dim int) *schemapb.FieldData {
	return &schemapb.FieldData{
		FieldId:   simpleFloatVecField.id,
		Type:      schemapb.DataType_FloatVector,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: int64(dim),
				Data: &schemapb.VectorField_FloatVector{
					FloatVector: &schemapb.FloatArray{
						Data: generateFloatVectors(numRows, dim),
					},
				},
			},
		},
	}
}

func newBinaryVectorFieldData(fieldName string, numRows, dim int) *schemapb.FieldData {
	return &schemapb.FieldData{
		FieldId:   simpleBinVecField.id,
		Type:      schemapb.DataType_BinaryVector,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: int64(dim),
				Data: &schemapb.VectorField_BinaryVector{
					BinaryVector: generateBinaryVectors(numRows, dim),
				},
			},
		},
	}
}

func newScalarFieldData(dType schemapb.DataType, fieldName string, numRows int) *schemapb.FieldData {
	ret := &schemapb.FieldData{
		Type:      dType,
		FieldName: fieldName,
		Field:     nil,
	}

	switch dType {
	case schemapb.DataType_Bool:
		ret.FieldId = simpleBoolField.id
		ret.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_BoolData{
					BoolData: &schemapb.BoolArray{
						Data: generateBoolArray(numRows),
					},
				},
			},
		}
	case schemapb.DataType_Int8:
		ret.FieldId = simpleInt8Field.id
		ret.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{
						Data: generateInt32Array(numRows),
					},
				},
			},
		}
	case schemapb.DataType_Int16:
		ret.FieldId = simpleInt16Field.id
		ret.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{
						Data: generateInt32Array(numRows),
					},
				},
			},
		}
	case schemapb.DataType_Int32:
		ret.FieldId = simpleInt32Field.id
		ret.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{
						Data: generateInt32Array(numRows),
					},
				},
			},
		}
	case schemapb.DataType_Int64:
		ret.FieldId = simpleInt64Field.id
		ret.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: generateInt64Array(numRows),
					},
				},
			},
		}
	case schemapb.DataType_Float:
		ret.FieldId = simpleFloatField.id
		ret.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_FloatData{
					FloatData: &schemapb.FloatArray{
						Data: generateFloat32Array(numRows),
					},
				},
			},
		}
	case schemapb.DataType_Double:
		ret.FieldId = simpleDoubleField.id
		ret.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_DoubleData{
					DoubleData: &schemapb.DoubleArray{
						Data: generateFloat64Array(numRows),
					},
				},
			},
		}
	case schemapb.DataType_VarChar:
		ret.FieldId = simpleVarCharField.id
		ret.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: generateStringArray(numRows),
					},
				},
			},
		}
	default:
		panic("data type not supported")
	}

	return ret
}

func genSimpleInsertMsg(schema *schemapb.CollectionSchema, numRows int) (*msgstream.InsertMsg, error) {
	fieldsData := make([]*schemapb.FieldData, 0)

	for _, f := range schema.Fields {
		switch f.DataType {
		case schemapb.DataType_Bool:
			fieldsData = append(fieldsData, newScalarFieldData(f.DataType, simpleBoolField.fieldName, numRows))
		case schemapb.DataType_Int8:
			fieldsData = append(fieldsData, newScalarFieldData(f.DataType, simpleInt8Field.fieldName, numRows))
		case schemapb.DataType_Int16:
			fieldsData = append(fieldsData, newScalarFieldData(f.DataType, simpleInt16Field.fieldName, numRows))
		case schemapb.DataType_Int32:
			fieldsData = append(fieldsData, newScalarFieldData(f.DataType, simpleInt32Field.fieldName, numRows))
		case schemapb.DataType_Int64:
			fieldsData = append(fieldsData, newScalarFieldData(f.DataType, simpleInt64Field.fieldName, numRows))
		case schemapb.DataType_Float:
			fieldsData = append(fieldsData, newScalarFieldData(f.DataType, simpleFloatField.fieldName, numRows))
		case schemapb.DataType_Double:
			fieldsData = append(fieldsData, newScalarFieldData(f.DataType, simpleDoubleField.fieldName, numRows))
		case schemapb.DataType_VarChar:
			fieldsData = append(fieldsData, newScalarFieldData(f.DataType, simpleVarCharField.fieldName, numRows))
		case schemapb.DataType_FloatVector:
			dim := simpleFloatVecField.dim // if no dim specified, use simpleFloatVecField's dim
			fieldsData = append(fieldsData, newFloatVectorFieldData(simpleFloatVecField.fieldName, numRows, dim))
		case schemapb.DataType_BinaryVector:
			dim := simpleBinVecField.dim // if no dim specified, use simpleFloatVecField's dim
			fieldsData = append(fieldsData, newBinaryVectorFieldData(simpleBinVecField.fieldName, numRows, dim))
		default:
			err := errors.New("data type not supported")
			return nil, err
		}
	}

	return &msgstream.InsertMsg{
		BaseMsg: genMsgStreamBaseMsg(),
		InsertRequest: internalpb.InsertRequest{
			Base:           genCommonMsgBase(commonpb.MsgType_Insert),
			CollectionName: defaultCollectionName,
			PartitionName:  defaultPartitionName,
			CollectionID:   defaultCollectionID,
			PartitionID:    defaultPartitionID,
			SegmentID:      defaultSegmentID,
			ShardName:      defaultDMLChannel,
			Timestamps:     genSimpleTimestampFieldData(numRows),
			RowIDs:         genSimpleRowIDField(numRows),
			FieldsData:     fieldsData,
			NumRows:        uint64(numRows),
			Version:        internalpb.InsertDataVersion_ColumnBased,
		},
	}, nil
}

func generateBinaryVectors(numRows, dim int) []byte {
	total := (numRows * dim) / 8
	ret := make([]byte, total)
	_, err := rand.Read(ret)
	if err != nil {
		panic(err)
	}
	return ret
}

func generateFloat64Array(numRows int) []float64 {
	ret := make([]float64, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, rand.Float64())
	}
	return ret
}
func generateInt32Array(numRows int) []int32 {
	ret := make([]int32, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, int32(i))
	}
	return ret
}

// ---------- unittest util functions ----------
func generateBoolArray(numRows int) []bool {
	ret := make([]bool, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, rand.Int()%2 == 0)
	}
	return ret
}

func generateInt8Array(numRows int) []int8 {
	ret := make([]int8, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, int8(rand.Int()))
	}
	return ret
}

func generateInt16Array(numRows int) []int16 {
	ret := make([]int16, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, int16(rand.Int()))
	}
	return ret
}

func genSimpleTimestampFieldData(numRows int) []Timestamp {
	times := make([]Timestamp, numRows)
	for i := 0; i < numRows; i++ {
		times[i] = Timestamp(i)
	}
	// timestamp 0 is not allowed
	times[0] = 1
	return times
}

func genTimestampFieldData(numRows int) []int64 {
	times := make([]int64, numRows)
	for i := 0; i < numRows; i++ {
		times[i] = int64(i)
	}
	// timestamp 0 is not allowed
	times[0] = 1
	return times
}

func genSimpleRowIDField(numRows int) []IntPrimaryKey {
	ids := make([]IntPrimaryKey, numRows)
	for i := 0; i < numRows; i++ {
		ids[i] = IntPrimaryKey(i)
	}
	return ids
}

func generateInt64Array(numRows int) []int64 {
	ret := make([]int64, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, int64(i))
	}
	return ret
}

func generateFloat32Array(numRows int) []float32 {
	ret := make([]float32, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, rand.Float32())
	}
	return ret
}

func generateStringArray(numRows int) []string {
	ret := make([]string, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, strconv.Itoa(i))
	}
	return ret
}

func genInsertData(msgLength int, schema *schemapb.CollectionSchema) (*storage.InsertData, error) {
	insertData := &storage.InsertData{
		Data: make(map[int64]storage.FieldData),
	}

	// set data for rowID field
	insertData.Data[rowIDFieldID] = &storage.Int64FieldData{
		NumRows: []int64{int64(msgLength)},
		Data:    generateInt64Array(msgLength),
	}
	// set data for ts field
	insertData.Data[timestampFieldID] = &storage.Int64FieldData{
		NumRows: []int64{int64(msgLength)},
		Data:    genTimestampFieldData(msgLength),
	}

	for _, f := range schema.Fields {
		switch f.DataType {
		case schemapb.DataType_Bool:
			insertData.Data[f.FieldID] = &storage.BoolFieldData{
				NumRows: []int64{int64(msgLength)},
				Data:    generateBoolArray(msgLength),
			}
		case schemapb.DataType_Int8:
			insertData.Data[f.FieldID] = &storage.Int8FieldData{
				NumRows: []int64{int64(msgLength)},
				Data:    generateInt8Array(msgLength),
			}
		case schemapb.DataType_Int16:
			insertData.Data[f.FieldID] = &storage.Int16FieldData{
				NumRows: []int64{int64(msgLength)},
				Data:    generateInt16Array(msgLength),
			}
		case schemapb.DataType_Int32:
			insertData.Data[f.FieldID] = &storage.Int32FieldData{
				NumRows: []int64{int64(msgLength)},
				Data:    generateInt32Array(msgLength),
			}
		case schemapb.DataType_Int64:
			insertData.Data[f.FieldID] = &storage.Int64FieldData{
				NumRows: []int64{int64(msgLength)},
				Data:    generateInt64Array(msgLength),
			}
		case schemapb.DataType_Float:
			insertData.Data[f.FieldID] = &storage.FloatFieldData{
				NumRows: []int64{int64(msgLength)},
				Data:    generateFloat32Array(msgLength),
			}
		case schemapb.DataType_Double:
			insertData.Data[f.FieldID] = &storage.DoubleFieldData{
				NumRows: []int64{int64(msgLength)},
				Data:    generateFloat64Array(msgLength),
			}
		case schemapb.DataType_String, schemapb.DataType_VarChar:
			insertData.Data[f.FieldID] = &storage.StringFieldData{
				NumRows: []int64{int64(msgLength)},
				Data:    generateStringArray(msgLength),
			}
		case schemapb.DataType_FloatVector:
			dim := simpleFloatVecField.dim // if no dim specified, use simpleFloatVecField's dim
			insertData.Data[f.FieldID] = &storage.FloatVectorFieldData{
				NumRows: []int64{int64(msgLength)},
				Data:    generateFloatVectors(msgLength, dim),
				Dim:     dim,
			}
		case schemapb.DataType_BinaryVector:
			dim := simpleBinVecField.dim
			insertData.Data[f.FieldID] = &storage.BinaryVectorFieldData{
				NumRows: []int64{int64(msgLength)},
				Data:    generateBinaryVectors(msgLength, dim),
				Dim:     dim,
			}
		default:
			err := errors.New("data type not supported")
			return nil, err
		}
	}

	return insertData, nil
}

func genSegmentAndInsertData(col *Collection,
	segID UniqueID,
	segType segmentType,
	rows int) (*Segment, error) {
	partitionID := col.getPartitionIDs()[0]
	vChannel := col.getVChannels()[0]
	seg, err := newSegment(col,
		segID,
		partitionID,
		col.ID(),
		vChannel,
		segType)
	if err != nil {
		return nil, err
	}
	schema := col.Schema()
	if segType == segmentTypeGrowing {
		insertMsg, err := genSimpleInsertMsg(schema, rows)
		if err != nil {
			panic(err)
		}

		insertRecord := &segcorepb.InsertRecord{
			FieldsData: insertMsg.FieldsData,
			NumRows:    int64(insertMsg.NumRows),
		}

		offset, err := seg.segmentPreInsert(rows)
		if err != nil {
			panic(err)
		}
		fmt.Println("nums", insertMsg.NumRows)
		err = seg.segmentInsert(offset, insertMsg.RowIDs, insertMsg.Timestamps, insertRecord)
		if err != nil {
			panic(err)
		}
	} else if segType == segmentTypeSealed {
		insertData, err := genInsertData(rows, schema)
		if err != nil {
			return nil, err
		}

		insertRecord, err := storage.TransferInsertDataToInsertRecord(insertData)
		if err != nil {
			return nil, err
		}
		numRows := insertRecord.NumRows
		fmt.Println("nums", numRows)
		for _, fieldData := range insertRecord.FieldsData {
			fieldID := fieldData.FieldId
			err := seg.segmentLoadFieldData(fieldID, numRows, fieldData)
			if err != nil {
				// TODO: return or continue?
				return nil, err
			}
		}
	}
	return seg, nil
}

func genPKFieldSchema(param constFieldParam) *schemapb.FieldSchema {
	field := &schemapb.FieldSchema{
		FieldID:      param.id,
		Name:         param.fieldName,
		IsPrimaryKey: true,
		DataType:     param.dataType,
	}
	return field
}

func genVectorFieldSchema(param vecFieldParam) *schemapb.FieldSchema {
	fieldVec := &schemapb.FieldSchema{
		FieldID:      param.id,
		Name:         param.fieldName,
		IsPrimaryKey: false,
		DataType:     param.vecType,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   dimKey,
				Value: strconv.Itoa(param.dim),
			},
		},
		IndexParams: []*commonpb.KeyValuePair{
			{
				Key:   metricTypeKey,
				Value: param.metricType,
			},
		},
	}
	return fieldVec
}

var simpleFloatField = constFieldParam{
	id:        107,
	dataType:  schemapb.DataType_Float,
	fieldName: "floatField",
}

var simpleDoubleField = constFieldParam{
	id:        108,
	dataType:  schemapb.DataType_Double,
	fieldName: "doubleField",
}

var simpleVarCharField = constFieldParam{
	id:        109,
	dataType:  schemapb.DataType_VarChar,
	fieldName: "varCharField",
}

var simpleInt8Field = constFieldParam{
	id:        103,
	dataType:  schemapb.DataType_Int8,
	fieldName: "int8Field",
}

var simpleInt16Field = constFieldParam{
	id:        104,
	dataType:  schemapb.DataType_Int16,
	fieldName: "int16Field",
}

var simpleInt32Field = constFieldParam{
	id:        105,
	dataType:  schemapb.DataType_Int32,
	fieldName: "int32Field",
}

var simpleInt64Field = constFieldParam{
	id:        106,
	dataType:  schemapb.DataType_Int64,
	fieldName: "int64Field",
}

const (
	// index type
	IndexFaissIDMap      = "FLAT"
	IndexFaissIVFFlat    = "IVF_FLAT"
	IndexFaissIVFPQ      = "IVF_PQ"
	IndexFaissIVFSQ8     = "IVF_SQ8"
	IndexFaissIVFSQ8H    = "IVF_SQ8_HYBRID"
	IndexFaissBinIDMap   = "BIN_FLAT"
	IndexFaissBinIVFFlat = "BIN_IVF_FLAT"
	IndexNsg             = "NSG"

	IndexHNSW      = "HNSW"
	IndexRHNSWFlat = "RHNSW_FLAT"
	IndexRHNSWPQ   = "RHNSW_PQ"
	IndexRHNSWSQ   = "RHNSW_SQ"
	IndexANNOY     = "ANNOY"
	IndexNGTPANNG  = "NGT_PANNG"
	IndexNGTONNG   = "NGT_ONNG"

	// metric type
	L2       = "L2"
	IP       = "IP"
	hamming  = "HAMMING"
	Jaccard  = "JACCARD"
	tanimoto = "TANIMOTO"

	nlist          = 100
	m              = 4
	nbits          = 8
	nprobe         = 8
	sliceSize      = 4
	efConstruction = 200
	ef             = 200
	edgeSize       = 10
	epsilon        = 0.1
	maxSearchEdges = 50
)

const (
	dimKey        = "dim"
	metricTypeKey = "metric_type"

	defaultPKFieldName  = "pk"
	defaultTopK         = int64(1000)
	defaultRoundDecimal = int64(6)
	defaultDim          = 128
	defaultNProb        = 10
	defaultEf           = 10
	defaultMetricType   = L2
	defaultNQ           = 10

	defaultDMLChannel   = "query-node-unittest-DML-0"
	defaultDeltaChannel = "query-node-unittest-delta-channel-0"
	defaultSubName      = "query-node-unittest-sub-name-0"

	defaultLocalStorage = "/tmp/milvus_test/querynode"
)

// ---------- unittest util functions ----------
// functions of init meta and generate meta
type vecFieldParam struct {
	id         int64
	dim        int
	metricType string
	vecType    schemapb.DataType
	fieldName  string
}

var simpleFloatVecField = vecFieldParam{
	id:         100,
	dim:        defaultDim,
	metricType: defaultMetricType,
	vecType:    schemapb.DataType_FloatVector,
	fieldName:  "floatVectorField",
}

var simpleBinVecField = vecFieldParam{
	id:         101,
	dim:        defaultDim,
	metricType: Jaccard,
	vecType:    schemapb.DataType_BinaryVector,
	fieldName:  "binVectorField",
}

var simpleBoolField = constFieldParam{
	id:        102,
	dataType:  schemapb.DataType_Bool,
	fieldName: "boolField",
}

type constFieldParam struct {
	id        int64
	dataType  schemapb.DataType
	fieldName string
}

func genConstantFieldSchema(param constFieldParam) *schemapb.FieldSchema {
	field := &schemapb.FieldSchema{
		FieldID:      param.id,
		Name:         param.fieldName,
		IsPrimaryKey: false,
		DataType:     param.dataType,
	}
	return field
}

func genTestCollectionSchema(pkTypes ...schemapb.DataType) *schemapb.CollectionSchema {
	fieldBool := genConstantFieldSchema(simpleBoolField)
	fieldInt8 := genConstantFieldSchema(simpleInt8Field)
	fieldInt16 := genConstantFieldSchema(simpleInt16Field)
	fieldInt32 := genConstantFieldSchema(simpleInt32Field)
	fieldFloat := genConstantFieldSchema(simpleFloatField)
	fieldDouble := genConstantFieldSchema(simpleDoubleField)
	floatVecFieldSchema := genVectorFieldSchema(simpleFloatVecField)
	binVecFieldSchema := genVectorFieldSchema(simpleBinVecField)
	var pkFieldSchema *schemapb.FieldSchema
	var pkType schemapb.DataType
	if len(pkTypes) == 0 {
		pkType = schemapb.DataType_Int64
	} else {
		pkType = pkTypes[0]
	}
	switch pkType {
	case schemapb.DataType_Int64:
		pkFieldSchema = genPKFieldSchema(simpleInt64Field)
	case schemapb.DataType_VarChar:
		pkFieldSchema = genPKFieldSchema(simpleVarCharField)
	}

	schema := schemapb.CollectionSchema{ // schema for segCore
		Name:   defaultCollectionName,
		AutoID: false,
		Fields: []*schemapb.FieldSchema{
			fieldBool,
			fieldInt8,
			fieldInt16,
			fieldInt32,
			fieldFloat,
			fieldDouble,
			floatVecFieldSchema,
			binVecFieldSchema,
			pkFieldSchema,
		},
	}
	return &schema
}

func genCollectionForReplica(r ReplicaInterface) (*Collection, error) {
	schema := genTestCollectionSchema()
	r.addCollection(defaultCollectionID, schema)
	err := r.addPartition(defaultCollectionID, defaultPartitionID)
	if err != nil {
		return nil, err
	}
	col, err := r.getCollectionByID(defaultCollectionID)
	if err != nil {
		return nil, err
	}
	col.addVChannels([]Channel{
		defaultDeltaChannel,
	})
	return col, nil
}

const (
	defaultCollectionID = UniqueID(0)
	defaultPartitionID  = UniqueID(1)
	defaultSegmentID    = UniqueID(2)
	defaultReplicaID    = UniqueID(10)

	defaultCollectionName = "query-node-unittest-default-collection"
	defaultPartitionName  = "query-node-unittest-default-partition"
)

const (
	defaultMsgLength = 10000
	defaultDelLength = 10
)

const (
	buildID   = UniqueID(0)
	indexID   = UniqueID(0)
	fieldID   = UniqueID(100)
	indexName = "query-node-index-0"
)

func generateFloatVectors(numRows, dim int) []float32 {
	total := numRows * dim
	ret := make([]float32, 0, total)
	for i := 0; i < total; i++ {
		ret = append(ret, rand.Float32())
	}
	return ret
}

func ABC() {
	//log.SetLevel(zapcore.ErrorLevel)
	go func() {
		http.ListenAndServe("0.0.0.0:9999", nil)
	}()
	nq := int64(500)
	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	replica := newCollectionReplica()
	col, err := genCollectionForReplica(replica)
	if err != nil {
		panic(err)
	}
	collection, err := replica.getCollectionByID(defaultCollectionID)
	vec := generateFloatVectors(1, defaultDim)
	var searchRawData []byte
	for i, ele := range vec {
		buf := make([]byte, 4)
		common.Endian.PutUint32(buf, math.Float32bits(ele+float32(i*2)))
		searchRawData = append(searchRawData, buf...)
	}

	placeholderValue := commonpb.PlaceholderValue{
		Tag:    "$0",
		Type:   commonpb.PlaceholderType_FloatVector,
		Values: [][]byte{},
	}

	for i := int64(0); i < nq; i++ {
		placeholderValue.Values = append(placeholderValue.Values, searchRawData)
	}

	placeholderGroup := commonpb.PlaceholderGroup{
		Placeholders: []*commonpb.PlaceholderValue{&placeholderValue},
	}

	placeGroupByte, _ := proto.Marshal(&placeholderGroup)

	dslString := "{\"bool\": { \n\"vector\": {\n \"floatVectorField\": {\n \"metric_type\": \"L2\", \n \"params\": {\n \"nprobe\": 10 \n},\n \"query\": \"$0\",\n \"topk\": 1000 \n,\"round_decimal\": 6\n } \n } \n } \n }"

	plan, err := createSearchPlan(collection, dslString)
	req, err := parseSearchRequest(plan, placeGroupByte)

	//_, err = genSegmentAndInsertData(col, 2, segmentTypeSealed, defaultMsgLength)
	//if err != nil {
	//		panic(err)
	//	}

	var segIDAs []UniqueID
	segNum := int64(100)
	for i := int64(0); i < segNum; i++ {
		segIDAs = append(segIDAs, i)
		//seg, err := genSegmentAndInsertData(col, i, segmentTypeGrowing, defaultMsgLength)
		seg, err := genSegmentAndInsertData(col, i, segmentTypeSealed, defaultMsgLength)
		if err != nil {
			panic(err)
		}
		err2 := replica.setSegment(seg)
		if err2 != nil {
			panic(err2)
		}
	}

	/*
		var segIDBs []UniqueID
		for i:=segNum; i < segNum*2; i++ {
			segIDBs = append(segIDBs, i)
			seg, err := genSegmentAndInsertData(col, i, segmentTypeSealed, defaultMsgLength)
			if err != nil {
				panic(err)
			}
			err2 := replica.setSegment(seg)
			if err2 != nil {
				panic(err2)
			}
		}
	*/

	result, err := searchOnSegments(replica, segmentTypeSealed, req, segIDAs)
	if err != nil {
		panic(err)
	}
	defer deleteSearchResults(result)

	for i := 0; i < 5000; i++ {
		//var results []*internalpb.SearchResults
		/*
			for _, segID := range segIDAs {
				result1 := getSearchResults(replica, segmentTypeSealed, req, nq, []UniqueID{segID})
				results = append(results, result1...)
			}
			for _, segID := range segIDBs {
				result1 := getSearchResults(replica, segmentTypeSealed, req, nq, []UniqueID{segID})
				results = append(results, result1...)
			}
		*/

		//_ = getSearchResults2(req, nq, result, segIDAs)
		_ = getSearchResults(replica, segmentTypeSealed, req, nq, segIDAs)
		//result2 := getSearchResults(replica, segmentTypeSealed, req, nq, segIDBs)
		//results = append(results, result1...)
		//results = append(results, result2...)
		/*
			_, err := reduceSearchResults(results, nq, req.plan.getTopK(), req.plan.getMetricType())
			if err != nil {
				panic(err)
			}
		*/
	}
	req.delete()
	replica.freeAll()
}

func getSearchResults2(
	req *searchRequest,
	nq int64,
	result []*SearchResult,
	segIDs []UniqueID) []*internalpb.SearchResults {

	//	defer debug.FreeOSMemory()
	//	defer runtime.GC()

	topK := defaultTopK
	//sliceNQs := []int64{nq / 5, nq / 5, nq / 5, nq / 5, nq / 5}
	//sliceTopKs := []int64{topK/2, topK / 2, topK/2, topK/2, topK / 2}

	sliceNQs := []int64{nq}
	sliceTopKs := []int64{topK}

	sInfo := parseSliceInfo(sliceNQs, sliceTopKs, nq)

	numSegment := int64(len(segIDs))
	blobs, err2 := reduceSearchResultsAndFillData(req.plan, result, numSegment, sInfo.sliceNQs, sInfo.sliceTopKs)
	if err2 != nil {
		panic(err2)
	}

	defer deleteSearchResultDataBlobs(blobs)

	var results []*internalpb.SearchResults
	cnt := len(sliceNQs)
	for i := 0; i < cnt; i++ {
		globalCnt++
		blob, err := getSearchResultDataBlob(blobs, i)
		if err != nil {
			panic(err)
		}
		bs := make([]byte, len(blob))
		copy(bs, blob)
		r := &internalpb.SearchResults{
			Status:         &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			MetricType:     req.plan.getMetricType(),
			NumQueries:     sliceNQs[i],
			TopK:           sliceTopKs[i],
			SlicedBlob:     bs,
			SlicedOffset:   1,
			SlicedNumCount: 1,
		}
		results = append(results, r)
	}

	fmt.Println(globalCnt)
	return results
}

func getSearchResults(replica ReplicaInterface,
	segType segmentType,
	req *searchRequest,
	nq int64,
	segIDs []UniqueID) []*internalpb.SearchResults {

	//	defer debug.FreeOSMemory()
	//	defer runtime.GC()

	result, err := searchOnSegments(replica, segType, req, segIDs)
	if err != nil {
		panic(err)
	}
	defer deleteSearchResults(result)

	topK := defaultTopK
	////sliceNQs := []int64{nq / 5, nq / 5, nq / 5, nq / 5, nq / 5}
	////sliceTopKs := []int64{topK/2, topK / 2, topK/2, topK/2, topK / 2}
	//
	sliceNQs := []int64{nq}
	sliceTopKs := []int64{topK}
	//
	sInfo := parseSliceInfo(sliceNQs, sliceTopKs, nq)
	//
	numSegment := int64(len(segIDs))
	blobs, err2 := reduceSearchResultsAndFillData(req.plan, result, numSegment, sInfo.sliceNQs, sInfo.sliceTopKs)
	if err2 != nil {
		panic(err2)
	}
	defer deleteSearchResultDataBlobs(blobs)
	//
	var results []*internalpb.SearchResults
	cnt := len(sliceNQs)
	for i := 0; i < cnt; i++ {
		globalCnt++
		blob, err := getSearchResultDataBlob(blobs, i)
		if err != nil {
			panic(err)
		}
		bs := make([]byte, len(blob))
		copy(bs, blob)
		r := &internalpb.SearchResults{
			Status:         &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			MetricType:     req.plan.getMetricType(),
			NumQueries:     sliceNQs[i],
			TopK:           sliceTopKs[i],
			SlicedBlob:     bs,
			SlicedOffset:   1,
			SlicedNumCount: 1,
		}
		results = append(results, r)
	}
	fmt.Println(globalCnt)
	return results
	//return nil

}
