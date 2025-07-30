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

package adaptor

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"runtime/debug"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/ocf"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

const schemaStr = `
{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "name", "type": "string"},
    {"name": "age", "type": "int"}
  ]
}
`

const segmentInfoSchemaStr = `{
  "name": "milvus.proto.datapb.segment_info",
  "type": "record",
  "fields": [
    {
      "name": "ID",
      "type": "long"
    },
    {
      "name": "CollectionID",
      "type": "long"
    },
    {
      "name": "PartitionID",
      "type": "long"
    },
    {
      "name": "InsertChannel",
      "type": "string"
    },
    {
      "name": "NumOfRows",
      "type": "long"
    },
    {
      "name": "State",
      "type": "int"
    },
    {
      "name": "MaxRowNum",
      "type": "long"
    },
    {
      "name": "LastExpireTime",
      "type": {"name":"dur0","type":"fixed","size":8}
    },
    {
      "name": "StartPosition",
      "type": [
        "null",
        {
          "name": "milvus.proto.datapb.start_position.msg_position",
          "type": "record",
          "fields": [
            {
              "name": "ChannelName",
              "type": "string"
            },
            {
              "name": "MsgID",
              "type": "bytes"
            },
            {
              "name": "MsgGroup",
              "type": "string"
            },
            {
              "name": "Timestamp",
              "type": {"name":"t0","type":"fixed","size":8}
            }
          ]
        }
      ]
    },
    {
      "name": "DmlPosition",
      "type": [
        "null",
        {
          "name": "milvus.proto.datapb.dml_position.msg_position",
          "type": "record",
          "fields": [
            {
              "name": "ChannelName",
              "type": "string"
            },
            {
              "name": "MsgID",
              "type": "bytes"
            },
            {
              "name": "MsgGroup",
              "type": "string"
            },
            {
              "name": "Timestamp",
              "type": {"name":"t1","type":"fixed","size":8}
            }
          ]
        }
      ]
    },
    {
      "name": "Binlogs",
      "type": {
        "type": "array",
        "items": [
          "null",
          {
            "name": "milvus.proto.datapb.binlogs.field_binlog",
            "type": "record",
            "fields": [
              {
                "name": "FieldID",
                "type": "long"
              },
              {
                "name": "Binlogs",
                "type": {
                  "type": "array",
                  "items": [
                    "null",
                    {
                      "name": "milvus.proto.datapb.binlogs.binlogs.binlog",
                      "type": "record",
                      "fields": [
                        {
                          "name": "EntriesNum",
                          "type": "long"
                        },
                        {
                          "name": "TimestampFrom",
                          "type": {"name":"dur1","type":"fixed","size":8}
                        },
                        {
                          "name": "TimestampTo",
                          "type": {"name":"dur2","type":"fixed","size":8}
                        },
                        {
                          "name": "LogPath",
                          "type": "string"
                        },
                        {
                          "name": "LogSize",
                          "type": "long"
                        },
                        {
                          "name": "LogID",
                          "type": "long"
                        },
                        {
                          "name": "MemorySize",
                          "type": "long"
                        }
                      ]
                    }
                  ]
                }
              }
            ]
          }
        ]
      }
    },
    {
      "name": "Statslogs",
      "type": {
        "type": "array",
        "items": [
          "null",
          {
            "name": "milvus.proto.datapb.statslogs.field_binlog",
            "type": "record",
            "fields": [
              {
                "name": "FieldID",
                "type": "long"
              },
              {
                "name": "Binlogs",
                "type": {
                  "type": "array",
                  "items": [
                    "null",
                    {
                      "name": "milvus.proto.datapb.statslogs.binlogs.binlog",
                      "type": "record",
                      "fields": [
                        {
                          "name": "EntriesNum",
                          "type": "long"
                        },
                        {
                          "name": "TimestampFrom",
                          "type": {"name":"dur3","type":"fixed","size":8}
                        },
                        {
                          "name": "TimestampTo",
                          "type": {"name":"dur4","type":"fixed","size":8}
                        },
                        {
                          "name": "LogPath",
                          "type": "string"
                        },
                        {
                          "name": "LogSize",
                          "type": "long"
                        },
                        {
                          "name": "LogID",
                          "type": "long"
                        },
                        {
                          "name": "MemorySize",
                          "type": "long"
                        }
                      ]
                    }
                  ]
                }
              }
            ]
          }
        ]
      }
    },
    {
      "name": "Deltalogs",
      "type": {
        "type": "array",
        "items": [
          "null",
          {
            "name": "milvus.proto.datapb.deltalogs.field_binlog",
            "type": "record",
            "fields": [
              {
                "name": "FieldID",
                "type": "long"
              },
              {
                "name": "Binlogs",
                "type": {
                  "type": "array",
                  "items": [
                    "null",
                    {
                      "name": "milvus.proto.datapb.deltalogs.binlogs.binlog",
                      "type": "record",
                      "fields": [
                        {
                          "name": "EntriesNum",
                          "type": "long"
                        },
                        {
                          "name": "TimestampFrom",
                          "type": {"name":"dur5","type":"fixed","size":8}
                        },
                        {
                          "name": "TimestampTo",
                          "type": {"name":"dur6","type":"fixed","size":8}
                        },
                        {
                          "name": "LogPath",
                          "type": "string"
                        },
                        {
                          "name": "LogSize",
                          "type": "long"
                        },
                        {
                          "name": "LogID",
                          "type": "long"
                        },
                        {
                          "name": "MemorySize",
                          "type": "long"
                        }
                      ]
                    }
                  ]
                }
              }
            ]
          }
        ]
      }
    },
    {
      "name": "CreatedByCompaction",
      "type": "boolean"
    },
    {
      "name": "CompactionFrom",
      "type": {
        "type": "array",
        "items": "long"
      }
    },
    {
      "name": "DroppedAt",
      "type": {"name":"drpped_at","type":"fixed","size":8}
    },
    {
      "name": "IsImporting",
      "type": "boolean"
    },
    {
      "name": "IsFake",
      "type": "boolean"
    },
    {
      "name": "Compacted",
      "type": "boolean"
    },
    {
      "name": "Level",
      "type": "int"
    },
    {
      "name": "StorageVersion",
      "type": "long"
    },
    {
      "name": "PartitionStatsVersion",
      "type": "long"
    },
    {
      "name": "LastLevel",
      "type": "int"
    },
    {
      "name": "LastPartitionStatsVersion",
      "type": "long"
    },
    {
      "name": "IsSorted",
      "type": "boolean"
    },
    {
      "name": "TextStatsLogs",
      "type": {
        "type": "map",
        "values": [
          "null",
          {
            "name": "milvus.proto.datapb.text_stats_logs.text_index_stats",
            "type": "record",
            "fields": [
              {
                "name": "FieldID",
                "type": "long"
              },
              {
                "name": "Version",
                "type": "long"
              },
              {
                "name": "Files",
                "type": {
                  "type": "array",
                  "items": "string"
                }
              },
              {
                "name": "LogSize",
                "type": "long"
              },
              {
                "name": "MemorySize",
                "type": "long"
              },
              {
                "name": "BuildID",
                "type": "long"
              }
            ]
          }
        ]
      }
    },
    {
      "name": "Bm25Statslogs",
      "type": {
        "type": "array",
        "items": [
          "null",
          {
            "name": "milvus.proto.datapb.bm25_statslogs.field_binlog",
            "type": "record",
            "fields": [
              {
                "name": "FieldID",
                "type": "long"
              },
              {
                "name": "Binlogs",
                "type": {
                  "type": "array",
                  "items": [
                    "null",
                    {
                      "name": "milvus.proto.datapb.bm25_statslogs.binlogs.binlog",
                      "type": "record",
                      "fields": [
                        {
                          "name": "EntriesNum",
                          "type": "long"
                        },
                        {
                          "name": "TimestampFrom",
                          "type": {"name":"dur7","type":"fixed","size":8}
                        },
                        {
                          "name": "TimestampTo",
                          "type": {"name":"dur8","type":"fixed","size":8}
                        },
                        {
                          "name": "LogPath",
                          "type": "string"
                        },
                        {
                          "name": "LogSize",
                          "type": "long"
                        },
                        {
                          "name": "LogID",
                          "type": "long"
                        },
                        {
                          "name": "MemorySize",
                          "type": "long"
                        }
                      ]
                    }
                  ]
                }
              }
            ]
          }
        ]
      }
    },
    {
      "name": "IsInvisible",
      "type": "boolean"
    },
    {
      "name": "JsonKeyStats",
      "type": {
        "type": "map",
        "values": [
          "null",
          {
            "name": "milvus.proto.datapb.json_key_stats.json_key_stats",
            "type": "record",
            "fields": [
              {
                "name": "FieldID",
                "type": "long"
              },
              {
                "name": "Version",
                "type": "long"
              },
              {
                "name": "Files",
                "type": {
                  "type": "array",
                  "items": "string"
                }
              },
              {
                "name": "LogSize",
                "type": "long"
              },
              {
                "name": "MemorySize",
                "type": "long"
              },
              {
                "name": "BuildID",
                "type": "long"
              },
              {
                "name": "JsonKeyStatsDataFormat",
                "type": "long"
              }
            ]
          }
        ]
      }
    },
    {
      "name": "IsCreatedByStreaming",
      "type": "boolean"
    }
  ]
}`

type User struct {
	Name string `avro:"name"`
	Age  int32  `avro:"age"`
}

func TestBase(t *testing.T) {
	schema, err := avro.Parse(schemaStr)
	if err != nil {
		panic(err)
	}

	// 写入端：创建 Avro container file，将 schema 嵌入
	f, err := os.Create("/tmp/users.avro")
	if err != nil {
		panic(err)
	}
	enc, err := ocf.NewEncoder(schema.String(), f, ocf.WithCodec(ocf.Null))
	if err != nil {
		panic(err)
	}

	users := []User{
		{Name: "Alice", Age: 30},
		{Name: "Bob", Age: 28},
	}
	for _, u := range users {
		if err := enc.Encode(u); err != nil {
			panic(err)
		}
	}
	enc.Close()
	f.Close()

	// 读取端：打开 Avro 文件，自动读取嵌入 schema
	r, err := os.Open("/tmp/users.avro")
	if err != nil {
		panic(err)
	}
	dec, err := ocf.NewDecoder(r)
	if err != nil {
		panic(err)
	}

	// 从 metadata 获取 schema
	embedded := dec.Schema()
	fmt.Println("嵌入的 schema:", embedded)

	// 按该 schema 解码记录
	for dec.HasNext() {
		var u User
		if err := dec.Decode(&u); err != nil {
			panic(err)
		}
		fmt.Printf("%+v\n", u)
	}
	r.Close()
}

func TestSegmentInfo(t *testing.T) {
	// 1.
	// // Get the protobuf message descriptor for SegmentInfo
	// msgDesc := (&datapb.SegmentInfo{}).ProtoReflect().Descriptor()
	// schema, err := MessageToSchema(msgDesc, "milvus.proto.datapb")

	// 2.
	schema, err := StructToSchema(reflect.TypeOf(datapb.SegmentInfo{}), "milvus.proto.datapb")
	if err != nil {
		panic(err)
	}

	// 3.
	// schema, err := avro.Parse(segmentInfoSchemaStr)
	// if err != nil {
	// 	panic(err)
	// }

	formattedSchemaJson, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(formattedSchemaJson))

	f, err := os.Create("/tmp/segment_info.avro")
	if err != nil {
		panic(err)
	}

	enc, err := ocf.NewEncoder(schema.String(), f, ocf.WithCodec(ocf.Null))
	if err != nil {
		panic(err)
	}

	segmentInfo := &datapb.SegmentInfo{
		ID:             1,
		CollectionID:   2,
		PartitionID:    1,
		LastExpireTime: 100,
		NumOfRows:      200,
		MaxRowNum:      300,
		InsertChannel:  "ch1",
		State:          commonpb.SegmentState_Flushed,
		Binlogs: []*datapb.FieldBinlog{
			{
				Binlogs: []*datapb.Binlog{
					{EntriesNum: 5, LogPath: "log1", LogSize: 100, MemorySize: 100},
				},
			},
		},
	}

	if err := enc.Encode(segmentInfo); err != nil {
		panic(err)
	}
	enc.Close()
	f.Close()

	r, err := os.Open("/tmp/segment_info.avro")
	if err != nil {
		panic(err)
	}
	dec, err := ocf.NewDecoder(r)
	if err != nil {
		panic(err)
	}

	embedded := dec.Schema()
	fmt.Println("嵌入的 schema:", embedded)
	assert.Equal(t, embedded.String(), schema.String(), "embedded schema should be equal to schema")

	for dec.HasNext() {
		res := &datapb.SegmentInfo{}
		if err := dec.Decode(res); err != nil {
			debug.PrintStack()
			panic(err)
		}
		fmt.Printf("%+v\n", res)
		assert.Equal(t, segmentInfo, res, "segmentInfo should be equal to res")
	}
	r.Close()
}

type Entity struct {
	IntField   int
	Int8Field  int8
	Int16Field int16
	Int32Field int32
	Int64Field int64

	Uint8Field  uint8
	Uint16Field uint16
	Uint32Field uint32
	Uint64Field uint64

	BoolField   bool
	FloatField  float32
	DoubleField float64
	BytesField  []byte
	ArrayField  []string
	MapField    map[string]string
}

func TestFullTypes(t *testing.T) {
	schema, err := StructToSchema(reflect.TypeOf(Entity{}), "entities")
	if err != nil {
		panic(err)
	}

	formattedSchemaJson, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(formattedSchemaJson))

	f, err := os.Create("/tmp/entity.avro")
	if err != nil {
		panic(err)
	}

	enc, err := ocf.NewEncoder(schema.String(), f, ocf.WithCodec(ocf.Null))
	if err != nil {
		panic(err)
	}

	entity := &Entity{
		IntField:    426152581543231492,
		Int8Field:   2,
		Int16Field:  3,
		Int32Field:  4,
		Int64Field:  436152581543231493,
		Uint8Field:  7,
		Uint16Field: 8,
		Uint32Field: 9,
		Uint64Field: 456152581543231495,
		BoolField:   true,
		FloatField:  1.1,
		DoubleField: 1.2,
		BytesField:  []byte{1, 2, 3},
		ArrayField:  []string{"a", "b", "c"},
		MapField:    map[string]string{"a": "1", "b": "2", "c": "3"},
	}

	if err := enc.Encode(entity); err != nil {
		panic(err)
	}
	enc.Close()
	f.Close()

	r, err := os.Open("/tmp/entity.avro")
	if err != nil {
		panic(err)
	}
	dec, err := ocf.NewDecoder(r)
	if err != nil {
		panic(err)
	}

	embedded := dec.Schema()
	fmt.Println("嵌入的 schema:", embedded)

	for dec.HasNext() {
		res := &Entity{}
		if err := dec.Decode(res); err != nil {
			debug.PrintStack()
			panic(err)
		}
		fmt.Printf("%+v\n", res)
		assert.Equal(t, entity, res, "entity should be equal to res")
	}
	r.Close()
}
