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
	"encoding/json"
	"fmt"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"golang.org/x/exp/maps"
	"io"
	"strings"
)

const RowRootNode = "rows"

type JsonReader struct {
	dec          *json.Decoder
	name2FieldID map[string]int64
	dynamicField *schemapb.FieldSchema
	pkField      *schemapb.FieldSchema
	schema       *schemapb.CollectionSchema
}

func (j *JsonReader) Init(reader io.Reader) error {
	j.dec = json.NewDecoder(reader)
	// treat number value as a string instead of a float64.
	// by default, json lib treat all number values as float64, but if an int64 value
	// has more than 15 digits, the value would be incorrect after converting from float64
	j.dec.UseNumber()
	t, err := j.dec.Token()
	if err != nil {
		return merr.WrapErrImportFailed(fmt.Sprintf("failed to decode the JSON file, error: %v", err))
	}
	if t != json.Delim('{') && t != json.Delim('[') {
		return merr.WrapErrImportFailed("invalid JSON format, the content should be started with '{' or '['")
	}
	// read the first level
	isOldFormat := t == json.Delim('{')
	if isOldFormat {
		// read the key
		t, err = j.dec.Token()
		if err != nil {
			return merr.WrapErrImportFailed(fmt.Sprintf("failed to decode the JSON file, error: %v", err))
		}
		key := t.(string)
		keyLower := strings.ToLower(key)
		// the root key should be RowRootNode
		if keyLower != RowRootNode {
			return merr.WrapErrImportFailed(fmt.Sprintf("invalid JSON format, the root key should be '%s', but get '%s'", RowRootNode, key))
		}

		// started by '['
		t, err = j.dec.Token()
		if err != nil {
			return merr.WrapErrImportFailed(fmt.Sprintf("failed to decode the JSON file, error: %v", err))
		}

		if t != json.Delim('[') {
			return merr.WrapErrImportFailed("invalid JSON format, rows list should begin with '['")
		}
	}
	// this break means we require the first node must be RowRootNode
	// once the RowRootNode is parsed, just finish
	_ = j.dec.More()
	return nil
}

func (j *JsonReader) Next(count int64) (*storage.InsertData, error) {
	rows := make([]map[storage.FieldID]interface{}, 0, count)
	for j.dec.More() && count > 0 {
		var value interface{}
		if err := j.dec.Decode(&value); err != nil {
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("failed to parse row value, error: %v", err))
		}
		row, err := j.verifyRow(value)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
		count--
	}

	// no more rows
	if !j.dec.More() {
		// end by ']'
		t, err := j.dec.Token()
		if err != nil {
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("failed to decode the JSON file, error: %v", err))
		}

		if t != json.Delim(']') {
			return nil, merr.WrapErrImportFailed("invalid JSON format, rows list should end with a ']'")
		}
	}

	// rows to InsertData
	insertData, err := storage.NewInsertData(j.schema)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		err = insertData.Append(row) // TODO: dyh, handle json.Number type in insertData.Append()
		if err != nil {
			return nil, err
		}
	}
	return insertData, nil
}

func (j *JsonReader) combineDynamicRow(dynamicValues map[string]interface{}, row map[storage.FieldID]interface{}) error {
	if j.dynamicField == nil {
		return nil
	}

	dynamicFieldID := j.dynamicField.GetFieldID()
	// combine the dynamic field value
	// valid input:
	// case 1: {"id": 1, "vector": [], "x": 8, "$meta": "{\"y\": 8}"} ==>> {"id": 1, "vector": [], "$meta": "{\"y\": 8, \"x\": 8}"}
	// case 2: {"id": 1, "vector": [], "x": 8, "$meta": {}} ==>> {"id": 1, "vector": [], "$meta": {\"x\": 8}}
	// case 3: {"id": 1, "vector": [], "$meta": "{\"x\": 8}"}
	// case 4: {"id": 1, "vector": [], "$meta": {"x": 8}}
	// case 5: {"id": 1, "vector": [], "$meta": {}}
	// case 6: {"id": 1, "vector": [], "x": 8} ==>> {"id": 1, "vector": [], "$meta": "{\"x\": 8}"}
	// case 7: {"id": 1, "vector": []}
	obj, ok := row[dynamicFieldID]
	if ok {
		if len(dynamicValues) > 0 {
			if value, is := obj.(string); is {
				// case 1
				mp := make(map[string]interface{})
				desc := json.NewDecoder(strings.NewReader(value))
				desc.UseNumber()
				err := desc.Decode(&mp)
				if err != nil {
					// invalid input
					return merr.WrapErrImportFailed("illegal value for dynamic field, not a JSON format string")
				}

				maps.Copy(dynamicValues, mp)
			} else if mp, is := obj.(map[string]interface{}); is {
				// case 2
				maps.Copy(dynamicValues, mp)
			} else {
				// invalid input
				return merr.WrapErrImportFailed("illegal value for dynamic field, not a JSON object")
			}
			row[dynamicFieldID] = dynamicValues
		}
		// else case 3/4/5
	} else {
		if len(dynamicValues) > 0 {
			// case 6
			row[dynamicFieldID] = dynamicValues
		} else {
			// case 7
			row[dynamicFieldID] = "{}"
		}
	}

	return nil
}

func (j *JsonReader) verifyRow(raw interface{}) (map[storage.FieldID]interface{}, error) {
	stringMap, ok := raw.(map[string]interface{})
	if !ok {
		return nil, merr.WrapErrImportFailed("invalid JSON format, each row should be a key-value map")
	}

	dynamicValues := make(map[string]interface{})
	row := make(map[storage.FieldID]interface{})
	// some fields redundant?
	for k, v := range stringMap {
		fieldID, ok := j.name2FieldID[k]
		if (fieldID == j.pkField.GetFieldID()) && j.pkField.GetAutoID() {
			// primary key is auto-id, no need to provide
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("the primary key '%s' is auto-generated, no need to provide", k))
		}

		if ok {
			row[fieldID] = v
		} else if j.dynamicField != nil {
			// has dynamic field. put redundant pair to dynamicValues
			dynamicValues[k] = v
		} else {
			// no dynamic field. if user provided redundant field, return error
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("the field '%s' is not defined in collection schema", k))
		}
	}

	// some fields not provided?
	if len(row) != len(j.name2FieldID) {
		for k, v := range j.name2FieldID {
			if (j.dynamicField != nil) && (v == j.dynamicField.GetFieldID()) {
				// ignore dyanmic field, user don't have to provide values for dynamic field
				continue
			}

			if v == j.pkField.GetFieldID() && j.pkField.GetAutoID() {
				// ignore auto-generaed primary key
				continue
			}

			_, ok = row[v]
			if !ok {
				// not auto-id primary key, no dynamic field,  must provide value
				return nil, merr.WrapErrImportFailed(fmt.Sprintf("value of field '%s' is missed", k))
			}
		}
	}

	// combine the redundant pairs into dynamic field(if has)
	err := j.combineDynamicRow(dynamicValues, row)
	if err != nil {
		return nil, err
	}

	return row, err
}
