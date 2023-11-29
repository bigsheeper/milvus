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
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"go.uber.org/zap"
	"io"
	"strings"
)

const RowRootNode = "rows"

type JsonReader struct {
	reader io.Reader
}

func (j *JsonReader) Next(count int64) (*storage.InsertData, error) {
	// treat number value as a string instead of a float64.
	// by default, json lib treat all number values as float64, but if an int64 value
	// has more than 15 digits, the value would be incorrect after converting from float64
	dec := json.NewDecoder(j.reader)
	dec.UseNumber()
	t, err := dec.Token()
	if err != nil {
		return nil, merr.WrapErrImportFailed(fmt.Sprintf("failed to decode the JSON file, error: %v", err))
	}
	if t != json.Delim('{') && t != json.Delim('[') {
		return nil, merr.WrapErrImportFailed("invalid JSON format, the content should be started with '{' or '['")
	}

	// read the first level
	isEmpty := true
	isOldFormat := t == json.Delim('{')
	for count > 0 {
		if isOldFormat {
			// read the key
			t, err = dec.Token()
			if err != nil {
				return nil, merr.WrapErrImportFailed(fmt.Sprintf("failed to decode the JSON file, error: %v", err))
			}
			key := t.(string)
			keyLower := strings.ToLower(key)
			// the root key should be RowRootNode
			if keyLower != RowRootNode {
				return nil, merr.WrapErrImportFailed(fmt.Sprintf("invalid JSON format, the root key should be '%s', but get '%s'", RowRootNode, key))
			}

			// started by '['
			t, err = dec.Token()
			if err != nil {
				return nil, merr.WrapErrImportFailed(fmt.Sprintf("failed to decode the JSON file, error: %v", err))
			}

			if t != json.Delim('[') {
				return nil, merr.WrapErrImportFailed("invalid JSON format, rows list should begin with '['")
			}
		}

		// read buffer
		buf := make([]map[storage.FieldID]interface{}, 0, p.bufRowCount)
		for dec.More() {
			var value interface{}
			if err := dec.Decode(&value); err != nil {
				return nil, merr.WrapErrImportFailed(fmt.Sprintf("failed to parse row value, error: %v", err))
			}

			row, err := p.verifyRow(value)
			if err != nil {
				return nil, err
			}

			buf = append(buf, row)
			if len(buf) >= p.bufRowCount {
				isEmpty = false
				if err = handler.Handle(buf); err != nil {
					log.Warn("JSON parser: failed to convert row value to entity", zap.Error(err))
					return merr.WrapErrImportFailed(fmt.Sprintf("failed to convert row value to entity, error: %v", err))
				}

				// clear the buffer
				buf = make([]map[storage.FieldID]interface{}, 0, p.bufRowCount)
			}
		}

		// some rows in buffer not parsed, parse them
		if len(buf) > 0 {
			isEmpty = false
			if err = handler.Handle(buf); err != nil {
				return nil, merr.WrapErrImportFailed(fmt.Sprintf("failed to convert row value to entity, error: %v", err))
			}
		}

		// end by ']'
		t, err = dec.Token()
		if err != nil {
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("failed to decode the JSON file, error: %v", err))
		}

		if t != json.Delim(']') {
			return nil, merr.WrapErrImportFailed("invalid JSON format, rows list should end with a ']'")
		}

		// outside context might be canceled(service stop, or future enhancement for canceling import task)
		if isCanceled(p.ctx) {
			return nil, merr.WrapErrImportFailed("import task was canceled")
		}

		// nolint
		// this break means we require the first node must be RowRootNode
		// once the RowRootNode is parsed, just finish
		break
	}

	// empty file is allowed, don't return error
	if isEmpty {
		return nil
	}

	updateProgress()

	// send nil to notify the handler all have done
	return handler.Handle(nil)
}
