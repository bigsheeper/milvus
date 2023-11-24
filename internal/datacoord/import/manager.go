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

package _import

import (
	"github.com/milvus-io/milvus/pkg/util/merr"
	"sync"

	"github.com/milvus-io/milvus/internal/metastore"
)

type Manager interface {
	Add(task Task) error
	Update(taskID int64, actions ...UpdateAction)
	Get(taskID int64) Task
	GetBy(filters ...TaskFilter) []Task
	Remove(taskID int64)
}

type manager struct {
	mu    sync.RWMutex // guards tasks
	tasks map[int64]Task

	catalog metastore.DataCoordCatalog
}

func (m *manager) Update(taskID int64, actions ...UpdateAction) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	task, ok := m.tasks[taskID]
	if !ok {
		return merr.XXX
	}
	updatedTask := task.Clone()
	for _, action := range actions {
		action(updatedTask)
	}
	err := m.catalog.UpdateImportTask(updatedTask)
	if err != nil {
		return err
	}
	m.tasks[taskID] = updatedTask
	return nil
}
