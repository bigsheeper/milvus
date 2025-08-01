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

package segments

import (
	"fmt"
	"sync"

	"github.com/samber/lo"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/util/vecindexmgr"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type CollectionManager interface {
	List() []int64
	ListWithName() map[int64]string
	Get(collectionID int64) *Collection
	PutOrRef(collectionID int64, schema *schemapb.CollectionSchema, meta *segcorepb.CollectionIndexMeta, loadMeta *querypb.LoadMetaInfo) error
	Ref(collectionID int64, count uint32) bool
	// unref the collection,
	// returns true if the collection ref count goes 0, or the collection not exists,
	// return false otherwise
	Unref(collectionID int64, count uint32) bool
	// UpdateSchema update the underlying collection schema of the provided collection.
	UpdateSchema(collectionID int64, schema *schemapb.CollectionSchema, version uint64) error
}

type collectionManager struct {
	mut         sync.RWMutex
	collections map[int64]*Collection
}

func NewCollectionManager() *collectionManager {
	return &collectionManager{
		collections: make(map[int64]*Collection),
	}
}

func (m *collectionManager) List() []int64 {
	m.mut.RLock()
	defer m.mut.RUnlock()

	return lo.Keys(m.collections)
}

// return all collections by map id --> name
func (m *collectionManager) ListWithName() map[int64]string {
	m.mut.RLock()
	defer m.mut.RUnlock()

	return lo.MapValues(m.collections, func(coll *Collection, _ int64) string {
		return coll.Schema().GetName()
	})
}

func (m *collectionManager) Get(collectionID int64) *Collection {
	m.mut.RLock()
	defer m.mut.RUnlock()

	return m.collections[collectionID]
}

func (m *collectionManager) PutOrRef(collectionID int64, schema *schemapb.CollectionSchema, meta *segcorepb.CollectionIndexMeta, loadMeta *querypb.LoadMetaInfo) error {
	m.mut.Lock()
	defer m.mut.Unlock()

	if collection, ok := m.collections[collectionID]; ok {
		if loadMeta.GetSchemaVersion() > collection.schemaVersion {
			// the schema may be changed even the collection is loaded
			collection.schema.Store(schema)
			collection.ccollection.UpdateSchema(schema, loadMeta.GetSchemaVersion())
			collection.schemaVersion = loadMeta.GetSchemaVersion()
			log.Info("update collection schema",
				zap.Int64("collectionID", collectionID),
				zap.Uint64("schemaVersion", loadMeta.GetSchemaVersion()),
				zap.Any("schema", schema),
			)
		}
		collection.Ref(1)
		return nil
	}

	log.Info("put new collection", zap.Int64("collectionID", collectionID), zap.Any("schema", schema))
	collection, err := NewCollection(collectionID, schema, meta, loadMeta)
	if err != nil {
		return err
	}
	collection.Ref(1)
	m.collections[collectionID] = collection
	m.updateMetric()
	return nil
}

func (m *collectionManager) UpdateSchema(collectionID int64, schema *schemapb.CollectionSchema, version uint64) error {
	m.mut.Lock()
	defer m.mut.Unlock()

	collection, ok := m.collections[collectionID]
	if !ok {
		return merr.WrapErrCollectionNotFound(collectionID, "collection not found in querynode collection manager")
	}

	if err := collection.ccollection.UpdateSchema(schema, version); err != nil {
		return err
	}
	collection.schema.Store(schema)
	return nil
}

func (m *collectionManager) updateMetric() {
	metrics.QueryNodeNumCollections.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Set(float64(len(m.collections)))
}

func (m *collectionManager) Ref(collectionID int64, count uint32) bool {
	m.mut.Lock()
	defer m.mut.Unlock()

	if collection, ok := m.collections[collectionID]; ok {
		collection.Ref(count)
		return true
	}

	return false
}

func (m *collectionManager) Unref(collectionID int64, count uint32) bool {
	m.mut.Lock()
	defer m.mut.Unlock()

	if collection, ok := m.collections[collectionID]; ok {
		if collection.Unref(count) == 0 {
			log.Info("release collection due to ref count to 0",
				zap.Int64("nodeID", paramtable.GetNodeID()), zap.Int64("collectionID", collectionID))
			delete(m.collections, collectionID)
			DeleteCollection(collection)

			metrics.CleanupQueryNodeCollectionMetrics(paramtable.GetNodeID(), collectionID)
			m.updateMetric()
			return true
		}
		return false
	}

	return true
}

// Collection is a wrapper of the underlying C-structure C.CCollection
// In a query node, `Collection` is a replica info of a collection in these query node.
type Collection struct {
	mu            sync.RWMutex // protects colllectionPtr
	ccollection   *segcore.CCollection
	id            int64
	partitions    *typeutil.ConcurrentSet[int64]
	loadType      querypb.LoadType
	dbName        string
	dbProperties  []*commonpb.KeyValuePair
	resourceGroup string
	// resource group of node may be changed if node transfer,
	// but Collection in Manager will be released before assign new replica of new resource group on these node.
	// so we don't need to update resource group in Collection.
	// if resource group is not updated, the reference count of collection manager works failed.
	metricType    atomic.String // deprecated
	schema        atomic.Pointer[schemapb.CollectionSchema]
	isGpuIndex    bool
	loadFields    typeutil.Set[int64]
	schemaVersion uint64

	refCount *atomic.Uint32
}

// GetDBName returns the database name of collection.
func (c *Collection) GetDBName() string {
	return c.dbName
}

func (c *Collection) GetDBProperties() []*commonpb.KeyValuePair {
	return c.dbProperties
}

// GetResourceGroup returns the resource group of collection.
func (c *Collection) GetResourceGroup() string {
	return c.resourceGroup
}

// ID returns collection id
func (c *Collection) ID() int64 {
	return c.id
}

// GetCCollection returns the CCollection of collection
func (c *Collection) GetCCollection() *segcore.CCollection {
	return c.ccollection
}

// Schema returns the schema of collection
func (c *Collection) Schema() *schemapb.CollectionSchema {
	return c.schema.Load()
}

// IsGpuIndex returns a boolean value indicating whether the collection is using a GPU index.
func (c *Collection) IsGpuIndex() bool {
	return c.isGpuIndex
}

// getPartitionIDs return partitionIDs of collection
func (c *Collection) GetPartitions() []int64 {
	return c.partitions.Collect()
}

func (c *Collection) ExistPartition(partitionIDs ...int64) bool {
	return c.partitions.Contain(partitionIDs...)
}

// addPartitionID would add a partition id to partition id list of collection
func (c *Collection) AddPartition(partitions ...int64) {
	for i := range partitions {
		c.partitions.Insert(partitions[i])
	}
	log.Info("add partitions", zap.Int64("collection", c.ID()), zap.Int64s("partitions", partitions))
}

// removePartitionID removes the partition id from partition id list of collection
func (c *Collection) RemovePartition(partitionID int64) {
	c.partitions.Remove(partitionID)
	log.Info("remove partition", zap.Int64("collection", c.ID()), zap.Int64("partition", partitionID))
}

// getLoadType get the loadType of collection, which is loadTypeCollection or loadTypePartition
func (c *Collection) GetLoadType() querypb.LoadType {
	return c.loadType
}

func (c *Collection) Ref(count uint32) uint32 {
	refCount := c.refCount.Add(count)
	log.Debug("collection ref increment",
		zap.Int64("nodeID", paramtable.GetNodeID()),
		zap.Int64("collectionID", c.ID()),
		zap.Uint32("refCount", refCount),
	)
	return refCount
}

func (c *Collection) Unref(count uint32) uint32 {
	refCount := c.refCount.Sub(count)
	log.Debug("collection ref decrement",
		zap.Int64("nodeID", paramtable.GetNodeID()),
		zap.Int64("collectionID", c.ID()),
		zap.Uint32("refCount", refCount),
	)
	return refCount
}

// newCollection returns a new Collection
func NewCollection(collectionID int64, schema *schemapb.CollectionSchema, indexMeta *segcorepb.CollectionIndexMeta, loadMetaInfo *querypb.LoadMetaInfo) (*Collection, error) {
	/*
		CCollection
		NewCollection(const char* schema_proto_blob);
	*/

	var loadFieldIDs typeutil.Set[int64]
	loadSchema := typeutil.Clone(schema)
	// if load fields is specified, do filtering logic
	// otherwise use all fields for backward compatibility
	if len(loadMetaInfo.GetLoadFields()) > 0 {
		loadFieldIDs = typeutil.NewSet(loadMetaInfo.GetLoadFields()...)
	} else {
		loadFieldIDs = typeutil.NewSet(lo.Map(loadSchema.GetFields(), func(field *schemapb.FieldSchema, _ int) int64 { return field.GetFieldID() })...)
		for _, structArrayField := range loadSchema.GetStructArrayFields() {
			for _, subField := range structArrayField.GetFields() {
				loadFieldIDs.Insert(subField.GetFieldID())
			}
		}
	}

	isGpuIndex := false
	req := &segcore.CreateCCollectionRequest{
		Schema:        loadSchema,
		LoadFieldList: loadFieldIDs.Collect(),
	}
	if indexMeta != nil && len(indexMeta.GetIndexMetas()) > 0 && indexMeta.GetMaxIndexRowCount() > 0 {
		req.IndexMeta = indexMeta
		for _, indexMeta := range indexMeta.GetIndexMetas() {
			isGpuIndex = lo.ContainsBy(indexMeta.GetIndexParams(), func(param *commonpb.KeyValuePair) bool {
				return param.Key == common.IndexTypeKey && vecindexmgr.GetVecIndexMgrInstance().IsGPUVecIndex(param.Value)
			})
			if isGpuIndex {
				break
			}
		}
	}

	ccollection, err := segcore.CreateCCollection(req)
	if err != nil {
		log.Warn("create collection failed", zap.Error(err))
		return nil, err
	}
	coll := &Collection{
		ccollection:   ccollection,
		id:            collectionID,
		partitions:    typeutil.NewConcurrentSet[int64](),
		loadType:      loadMetaInfo.GetLoadType(),
		dbName:        loadMetaInfo.GetDbName(),
		dbProperties:  loadMetaInfo.GetDbProperties(),
		resourceGroup: loadMetaInfo.GetResourceGroup(),
		refCount:      atomic.NewUint32(0),
		isGpuIndex:    isGpuIndex,
		loadFields:    loadFieldIDs,
	}
	for _, partitionID := range loadMetaInfo.GetPartitionIDs() {
		coll.partitions.Insert(partitionID)
	}
	coll.schema.Store(schema)

	return coll, nil
}

// Only for test
func NewTestCollection(collectionID int64, loadType querypb.LoadType, schema *schemapb.CollectionSchema) *Collection {
	col := &Collection{
		id:         collectionID,
		partitions: typeutil.NewConcurrentSet[int64](),
		loadType:   loadType,
		refCount:   atomic.NewUint32(0),
	}
	col.schema.Store(schema)
	return col
}

// new collection without segcore prepare
// ONLY FOR TEST
func NewCollectionWithoutSegcoreForTest(collectionID int64, schema *schemapb.CollectionSchema) *Collection {
	coll := &Collection{
		id:         collectionID,
		partitions: typeutil.NewConcurrentSet[int64](),
		refCount:   atomic.NewUint32(0),
	}
	coll.schema.Store(schema)
	return coll
}

// deleteCollection delete collection and free the collection memory
func DeleteCollection(collection *Collection) {
	/*
		void
		deleteCollection(CCollection collection);
	*/
	collection.mu.Lock()
	defer collection.mu.Unlock()

	if collection.ccollection == nil {
		return
	}
	collection.ccollection.Release()
	collection.ccollection = nil
}
