# Primary Key Index Design for Milvus

**Date:** 2026-03-16
**Status:** Design Review
**Author:** Collaborative Design Session

## Executive Summary

This document describes the design for a collection/shard-level primary key (PK) index system for Milvus using BBHash (minimal perfect hash function). The PK index will significantly improve performance for point queries, upsert operations, and delete operations by providing O(1) PK lookups instead of the current O(n segments) bloom filter-based pruning.

**Target Operations:**
- Point queries (GET by PK)
- Upsert operations (insert-or-update)
- Delete operations (PK-based deletion)

**Target Scale:**
- Billions of entities per collection
- 100k+ operations per second
- Distributed multi-node clusters

**Performance Goals:**
- Balanced read/write performance
- 50%+ reduction in point query P95 latency
- 40%+ improvement in upsert throughput
- Memory-efficient (3-4 bits/key with BBHash vs. 8-12 bits/key with bloom filters)

## 1. Current State Analysis

### 1.1 Existing PK Handling

**Current Approach:**
- Bloom filter-based segment pruning (probabilistic)
- Per-segment bloom filters track which PKs may exist
- Range pre-filtering (min/max PK) before bloom filter check
- Growing segments: incrementally updated bloom filters
- Sealed segments: bloom filter stats loaded from binlog

**Current Limitations:**
1. **No O(1) PK lookup:** Requires scanning candidate segments after BF pruning
2. **False positives:** BF may return 20-50 candidate segments instead of 1-3
3. **Memory vs. accuracy tradeoff:** Lower FP rate requires more memory
4. **Segment-level only:** No shard-level or collection-level index

**Current Performance:**
- Point query: O(n_segments) BF checks + O(m) segment scans (m = candidates)
- Upsert: Same as point query for deduplication check
- Delete: BF-based forwarding to candidate segments

### 1.2 Opportunity

A dedicated PK index at shard level can:
- Provide exact PK→SegmentID mapping (no false positives)
- Reduce query latency by eliminating segment scanning
- Use less memory per key with BBHash (3-4 bits vs. 8-12 bits)
- Enable faster upsert deduplication and delete routing

## 2. Overall Architecture

### 2.1 Core Components

```
┌─────────────────────────────────────────────────────────────┐
│                      Milvus Cluster                          │
│                                                              │
│  ┌─────────────┐                                            │
│  │  DataCoord  │                                            │
│  │             │                                            │
│  │ - Triggers  │                                            │
│  │   PK Index  │        ┌──────────────┐                   │
│  │   Build Task│───────>│  DataNode    │                   │
│  │ - Tracks    │        │              │                   │
│  │   Metadata  │        │ - Builds     │                   │
│  └─────────────┘        │   BBHash     │                   │
│        │                │   Index      │                   │
│        │                │ - Uploads to │                   │
│        │                │   Storage    │                   │
│        │                └──────────────┘                   │
│        │                        │                           │
│        │                        ↓                           │
│        │                ┌──────────────┐                   │
│        │                │Object Storage│                   │
│        │                │  (PK Index)  │                   │
│        │                └──────────────┘                   │
│        │                        │                           │
│        │                        ↓                           │
│        │                ┌──────────────┐                   │
│        └───Metadata────>│StreamingNode │                   │
│                         │              │                   │
│                         │ - Loads PK   │                   │
│                         │   Index      │                   │
│                         │ - Serves PK  │                   │
│                         │   Queries    │                   │
│                         └──────────────┘                   │
│                                │                             │
│                         [PK Lookup Service]                 │
│                                │                             │
│              ┌─────────────────┴─────────────────┐          │
│              ↓                                   ↓          │
│      ┌──────────────┐                   ┌──────────────┐   │
│      │  QueryNode   │                   │  DataNode    │   │
│      │              │                   │              │   │
│      │ - Point Query│                   │ - Insert     │   │
│      │ - Sealed Seg │                   │ - Upsert     │   │
│      └──────────────┘                   │ - Delete     │   │
│                                         └──────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 Key Design Decisions

| Aspect | Decision | Rationale |
|--------|----------|-----------|
| **Granularity** | One PK index per shard | Balances index size with query efficiency |
| **Index Type** | BBHash (minimal perfect hash) | Space-efficient (3-4 bits/key), O(1) lookup |
| **Builder** | DataNode (triggered by DataCoord) | Consistent with compaction/index building |
| **Storage** | Object storage | Same infrastructure as vector indices |
| **Query Service** | StreamingNode | Unified PK lookup for both reads and writes |
| **Sealed Segments** | BBHash-based index | Immutable data, can use static hash structure |
| **Growing Segments** | Current bloom filter | Mutable data, need incremental updates |

### 2.3 Build Flow

The PK index build flow follows the same pattern as compaction and index building tasks:

1. **DataCoord triggers build task**
   - Baseline: Hourly scheduled rebuild
   - Event-triggered: After compaction, partition drop, manual trigger

2. **DataNode receives and executes task**
   - Collect all PKs from sealed segments in the shard
   - Build BBHash: PK → integer i
   - Build auxiliary structure: i → SegmentID(s) [see Section 4]
   - Serialize both structures

3. **DataNode uploads to object storage**
   - Upload index files (BBHash + auxiliary data)
   - Upload metadata (version, stats, checksums)

4. **DataNode reports completion**
   - Return: index file paths, version, statistics
   - Report to DataCoord

5. **DataCoord updates metadata**
   - Record new index version
   - Notify StreamingNodes of index availability
   - StreamingNodes load/refresh index

## 3. Index Lifecycle

### 3.1 Sealed vs Growing Segments

| Segment Type | Index Strategy | Rationale |
|-------------|----------------|-----------|
| **Sealed** | BBHash (shard-level) | Immutable data, can use space-efficient perfect hash |
| **Growing** | Bloom Filter (current) | Mutable data, need incremental updates |

**PK Lookup Flow:**
```
Query PK → StreamingNode
              ├─> Sealed segments: Query BBHash index → SegmentID(s)
              └─> Growing segments: Query Bloom Filter → Scan candidates
```

### 3.2 Rebuild Strategy (Hybrid)

**Baseline Rebuild:**
- **Trigger:** Time-based (every 1 hour, configurable)
- **Scope:** Full shard BBHash rebuild for all sealed segments
- **Process:** DataCoord schedules task → DataNode builds → uploads to object storage
- **Rationale:** Handles gradual changes (compaction, partition drops)

**Event-Triggered Rebuild:**
- **Triggers:**
  - Compaction completion (sealed segments changed)
  - Partition drop (need to remove PKs from index)
  - Manual trigger (operational maintenance)
- **Scope:** Incremental or full rebuild depending on event type
- **Priority:** Higher than baseline rebuilds
- **Debouncing:** Coalesce multiple events within 5-minute window

**Build Task Execution:**

```
1. DataCoord: Create PK index build task
   - Input: Shard ID, sealed segment list, target index version

2. DataNode: Execute build
   - Collect all PKs from sealed segments in shard
   - Build BBHash: PK → integer i
   - Build auxiliary structure: i → SegmentID(s)
   - Serialize and upload to object storage

3. DataNode: Report completion
   - Return: index file path, version, statistics

4. DataCoord: Update metadata
   - Record new index version
   - Notify StreamingNodes of index availability
```

### 3.3 Index Versioning

- Each rebuild produces a new index version (timestamp-based)
- Old versions retained for 24 hours (configurable) for rollback
- StreamingNode loads latest available version
- Graceful transition: queries can use old version during new version loading

## 4. Data Structures and Storage

### 4.1 BBHash Index Structure

**[DEFERRED - See Section 9.2 for detailed analysis]**

**High-level concept:**
- BBHash function: PK → integer i ∈ [0, N-1]
- Auxiliary mapping: i → SegmentID(s)
- Supports multi-location mapping (PK can exist in multiple segments due to upserts/deletes)

**Key characteristics:**
- Minimal perfect hash: 3-4 bits per key
- Immutable after construction
- O(1) lookup time

**Design Details:** Section 9.2 provides comprehensive analysis of:
- BBHash library options (Go vs. C++ vs. custom)
- Auxiliary data structure options (array, bitmap, hybrid)
- Memory estimates and trade-offs
- PK type handling (int64, varchar)
- Serialization format

### 4.2 Storage Format in Object Storage

**File Structure:**
```
<bucket>/indexes/pk/<collectionID>/<shardID>/
    ├── v<timestamp>.index         # BBHash index data
    ├── v<timestamp>.meta          # Metadata (version, stats, checksums)
    └── v<timestamp>.aux           # Auxiliary data (i→SegmentID mappings)
```

**Metadata File Content:**
```json
{
  "version": "<timestamp>",
  "shard_id": "<shardID>",
  "collection_id": "<collectionID>",
  "num_keys": 1000000000,
  "sealed_segments": ["seg1", "seg2", ...],
  "build_time": "2026-03-16T10:00:00Z",
  "index_file": "v<timestamp>.index",
  "aux_file": "v<timestamp>.aux",
  "checksums": {
    "index": "sha256:...",
    "aux": "sha256:..."
  },
  "stats": {
    "index_size_bytes": 500000000,
    "aux_size_bytes": 16000000000,
    "num_segments": 1000,
    "pk_type": "int64"
  }
}
```

### 4.3 Memory Management in StreamingNode

**[DEFERRED - See Section 9.3 for detailed analysis]**

**Options under consideration:**
- **Eager loading:** Load entire shard index into memory
- **Lazy loading:** Load on first query, keep in memory (LRU cache)
- **On-demand/streaming:** Fetch portions from storage per query (partitioned BBHash)
- **Hybrid:** Hot shards in memory, cold shards on-demand

**Dependencies:**
- BBHash partitioning support (affects on-demand feasibility)
- Memory budget allocation (determines which strategy is feasible)
- Access pattern analysis (hot/cold shard distribution)

**Design Details:** Section 9.3 provides comprehensive analysis of:
- Each loading strategy with memory estimates
- LRU cache implementation details
- Partitioned BBHash chunk loading
- Hot/cold classification algorithms
- Trade-offs and decision criteria
- Recommendation: Hybrid (hot eager, cold lazy) for production

### 4.4 Error Handling

| Error Scenario | Handling Strategy |
|---------------|-------------------|
| **Index build failure** | Retry with exponential backoff, alert on persistent failures |
| **Index load failure** | Fall back to bloom filter-only mode for affected shard |
| **Corrupted index** | Detect via checksum, trigger rebuild, use previous version |
| **Missing index** | Use bloom filter system until index is built |

## 5. Query Flow

### 5.1 Point Query (GET by PK)

```
Client → Proxy → StreamingNode (PK Lookup)
                      ├─> BBHash lookup (sealed segments) → SegmentID(s)
                      └─> Bloom filter (growing segments) → candidate segments
                 ↓
            QueryNode: Retrieve data from identified segments
                 ↓
            Return result to client
```

**Steps:**
1. Proxy receives point query with PK
2. Proxy forwards PK lookup request to StreamingNode
3. StreamingNode performs lookup:
   - Query BBHash index for sealed segments → exact SegmentID(s)
   - Query bloom filter for growing segments → candidate segments
4. Proxy routes query to appropriate QueryNode(s) with segment list
5. QueryNode retrieves actual data from segments
6. If multiple segments returned (multi-version), check timestamps for latest version
7. Return result to client

### 5.2 Upsert Operation

```
Client → Proxy → StreamingNode (PK Lookup for deduplication)
                      ├─> Check if PK exists (BBHash + BF)
                      └─> Return existing segment locations
                 ↓
            DataNode: Process upsert
                      ├─> If exists: Issue delete to old locations
                      └─> Insert new version
                 ↓
            Update growing segment BF
```

**Steps:**
1. Proxy receives upsert request
2. Proxy queries StreamingNode to check if PK exists
3. StreamingNode returns segment locations (sealed + growing)
4. DataNode processes:
   - If PK exists: Mark old versions as deleted (delta operations)
   - Insert new version into growing segment
5. Update growing segment bloom filter
6. Return success to client

### 5.3 Delete Operation

```
Client → Proxy → StreamingNode (PK Lookup)
                      ├─> BBHash lookup → sealed segment(s)
                      └─> BF lookup → growing segment(s)
                 ↓
            DataNode: Forward deletes to identified segments
                 ↓
            Mark as deleted (delta operations)
```

**Steps:**
1. Proxy receives delete request with PK
2. Proxy queries StreamingNode for segment locations
3. StreamingNode returns all segments containing the PK
4. DataNode forwards delete to all identified segments
5. Segments mark PK as deleted via delta operations
6. Return success to client

### 5.4 Key Query Flow Principles

- **StreamingNode as routing service:** Returns segment locations only, doesn't access actual data
- **Multiple segment results:** BBHash can map PK to multiple segments (upserts/deletes create versions)
- **Timestamp ordering:** QueryNode uses timestamps to identify latest version
- **Graceful fallback:** If BBHash index unavailable, fall back to bloom filter pruning

### 5.5 Integration with Existing BF System

**[DEFERRED - See Section 9.4 for detailed analysis]**

**Options under consideration:**
- **Replace BF for sealed segments:** Only use BBHash for sealed, BF for growing
- **Parallel lookup:** Check both systems, merge results (validation mode)
- **Conditional:** Use BBHash if available, otherwise fall back to BF
- **Two-phase:** BF prunes first, BBHash verifies

**Design Details:** Section 9.4 provides comprehensive analysis of:
- Each integration option with code examples
- Query flow changes for each approach
- Memory implications (can we free sealed segment BF memory?)
- Rollout safety (fallback mechanisms)
- Recommendation: Conditional fallback for Phase 2-3, migrate to replace for Phase 4

## 6. Operational Considerations

### 6.1 Build Resource Management

**DataNode Resources:**
- PK index builds consume CPU and memory during construction
- **Build priority:** Below real-time ingestion, above background compaction
- **Resource limits:** Configurable max memory per build task (default: 10GB)
- **Concurrent builds:** Limit to 2 concurrent PK index builds per DataNode (configurable)

**Build Scheduling:**
- **Baseline rebuilds:** Spread across time to avoid cluster-wide load spikes
  - Example: 1000 shards, 1-hour baseline → start 1 build every 3.6 seconds
- **Event-triggered:** Coalesce multiple events within 5-minute window
  - Multiple compactions in same shard → single rebuild
- **Backpressure:** Delay new builds if DataNode queue is full

### 6.2 Configuration Parameters

```yaml
# PK Index Configuration
pk_index:
  enabled: true

  # Build scheduling
  baseline_rebuild_interval: 1h
  event_rebuild_debounce: 5m
  max_concurrent_builds_per_node: 2

  # Index retention
  version_retention_hours: 24

  # Memory limits
  build_memory_limit_gb: 10

  # [DEFERRED] StreamingNode configuration
  # loading_strategy: eager|lazy|on-demand
  # cache_size_gb: 50
  # index_fetch_timeout_ms: 5000
```

### 6.3 Monitoring and Metrics

**Build Metrics:**
- `pk_index_build_duration_seconds` - Histogram of build times by shard
  - **Target:** < 300s per billion keys (< 5 minutes)
- `pk_index_build_failures_total` - Counter of failed builds with reason labels
  - **Target:** < 0.1% failure rate
- `pk_index_size_bytes` - Gauge of index size per shard
  - **Target:** 16-20 bytes per key average
- `pk_index_version_age_seconds` - How stale is the current index per shard
  - **Target:** < 2x rebuild interval (< 2 hours for 1-hour baseline)
- `pk_index_keys_total` - Gauge of number of PKs indexed per shard

**Query Metrics:**
- `pk_lookup_duration_ms` - Latency of PK lookups in StreamingNode (P50/P95/P99)
- `pk_lookup_hit_rate` - Ratio of successful PK lookups (found vs. not found)
- `pk_lookup_segment_candidates` - Histogram of segments returned per lookup
- `pk_lookup_failures_total` - Counter of lookup failures with reason labels

**StreamingNode Metrics:**
- `pk_index_load_duration_seconds` - Time to load index from storage
  - **Target:** < 30s per shard (for 16GB index)
- `pk_index_memory_bytes` - Memory consumed by loaded indices
  - **Target:** < 20 bytes per key average (including BBHash + auxiliary)
- `pk_index_cache_hit_rate` - Cache effectiveness (if caching strategy used)
  - **Target:** > 95% for hot shards
- `pk_lookup_duration_ms` - P50/P95/P99 latency for PK lookups
  - **Target:** P99 < 10ms (cache hit), P99 < 2000ms (cache miss with fetch)

### 6.4 Alerts

**Critical:**
- PK index build failing for > 3 consecutive attempts for same shard
- StreamingNode PK lookup availability < 99.9%
- Index corruption detected (checksum mismatch)

**Warning:**
- PK index age exceeds 2x baseline rebuild interval (> 2 hours for hourly baseline)
- StreamingNode PK lookup latency > P99 threshold (e.g., > 100ms)
- Index load failures in StreamingNode
- Build queue depth > 10

### 6.5 Rollout Strategy

**Phase 1: Build Infrastructure (Weeks 1-4)**
- Implement DataNode build task execution
- Object storage integration for index files
- DataCoord task scheduling and metadata tracking
- Monitoring and metrics infrastructure
- **No query path changes yet** - build index but don't use it
- Validate: Index builds successfully, storage format correct

**Phase 2: StreamingNode Integration (Weeks 5-8)**
- StreamingNode loads and serves PK index
- Enable for **read-only point queries first**
- A/B testing: compare BBHash vs. bloom filter performance
- Gradual rollout: collection-by-collection or shard-by-shard
- Feature flag control: enable/disable per collection
- Validate: Query correctness, performance improvement

**Phase 3: Write Path Integration (Weeks 9-12)**
- Enable for upsert deduplication
- Enable for delete routing
- Full production rollout across all collections
- Remove feature flags (always-on)
- Validate: End-to-end correctness, no regressions

### 6.6 Backward Compatibility

- **Feature flag:** `enable_pk_index` (default: false initially, true after rollout)
- **Graceful degradation:** Fall back to bloom filter if index unavailable
- **No API changes:** Transparent to users, no breaking changes
- **Mixed-mode operation:** Supports clusters with both indexed and non-indexed collections
- **Rollback safety:** Can disable feature without data loss

## 7. Testing Strategy

### 7.1 Unit Tests

**DataNode Build Logic:**
- BBHash construction from various PK datasets (int64, varchar)
- Correct handling of multiple segments per shard
- Edge cases: empty segments, single PK segment, duplicate PKs
- Serialization/deserialization correctness
- Memory limit enforcement during build
- Build cancellation and cleanup

**StreamingNode Query Logic:**
- PK lookup returns correct segment IDs
- Multiple segment results (multi-version scenarios)
- Handling missing index (fallback to BF)
- Concurrent query handling
- Index version refresh

### 7.2 Integration Tests

**End-to-End Build Flow:**
- DataCoord triggers build → DataNode builds → uploads → metadata update
- Verify index file structure in object storage
- Build retry on failure with exponential backoff
- Build cancellation and cleanup
- Multiple concurrent builds across different shards

**End-to-End Query Flow:**
- Point query uses PK index to locate segment, retrieves data
- Upsert checks existing PK via index, updates correctly
- Delete routes to correct segments via index
- Fallback to BF when index unavailable or being rebuilt
- Multi-version handling (upsert same PK multiple times)

### 7.3 Performance Tests

**Build Performance:**
- Build time vs. dataset size: 1M, 10M, 100M, 1B PKs
- Memory usage during build
- Concurrent build impact on real-time ingestion throughput
- Build time with different segment counts (10, 100, 1000 segments per shard)

**Query Performance:**
- PK lookup latency: measure P50, P95, P99
- Throughput: queries per second per StreamingNode
- Lookup latency vs. index size (correlation)
- Comparison: BBHash vs. bloom filter pruning (side-by-side)
- Concurrent query scalability

**Memory Efficiency:**
- Memory per PK: BBHash vs. bloom filter
- Total memory usage at billion-scale
- Memory overhead for auxiliary data structures

### 7.4 Chaos Tests

**Build Resilience:**
- DataNode crashes during build (partial index files)
- Object storage unavailable during upload (retry behavior)
- Concurrent rebuilds of same shard (coordination)
- DataCoord crashes after triggering but before receiving completion

**Query Resilience:**
- StreamingNode crashes during index load
- Corrupted index file (checksum detection)
- Index version mismatch (old version used by query, new version available)
- Object storage unavailable during index load (fallback)

### 7.5 Correctness Tests

**Data Correctness:**
- PK uniqueness: Insert duplicate PKs, verify latest version returned
- Delete correctness: Delete via PK, verify data not returned in queries
- Multi-version: Upsert same PK multiple times, verify correct ordering
- Compaction + rebuild: Verify index reflects compacted segments correctly
- Partition drop + rebuild: Verify dropped partition PKs removed from index

**Consistency:**
- Growing → Sealed transition: PK moves from BF to BBHash
- Index rebuild during queries: Queries use consistent version
- Concurrent upsert and query: Read-your-writes consistency

## 8. Success Metrics

### 8.1 Performance Improvements

**Target Metrics:**
- **Point query latency:** Reduce P95 latency by **50%+** compared to bloom filter-only
- **Upsert throughput:** Improve deduplication performance by **40%+**
- **Delete routing:** Reduce candidate segments from 20-50 to **1-5** per delete
- **Memory efficiency:** BBHash uses **3-4 bits/key** vs. bloom filter's 8-12 bits/key

**Measurement Method:**
- A/B testing: same workload, indexed vs. non-indexed collections
- Production metrics: compare before/after rollout
- Benchmark suite: standardized test cases

### 8.2 System Health

**Operational Metrics:**
- PK index build success rate: **> 99.9%**
- Index staleness: **< 2x rebuild interval** (< 2 hours for hourly baseline)
- StreamingNode PK lookup availability: **> 99.99%**
- Zero data corruption or loss from PK index feature
- Build impact on ingestion: **< 5%** throughput reduction during builds

### 8.3 Adoption Metrics

**Rollout Progress:**
- Collections using PK index (count and percentage)
- Query traffic utilizing PK index (requests per second)
- A/B test results comparing indexed vs. non-indexed performance
- User-reported improvements or issues

## 9. Deferred Design Decisions

The following topics were identified during brainstorming but deferred for future resolution. Each section provides detailed analysis of options to facilitate informed decision-making.

### 9.1 Architectural Approach: StreamingNode Deployment Model

**Decision Needed:** Centralized vs. Distributed StreamingNode deployment for PK index query service

**Resolution Needed Before:** Phase 2 rollout

#### Option A: Centralized (Single StreamingNode Instance)

**Architecture:**
- Single StreamingNode per cluster (or one per availability zone)
- Loads all shard PK indices
- All read and write path queries route through this node

**Pros:**
- Simplest implementation and deployment
- Easier to reason about consistency
- Lower network chattiness (single hop for PK lookup)
- Simpler metadata management

**Cons:**
- Single point of failure (requires HA solution)
- Memory bottleneck: Must hold all shard indices in memory
  - At 1000 shards × 1B PKs/shard × 20 bytes/PK = ~20TB of index data
  - Even with compression, likely 2-5TB memory required
- CPU bottleneck: All 100k+ ops/sec go through one node
- Scalability ceiling

**Memory Estimate:**
```
Baseline: 1B PKs per shard
BBHash: 4 bits/key = 0.5 bytes/key
Auxiliary: ~16 bytes/key (SegmentID list)
Total per shard: ~16.5 bytes × 1B = 16.5 GB

For 1000 shards: 16.5 TB
For 100 shards: 1.65 TB (more realistic for single node)
```

**When to Choose:**
- Cluster has < 100 shards
- PK query rate < 10k ops/sec
- Single large-memory node available (512GB+ RAM)
- Rapid prototyping phase

---

#### Option B: Distributed (Multiple StreamingNode Replicas)

**Architecture:**
- Multiple StreamingNode instances (N replicas)
- Each node capable of serving any shard's PK index
- Load balancing across nodes (round-robin or consistent hashing)
- Shared distributed cache (Redis/Memcached) or local caches with coordination

**Pros:**
- Horizontal scalability (add nodes for more capacity)
- High availability (node failure doesn't lose service)
- Memory distribution (each node caches subset of hot indices)
- CPU distribution (queries spread across nodes)

**Cons:**
- Complex cache coherency (stale reads possible)
- Network overhead (cache fetches on miss)
- More complex deployment and monitoring
- Potential cache stampede on index rebuild

**Cache Architecture Options:**

**B1: Shared Distributed Cache (Redis)**
```
StreamingNode 1 ─┐
StreamingNode 2 ─┼─> Redis Cluster ─> Object Storage
StreamingNode N ─┘
```
- **Pros:** Single source of truth, consistent across nodes
- **Cons:** Network RTT per query, Redis becomes bottleneck

**B2: Local Cache with Invalidation**
```
StreamingNode 1 [Local Cache] ─┐
StreamingNode 2 [Local Cache] ─┼─> Object Storage
StreamingNode N [Local Cache] ─┘
         ↑
    Invalidation messages (via message queue)
```
- **Pros:** Fast local reads, no shared cache bottleneck
- **Cons:** Invalidation lag, larger memory footprint per node

**Memory Estimate (per node):**
```
Assuming hot/cold split (20/80 rule):
Hot indices: 20% of shards in memory
1000 shards × 20% × 16.5 GB/shard = 3.3 TB total hot data
With 10 nodes: 330 GB per node (feasible)

With 100 shards × 20% × 16.5 GB: 330 GB total
With 10 nodes: 33 GB per node (very feasible)
```

**When to Choose:**
- Cluster has > 100 shards
- PK query rate > 10k ops/sec
- HA requirements
- Production deployment at scale

---

#### Option C: Sharding-Based Distribution

**Architecture:**
- Each StreamingNode responsible for specific shard subset (partitioning)
- Proxy routes query to appropriate StreamingNode based on shard
- No cache sharing (each node owns its shards' indices)

**Pros:**
- Predictable memory usage (shard assignment is explicit)
- No cache coherency issues
- Simpler than Option B (no cache coordination)
- Easy to add capacity (assign shards to new nodes)

**Cons:**
- Routing complexity (Proxy must know shard→node mapping)
- Uneven load if shard access patterns skewed
- Resharding complexity on node addition/removal
- Node failure loses specific shards (not fully HA without replication)

**Shard Assignment:**
```
StreamingNode 1: Shards 1-333   (5.5 TB if 1000 shards)
StreamingNode 2: Shards 334-666
StreamingNode 3: Shards 667-1000

With replication:
Node 1: Primary(1-333), Replica(334-666)
Node 2: Primary(334-666), Replica(667-1000)
Node 3: Primary(667-1000), Replica(1-333)
```

**When to Choose:**
- Predictable access patterns (shards accessed uniformly)
- Want simple HA via replication
- Willing to implement smart routing in Proxy

---

#### Decision Criteria and Evaluation

**Information Needed:**
1. **Shard count distribution** - How many shards typical? Median/P95/P99
2. **PK index size distribution** - Average index size per shard
3. **Query rate and patterns** - ops/sec, hot shard concentration
4. **Memory budget** - Available RAM per node
5. **HA requirements** - Acceptable downtime on node failure

**Recommended Approach:**
1. **Phase 2 Prototype:** Start with **Option A (Centralized)** for simplicity
   - Test with subset of shards (< 100)
   - Measure memory usage and query latency
   - Validate correctness and integration
2. **Phase 3 Production:** Migrate to **Option B (Distributed)** or **Option C (Sharding)**
   - Based on prototype metrics and production scale
   - If memory per node < 256GB → Option B or C required
   - If HA critical → Option B or C with replication

**Default Recommendation:** **Option C (Sharding-Based)** with replication for production
- Predictable, simple, scalable
- Easy to capacity plan (X GB per node = Y shards per node)
- HA via replica failover

---

### 9.2 BBHash Data Structure Design

**Decision Needed:** Detailed BBHash structure and auxiliary data format

**Resolution Needed Before:** Implementation start

**User to Provide:** The implementation team will select BBHash library and design auxiliary structures. This section outlines requirements and detailed technical considerations.

#### Context: Why BBHash?

**BBHash (Minimal Perfect Hash Function)** properties:
- **Space efficiency:** 2-4 bits per key (vs. 8-12 for bloom filters)
- **Perfect hash:** No collisions, every PK maps to unique integer
- **Minimal:** Output range is [0, N-1] for N keys (no gaps)
- **Static:** Immutable after construction (fits sealed segment model)
- **Fast construction:** O(N) build time with small constants
- **Fast query:** O(1) lookup, 1-3 hash computations

**Why not regular hash table?**
- Hash table: ~16-24 bytes/key (pointer + key + value)
- BBHash + auxiliary: ~16-20 bytes/key total (3 bits for hash + 16 bytes for segment mapping)
- At billion-scale: 16 GB vs. 24 GB (33% savings)

#### BBHash Requirements

**Functional Requirements:**
1. **PK → Integer mapping:** Map arbitrary PK (int64 or varchar) to integer i ∈ [0, N-1]
2. **Minimal perfect hash:** Every PK maps to unique integer, no collisions
3. **Multi-value support:** Single PK can map to multiple SegmentIDs (via auxiliary structure)
4. **Serialization:** Must support save/load from object storage
5. **Deterministic:** Same input PKs produce same mapping (for rebuild consistency)

**Performance Requirements:**
1. **Space efficiency:** Target 3-5 bits per key (vs. 8-12 for bloom filters)
2. **Query latency:** O(1) lookup, target < 1 microsecond per PK
3. **Build time:** < 5 minutes per billion keys
4. **Concurrent reads:** Thread-safe for query workload

#### Auxiliary Data Structure Options

Once BBHash maps PK → integer i, we need auxiliary structure: i → SegmentID(s)

**Option A: Array of Segment ID Lists**

```go
type PKIndex struct {
    bbhash BBHash                    // PK → i
    segments [][]SegmentID           // i → list of SegmentIDs
    timestamps [][]uint64            // i → list of timestamps (optional)
}
```

**Memory:**
```
segments[i] = [seg1, seg2, ...]
Per entry:
  - SegmentID: 8 bytes (int64)
  - Timestamp: 8 bytes (optional, for version ordering)

Average case (1 segment per PK): 8 bytes/key
Worst case (10 segments per PK): 80 bytes/key
```

**Pros:**
- Simple, direct lookup
- Supports variable segments per PK
- Easy to serialize

**Cons:**
- Memory overhead if most PKs have 1 segment (array overhead)
- Not compressed

---

**Option B: Compressed Segment Bitmap**

```go
type PKIndex struct {
    bbhash BBHash                    // PK → i
    segmentBitmap []roaring.Bitmap   // i → bitmap of segment IDs
    maxSegmentID int64
}
```

**Memory:**
```
Roaring bitmap: ~1 bit per segment + overhead
1000 segments per shard = 125 bytes per PK worst case
Average case (1 segment): ~16 bytes (bitmap overhead)
```

**Pros:**
- Memory-efficient for many segments per PK
- Fast set operations (union, intersection)
- Good compression for sparse segment IDs

**Cons:**
- Overhead for single-segment case
- More complex serialization
- Requires roaring bitmap library

---

**Option C: Hybrid (Inline + Overflow)**

```go
type PKIndex struct {
    bbhash BBHash
    primary []SegmentIDEntry    // i → inline entry (1 segment + overflow pointer)
    overflow [][]SegmentID      // overflow entries for multi-segment PKs
}

type SegmentIDEntry struct {
    SegmentID int64    // Primary segment (most common case)
    Overflow  int32    // Index into overflow array (-1 if none)
}
```

**Memory:**
```
Most PKs (99%): 8 bytes (SegmentID) + 4 bytes (overflow marker) = 12 bytes
Multi-segment PKs (1%): 12 bytes + overflow array (8 bytes × N segments)
Average: ~12-13 bytes per PK
```

**Pros:**
- Optimized for common case (1 segment per PK)
- Minimal memory overhead
- Simple lookup path

**Cons:**
- Two-level indirection for multi-segment PKs
- More complex implementation

---

#### Timestamp Handling for Multi-Version PKs

If PK exists in multiple segments (due to upserts), need timestamps to identify latest version.

**Option 1: Store timestamps in auxiliary structure**
```go
segments [][]SegmentIDWithTimestamp
```
- Pros: Self-contained, no external lookup
- Cons: Extra 8 bytes per segment

**Option 2: Query segments for timestamps**
- Pros: No index memory overhead
- Cons: Extra query to segments (latency hit)

**Recommendation:** **Option 1** - include timestamps in auxiliary structure
- Latency is critical for PK lookups
- 8 bytes per segment is acceptable overhead

---

#### BBHash Library Candidates

**Option A: Go Implementation (github.com/shenwei356/go-mphf)**

**Algorithm:** Based on PTHash or similar minimal perfect hash algorithm

**Technical Details:**
- **Space:** 3-4 bits per key
- **Build time:** O(N) with low constant factor
- **Query time:** 2-3 hash computations
- **Thread-safety:** Read-only after construction (safe for concurrent queries)
- **Integration:** Pure Go, `import "github.com/shenwei356/go-mphf"`

**API Example:**
```go
// Build
keys := [][]byte{[]byte("key1"), []byte("key2"), ...}
mph, _ := mphf.NewMPHF(keys, false)

// Query
idx := mph.Lookup([]byte("key1"))  // Returns integer i

// Serialize
data := mph.Marshal()
mph2, _ := mphf.Unmarshal(data)
```

**PK Type Handling:**
- **Int64:** Convert to 8-byte big-endian []byte
- **VarChar:** Use string bytes directly

**Pros:**
- Pure Go (no CGO), easy to integrate
- Battle-tested (used in bioinformatics tools)
- Simple API
- Good balance of space and speed

**Cons:**
- 3-4 bits/key (not the absolute minimum)
- Go performance ceiling (vs. optimized C++)

**Memory Estimate:**
```
1B keys × 4 bits/key = 500 MB per shard (BBHash only)
```

---

**Option B: C++ Library with CGO (cmph, emphf, bbhash-original)**

**Libraries:**
- **emphf:** Modern C++11, 2.5 bits/key
- **bbhash:** Original BBHash implementation, 2.5-3 bits/key
- **cmph:** Classic minimal perfect hash library, 2-4 bits/key

**Technical Details:**
- **Space:** 2-3 bits per key (more efficient than Go)
- **Build time:** Highly optimized (SIMD, cache-friendly)
- **Query time:** 1-2 hash computations (faster than Go)

**Integration Approach:**
```go
// CGO wrapper
/*
#cgo LDFLAGS: -lbbhash
#include "bbhash.h"
*/
import "C"

type BBHashCGO struct {
    handle unsafe.Pointer
}

func (b *BBHashCGO) Lookup(pk []byte) int {
    return int(C.bbhash_lookup(b.handle, (*C.char)(unsafe.Pointer(&pk[0])), C.int(len(pk))))
}
```

**Pros:**
- Maximum space efficiency (2-3 bits/key)
- Fastest query performance
- Proven at massive scale (billion+ keys)

**Cons:**
- CGO complexity and overhead (FFI calls)
- Build system complexity (C++ compilation, linking)
- Platform portability issues
- Debugging difficulty (cross-language)
- Milvus already has CGO for segcore, but adds more dependency

**Memory Estimate:**
```
1B keys × 3 bits/key = 375 MB per shard (25% better than Go)
```

---

**Option C: Custom Go Implementation**

**Approach:** Implement minimal perfect hash from scratch in Go

**Algorithm Options:**
- PTHash (partitioned hash table)
- CHD (Compress, Hash, Displace)
- BBHash (Bloom filter construction)

**Pros:**
- Full control over implementation
- Optimized for Milvus PK characteristics
- No external dependencies
- Can optimize for int64 (don't need generic []byte hashing)

**Cons:**
- Significant development time (2-4 weeks)
- Testing burden (correctness, edge cases)
- Unlikely to beat mature libraries on performance
- Maintenance burden

**Not recommended** unless specific requirements unmet by existing libraries.

---

**Option D: Use Existing Milvus Hash Infrastructure**

**Approach:** Leverage existing hash functions in Milvus codebase

Looking at `/internal/util/bloomfilter/bloom_filter.go`:
- Already uses `xxh3` (xxHash3) for bloom filters
- Fast, high-quality 64-bit hash

**Could we build simple perfect hash?**
- Use xxh3 with multiple salts to resolve collisions
- Not minimal (may have gaps), but simple
- Space: ~8-16 bits per key (less efficient than BBHash)

**Pros:**
- Reuse existing hash infrastructure
- No new dependencies
- Pure Go, simple

**Cons:**
- Less space-efficient than true minimal perfect hash
- Build algorithm more complex (collision resolution)

**Evaluation needed** - prototype to see if acceptable space/time.

---

**Library Selection Recommendation:**

1. **Phase 1 Prototype:** **Option A (go-mphf)**
   - Fast to integrate
   - Validate overall architecture
   - Measure performance on real Milvus data

2. **Phase 3 Optimization:** Evaluate **Option B (CGO)** if:
   - Memory pressure is critical (savings worth complexity)
   - Profile shows BBHash is query bottleneck
   - Team comfortable with CGO maintenance

3. **Avoid Option C (custom)** unless blockers found in A/B

**PK Type Handling:**

**Int64 PK:**
```go
func Int64ToBytes(pk int64) []byte {
    buf := make([]byte, 8)
    binary.BigEndian.PutUint64(buf, uint64(pk))
    return buf
}
```

**VarChar PK:**
```go
func VarCharToBytes(pk string) []byte {
    return []byte(pk)  // Direct conversion
}
```

**Type-specific optimization:** Could have separate BBHash instances for int64 vs. varchar shards to optimize further.

---

#### Serialization Format

**Index File Structure:**
```
v<timestamp>.index:
  [Header: 32 bytes]
    - Magic number: 8 bytes
    - Version: 4 bytes
    - Num keys: 8 bytes
    - BBHash size: 8 bytes
    - Reserved: 4 bytes
  [BBHash data: variable]
  [Footer checksum: 32 bytes (SHA256)]

v<timestamp>.aux:
  [Header: 32 bytes]
    - Magic number: 8 bytes
    - Version: 4 bytes
    - Num entries: 8 bytes
    - Format type: 4 bytes (array/bitmap/hybrid)
    - Reserved: 4 bytes
  [Auxiliary data: variable]
  [Footer checksum: 32 bytes (SHA256)]
```

---

#### Decision Criteria

**To finalize:**
1. Select BBHash library (performance benchmark on Milvus PK data)
2. Select auxiliary structure (Option A/B/C based on multi-segment PK frequency)
3. Test with representative dataset:
   - 1B int64 PKs
   - 1B varchar PKs (varying lengths)
   - Measure: build time, query latency, memory usage
4. Validate serialization roundtrip

**Recommendation:** Prototype all three auxiliary structure options with 100M PK sample to measure real memory/latency.

---

### 9.3 PK Index Loading Strategy

**Decision Needed:** How StreamingNode loads and manages PK indices in memory

**Resolution Needed Before:** Phase 2 implementation

This decision is tightly coupled with 9.1 (deployment model) and 9.2 (data structure).

#### Option A: Eager Loading (Full Index in Memory)

**Behavior:**
- StreamingNode loads entire shard PK index on startup or when notified of new version
- All indices kept in memory for lifetime of process
- No disk I/O during queries

**Memory Requirement:**
```
All shards × index size per shard
Example: 100 shards × 16.5 GB = 1.65 TB
```

**Pros:**
- Fastest query path (no I/O)
- Predictable latency (no cache misses)
- Simple implementation (no cache logic)

**Cons:**
- Massive memory requirement at scale
- Slow cold start (load all indices before serving)
- Wastes memory on cold shards (rarely queried)

**When to Use:**
- Deployment model is sharding-based (Option C from 9.1)
- Each StreamingNode owns small shard subset (< 50 shards)
- Memory is abundant (> 1TB RAM per node)
- Latency is absolute priority (no variance tolerated)

---

#### Option B: Lazy Loading with LRU Cache

**Behavior:**
- StreamingNode loads index on first query to that shard
- Keep most-recently-used N shards in memory
- Evict least-recently-used when memory limit reached
- Fetch from object storage on cache miss

**Memory Requirement:**
```
Cache size × index size per shard
Example: 50 shard cache × 16.5 GB = 825 GB
```

**Pros:**
- Memory usage adapts to working set (hot shards)
- Fast cold start (only load on demand)
- Better memory efficiency than Option A

**Cons:**
- Variable query latency (cache miss = object storage fetch)
- Cold shard queries slow (first access)
- Cache eviction logic complexity
- Need to handle cache stampede on index rebuild

**Implementation Details:**

**LRU Cache:**
```go
type IndexCache struct {
    mu sync.RWMutex
    cache map[ShardID]*PKIndex
    lru *list.List  // LRU order
    maxSizeBytes int64
    currentSizeBytes int64
}

func (c *IndexCache) Get(shardID ShardID) (*PKIndex, error) {
    c.mu.RLock()
    if idx, ok := c.cache[shardID]; ok {
        c.mu.RUnlock()
        c.promoteToFront(shardID)  // Update LRU
        return idx, nil
    }
    c.mu.RUnlock()

    // Cache miss - load from storage
    return c.loadAndCache(shardID)
}
```

**Cache Eviction:**
- Evict when: `currentSizeBytes + newIndexSize > maxSizeBytes`
- Evict LRU shard until space available
- Configurable: `pk_index.cache_size_gb = 500`

**Cache Miss Handling:**
- Fetch index from object storage (1-5 seconds)
- During fetch, concurrent queries for same shard wait (coalescing)
- On failure, fall back to bloom filter

**When to Use:**
- Deployment model is distributed (Option B from 9.1)
- Large shard count (> 100) with skewed access pattern
- Memory limited (< 500GB per node)
- Can tolerate variable latency (P99 = P50 + object fetch time)

---

#### Option C: Partitioned BBHash with On-Demand Chunk Loading

**Behavior:**
- BBHash index is partitioned into chunks (e.g., by PK range or hash bucket)
- StreamingNode loads BBHash metadata (small) into memory
- Load specific chunks on demand per query
- Chunks cached in memory (LRU)

**Memory Requirement:**
```
Metadata: ~1 byte/key (routing info)
Chunks in cache: configurable, e.g., 50GB cache for hot chunks

For 1B PKs:
Metadata: 1 GB (always in memory)
Chunks: 50-100 GB cache (hot data)
Total per node: ~100 GB
```

**Pros:**
- Smallest memory footprint
- Scales to massive shard counts
- Fast queries for hot data (chunk cached)

**Cons:**
- BBHash must support partitioning (implementation complexity)
- Chunk boundary logic needed
- Query latency variance (chunk cache miss)
- More complex than Options A or B

**Partitioning Strategy:**

**By PK Range:**
```
1B PKs divided into 1000 chunks of 1M PKs each
Chunk 0: PKs 0-999,999
Chunk 1: PKs 1M-1,999,999
...

Routing: BBHash(PK) → determine chunk → load chunk
```

**By Hash Bucket:**
```
BBHash function internally organized as buckets
Load buckets on demand based on PK hash

Each bucket: ~1MB compressed
1000 buckets = 1GB per shard, cache 50-100 buckets
```

**Chunk Size Trade-off:**
- Too small (< 1MB): Many chunk loads, overhead
- Too large (> 100MB): Wastes memory, long load time
- Recommended: **10-50 MB per chunk** (sweet spot)

**When to Use:**
- Deployment model is centralized or distributed (any)
- Shard count is massive (> 1000 shards)
- Memory severely constrained (< 256GB per node)
- Willing to implement BBHash partitioning (complex)

---

#### Option D: Hybrid (Eager for Hot, Lazy for Cold)

**Behavior:**
- Identify "hot shards" via access pattern analysis
- Eagerly load hot shards (keep in memory always)
- Lazily load cold shards (on-demand, with eviction)

**Memory Requirement:**
```
Hot shards: 20% of total (always loaded)
Cold shard cache: 10% of total (LRU cache)
Total: 30% of full dataset

Example:
1000 shards × 16.5 GB = 16.5 TB total
Hot (20%): 200 shards × 16.5 GB = 3.3 TB
Cold cache (10%): 100 shards × 16.5 GB = 1.65 TB
Total: 4.95 TB

With 10 nodes: 495 GB per node
```

**Pros:**
- Best of both worlds (fast for hot, efficient for cold)
- Adapts to access patterns over time
- Balances memory and latency

**Cons:**
- Need hot/cold classification logic
- Must periodically re-evaluate hot/cold (access pattern changes)
- More complex than pure eager or lazy

**Hot Shard Identification:**
- Track query rate per shard (rolling 1-hour window)
- Classify as hot if: queries_per_hour > threshold (e.g., 100)
- Re-evaluate every 15 minutes
- Pin hot shards in memory (no eviction)

**When to Use:**
- Access patterns are skewed (80/20 rule applies)
- Want to minimize latency for common queries
- Can afford 30-50% of full memory footprint
- Production deployment at scale

---

#### Decision Criteria and Evaluation

**Information Needed:**
1. **Shard count and size distribution**
2. **Access pattern skew** - Are 20% of shards responsible for 80% of queries?
3. **Memory budget per StreamingNode**
4. **Latency SLA** - Acceptable P99 latency?
5. **Cold start tolerance** - How long can startup take?

**Evaluation Plan:**
1. **Instrument current system** - Measure query rate per shard for 1 week
2. **Analyze skew** - Plot query distribution (Pareto chart)
3. **Prototype** - Test Option B (LRU) and Option D (Hybrid) with real data
4. **Benchmark** - Measure:
   - Cache hit rate (target > 95%)
   - P50, P99 query latency (cache hit vs. miss)
   - Memory usage over time
5. **Select** - Based on:
   - If skew > 80/20 → **Option D (Hybrid)**
   - If memory abundant → **Option A (Eager)**
   - If memory constrained + uniform access → **Option B (LRU)**
   - If extreme scale (> 10k shards) → **Option C (Partitioned)**

**Default Recommendation:** **Option D (Hybrid)** for production
- Realistic assumption: access patterns are skewed
- Balances latency and memory
- Adapts to changing patterns

---

### 9.4 Query Path Integration with Existing Bloom Filter System

**Decision Needed:** How to integrate PK index with existing bloom filter-based pruning

**Resolution Needed Before:** Phase 2 implementation

#### Background: Current Bloom Filter System

**Current Query Flow:**
```
Query PK → PkOracle
  → Bloom filter prunes segments (1000 → 5-20 candidates)
  → Scan candidate segments for exact PK match
  → Return result
```

**Current Code:**
- `/internal/querynodev2/pkoracle/pk_oracle.go` - Segment lookup
- `/internal/querynodev2/pkoracle/bloom_filter_set.go` - BF pruning
- `/internal/querynodev2/segments/segment_interface.go` - Segment scan

#### Option A: Replace BF for Sealed Segments

**Behavior:**
- Sealed segments: Use BBHash index only (skip BF entirely)
- Growing segments: Continue using current BF system
- StreamingNode query returns exact segments for sealed, candidates for growing

**Query Flow:**
```
Query PK → StreamingNode PK Index Service
  ├─> Sealed segments: BBHash lookup → exact SegmentID(s)
  └─> Growing segments: Bloom filter → candidates

QueryNode receives:
  - Sealed: [seg1, seg5] (exact)
  - Growing: [seg100, seg101, seg102] (candidates, need scan)
```

**Code Changes:**
```go
// In StreamingNode
func (s *StreamingNode) LookupPK(pk PrimaryKey, shardID ShardID) (*PKLookupResult, error) {
    sealedSegs := s.pkIndex.Query(pk, shardID)  // BBHash lookup
    growingCandidates := s.bfOracle.Query(pk, shardID)  // Existing BF
    return &PKLookupResult{
        SealedSegments: sealedSegs,      // Exact
        GrowingCandidates: growingCandidates,  // Approximate
    }
}
```

**Pros:**
- Clean separation (sealed = exact, growing = approximate)
- No BF overhead for sealed segments (memory savings)
- Maximum performance for sealed segment queries

**Cons:**
- Must maintain both code paths
- Migration complexity (transition sealed segments from BF to index)
- QueryNode must handle two result types

**When to Use:**
- BF memory is constrained (want to free up sealed segment BF memory)
- Sealed segment queries are majority of workload
- Clean separation valued over code simplicity

---

#### Option B: Parallel Lookup (BF + Index)

**Behavior:**
- Query both BF and BBHash index in parallel
- Merge results: `segments = BF_results ∩ BBHash_results`
- Use intersection as final candidate set
- Both sealed and growing use BF + index

**Query Flow:**
```
Query PK → Parallel
  ├─> BF Oracle: segments = BF_prune(PK)
  └─> PK Index: segments = BBHash_lookup(PK)

Merge: final_segments = segments_BF ∩ segments_BBHash
QueryNode scans: final_segments
```

**Pros:**
- Extra validation (index and BF must agree)
- Catches index bugs (if BF says "not here" but index says "here", investigate)
- Gradual rollout (can compare BF vs. index accuracy)

**Cons:**
- Redundant work (why query both if index is exact?)
- Slower than single-path (both lookups + merge)
- Still pay BF memory cost

**When to Use:**
- Validation/testing phase (Phase 2 rollout)
- Want to compare BF and index results for correctness
- Temporary approach during migration

**Not recommended for production** - too much overhead for little benefit.

---

#### Option C: Conditional Fallback

**Behavior:**
- Check if BBHash index is available and loaded for the shard
- If yes: Use index (skip BF)
- If no: Fall back to BF

**Query Flow:**
```
Query PK → StreamingNode
  if pk_index.Loaded(shardID):
      segments = pk_index.Query(PK, shardID)  // Exact
  else:
      segments = bf_oracle.Query(PK, shardID)  // Approximate
  return segments
```

**Code Changes:**
```go
func (s *StreamingNode) LookupPK(pk PrimaryKey, shardID ShardID) ([]SegmentID, error) {
    if index, ok := s.pkIndexCache.Get(shardID); ok {
        return index.Query(pk)  // BBHash lookup
    }
    // Fallback to BF
    return s.bfOracle.Query(pk, shardID)
}
```

**Pros:**
- Graceful degradation (works even if index unavailable)
- Simple conditional logic
- Unified interface (both return segment list)
- Easy to roll out (enable per shard/collection)

**Cons:**
- Must maintain both systems long-term
- BF memory still allocated (even if unused when index loaded)

**When to Use:**
- Want gradual rollout with safety net
- Concerned about index availability/reliability
- Need backward compatibility during migration

**Recommended for Phase 2-3 rollout** - safe migration path.

---

#### Option D: Two-Phase (BF Pre-filter + Index Verification)

**Behavior:**
- Phase 1: BF prunes segments (cheap, probabilistic)
- Phase 2: Index verifies exact match within BF candidates (expensive, exact)

**Query Flow:**
```
Query PK → BF Oracle
  candidates = BF_prune(PK)  // e.g., 1000 → 5 segments

For each candidate:
  exact_match = pk_index.QuerySegment(PK, segment)
  if exact_match:
      return segment

If no exact match found:
  return "PK not found"
```

**Pros:**
- Combines BF speed with index accuracy
- BF reduces index lookup scope (cheaper than full index scan)

**Cons:**
- Only useful if index is segment-level, not shard-level
- Shard-level BBHash index (our design) makes this unnecessary
- Complexity without benefit

**Not recommended** - Our design is shard-level index, not segment-level, so BF doesn't reduce index search space.

---

#### Decision Criteria and Evaluation

**Information Needed:**
1. **BF false positive rate in production** - How many false positives currently?
2. **BF memory usage** - How much memory freed by removing sealed segment BFs?
3. **Query pattern** - What % of queries hit sealed vs. growing segments?
4. **Rollout risk tolerance** - How much fallback safety needed?

**Evaluation Plan:**
1. **Instrument current system** - Measure BF FP rate for sealed segments
2. **Prototype** - Implement Option A and Option C in Phase 2
3. **A/B test** - Compare:
   - Latency: BF-only vs. Index-only vs. Conditional
   - Memory: BF + Index vs. Index-only
   - Correctness: Any discrepancies?
4. **Select** - Based on:
   - If BF FP rate high (> 10%) → **Option A** (replace)
   - If memory constrained → **Option A** (replace to free BF memory)
   - If safety critical → **Option C** (conditional fallback)

**Default Recommendation:** **Option C (Conditional Fallback)** for Phase 2-3
- Safe gradual rollout
- Easy to revert (disable index, use BF)
- Can transition to Option A (replace) in Phase 4 once proven

**Long-term Goal:** **Option A (Replace)** - eliminate sealed segment BFs entirely once index is proven.

---

### 9.5 Consistency Model for PK Index

**Decision Needed:** Consistency guarantees between PK index and actual data

**Resolution Needed Before:** Phase 2 design

#### Background: Index Staleness

**Staleness Sources:**
1. **Baseline rebuild:** Index rebuilt every 1 hour → up to 1 hour stale
2. **Event-triggered rebuild:** Compaction completes, but index rebuild delayed (5 min debounce)
3. **Growing→Sealed transition:** Segment sealed, but not yet in next index rebuild
4. **Index load lag:** New index version built, but StreamingNode hasn't loaded it yet

**Impact:**
- **False negatives:** PK exists in data, but not in index (segment sealed after last rebuild)
- **False positives:** PK in index, but deleted from data (delete happened after last rebuild)
- **Stale segment mapping:** Index points to old segment, data moved during compaction

#### Option A: Eventually Consistent (Hourly Refresh)

**Guarantee:**
- Index reflects data as of last successful rebuild
- Staleness bounded by rebuild interval (1 hour baseline)
- No real-time consistency

**Behavior:**
```
T=0:00 - Index rebuilt, reflects data at T=0:00
T=0:30 - Insert PK=123 into segment S1
         Index still thinks PK=123 doesn't exist (stale)
T=1:00 - Index rebuilt, now reflects PK=123 in S1
```

**Query Behavior:**
- **Point query at T=0:30:** Index says "not found" → must query growing segments via BF
- **Upsert at T=0:30:** Index says "not found" → inserts duplicate → corrected at T=1:00 rebuild
- **Delete at T=0:30:** Index says "segment S2" → forwards delete to S2 (correct)

**Pros:**
- Simplest implementation (no special handling)
- Lowest rebuild cost (only 1/hour)
- Existing BF system handles growing segment queries

**Cons:**
- Up to 1-hour staleness (users see inconsistent results)
- Upsert may create duplicates (until next rebuild)
- Point queries miss recently inserted PKs

**When to Use:**
- Use case tolerates eventual consistency (e.g., batch analytics)
- Insert rate low (few new PKs per hour)
- Duplicate handling acceptable (deduplication on read)

---

#### Option B: Bounded Staleness (5-15 minute refresh)

**Guarantee:**
- Index reflects data as of last N minutes (configurable)
- Staleness bounded by shorter rebuild interval (e.g., 15 min)
- Trade-off: more frequent rebuilds

**Behavior:**
```
Rebuild interval: 15 minutes
T=0:00 - Index rebuilt
T=0:10 - Insert PK=123 into segment S1 (index stale)
T=0:15 - Index rebuilt, now includes PK=123
```

**Query Behavior:**
- **Point query at T=0:10:** Index says "not found" → must query growing segments
  - **Fallback:** Query both index (sealed) and BF (growing)
- **Staleness:** Max 15 minutes behind

**Pros:**
- Tighter consistency than Option A (15 min vs. 1 hour)
- Still simple (just shorter interval)
- Better user experience (less stale data)

**Cons:**
- 4x more rebuilds (every 15 min vs. 1 hour)
  - More CPU/memory cost for builds
  - More object storage writes
- Still not read-your-writes (15 min lag)

**Configuration:**
```yaml
pk_index:
  baseline_rebuild_interval: 15m  # Down from 1h
  event_rebuild_debounce: 1m      # Down from 5m
```

**When to Use:**
- Use case requires fresher data (< 1 hour unacceptable)
- Can afford more frequent rebuilds (resource budget available)
- 15-minute staleness acceptable for business logic

---

#### Option C: Read-Your-Writes (Delta Index)

**Guarantee:**
- After insert/upsert/delete completes, subsequent queries see the change
- Index = Base index (sealed, rebuilt hourly) + Delta index (growing, real-time)

**Architecture:**
```
PK Index Service:
  Base Index (sealed segments):
    - Rebuilt every 1 hour
    - Stored in object storage
  Delta Index (growing segments):
    - Updated in real-time on insert/upsert/delete
    - In-memory hash table or bloom filter

Query: Check delta index first, then base index
```

**Behavior:**
```
T=0:00 - Base index rebuilt
T=0:10 - Insert PK=123 into growing segment S1
         → Delta index updated (PK=123 → S1)
T=0:11 - Query PK=123
         → Delta index hit → return S1 (read-your-writes)
```

**Data Structures:**
```go
type PKIndexService struct {
    baseIndex *BBHashIndex        // Sealed segments (rebuilt hourly)
    deltaIndex map[PK][]SegmentID // Growing segments (real-time)
    mu sync.RWMutex
}

func (s *PKIndexService) Query(pk PrimaryKey) []SegmentID {
    s.mu.RLock()
    defer s.mu.RUnlock()

    // Check delta first (growing segments)
    if segs, ok := s.deltaIndex[pk]; ok {
        return segs
    }

    // Fall back to base index (sealed segments)
    return s.baseIndex.Query(pk)
}
```

**Pros:**
- Read-your-writes consistency (users see own changes immediately)
- Base index still benefits from space-efficient BBHash
- Delta index small (only growing segment PKs)

**Cons:**
- More complex (two-tier index)
- Delta index memory overhead (hash table for growing PKs)
- Must sync delta index updates across StreamingNodes (if distributed)
- Growing→Sealed transition requires delta cleanup

**Delta Index Synchronization:**

**Centralized StreamingNode (9.1 Option A):**
- Single node, no sync needed
- Delta index is local in-memory map

**Distributed StreamingNode (9.1 Option B/C):**
- Need shared delta index or per-node delta + invalidation
- **Option C1:** Shared Redis for delta index
  - Every insert updates Redis
  - Every query checks Redis + base index
  - Cons: Network RTT per query
- **Option C2:** Per-node delta with message-based invalidation
  - Insert updates local delta on one node
  - Broadcast invalidation to other nodes via message queue
  - Cons: Invalidation lag (eventual consistency across nodes)

**Memory Estimate (Delta Index):**
```
Growing segment PK count: ~1% of total (assume)
1B total PKs → 10M growing PKs
Hash map: 16 bytes/key (PK) + 8 bytes/value (SegmentID) = 24 bytes/key
Delta index: 10M × 24 = 240 MB (very manageable)
```

**When to Use:**
- Use case requires read-your-writes (e.g., interactive applications)
- Can afford delta index memory (240 MB - 2 GB)
- StreamingNode deployment is centralized or uses shared delta store

---

#### Option D: Hybrid (Bounded Staleness + Delta for Hot PKs)

**Guarantee:**
- Base index: 15-minute bounded staleness (Option B)
- Delta index: Read-your-writes for recently modified PKs (Option C)

**Behavior:**
```
Base index: Rebuilt every 15 minutes
Delta index: Tracks PKs modified since last rebuild

T=0:00 - Base index rebuilt, delta cleared
T=0:05 - Upsert PK=123 → delta index updated
T=0:10 - Query PK=123 → delta hit (read-your-writes)
T=0:15 - Base index rebuilt → includes PK=123 → delta cleared
```

**Delta Index Lifecycle:**
```go
func (s *PKIndexService) OnInsert(pk PrimaryKey, segID SegmentID) {
    s.deltaIndex[pk] = append(s.deltaIndex[pk], segID)
}

func (s *PKIndexService) OnIndexRebuild(newBaseIndex *BBHashIndex) {
    s.mu.Lock()
    defer s.mu.Unlock()

    s.baseIndex = newBaseIndex
    s.deltaIndex = make(map[PK][]SegmentID)  // Clear delta
}
```

**Pros:**
- Best of both: read-your-writes + reasonable rebuild cost
- Delta index smaller (only 15 min of changes, not 1 hour)
- Balances consistency and performance

**Cons:**
- Most complex option (two-tier index + frequent rebuilds)
- Must coordinate delta clear with base rebuild

**Delta Index Size:**
```
Insert rate: 100k inserts/sec
Rebuild interval: 15 minutes = 900 seconds
Delta PKs: 100k × 900 = 90M PKs
Delta memory: 90M × 24 bytes = 2.16 GB (manageable)
```

**When to Use:**
- Use case requires read-your-writes but can't afford constant rebuilds
- High insert rate (delta grows quickly)
- Want balance between consistency and resource cost

---

#### Decision Criteria and Evaluation

**Information Needed:**
1. **Consistency requirements** - Does use case need read-your-writes? Or eventual OK?
2. **Insert/upsert rate** - How many PKs modified per hour?
3. **Query patterns** - What % of queries are for recently inserted PKs?
4. **Resource budget** - Can afford 4x more rebuilds (15 min vs. 1 hour)?

**Evaluation Questions:**
- Can users tolerate 1-hour stale data? → **Option A**
- Need fresher data (< 1 hour) but not real-time? → **Option B**
- Must support read-your-writes for interactive apps? → **Option C** or **Option D**
- High insert rate + read-your-writes needed? → **Option D**

**Default Recommendation:** **Option B (Bounded Staleness)** for most use cases
- 15-minute staleness acceptable for most applications
- Simpler than delta index approaches
- Reasonable rebuild cost (4x baseline)

**For Interactive Apps:** **Option D (Hybrid)** if read-your-writes is critical
- More complex, but provides strong consistency where needed

---

### 9.6 StreamingNode Index Access (Consolidated with 9.3)

**Note:** This decision is identical to Section 9.3 (PK Index Loading Strategy). The two sections address the same concern: how StreamingNode loads and accesses PK indices.

**Refer to Section 9.3** for full analysis of:
- Option A: Eager Loading
- Option B: Lazy Loading with LRU Cache
- Option C: Partitioned BBHash with On-Demand Chunks
- Option D: Hybrid (Hot Eager, Cold Lazy)

**Decision**: Recommend **Option D (Hybrid)** for production, based on analysis in 9.3.

## 10. Next Steps

### 10.1 Immediate Actions

1. **Resolve deferred decisions** through:
   - User provides BBHash data structure design
   - Prototype testing for loading strategies
   - Production traffic analysis for consistency requirements
   - Memory budget planning for deployment model

2. **Create implementation plan**
   - Break down Phase 1 (Build Infrastructure) into tasks
   - Assign ownership for DataCoord and DataNode work
   - Set up monitoring and metrics infrastructure
   - Create test plan and acceptance criteria

3. **Prototype validation** (optional but recommended)
   - Small-scale prototype of BBHash build and query
   - Validate memory usage and performance characteristics
   - Test integration points with existing code

### 10.2 Phase 1 Kickoff Checklist

- [ ] BBHash data structure design finalized
- [ ] DataNode build task interface defined
- [ ] Object storage format finalized
- [ ] DataCoord scheduling logic designed
- [ ] Monitoring metrics list finalized
- [ ] Test plan approved
- [ ] Team assignments complete

## 11. References

### 11.1 Existing Milvus Components

**Relevant code locations:**
- `/internal/storage/primary_key.go` - PK types and operations
- `/internal/storage/pk_statistics.go` - Current PK statistics with bloom filters
- `/internal/querynodev2/pkoracle/` - Current PK oracle and bloom filter set
- `/internal/util/bloomfilter/bloom_filter.go` - Bloom filter implementations
- `/internal/datacoord/` - DataCoord task scheduling
- `/internal/datanode/` - DataNode task execution

**Related designs:**
- Compaction design (similar build task pattern)
- Index building design (similar storage pattern)
- Segment loading design (similar metadata propagation)

### 11.2 External References

- **BBHash:** Minimal perfect hash function with 3-4 bits/key overhead
- **Vector Index Storage:** Object storage patterns in Milvus

---

**End of Design Document**
