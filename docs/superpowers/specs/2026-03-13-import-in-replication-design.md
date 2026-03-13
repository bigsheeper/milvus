# Import in Replication Scenarios - Design Document

**Date:** 2026-03-13
**Author:** Design Collaboration Session
**Status:** Draft v1

## Overview

Enable data import operations in Milvus clusters with active CDC replication using a manual two-phase commit protocol. This allows primary clusters to import data while maintaining strong consistency across all replicated secondary clusters.

### Current Limitation

Import is explicitly blocked when replication is active (`internal/datacoord/ddl_callbacks_import.go:121-123`):

```go
// Import in replicating cluster is not supported yet
if channelAssignment.ReplicateConfiguration != nil &&
   len(channelAssignment.ReplicateConfiguration.GetClusters()) > 1 {
    return merr.WrapErrImportFailed("import in replicating cluster is not supported yet")
}
```

### Goals

1. **Remove replication blocking** - Allow imports when CDC replication is active
2. **Strong consistency** - All-or-nothing semantics across all clusters
3. **Manual coordination** - Operator controls when imported data becomes visible
4. **CDC compatibility** - Fix per-vchannel TimeTick for proper checkpoint recovery
5. **Minimal complexity** - Reuse existing broadcast and CDC mechanisms

### Non-Goals

- Automatic coordination between primary and secondaries
- Import on secondary (follower) clusters
- Backward-incompatible API changes

---

## Architecture Overview

### High-Level Flow

**Replicating Clusters (Manual Commit):**
```
┌─────────────────────────────────────────────────────────────┐
│                     PRIMARY CLUSTER                          │
│  User → Proxy → DataCoord.ImportV2()                        │
│         ↓                                                    │
│  DataCoord broadcasts ImportMessage (via CDC)               │
│         ↓                                                    │
│  ImportJob: Pending → ... → IndexBuilding → WaitingCommit  │
│                                                              │
│  User calls: CommitImport(jobID) or AbortImport(jobID)     │
│         ↓                                                    │
│  DataCoord broadcasts: CommitImportMessage / AbortImport    │
└─────────────────────────────────────────────────────────────┘
                           │
                           │ CDC Replication
                           ↓
┌─────────────────────────────────────────────────────────────┐
│                   SECONDARY CLUSTER(S)                       │
│  Proxy receives ImportMessage → DataCoord.importV1AckCallback│
│         ↓                                                    │
│  ImportJob: Pending → ... → IndexBuilding → WaitingCommit  │
│         ↓                                                    │
│  Receives CommitImportMessage or AbortImportMessage         │
│         ↓                                                    │
│  Transitions: WaitingCommit → Completed or Failed           │
└─────────────────────────────────────────────────────────────┘
```

**Non-Replicating Clusters (Auto-Commit):**
```
┌─────────────────────────────────────────────────────────────┐
│                   NON-REPLICATING CLUSTER                    │
│  User → Proxy → DataCoord.ImportV2()                        │
│         ↓                                                    │
│  ImportJob: Pending → ... → IndexBuilding → WaitingCommit  │
│         ↓                                                    │
│  DataCoord auto-broadcasts CommitImportMessage              │
│         ↓                                                    │
│  Transitions: WaitingCommit → Completed (seamless)          │
│                                                              │
│  Result: User sees IndexBuilding → Completed (fast)         │
└─────────────────────────────────────────────────────────────┘
```

### Key Design Decisions

1. **Unified FSM with conditional trigger** - All clusters use same state machine (IndexBuilding → WaitingCommit → Completed)
2. **Auto-commit for non-replicating clusters** - Non-replicating clusters automatically broadcast CommitImportMessage (backward compatible)
3. **Manual commit for replicating clusters** - Replicating clusters wait for user to call CommitImport RPC
4. **User responsibility for consistency** - User must manually check all clusters are in WaitingCommit before calling CommitImport (no automatic validation)
5. **Reuse existing ImportMessage** - No new PreImport message type needed
6. **Same JobID everywhere** - All clusters use identical JobID from ImportMessage
7. **Shared object storage** - All clusters read from same S3/MinIO location
8. **Independent execution** - Each cluster manages its own ImportJob lifecycle
9. **Idempotent operations** - Commit/abort can be retried safely

---

## State Machine Changes

### Current FSM (8 states)

```
Pending → PreImporting → Importing → Sorting → IndexBuilding → Completed
            ↓              ↓           ↓            ↓              ↓
                             Failed (any stage)
```

### New FSM (9 states)

```
Pending → PreImporting → Importing → Sorting → IndexBuilding → WaitingCommit → Completed
            ↓              ↓           ↓            ↓               ↓              ↓
                             Failed (any stage, or explicit abort)
```

### New State: WaitingCommit

**Purpose:** Gate before making imported segments queryable, waiting for commit trigger (automatic or manual).

**Entry Condition:**
- IndexBuilding completed successfully
- **All clusters** enter this state (unified FSM)

**Exit Conditions:**
- Receives `CommitImportMessage` → `Completed`
- Receives `AbortImportMessage` → `Failed`
- Timeout (default 1 hour) → `Failed` with auto-cleanup

**Commit Trigger Behavior:**
- **Non-replicating clusters:** DataCoord automatically broadcasts `CommitImportMessage` upon entering WaitingCommit (seamless auto-commit)
- **Replicating clusters:** Wait for user to call `CommitImport(jobID)` RPC (manual commit)

**Characteristics:**
- Segments exist with binlogs in object storage
- Segments are indexed
- Segments have flag `importing = true` (not queryable)
- Job metadata persisted in etcd

**Backward Compatibility:**
- Non-replicating clusters: Auto-commit maintains existing behavior (IndexBuilding → WaitingCommit → auto-broadcast → Completed)
- No user-visible changes for existing deployments without CDC replication

### State Transition Rules

| From State | To State | Trigger | Notes |
|------------|----------|---------|-------|
| IndexBuilding | WaitingCommit | Auto (all clusters) | Unified FSM - all clusters enter WaitingCommit |
| WaitingCommit | Completed | CommitImportMessage broadcast | Auto-broadcast (non-replicating) or manual RPC (replicating) |
| WaitingCommit | Failed | AbortImportMessage broadcast | Manual trigger |
| WaitingCommit | Failed | Timeout (1 hour default) | Automatic safety |
| Any state (except Completed/Failed) | Failed | AbortImportMessage | Manual abort allowed anytime |

---

## Consistency Model and User Responsibilities

### Design Philosophy

This design provides **eventual strong consistency with manual coordination**:
- All clusters use the **same JobID** and execute the **same import job**
- **User is the coordinator** - responsible for checking readiness and triggering commit
- **No automatic validation** - Primary does not query secondaries before committing
- **Idempotent broadcasts** - Commit/abort messages can be retried safely

### Consistency Guarantees

**What the system guarantees:**
1. ✅ **Atomic local transitions** - Each cluster's state transition (WaitingCommit → Completed) is atomic
2. ✅ **Broadcast delivery** - CommitImportMessage delivered to all clusters via CDC (at-least-once semantics)
3. ✅ **Idempotent operations** - Receiving CommitImportMessage multiple times is safe (no-op if already Completed)
4. ✅ **Segment visibility control** - Segments not queryable until WaitingCommit → Completed transition

**What the system does NOT guarantee:**
1. ❌ **No automatic secondary validation** - Primary does not check if secondaries are in WaitingCommit before broadcasting
2. ❌ **No distributed transaction** - No 2PC coordinator that ensures all-or-nothing atomically
3. ❌ **No automatic rollback** - If one secondary fails, others do not auto-rollback
4. ❌ **No cross-cluster state synchronization** - Clusters do not wait for each other

### User Responsibilities

**The user must:**

1. **Manually check all clusters** before calling `CommitImport`:
   ```bash
   # Check primary
   curl "http://primary:19530/v2/vectordb/jobs/import/get_progress?jobId=123"
   # Output: {"state": "WaitingCommit"}

   # Check secondary1
   curl "http://secondary1:19530/v2/vectordb/jobs/import/get_progress?jobId=123"
   # Output: {"state": "WaitingCommit"}

   # Check secondary2
   curl "http://secondary2:19530/v2/vectordb/jobs/import/get_progress?jobId=123"
   # Output: {"state": "WaitingCommit"}

   # ALL must be in WaitingCommit before committing!
   ```

2. **Handle partial failures**:
   - If any secondary stuck in IndexBuilding → Wait or call `AbortImport`
   - If any secondary transitions to Failed → Call `AbortImport` to clean up all clusters

3. **Retry on partial broadcast failure**:
   - If CommitImport succeeds but some secondaries still in WaitingCommit → Retry CommitImport
   - Idempotent - safe to retry multiple times

4. **Monitor for divergence**:
   - After CommitImport, verify all clusters transitioned to Completed
   - If divergence detected (some Completed, some WaitingCommit) → Retry CommitImport

### Why Manual Coordination?

**Design rationale:**
1. **Simplicity** - No complex distributed coordination protocol
2. **Visibility** - Operator has full visibility and control
3. **Flexibility** - Operator can decide to wait, abort, or investigate
4. **Production-friendly** - Many enterprise ops prefer explicit control over automatic behavior
5. **Failure handling** - Operator can make context-aware decisions (e.g., "secondary is doing maintenance, proceed anyway")

**Trade-off accepted:**
- User must perform manual checks (error-prone)
- No automatic enforcement of readiness
- Potential for operator error (committing when secondary not ready)

### Consistency Scenarios

**Scenario 1: Happy Path**
```
Primary: IndexBuilding → WaitingCommit ✅
Secondary1: IndexBuilding → WaitingCommit ✅
Secondary2: IndexBuilding → WaitingCommit ✅

User checks all clusters: All in WaitingCommit
User calls CommitImport(jobID)
Primary broadcasts CommitImportMessage

Primary: WaitingCommit → Completed ✅
Secondary1: WaitingCommit → Completed ✅
Secondary2: WaitingCommit → Completed ✅

Result: Strong consistency achieved
```

**Scenario 2: Secondary Not Ready**
```
Primary: WaitingCommit ✅
Secondary1: WaitingCommit ✅
Secondary2: Still in Importing ❌

User checks all clusters: Secondary2 NOT ready!
User options:
  A) Wait for Secondary2 to catch up
  B) Call AbortImport (clean up all clusters)
  C) Investigate why Secondary2 is slow

User does NOT call CommitImport yet.
```

**Scenario 3: User Commits Too Early (Error)**
```
Primary: WaitingCommit ✅
Secondary1: WaitingCommit ✅
Secondary2: Still in Importing ❌

User mistakenly calls CommitImport (didn't check Secondary2)
Primary broadcasts CommitImportMessage

Primary: WaitingCommit → Completed ✅
Secondary1: WaitingCommit → Completed ✅
Secondary2: Receives CommitImportMessage while in Importing 💥

What happens to Secondary2?
→ See next section "Handling Out-of-Order Messages"
```

**Scenario 4: Partial Broadcast Failure**
```
Primary: WaitingCommit → Completed ✅
Secondary1: WaitingCommit → Completed ✅
Secondary2: Network issue, CommitImportMessage lost ❌

User detects divergence (polls show Secondary2 still WaitingCommit)
User retries: CommitImport(jobID)
Primary re-broadcasts CommitImportMessage (idempotent)

Secondary2: Receives message → WaitingCommit → Completed ✅

Result: Eventual consistency achieved via retry
```

### Handling Out-of-Order Messages

**Problem:** What if CommitImportMessage arrives when cluster not in WaitingCommit?

**Current Design Decision:** **Message is processed based on current state**

**Implementation:**
```go
func (c *DDLCallbacks) commitImportAckCallback(ctx context.Context, result message.BroadcastResultCommitImportMessageV1) error {
    jobID := result.Message.MustBody().GetJobId()
    job := c.importMeta.GetJob(jobID)

    if job == nil {
        // Job doesn't exist - already cleaned up or wrong cluster
        log.Warn("commit import message for non-existent job, ignoring",
            zap.Int64("job_id", jobID))
        return nil
    }

    state := job.GetState()
    switch state {
    case internalpb.ImportJobStateV2_WaitingCommit:
        // Expected case: process normally
        return c.processCommitImport(ctx, jobID)

    case internalpb.ImportJobStateV2_Completed:
        // Already completed (duplicate message) - idempotent no-op
        log.Info("job already completed, ignoring duplicate commit message",
            zap.Int64("job_id", jobID))
        return nil

    default:
        // Job not yet in WaitingCommit (too early) OR already Failed
        log.Error("received commit import message but job not in WaitingCommit",
            zap.Int64("job_id", jobID),
            zap.String("current_state", state.String()))

        // Option 1: Drop message (current design - user's mistake)
        return merr.WrapErrImportFailed(
            fmt.Sprintf("job %d in state %s, cannot commit", jobID, state))

        // Option 2: Queue message for later (future enhancement)
        // c.pendingCommits.Store(jobID, result.Message)
        // return nil
    }
}
```

**Current Behavior:** If CommitImportMessage arrives too early → Error logged, message dropped, job stays in current state

**Consequence:** User made a mistake (committed too early) → Secondary stays in current state → Timeout → Auto-abort

**User Recovery:** Call `AbortImport` to clean up all clusters, fix the slow secondary, retry import

---

## Write Consistency: Import Data Timestamps

### Problem Statement

**The Critical Issue:**

When import executes, all imported rows are assigned a **single timestamp** from the ImportMessage broadcast time (`task.req.GetTs()`). This timestamp is written to storage during segment flush and persists in binlogs.

```go
// File: internal/datanode/importv2/util.go (lines 188-226)
func AppendSystemFieldsData(task *ImportTask, data *storage.InsertData, rowNum int) error {
    tss := make([]int64, rowNum)
    ts := int64(task.req.GetTs())  // ALL rows get T_import
    for i := 0; i < rowNum; i++ {
        tss[i] = ts
    }
    data.Data[common.TimeStampField] = &storage.Int64FieldData{Data: tss}
}
```

**Timeline of the Problem:**

```
T=1000: ImportMessage broadcast → import executes
        → All rows written to binlogs with timestamp = 1000
        → Segments in WaitingCommit (importing=true, NOT queryable)

T=2000: User executes: DELETE pk=2
        → Delta log written: (pk=2, ts=2000)
        → Delete applies to VISIBLE segments only (import segments hidden)

T=3000: User calls CommitImport → CommitImportMessage broadcast
        → Segments become queryable (importing=false)
        → QueryNode filtering: row.ts=1000 < delete.ts=2000
        → DELETE APPLIES! Row with pk=2 is deleted

Result: The DELETE at T=2000 deleted data that was "logically non-existent"
        at that time (hidden in WaitingCommit).
```

### Why Current Behavior Is Semantically Wrong

**Semantic Expectation:**

- Import data should be treated as "appearing" at T_commit (when it becomes queryable)
- DML operations before T_commit should NOT affect import data
- Only DML operations AFTER T_commit should apply

**Current Reality:**

- Import data has T_import (old timestamp from broadcast)
- DML operations between T_import and T_commit incorrectly affect import data
- Timestamp ordering does not reflect logical visibility order

**Violations:**

1. **Causality violation**: DELETE at T=2000 deletes data that "doesn't exist yet" (hidden until T=3000)
2. **Cross-cluster inconsistency**: Primary users see DML results immediately, but secondaries see them after CDC lag
3. **Replay issues**: If import is aborted and retried, DML operations would apply differently on retry

### Solution: Segment-Level Visible Timestamp

**Design Choice: Approach C from analysis**

Use **segment-level metadata** to override row timestamps for filtering purposes, with eventual normalization via compaction.

**Why This Approach:**

✅ **No expensive rewrite** - commit is instant (just metadata update)
✅ **Immutable binlogs** - original data unchanged
✅ **Correct semantics** - DML filtering works correctly
✅ **Backward compatible** - non-import segments work as before
✅ **Natural granularity** - segments are already the unit of management
✅ **Eventual cleanup** - compaction normalizes data over time
✅ **Clean rollback** - abort just deletes segments
✅ **Minimal changes** - one metadata field + QueryNode filtering logic

### Implementation Design

#### Proto Changes

**File: `pkg/v2/proto/datapb/segment.proto`**

```protobuf
message SegmentInfo {
    // ... existing fields ...

    // visible_timestamp overrides row-level timestamps for filtering purposes.
    // Used for import segments to ensure DML operations respect logical visibility order.
    //
    // When set (non-zero):
    // - QueryNode uses this timestamp for filtering instead of row.timestamp
    // - Ensures DML operations before visible_timestamp do not affect this segment
    // - Ensures DML operations after visible_timestamp correctly apply
    //
    // When zero:
    // - Normal segment (not import), use row.timestamp for filtering
    //
    // Lifecycle:
    // - Set to T_commit when CommitImportMessage received
    // - Cleared (set to 0) after compaction rewrites row timestamps
    optional uint64 visible_timestamp = X;
}
```

#### DataCoord Changes

**File: `internal/datacoord/ddl_callbacks_import.go`**

```go
// commitImportAckCallback handles acknowledgment of CommitImportMessage broadcast.
// Transitions job to Completed and makes segments queryable with correct visible timestamp.
func (c *DDLCallbacks) commitImportAckCallback(ctx context.Context, result message.BroadcastResultCommitImportMessageV1) error {
    body := result.Message.MustBody()
    jobID := body.GetJobId()

    // Get timestamp from CommitImportMessage broadcast
    commitTimestamp := result.GetMaxTimeTick()  // T_commit from broadcast

    log.Ctx(ctx).Info("processing commit import ack",
        zap.Int64("job_id", jobID),
        zap.Uint64("commit_timestamp", commitTimestamp))

    // 1. Update job state to Completed
    err := c.importMeta.UpdateJobState(jobID, internalpb.ImportJobStateV2_Completed)
    if err != nil {
        return err
    }

    // 2. Mark all segments as queryable with visible_timestamp = T_commit
    job := c.importMeta.GetJob(jobID)
    if job == nil {
        return merr.WrapErrImportJobNotExist(jobID)
    }

    for _, task := range job.GetTasks() {
        for _, segmentID := range task.GetSegmentIDs() {
            segment := c.meta.GetSegment(segmentID)
            if segment != nil && segment.GetImporting() {
                // Atomic update: set visible_timestamp AND clear importing flag
                err := c.meta.UpdateSegmentVisibility(segmentID, commitTimestamp, false)
                if err != nil {
                    log.Ctx(ctx).Error("failed to update segment visibility",
                        zap.Int64("segment_id", segmentID), zap.Error(err))
                    return err
                }
            }
        }
    }

    log.Ctx(ctx).Info("import job committed successfully",
        zap.Int64("job_id", jobID),
        zap.Uint64("visible_timestamp", commitTimestamp),
        zap.Int("segment_count", len(job.GetTasks())))

    return nil
}
```

**New Meta Method:**

```go
// File: internal/datacoord/meta.go

// UpdateSegmentVisibility atomically updates segment's visible_timestamp and importing flag.
// Used during import commit to make segments queryable with correct timestamp semantics.
func (m *meta) UpdateSegmentVisibility(segmentID int64, visibleTimestamp uint64, importing bool) error {
    m.Lock()
    defer m.Unlock()

    segment := m.segments.GetSegment(segmentID)
    if segment == nil {
        return merr.WrapErrSegmentNotFound(segmentID)
    }

    // Clone segment for update
    cloned := proto.Clone(segment.SegmentInfo).(*datapb.SegmentInfo)
    cloned.VisibleTimestamp = visibleTimestamp
    cloned.Importing = importing

    // Persist to etcd
    err := m.catalog.AlterSegment(m.ctx, cloned)
    if err != nil {
        return err
    }

    // Update in-memory
    m.segments.SetSegment(segmentID, NewSegmentInfo(cloned))

    log.Info("updated segment visibility",
        zap.Int64("segment_id", segmentID),
        zap.Uint64("visible_timestamp", visibleTimestamp),
        zap.Bool("importing", importing))

    return nil
}
```

#### QueryNode Changes

**File: `internal/querynodev2/segments/segment.go` or relevant filtering code**

```go
// GetEffectiveTimestamp returns the timestamp to use for DML filtering.
// For import segments, returns visible_timestamp if set; otherwise row timestamp.
func (s *Segment) GetEffectiveTimestamp(rowTimestamp uint64) uint64 {
    // Check segment-level override
    if s.segmentInfo.GetVisibleTimestamp() != 0 {
        return s.segmentInfo.GetVisibleTimestamp()
    }

    // Normal segment: use row timestamp
    return rowTimestamp
}

// ApplyDelete filters rows based on effective timestamp semantics.
func (s *Segment) ApplyDelete(pks []PrimaryKey, deleteTss []uint64) {
    for i, pk := range pks {
        deleteTs := deleteTss[i]

        // Find matching rows
        rowOffsets := s.pkIndex.Query(pk)
        for _, offset := range rowOffsets {
            rowTs := s.timestampField.Get(offset)

            // Use effective timestamp for comparison
            effectiveTs := s.GetEffectiveTimestamp(rowTs)

            // Delete applies if: effectiveTs <= deleteTs
            if effectiveTs <= deleteTs {
                s.deleteBuffer.Add(offset)
            }
        }
    }
}
```

**Alternative Implementation (if filtering is centralized):**

```go
// File: internal/querynodev2/delegator/deletebuffer/delete_filter.go

func ApplyDeleteFiltering(segment *Segment, deleteData *storage.DeleteData) {
    visibleTs := segment.GetVisibleTimestamp()

    for i := 0; i < len(deleteData.Pks); i++ {
        pk := deleteData.Pks[i]
        deleteTs := deleteData.Tss[i]

        // Find rows with this PK in segment
        rows := segment.SearchPK(pk)
        for _, row := range rows {
            rowTs := row.Timestamp

            // Override with segment visible timestamp if set
            effectiveTs := rowTs
            if visibleTs != 0 {
                effectiveTs = visibleTs
            }

            // Apply delete if effective timestamp <= delete timestamp
            if effectiveTs <= deleteTs {
                segment.MarkDeleted(row.Offset)
            }
        }
    }
}
```

#### Compaction Changes

**File: `internal/datanode/compaction/mix_compactor.go` or relevant compaction code**

```go
// CompactSegments performs L0/L1 compaction, normalizing import segments.
func (c *MixCompactor) CompactSegments(segments []*datapb.SegmentInfo) (*datapb.CompactionResult, error) {
    // ... existing compaction logic ...

    // For each segment being compacted
    for _, segment := range segments {
        visibleTs := segment.GetVisibleTimestamp()

        if visibleTs != 0 {
            // This is an import segment with overridden timestamp
            log.Info("normalizing import segment during compaction",
                zap.Int64("segment_id", segment.GetID()),
                zap.Uint64("visible_timestamp", visibleTs))

            // Rewrite row timestamps to visible_timestamp
            for rowOffset := 0; rowOffset < segment.NumRows; rowOffset++ {
                originalTs := segment.GetTimestamp(rowOffset)
                segment.SetTimestamp(rowOffset, visibleTs)
            }

            // Clear visible_timestamp in output segment metadata
            // (row timestamps now reflect correct values)
            outputSegment.VisibleTimestamp = 0
        }

        // ... continue compaction with normalized timestamps ...
    }

    return compactionResult, nil
}
```

**Note:** The exact compaction integration depends on Milvus compaction architecture. The key points are:

1. **Detect import segments**: Check `segment.GetVisibleTimestamp() != 0`
2. **Rewrite timestamps**: Set all row timestamps to `visible_timestamp` value
3. **Clear metadata**: Set output segment's `visible_timestamp = 0`
4. **Result**: Normalized segment with correct row-level timestamps

### Cross-Cluster Consistency

**How This Achieves Write Consistency:**

```
PRIMARY:
T=1000: ImportMessage → import executes → rows written with ts=1000
T=2000: DELETE pk=2 → delta log (pk=2, ts=2000)
T=3000: CommitImportMessage (broadcast with ts=3000)
        → segment.visible_timestamp = 3000
        → segment.importing = false
        → QueryNode: effectiveTs=3000 > deleteTs=2000 → DELETE DOES NOT APPLY ✓

SECONDARY (via CDC):
T=1000: ImportMessage → import executes → rows written with ts=1000
T=3000: CommitImportMessage arrives (ts=3000)
        → segment.visible_timestamp = 3000
        → segment.importing = false
T=3000+lag: DELETE message arrives (pk=2, ts=2000)
        → QueryNode: effectiveTs=3000 > deleteTs=2000 → DELETE DOES NOT APPLY ✓

RESULT: Both clusters have IDENTICAL filtering behavior!
```

**Key Properties:**

1. **Same visible_timestamp**: Both clusters set `segment.visible_timestamp = T_commit` from same broadcast message
2. **Same DML timestamps**: DML operations have same timestamps on both clusters (replicated via CDC)
3. **Same filtering logic**: `effectiveTs > deleteTs` produces same result on all clusters
4. **Order-independent**: Whether DELETE arrives before or after CommitImportMessage doesn't matter - filtering logic is consistent

### Example Scenarios

**Scenario 1: DELETE Before Commit**

```
T=1000: Import executes → rows: (pk=1, ts=1000), (pk=2, ts=1000), (pk=3, ts=1000)
T=2000: DELETE pk=2 → delta log: (pk=2, ts=2000)
T=3000: CommitImport → segment.visible_timestamp = 3000

Query after commit:
- Row pk=1: effectiveTs=3000, no delete → VISIBLE ✓
- Row pk=2: effectiveTs=3000, deleteTs=2000, 3000 > 2000 → VISIBLE ✓ (delete does not apply)
- Row pk=3: effectiveTs=3000, no delete → VISIBLE ✓

Result: All 3 rows visible (correct - DELETE was before commit)
```

**Scenario 2: DELETE After Commit**

```
T=1000: Import executes → rows: (pk=1, ts=1000), (pk=2, ts=1000), (pk=3, ts=1000)
T=3000: CommitImport → segment.visible_timestamp = 3000
T=4000: DELETE pk=2 → delta log: (pk=2, ts=4000)

Query after delete:
- Row pk=1: effectiveTs=3000, no delete → VISIBLE ✓
- Row pk=2: effectiveTs=3000, deleteTs=4000, 3000 < 4000 → DELETED ✓ (delete applies)
- Row pk=3: effectiveTs=3000, no delete → VISIBLE ✓

Result: 2 rows visible (correct - DELETE was after commit)
```

**Scenario 3: INSERT Duplicate PK Before Commit**

```
T=1000: Import executes → import segment: (pk=2, ts=1000), importing=true (hidden)
T=2000: INSERT pk=2 → growing segment: (pk=2, ts=2000), queryable immediately
T=3000: CommitImport → import segment.visible_timestamp = 3000, importing=false

Query after commit:
- Growing segment row: rowTs=2000 → VISIBLE (older)
- Import segment row: effectiveTs=3000 → VISIBLE (newer)
- DUPLICATE PK! Both visible

Primary key deduplication during query:
- System sees: pk=2 at ts=2000 and pk=2 at ts=3000
- Picks LATEST: pk=2 at ts=3000 (from import segment)
- Result: Import data wins (correct - import "appeared" at T=3000)
```

**Scenario 4: After Compaction (Normalization)**

```
Before compaction:
- Segment 100: rows with ts=1000, visible_timestamp=3000
- Segment 101: rows with ts=1000, visible_timestamp=3000

During compaction:
- Read segments 100, 101
- Rewrite ALL row timestamps: ts=1000 → ts=3000
- Output segment 200: rows with ts=3000, visible_timestamp=0 (cleared)

After compaction:
- Segment 200: Normal segment, uses row.timestamp=3000 for filtering
- No special handling needed anymore
- Binlogs now contain correct timestamps
```

### Implementation Impact Summary

| Component | Change | Complexity |
|-----------|--------|------------|
| **SegmentInfo Proto** | Add `visible_timestamp` field | Low (1 field) |
| **DataCoord Commit** | Set `visible_timestamp = T_commit` | Low (~10 LOC) |
| **Meta Operations** | Add `UpdateSegmentVisibility()` | Low (~30 LOC) |
| **QueryNode Filtering** | Use effective timestamp for deletes | Medium (~50 LOC) |
| **Compaction** | Normalize timestamps, clear metadata | Medium (~50 LOC) |

**Total Estimated LOC:** ~150 lines

**Risk Level:** Low-Medium
- QueryNode filtering logic is critical path
- Requires careful testing of timestamp semantics
- Compaction normalization is non-urgent (can be added later)

**Performance Impact:**
- ✅ No extra cost during import (just metadata write)
- ✅ No extra cost during commit (same as before)
- ✅ Minimal cost in QueryNode (one extra if-check per delete operation)
- ✅ No extra storage cost (one uint64 per segment metadata)

---

## Protocol Buffer Definitions

### New Message Types

**File: `pkg/v2/proto/msg.proto`**

```protobuf
message CommitImportMessageHeader {
    int64 job_id = 1;
}

message CommitImportMsg {
    commonpb.MsgBase base = 1;
    int64 job_id = 2;
}

message AbortImportMessageHeader {
    int64 job_id = 1;
}

message AbortImportMsg {
    commonpb.MsgBase base = 1;
    int64 job_id = 2;
}
```

### New RPC Definitions

**File: `pkg/v2/proto/data_coord.proto`**

```protobuf
service DataCoord {
    // ... existing methods ...

    rpc CommitImport(CommitImportRequest) returns(common.Status) {}
    rpc AbortImport(AbortImportRequest) returns(common.Status) {}
}

message CommitImportRequest {
    common.MsgBase base = 1;
    string job_id = 2;
}

message AbortImportRequest {
    common.MsgBase base = 1;
    string job_id = 2;
}
```

### Enhanced Existing Protos

**File: `pkg/v2/proto/internal.proto`**

```protobuf
enum ImportJobStateV2 {
    None = 0;
    Pending = 1;
    PreImporting = 2;
    Importing = 3;
    Sorting = 4;
    IndexBuilding = 5;
    WaitingCommit = 6;  // NEW STATE
    Completed = 7;
    Failed = 8;
}

message ImportRequestInternal {
    // ... existing fields ...

    // DEPRECATED: Use vchannel_timestamps instead
    uint64 data_timestamp = 10 [deprecated=true];

    // NEW: Per-vchannel timestamps for CDC checkpoint recovery
    map<string, uint64> vchannel_timestamps = 11;
}

message ImportJob {
    // ... existing fields ...

    // DEPRECATED
    uint64 data_timestamp = 10 [deprecated=true];

    // NEW: Per-vchannel timestamps
    map<string, uint64> vchannel_timestamps = 11;
}
```

### Message Type Registration

**File: `pkg/v2/streaming/util/message/message_type.go`**

```go
const (
    // ... existing types ...
    MessageTypeCommitImport MessageType = 20
    MessageTypeAbortImport  MessageType = 21
)
```

---

## RPC Implementation

### CommitImport RPC

**File: `internal/datacoord/server.go`**

```go
// CommitImport commits an import job, making imported segments queryable across all replicated clusters.
// Can only be called when THIS cluster's job is in WaitingCommit state.
// Broadcasts CommitImportMessage to all vchannels via CDC.
//
// IMPORTANT: This RPC does NOT validate secondary cluster states.
// User must manually ensure all clusters are in WaitingCommit before calling this RPC.
// Call GetImportProgress on each cluster to verify readiness.
func (s *Server) CommitImport(ctx context.Context, req *datapb.CommitImportRequest) (*commonpb.Status, error) {
    log := log.Ctx(ctx).With(zap.String("job_id", req.GetJobId()))

    // 1. Validate request
    if req.GetJobId() == "" {
        return merr.Status(merr.WrapErrParameterInvalidMsg("job_id is required")), nil
    }

    // 2. Get import job from meta
    job := s.importMeta.GetJob(req.GetJobId())
    if job == nil {
        return merr.Status(merr.WrapErrImportJobNotExist(req.GetJobId())), nil
    }

    // 3. Validate THIS cluster's job state is WaitingCommit
    // NOTE: Does NOT check secondary clusters - user responsibility
    if job.GetState() != internalpb.ImportJobStateV2_WaitingCommit {
        return merr.Status(merr.WrapErrImportFailed(
            fmt.Sprintf("job %s is in state %s, expected WaitingCommit",
                req.GetJobId(), job.GetState()))), nil
    }

    // 4. Broadcast CommitImportMessage to all vchannels (including secondaries via CDC)
    log.Info("broadcasting commit import message")
    err := s.broadcastCommitImport(ctx, job.GetJobID(), job.GetCollectionID(), job.GetVchannels())
    if err != nil {
        log.Error("failed to broadcast commit import", zap.Error(err))
        return merr.Status(err), nil
    }

    log.Info("commit import message broadcasted successfully")
    return merr.Success(), nil
}
```

### AbortImport RPC

```go
// AbortImport aborts an import job, marking it as Failed and cleaning up segments.
// Can be called in any state except Completed.
// Broadcasts AbortImportMessage to all vchannels via CDC.
func (s *Server) AbortImport(ctx context.Context, req *datapb.AbortImportRequest) (*commonpb.Status, error) {
    log := log.Ctx(ctx).With(zap.String("job_id", req.GetJobId()))

    // 1. Validate request
    if req.GetJobId() == "" {
        return merr.Status(merr.WrapErrParameterInvalidMsg("job_id is required")), nil
    }

    // 2. Get import job from meta
    job := s.importMeta.GetJob(req.GetJobId())
    if job == nil {
        return merr.Status(merr.WrapErrImportJobNotExist(req.GetJobId())), nil
    }

    // 3. Validate state - can abort any state except terminal states
    state := job.GetState()
    if state == internalpb.ImportJobStateV2_Completed {
        return merr.Status(merr.WrapErrImportFailed(
            fmt.Sprintf("job %s already completed, cannot abort", req.GetJobId()))), nil
    }
    if state == internalpb.ImportJobStateV2_Failed {
        log.Info("job already failed, abort is no-op (idempotent)")
        return merr.Success(), nil
    }

    // 4. Broadcast AbortImportMessage to all vchannels
    log.Info("broadcasting abort import message", zap.String("state", state.String()))
    err := s.broadcastAbortImport(ctx, job.GetJobID(), job.GetCollectionID(), job.GetVchannels())
    if err != nil {
        log.Error("failed to broadcast abort import", zap.Error(err))
        return merr.Status(err), nil
    }

    log.Info("abort import message broadcasted successfully")
    return merr.Success(), nil
}
```

### Broadcast Helper Methods

**File: `internal/datacoord/ddl_callbacks_import.go`**

```go
// broadcastCommitImport broadcasts commit message to all vchannels of the import job.
func (s *Server) broadcastCommitImport(ctx context.Context, jobID int64, collectionID int64, vchannels []string) error {
    broadcaster, err := s.startBroadcastWithCollectionID(ctx, collectionID)
    if err != nil {
        return errors.Wrap(err, "failed to start broadcast")
    }
    defer broadcaster.Close()

    msg := message.NewCommitImportMessageBuilderV1().
        WithHeader(&message.CommitImportMessageHeader{
            JobId: jobID,
        }).
        WithBody(&msgpb.CommitImportMsg{
            Base: &commonpb.MsgBase{
                MsgType:   commonpb.MsgType_CommitImport,
                Timestamp: 0,
            },
            JobId: jobID,
        }).
        WithBroadcast(vchannels).
        MustBuildBroadcast()

    _, err = broadcaster.Broadcast(ctx, msg)
    return err
}

// broadcastAbortImport broadcasts abort message to all vchannels of the import job.
func (s *Server) broadcastAbortImport(ctx context.Context, jobID int64, collectionID int64, vchannels []string) error {
    broadcaster, err := s.startBroadcastWithCollectionID(ctx, collectionID)
    if err != nil {
        return errors.Wrap(err, "failed to start broadcast")
    }
    defer broadcaster.Close()

    msg := message.NewAbortImportMessageBuilderV1().
        WithHeader(&message.AbortImportMessageHeader{
            JobId: jobID,
        }).
        WithBody(&msgpb.AbortImportMsg{
            Base: &commonpb.MsgBase{
                MsgType:   commonpb.MsgType_AbortImport,
                Timestamp: 0,
            },
            JobId: jobID,
        }).
        WithBroadcast(vchannels).
        MustBuildBroadcast()

    _, err = broadcaster.Broadcast(ctx, msg)
    return err
}
```

---

## Per-VChannel TimeTick Support

### Problem Statement

Current code at `ddl_callbacks_import.go:74`:
```go
DataTimestamp: result.GetMaxTimeTick(), // TODO: use per-vchannel TimeTick in future, must be supported for CDC.
```

**Issue:** Using `MaxTimeTick` across all vchannels breaks CDC's per-channel checkpoint recovery semantics. When a secondary cluster restarts, CDC needs to resume from the exact position on each vchannel independently.

### Solution

Store per-vchannel timestamps in ImportJob instead of single MaxTimeTick.

**Code Change: `internal/datacoord/ddl_callbacks_import.go`**

```go
func (c *DDLCallbacks) importV1AckCallback(ctx context.Context, result message.BroadcastResultImportMessageV1) error {
    body := result.Message.MustBody()

    // Ensure Schema.DbName is populated
    if body.Schema != nil {
        body.Schema.DbName = body.DbName
    }

    // Build per-vchannel timestamp map from broadcast results
    vchannelTimestamps := make(map[string]uint64)
    vchannels := make([]string, 0, len(result.Results))
    for vchannel, br := range result.Results {
        if funcutil.IsControlChannel(vchannel) {
            continue
        }
        vchannels = append(vchannels, vchannel)
        vchannelTimestamps[vchannel] = br.TimeTick  // Per-vchannel instead of max
    }

    importResp, err := c.createImportJobFromAck(ctx, &internalpb.ImportRequestInternal{
        DbID:               0, // deprecated
        CollectionID:       body.GetCollectionID(),
        CollectionName:     body.GetCollectionName(),
        PartitionIDs:       body.GetPartitionIDs(),
        ChannelNames:       vchannels,
        Schema:             body.GetSchema(),
        Files:              convertFiles(body.GetFiles()),
        Options:            funcutil.Map2KeyValuePair(body.GetOptions()),
        VchannelTimestamps: vchannelTimestamps,  // NEW: per-vchannel map
        JobID:              body.GetJobID(),
    })

    return merr.CheckRPCCall(importResp, err)
}
```

**Impact:**
- ImportJob stores `map<string, uint64> vchannel_timestamps` instead of single `data_timestamp`
- When creating segments, use the specific vchannel's timestamp
- CDC checkpoint recovery uses exact per-channel position
- No change to external API (transparent to users)
- `data_timestamp` field deprecated but kept for backward compatibility

---

## ImportChecker State Transition Logic

### Modified Check Loop

**File: `internal/datacoord/import_checker.go`**

```go
func (c *importChecker) checkJobs() {
    for _, job := range c.meta.GetJobBy() {
        switch job.GetState() {
        case internalpb.ImportJobStateV2_Pending:
            c.tryPreImport(job)
        case internalpb.ImportJobStateV2_PreImporting:
            c.checkPreImport(job)
        case internalpb.ImportJobStateV2_Importing:
            c.checkImport(job)
        case internalpb.ImportJobStateV2_Sorting:
            c.checkSort(job)
        case internalpb.ImportJobStateV2_IndexBuilding:
            c.checkIndexBuilding(job)
        case internalpb.ImportJobStateV2_WaitingCommit:  // NEW
            c.checkWaitingCommit(job)
        }
    }
}
```

### Modified IndexBuilding Check

```go
func (c *importChecker) checkIndexBuilding(job ImportJob) {
    // ... existing index building check logic ...

    if allIndexesBuilt {
        // NEW: All clusters transition to WaitingCommit (unified FSM)
        c.meta.UpdateJobState(job.GetJobID(), internalpb.ImportJobStateV2_WaitingCommit)
        c.meta.UpdateJobStartTime(job.GetJobID(), time.Now().Unix()) // Start timeout timer

        // NEW: Auto-commit for non-replicating clusters
        if !c.isReplicationEnabled() {
            log.Info("non-replicating cluster, auto-committing import",
                zap.String("job_id", job.GetJobID()))

            // Auto-broadcast CommitImportMessage
            err := c.coord.broadcastCommitImport(
                context.Background(),
                job.GetJobID(),
                job.GetCollectionID(),
                job.GetVchannels(),
            )
            if err != nil {
                log.Error("failed to auto-commit import",
                    zap.String("job_id", job.GetJobID()), zap.Error(err))
            }
        } else {
            // Replicating clusters: wait for manual CommitImport RPC
            log.Info("replicating cluster, awaiting manual commit",
                zap.String("job_id", job.GetJobID()))
        }
    }
}
```

### New WaitingCommit Check

```go
// checkWaitingCommit monitors jobs in WaitingCommit state for timeout.
func (c *importChecker) checkWaitingCommit(job ImportJob) {
    // Check timeout (default 1 hour, configurable)
    timeout := Params.DataCoordCfg.ImportCommitTimeout.GetAsDuration(time.Hour)
    startTime := time.Unix(job.GetStartTime(), 0)

    if time.Since(startTime) > timeout {
        log.Warn("import job waiting commit timeout, auto-aborting",
            zap.String("job_id", job.GetJobID()),
            zap.Duration("timeout", timeout),
            zap.Time("start_time", startTime))

        // Auto-abort: broadcast AbortImportMessage
        err := c.coord.broadcastAbortImport(
            context.Background(),
            job.GetJobID(),
            job.GetCollectionID(),
            job.GetVchannels(),
        )

        if err != nil {
            log.Error("failed to auto-abort timed out import job",
                zap.String("job_id", job.GetJobID()), zap.Error(err))
        }
    }
}

// isReplicationEnabled checks if CDC replication is currently active.
func (c *importChecker) isReplicationEnabled() bool {
    balancer, err := balance.GetWithContext(context.Background())
    if err != nil {
        return false
    }
    channelAssignment, err := balancer.GetLatestChannelAssignment()
    if err != nil {
        return false
    }
    return channelAssignment.ReplicateConfiguration != nil &&
           len(channelAssignment.ReplicateConfiguration.GetClusters()) > 1
}
```

---

## DDL Callback Handlers

### CommitImport Callback

**File: `internal/datacoord/ddl_callbacks_import.go`**

```go
// commitImportAckCallback handles acknowledgment of CommitImportMessage broadcast.
// Transitions job to Completed and makes segments queryable.
func (c *DDLCallbacks) commitImportAckCallback(ctx context.Context, result message.BroadcastResultCommitImportMessageV1) error {
    body := result.Message.MustBody()
    jobID := body.GetJobId()

    log.Ctx(ctx).Info("processing commit import ack",
        zap.Int64("job_id", jobID))

    // 1. Update job state to Completed
    err := c.importMeta.UpdateJobState(jobID, internalpb.ImportJobStateV2_Completed)
    if err != nil {
        return err
    }

    // 2. Mark all segments as no longer importing (make queryable atomically)
    job := c.importMeta.GetJob(jobID)
    if job == nil {
        return merr.WrapErrImportJobNotExist(jobID)
    }

    for _, task := range job.GetTasks() {
        for _, segmentID := range task.GetSegmentIDs() {
            c.meta.SetSegmentImporting(segmentID, false)
        }
    }

    log.Ctx(ctx).Info("import job committed successfully",
        zap.Int64("job_id", jobID),
        zap.Int("segment_count", len(job.GetTasks())))

    return nil
}
```

### AbortImport Callback

```go
// abortImportAckCallback handles acknowledgment of AbortImportMessage broadcast.
// Marks job as Failed and schedules segment cleanup.
func (c *DDLCallbacks) abortImportAckCallback(ctx context.Context, result message.BroadcastResultAbortImportMessageV1) error {
    body := result.Message.MustBody()
    jobID := body.GetJobId()

    log.Ctx(ctx).Info("processing abort import ack",
        zap.Int64("job_id", jobID))

    // 1. Mark job as Failed
    err := c.importMeta.UpdateJobState(jobID, internalpb.ImportJobStateV2_Failed)
    if err != nil {
        return err
    }

    // 2. Trigger cleanup: mark segments for GC deletion
    job := c.importMeta.GetJob(jobID)
    if job != nil {
        return c.cleanupImportJob(ctx, job)
    }

    return nil
}

// cleanupImportJob marks segments as Dropped for GC deletion.
func (c *DDLCallbacks) cleanupImportJob(ctx context.Context, job ImportJob) error {
    log.Ctx(ctx).Info("cleaning up aborted import job",
        zap.String("job_id", job.GetJobID()))

    // Mark segments as Dropped - GC will delete binlogs asynchronously
    for _, task := range job.GetTasks() {
        for _, segmentID := range task.GetSegmentIDs() {
            segment := c.meta.GetSegment(segmentID)
            if segment != nil && segment.GetImporting() {
                // Atomic state transition
                err := c.meta.SetSegmentState(segmentID, commonpb.SegmentState_Dropped)
                if err != nil {
                    log.Ctx(ctx).Error("failed to mark segment as dropped",
                        zap.Int64("segment_id", segmentID), zap.Error(err))
                    // Continue with other segments
                }
            }
        }
    }

    log.Ctx(ctx).Info("import job cleanup completed",
        zap.String("job_id", job.GetJobID()))

    return nil
}
```

---

## Segment State Management

### Segment Lifecycle

**During Import:**
1. Segment created with `importing = true` (not queryable)
2. Binlogs written to object storage
3. Index built on segment
4. Job transitions to WaitingCommit
5. Segment remains `importing = true`

**On CommitImport:**
```go
// Atomic flip: importing = false → segment becomes queryable
c.meta.SetSegmentImporting(segmentID, false)
```

**On AbortImport:**
```go
// Mark as Dropped → GC deletes binlogs + metadata asynchronously
c.meta.SetSegmentState(segmentID, commonpb.SegmentState_Dropped)
```

### Idempotency Guarantees

**CommitImport idempotency:**
- Can call multiple times safely
- If job already `Completed`, callback is no-op
- If segments already `importing = false`, no-op
- Broadcast can be retried without side effects

**AbortImport idempotency:**
- Can call multiple times safely
- If job already `Failed`, return success (no-op)
- If segments already `Dropped`, no-op
- Broadcast can be retried without side effects

---

## Error Handling and Edge Cases

### Edge Case 1: CommitImport on Wrong State

**Scenario:** User calls `CommitImport(jobID)` when job is still in `IndexBuilding`.

**Handling:**
```go
if job.GetState() != internalpb.ImportJobStateV2_WaitingCommit {
    return merr.Status(merr.WrapErrImportFailed(
        fmt.Sprintf("job %s is in state %s, expected WaitingCommit",
            req.GetJobId(), job.GetState())))
}
```

**User Action:** Poll `GetImportProgress(jobID)` until state is `WaitingCommit`.

---

### Edge Case 2: Partial Broadcast Failure

**Scenario:** CommitImportMessage reaches 2 out of 3 secondaries, network fails.

**Detection:**
```bash
# User checks all clusters
curl http://primary:19530/v2/vectordb/jobs/import/get_progress?jobId=123
# Output: {"state": "Completed"}

curl http://secondary1:19530/v2/vectordb/jobs/import/get_progress?jobId=123
# Output: {"state": "Completed"}

curl http://secondary2:19530/v2/vectordb/jobs/import/get_progress?jobId=123
# Output: {"state": "WaitingCommit"}  <-- Still waiting!
```

**Recovery:**
- Broadcast is idempotent - can retry safely
- User calls `CommitImport(jobID)` again on primary
- Already-committed clusters ignore duplicate (job already Completed)
- Failed cluster processes message normally
- No partial state corruption

---

### Edge Case 3: Timeout During WaitingCommit

**Scenario:** User starts import but forgets to commit for hours.

**Handling:**
- ImportChecker detects timeout (default 1 hour, configurable)
- Auto-broadcasts `AbortImportMessage`
- Job transitions to `Failed` across all clusters
- Segments marked `Dropped` for GC deletion

**Configuration:**
```yaml
# configs/milvus.yaml
dataCoord:
  import:
    commitTimeout: 3600  # seconds, increase for large imports
```

---

### Edge Case 4: Secondary Offline During Import

**Scenario:** Secondary cluster crashes during `Importing` stage, comes back online later.

**Handling:**
- **Primary:** Continues to WaitingCommit normally
- **Secondary:** On restart:
  - Recovers ImportJob from etcd
  - Resumes from last checkpoint (per-vchannel TimeTick)
  - Continues: Importing → Sorting → IndexBuilding → WaitingCommit
- **User:** Must wait for secondary to catch up before calling CommitImport
- **If timeout expires:** Auto-abort cleans up across all clusters

**No automatic retry** - operator decides whether to wait or manually abort.

---

### Edge Case 5: Race Between CommitImport and AbortImport

**Scenario:** User accidentally calls both `CommitImport` and `AbortImport` concurrently.

**Handling:**
```go
// State transition is atomic via etcd transaction
// First message to transition out of WaitingCommit wins
if job.GetState() == internalpb.ImportJobStateV2_WaitingCommit {
    // Process commit or abort
} else {
    // Job already transitioned
    return merr.Status(merr.WrapErrImportFailed("job already committed or aborted"))
}
```

**Guarantee:** No partial state - either all clusters commit or all abort.

---

### Edge Case 6: AbortImport in Non-Terminal States

**Scenario:** User calls `AbortImport` while job is in `Importing` or `Sorting`.

**Handling:**
- **Allowed** - can abort in any state except `Completed`
- Broadcast AbortImportMessage immediately
- Job transitions to `Failed`
- Ongoing tasks (PreImport, Import, Sorting) interrupted
- Segments cleaned up by GC

**Valid abort transitions:**
- `Pending → Failed`
- `PreImporting → Failed`
- `Importing → Failed`
- `Sorting → Failed`
- `IndexBuilding → Failed`
- `WaitingCommit → Failed`

---

### Edge Case 7: GC of Uncommitted Segments

**Scenario:** AbortImport called, segments marked `Dropped`, binlogs in object storage.

**Handling:**
- Segments marked `SegmentState_Dropped` immediately (synchronous)
- Existing GC process asynchronously deletes:
  - Segment binlogs from object storage
  - Index files
  - Segment metadata from etcd
- GC runs every 1 hour by default (`dataCoord.gcInterval`)
- No changes to GC logic - reuses existing cleanup mechanism

**Safety:** Segments never become queryable if aborted.

---

## Configuration Parameters

### New Parameters

**File: `pkg/v2/util/paramtable/component_param.go`**

```go
type dataCoordConfig struct {
    // ... existing params ...

    // ImportCommitTimeout defines how long a job can stay in WaitingCommit
    // before being auto-aborted. Default: 3600 seconds (1 hour)
    ImportCommitTimeout ParamItem `refreshable:"true"`
}

func (p *dataCoordConfig) init(base *BaseTable) {
    // ... existing init ...

    p.ImportCommitTimeout = ParamItem{
        Key:          "dataCoord.import.commitTimeout",
        Version:      "2.5.0",
        DefaultValue: "3600",
        Doc:          "Maximum time (seconds) an import job can wait for commit before auto-aborting",
        Export:       true,
    }
    p.ImportCommitTimeout.Init(base.mgr)
}
```

### Configuration File

**File: `configs/milvus.yaml`**

```yaml
dataCoord:
  import:
    # Maximum time (seconds) an import job can wait in WaitingCommit state
    # before being automatically aborted. Increase for large imports that
    # require extended validation time.
    # Default: 3600 (1 hour)
    commitTimeout: 3600
```

---

## API Summary

### New RPCs (DataCoord)

| RPC | Request | Response | Description |
|-----|---------|----------|-------------|
| `CommitImport` | `CommitImportRequest{job_id}` | `common.Status` | Commits import job, broadcasts CommitImportMessage |
| `AbortImport` | `AbortImportRequest{job_id}` | `common.Status` | Aborts import job (any state except Completed), broadcasts AbortImportMessage |

### New Message Types

| Type | Replicated via CDC | Purpose |
|------|-------------------|---------|
| `CommitImportMessage` | ✅ Yes | Transition WaitingCommit → Completed across all clusters |
| `AbortImportMessage` | ✅ Yes | Transition any state → Failed across all clusters |

### Enhanced Existing APIs

| API | Change | Backward Compatible |
|-----|--------|-------------------|
| `GetImportProgress` | Returns new state `WaitingCommit` | ✅ Yes (new enum value) |
| `ImportRequestInternal` | Adds `vchannel_timestamps` map, deprecates `data_timestamp` | ✅ Yes (optional field) |

### Removed Validation

| Location | Before | After |
|----------|--------|-------|
| `ddl_callbacks_import.go:121-123` | Blocks import when replication active | ✅ Validation removed |

---

## Testing Strategy

### Unit Tests

**File: `internal/datacoord/import_checker_test.go`**

- [ ] `TestImportChecker_WaitingCommitState` - IndexBuilding → WaitingCommit when replication enabled
- [ ] `TestImportChecker_WaitingCommitTimeout` - Auto-abort after timeout
- [ ] `TestImportChecker_NonReplicatedCluster` - IndexBuilding → Completed when replication disabled
- [ ] `TestCommitImport_Success` - CommitImport RPC happy path
- [ ] `TestCommitImport_InvalidState` - CommitImport fails when not in WaitingCommit
- [ ] `TestAbortImport_AnyState` - AbortImport succeeds in all non-terminal states
- [ ] `TestAbortImport_Idempotent` - Multiple AbortImport calls are no-op
- [ ] `TestAbortImport_AfterCompleted` - AbortImport fails after Completed

**File: `internal/datacoord/ddl_callbacks_import_test.go`**

- [ ] `TestCommitImportCallback` - Segments transition to non-importing with visible_timestamp
- [ ] `TestAbortImportCallback` - Segments marked as Dropped
- [ ] `TestPerVChannelTimeTick` - ImportJob stores per-vchannel timestamps
- [ ] `TestBroadcastCommitImport` - Message broadcast succeeds
- [ ] `TestBroadcastAbortImport` - Message broadcast succeeds
- [ ] `TestVisibleTimestamp_Set` - visible_timestamp correctly set during commit
- [ ] `TestVisibleTimestamp_QueryNodeFiltering` - DML filtering respects visible_timestamp

**File: `internal/querynodev2/segments/segment_test.go`**

- [ ] `TestGetEffectiveTimestamp_ImportSegment` - Returns visible_timestamp for import segments
- [ ] `TestGetEffectiveTimestamp_NormalSegment` - Returns row timestamp for normal segments
- [ ] `TestDeleteFiltering_BeforeCommit` - DELETE before commit does not apply
- [ ] `TestDeleteFiltering_AfterCommit` - DELETE after commit correctly applies
- [ ] `TestInsertDuplicate_BeforeCommit` - INSERT before commit wins on deduplication

**File: `internal/datanode/compaction/compactor_test.go`**

- [ ] `TestCompaction_NormalizeImportSegment` - Row timestamps rewritten correctly
- [ ] `TestCompaction_ClearVisibleTimestamp` - Metadata cleared after normalization

### Integration Tests

**File: `tests/integration/import_replication_test.go`**

- [ ] `TestImportInReplicationScenario` - End-to-end happy path
  - Setup: Primary + 2 secondaries with CDC
  - Start import on primary
  - Verify all reach WaitingCommit
  - Call CommitImport
  - Verify all transition to Completed
  - Query data on all clusters, verify consistency

- [ ] `TestImportAbortInReplication` - Abort flow
  - Setup: Primary + secondary
  - Start import
  - Call AbortImport
  - Verify both transition to Failed
  - Verify segments cleaned up

- [ ] `TestCommitImportIdempotency` - Retry safety
  - Call CommitImport twice
  - Verify second call is no-op

- [ ] `TestPartialBroadcastRecovery` - Network failure handling
  - Simulate network failure during CommitImport broadcast
  - Verify retry succeeds
  - Verify no partial state

- [ ] `TestWaitingCommitTimeout` - Auto-abort
  - Start import
  - Wait for timeout (use short timeout for test)
  - Verify auto-abort triggered

- [ ] `TestAbortImportDuringImporting` - Abort in non-WaitingCommit state
  - Start import
  - Call AbortImport while in Importing state
  - Verify job transitions to Failed

- [ ] `TestWriteConsistency_DeleteBeforeCommit` - DELETE before commit does not affect import
  - Start import (T=1000)
  - DELETE pk=2 during WaitingCommit (T=2000)
  - CommitImport (T=3000)
  - Query: Verify pk=2 still exists (DELETE did not apply)

- [ ] `TestWriteConsistency_DeleteAfterCommit` - DELETE after commit correctly applies
  - Start import (T=1000)
  - CommitImport (T=3000)
  - DELETE pk=2 after commit (T=4000)
  - Query: Verify pk=2 deleted (DELETE applied)

- [ ] `TestWriteConsistency_InsertDuplicate` - INSERT during WaitingCommit
  - Start import with pk=2 (T=1000)
  - INSERT pk=2 during WaitingCommit (T=2000)
  - CommitImport (T=3000)
  - Query: Verify pk=2 from import visible (newer timestamp wins)

- [ ] `TestWriteConsistency_Compaction` - Normalization after compaction
  - Import segment with visible_timestamp=3000
  - Trigger compaction
  - Verify output segment: row timestamps=3000, visible_timestamp=0

### Manual Testing Procedure

```bash
# 1. Setup 3-node cluster: primary + 2 secondaries with CDC replication

# 2. Start import on primary
curl -X POST "http://primary:19530/v2/vectordb/jobs/import/create" \
  -d '{"collectionName": "test_collection", "files": ["s3://bucket/data.parquet"]}'
# Output: {"jobId": "448293190903759186"}

# 3. Poll all clusters until WaitingCommit (expect ~2-5 minutes)
watch -n 2 'echo "=== PRIMARY ===" && \
  curl -s http://primary:19530/v2/vectordb/jobs/import/get_progress?jobId=448293190903759186 | jq .state && \
  echo "=== SECONDARY1 ===" && \
  curl -s http://secondary1:19530/v2/vectordb/jobs/import/get_progress?jobId=448293190903759186 | jq .state && \
  echo "=== SECONDARY2 ===" && \
  curl -s http://secondary2:19530/v2/vectordb/jobs/import/get_progress?jobId=448293190903759186 | jq .state'

# Expected output (all showing):
# === PRIMARY ===
# "WaitingCommit"
# === SECONDARY1 ===
# "WaitingCommit"
# === SECONDARY2 ===
# "WaitingCommit"

# 4. Commit import
curl -X POST "http://primary:19530/v2/vectordb/jobs/import/commit" \
  -d '{"jobId": "448293190903759186"}'

# 5. Verify all clusters reach Completed (within seconds)
# 6. Query data on all clusters, verify row counts match
curl "http://primary:19530/v2/vectordb/entities/query" \
  -d '{"collectionName": "test_collection", "filter": "", "outputFields": ["count(*)"]}'
# Repeat for secondary1 and secondary2

# 7. Test abort flow (new import)
# ... repeat steps 2-3 ...
# Then abort instead of commit:
curl -X POST "http://primary:19530/v2/vectordb/jobs/import/abort" \
  -d '{"jobId": "NEW_JOB_ID"}'
# Verify all clusters show state="Failed"
```

---

## Migration and Backward Compatibility

### Backward Compatibility Guarantees

**Unified FSM for all clusters:**
- All clusters use the same state machine: `IndexBuilding → WaitingCommit → Completed`
- Difference is in the commit trigger, not the states themselves

**Non-replicated clusters (existing behavior maintained):**
- Transition: `IndexBuilding → WaitingCommit → (auto-commit) → Completed`
- **Auto-commit:** DataCoord automatically broadcasts `CommitImportMessage` upon entering WaitingCommit
- **User-invisible:** No manual action required, seamless completion
- **No API changes required** - Existing import workflows work identically
- **Zero migration needed** - Behavior appears unchanged to users

**Replicated clusters (new behavior):**
- Transition: `IndexBuilding → WaitingCommit → (manual commit) → Completed`
- **Manual commit:** User must call `CommitImport(jobID)` RPC
- **Requires operator action** to complete import
- New RPCs (`CommitImport`, `AbortImport`) available

### Behavior Comparison Table

| Cluster Type | State Flow | Commit Trigger | User Action Required | Time in WaitingCommit |
|--------------|------------|----------------|---------------------|---------------------|
| **Non-replicating** | IndexBuilding → WaitingCommit → Completed | Auto-broadcast CommitImportMessage | ❌ No | ~Milliseconds (auto) |
| **Replicating** | IndexBuilding → WaitingCommit → Completed | Manual `CommitImport(jobID)` RPC | ✅ Yes | Until manual commit or timeout |

### Rolling Upgrade Strategy

**Phase 1: Upgrade DataCoord**
- New state `WaitingCommit` recognized
- New RPCs available
- Validation at `ddl_callbacks_import.go:121-123` removed
- Import jobs started before upgrade: complete with old logic (no WaitingCommit)
- Import jobs started after upgrade: use new logic

**Phase 2: Upgrade DataNode**
- No changes needed (DataNode unaware of commit/abort)

**Phase 3: Upgrade Proxy**
- Support new message types in CDC replication
- Message builders registered

**Phase 4: Upgrade CDC Server**
- New message types added to replication filter

**Coordination during upgrade:**
- Import jobs in-flight during upgrade: complete with old state machine
- No partial-state corruption
- No downtime required

### Deprecation Plan

**Deprecated fields (retain for compatibility):**
- `ImportRequestInternal.data_timestamp` - Use `vchannel_timestamps` instead
- `ImportJob.data_timestamp` - Use `vchannel_timestamps` instead

**Removal timeline:**
- Mark as deprecated in 2.5.0
- Remove in 3.0.0 (major version)

---

## Implementation Checklist

### Protocol Buffers
- [ ] Add `CommitImportMessage` and `AbortImportMessage` to `msg.proto`
- [ ] Add `CommitImportRequest`, `AbortImportRequest` to `data_coord.proto`
- [ ] Add `WaitingCommit` state to `ImportJobStateV2` enum
- [ ] Add `vchannel_timestamps` map to `ImportRequestInternal`
- [ ] Add `vchannel_timestamps` map to `ImportJob`
- [ ] Add `visible_timestamp` field to `SegmentInfo` proto (write consistency)
- [ ] Mark `data_timestamp` as deprecated in both protos
- [ ] Generate proto code: `make generated-proto-without-cpp`

### DataCoord Implementation
- [ ] Remove replication validation at `ddl_callbacks_import.go:121-123`
- [ ] Implement `CommitImport` RPC in `server.go`
- [ ] Implement `AbortImport` RPC in `server.go`
- [ ] Implement `broadcastCommitImport` helper in `ddl_callbacks_import.go`
- [ ] Implement `broadcastAbortImport` helper in `ddl_callbacks_import.go`
- [ ] Add `commitImportAckCallback` to DDL callbacks (with visible_timestamp)
- [ ] Add `abortImportAckCallback` to DDL callbacks
- [ ] Implement `cleanupImportJob` for segment cleanup
- [ ] Update `importV1AckCallback` to use per-vchannel timestamps
- [ ] Modify `checkIndexBuilding` to transition to WaitingCommit
- [ ] Add `checkWaitingCommit` with timeout logic
- [ ] Add `isReplicationEnabled` helper
- [ ] Implement `UpdateSegmentVisibility()` in meta.go (write consistency)

### QueryNode Implementation
- [ ] Implement `GetEffectiveTimestamp()` for timestamp override logic
- [ ] Update delete filtering to use effective timestamp instead of row timestamp
- [ ] Ensure segment-level `visible_timestamp` propagates from DataCoord to QueryNode
- [ ] Test timestamp filtering with import segments

### Compaction Implementation
- [ ] Detect import segments with `visible_timestamp != 0` during compaction
- [ ] Rewrite row timestamps to match `visible_timestamp` during compaction
- [ ] Clear `visible_timestamp` metadata in output segments after normalization
- [ ] Test compaction normalization with import segments

### Message/Streaming Changes
- [ ] Register `MessageTypeCommitImport` in `message_type.go`
- [ ] Register `MessageTypeAbortImport` in `message_type.go`
- [ ] Implement `NewCommitImportMessageBuilderV1()` builder
- [ ] Implement `NewAbortImportMessageBuilderV1()` builder
- [ ] Add message types to CDC replication allow-list

### Configuration
- [ ] Add `dataCoord.import.commitTimeout` parameter to `component_param.go`
- [ ] Document configuration in `configs/milvus.yaml`
- [ ] Add parameter validation (min: 60s, max: 86400s)

### Testing
- [ ] Unit tests for state transitions (9 tests)
- [ ] Unit tests for RPC validation (5 tests)
- [ ] Unit tests for timeout logic (3 tests)
- [ ] Integration test: import in replication scenario
- [ ] Integration test: abort import
- [ ] Integration test: commit idempotency
- [ ] Integration test: partial broadcast recovery
- [ ] Integration test: abort in non-WaitingCommit states
- [ ] Manual testing procedure documented and executed

### Documentation
- [ ] Update import API docs with new commit/abort RPCs
- [ ] Add operational guide for import in replicated clusters
- [ ] Document timeout behavior and configuration
- [ ] Add troubleshooting guide (common errors, recovery procedures)
- [ ] Update CDC documentation with import replication flow

### Code Generation
- [ ] Generate mockery mocks: `make generate-mockery-datacoord`
- [ ] Verify proto generation: `make generated-proto-without-cpp`

---

## Operational Guide

### ⚠️ CRITICAL: User Responsibility for Consistency

**The system does NOT automatically validate secondary clusters before committing.**

**YOU MUST:**
1. ✅ Manually check **ALL clusters** are in `WaitingCommit` state
2. ✅ Verify **ALL clusters** have the same JobID
3. ✅ Ensure **ALL clusters** show `progress: 100%`
4. ✅ Only call `CommitImport` after verifying all above

**IF YOU COMMIT TOO EARLY:**
- Some clusters will complete, others may fail or timeout
- Data visible on some clusters but not others (inconsistency)
- Recovery requires manual `AbortImport` and re-import

**This is by design** - manual coordination provides:
- Full operator visibility and control
- Ability to validate data during WaitingCommit
- Context-aware decisions (e.g., wait for slow secondary vs abort)

### User Workflow

**1. Start Import**
```bash
# On primary cluster
curl -X POST "http://primary:19530/v2/vectordb/jobs/import/create" \
  -d '{"collectionName": "my_collection", "files": ["s3://bucket/data.parquet"]}'

# Output: {"jobId": "448293190903759186"}
```

**2. Monitor Progress (CRITICAL - User Responsibility)**
```bash
# IMPORTANT: You MUST check ALL clusters before committing!
# The system does NOT automatically validate secondaries.

# Poll primary
curl "http://primary:19530/v2/vectordb/jobs/import/get_progress?jobId=448293190903759186"
# Expected: {"state": "WaitingCommit", "progress": 100}

# Poll ALL secondaries (do not skip any!)
curl "http://secondary1:19530/v2/vectordb/jobs/import/get_progress?jobId=448293190903759186"
# Expected: {"state": "WaitingCommit", "progress": 100}

curl "http://secondary2:19530/v2/vectordb/jobs/import/get_progress?jobId=448293190903759186"
# Expected: {"state": "WaitingCommit", "progress": 100}

# Wait until ALL clusters show: {"state": "WaitingCommit"}
# DO NOT proceed to commit if any cluster is not in WaitingCommit!
```

**3. Validate (Optional)**
```bash
# Run smoke tests, data quality checks during WaitingCommit
# Segments exist but not queryable yet - safe validation window
```

**4. Commit or Abort**
```bash
# If validation passes - commit
curl -X POST "http://primary:19530/v2/vectordb/jobs/import/commit" \
  -d '{"jobId": "448293190903759186"}'

# OR if validation fails - abort
curl -X POST "http://primary:19530/v2/vectordb/jobs/import/abort" \
  -d '{"jobId": "448293190903759186"}'
```

**5. Verify Completion**
```bash
# Check all clusters transitioned to Completed
# Query data to verify consistency
```

### Troubleshooting

**Problem:** Secondary stuck in IndexBuilding, primary in WaitingCommit

**Diagnosis:**
```bash
# Check secondary logs for errors
kubectl logs -n milvus secondary-datacoord-0 | grep -i "import\|error"

# Check secondary disk/memory resources
kubectl top pods -n milvus
```

**Resolution:**
- If transient issue: Wait for secondary to catch up
- If persistent failure: Call `AbortImport`, investigate secondary, retry import
- If timeout approaching: Either abort or increase timeout in config

---

**Problem:** Partial broadcast - some clusters Completed, others WaitingCommit

**Diagnosis:**
```bash
# Check status on all clusters
for host in primary secondary1 secondary2; do
  echo "=== $host ==="
  curl "http://$host:19530/v2/vectordb/jobs/import/get_progress?jobId=123"
done
```

**Resolution:**
```bash
# Retry commit (idempotent, safe to retry)
curl -X POST "http://primary:19530/v2/vectordb/jobs/import/commit" \
  -d '{"jobId": "123"}'
```

---

**Problem:** Import timed out in WaitingCommit

**Diagnosis:**
```bash
# Check logs for auto-abort message
kubectl logs -n milvus primary-datacoord-0 | grep "timeout.*auto-abort"
```

**Resolution:**
- Job already aborted, segments cleaned up
- Investigate why validation took so long
- Consider increasing timeout: `dataCoord.import.commitTimeout`
- Retry import with larger timeout

---

## Future Enhancements

**Out of scope for v1, potential future work:**

1. **Automatic coordination** - Primary auto-tracks secondary status, commits when all ready
2. **Aggregated status API** - Single RPC returns status of all clusters
3. **Partial commit** - Commit on subset of clusters (relaxed consistency)
4. **Import on secondaries** - Allow direct import to follower clusters
5. **Streaming import** - Continuous import without commit gate
6. **Retry policies** - Automatic retry for failed secondary imports

---

## Summary

This design enables import in replicated Milvus clusters using a unified FSM with conditional commit triggers:

**Key Features:**
- ✅ **Unified FSM** - All clusters use same state machine (IndexBuilding → WaitingCommit → Completed)
- ✅ **Auto-commit for non-replicating clusters** - Seamless backward compatibility via automatic CommitImportMessage broadcast
- ✅ **Manual commit for replicating clusters** - Strong consistency via all-or-nothing operator-controlled commit
- ✅ **Per-vchannel TimeTick** - Proper CDC checkpoint recovery semantics
- ✅ **Idempotent operations** - Safe to retry commit/abort
- ✅ **Timeout protection** - Auto-abort after 1 hour (configurable)
- ✅ **Minimal code changes** - ~500-800 LOC estimated
- ✅ **Zero user impact** - Non-replicated clusters maintain existing behavior

**Implementation Complexity:** Low-Medium
**Risk Level:** Low (reuses existing broadcast/CDC mechanisms)
**Estimated Effort:** 2-3 weeks (implementation + testing)

---

**Document Version:** v1.3
**Last Updated:** 2026-03-13
**Changelog:**
- v1.3: Added write consistency solution with segment-level visible_timestamp to fix DML timestamp ordering
- v1.2: Clarified consistency model - user responsibility to check all clusters, no automatic secondary validation
- v1.1: Added auto-commit for non-replicating clusters (unified FSM)
- v1.0: Initial design with manual two-phase commit
