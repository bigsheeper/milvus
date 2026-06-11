package index

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/pkindex/pkengine"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

// PKCompactionTask carries the primary-key-index external compaction (C4) on the
// DataNode/IndexNode task framework. It wraps an engine-specific
// pkengine.Compactor: Execute downloads + merges (drop tombstones) + uploads the
// merged SST and records the result + covered WAL position for the streamingnode
// to install (C5). For the Pebble engine this is the self-built merge; for
// SlateDB it degrades to triggering native compaction (R-S2(a)).
//
// See design doc §6.4 and §12 (internal/datanode/index/task_pkcompaction.go).
type PKCompactionTask struct {
	ctx       context.Context
	name      string
	compactor pkengine.Compactor
	job       []byte

	mu      sync.Mutex
	state   indexpb.JobState
	failch  string
	result  []byte
	covered pkengine.WALCheckpoint
}

// NewPKCompactionTask creates a compaction task for the given opaque job payload
// (produced by Engine.PlanCompaction).
func NewPKCompactionTask(ctx context.Context, name string, compactor pkengine.Compactor, job []byte) *PKCompactionTask {
	return &PKCompactionTask{
		ctx:       ctx,
		name:      name,
		compactor: compactor,
		job:       job,
		state:     indexpb.JobState_JobStateInit,
	}
}

var _ Task = (*PKCompactionTask)(nil)

func (t *PKCompactionTask) Ctx() context.Context { return t.ctx }

func (t *PKCompactionTask) Name() string { return t.name }

// OnEnqueue is called by the scheduler when the task is accepted; it marks the
// task in-progress.
func (t *PKCompactionTask) OnEnqueue(context.Context) error {
	t.SetState(indexpb.JobState_JobStateInProgress, "")
	return nil
}

func (t *PKCompactionTask) SetState(state indexpb.JobState, failReason string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.state = state
	t.failch = failReason
}

func (t *PKCompactionTask) GetState() indexpb.JobState {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.state
}

func (t *PKCompactionTask) PreExecute(context.Context) error { return nil }

// Execute runs the engine-specific compaction and records the opaque result +
// covered WAL position for the streamingnode to install (C5).
func (t *PKCompactionTask) Execute(ctx context.Context) error {
	result, covered, err := t.compactor.Compact(ctx, t.job)
	if err != nil {
		t.SetState(indexpb.JobState_JobStateFailed, err.Error())
		log.Ctx(ctx).Warn("pkindex compaction task failed: " + err.Error())
		return err
	}
	t.mu.Lock()
	t.result = result
	t.covered = covered
	t.mu.Unlock()
	t.SetState(indexpb.JobState_JobStateFinished, "")
	return nil
}

func (t *PKCompactionTask) PostExecute(context.Context) error { return nil }

func (t *PKCompactionTask) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.ctx = nil
	t.compactor = nil
	t.job = nil
	t.result = nil
}

func (t *PKCompactionTask) GetSlot() int64 { return 1 }

func (t *PKCompactionTask) IsVectorIndex() bool { return false }

// Result returns the opaque compaction result + covered WAL position to be
// handed back to Engine.InstallCompaction (C5). Demo accessor.
func (t *PKCompactionTask) Result() ([]byte, pkengine.WALCheckpoint) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.result, t.covered
}
