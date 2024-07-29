package inspector

import (
	"time"

	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// NewTimeTickSyncInspector creates a new time tick sync inspector.
func NewTimeTickSyncInspector() TimeTickSyncInspector {
	inspector := &timeTickSyncInspectorImpl{
		taskNotifier: syncutil.NewAsyncTaskNotifier[struct{}](),
		syncNotifier: newSyncNotifier(),
		operators:    typeutil.NewConcurrentMap[string, TimeTickSyncOperator](),
	}
	go inspector.background()
	return inspector
}

type timeTickSyncInspectorImpl struct {
	taskNotifier *syncutil.AsyncTaskNotifier[struct{}]
	syncNotifier *syncNotifier
	operators    *typeutil.ConcurrentMap[string, TimeTickSyncOperator]
}

func (s *timeTickSyncInspectorImpl) TriggerSync(pChannelInfo types.PChannelInfo) {
	s.syncNotifier.AddAndNotify(pChannelInfo)
}

func (s *timeTickSyncInspectorImpl) RegisterSyncOperator(operator TimeTickSyncOperator) {
	_, loaded := s.operators.GetOrInsert(operator.Channel().Name, operator)
	if loaded {
		panic("sync operator already exists, critical bug in code")
	}
}

func (s *timeTickSyncInspectorImpl) UnregisterSyncOperator(operator TimeTickSyncOperator) {
	_, loaded := s.operators.GetAndRemove(operator.Channel().Name)
	if !loaded {
		panic("sync operator not found, critical bug in code")
	}
}

// background executes the time tick sync inspector.
func (s *timeTickSyncInspectorImpl) background() {
	defer s.taskNotifier.Finish(struct{}{})

	interval := paramtable.Get().ProxyCfg.TimeTickInterval.GetAsDuration(time.Millisecond)
	ticker := time.NewTicker(interval)
	for {
		select {
		case <-s.taskNotifier.Context().Done():
			return
		case <-ticker.C:
			s.operators.Range(func(_ string, operator TimeTickSyncOperator) bool {
				operator.Sync(s.taskNotifier.Context())
				return true
			})
		case <-s.syncNotifier.WaitChan():
			s.syncNotifier.Get().Range(func(pchannel types.PChannelInfo) bool {
				if operator, ok := s.operators.Get(pchannel.Name); ok {
					operator.Sync(s.taskNotifier.Context())
				}
				return true
			})
		}
	}
}

func (s *timeTickSyncInspectorImpl) Close() {
	s.taskNotifier.Cancel()
	s.taskNotifier.BlockUntilFinish()
}
