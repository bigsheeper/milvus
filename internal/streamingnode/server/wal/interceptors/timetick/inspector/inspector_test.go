package inspector_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/interceptors/timetick/mock_inspector"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/inspector"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func TestInsepctor(t *testing.T) {
	paramtable.Init()

	insepector := inspector.NewTimeTickSyncInspector()
	operator := mock_inspector.NewMockTimeTickSyncOperator(t)
	pchannel := types.PChannelInfo{
		Name: "test",
		Term: 1,
	}
	operator.EXPECT().Channel().Return(pchannel)
	operator.EXPECT().Sync(mock.Anything).Run(func(ctx context.Context) {})

	insepector.RegisterSyncOperator(operator)
	insepector.TriggerSync(pchannel)
	time.Sleep(100 * time.Millisecond)
	insepector.UnregisterSyncOperator(operator)

	insepector.Close()
}
