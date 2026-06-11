// Package pkindex is the WAL interceptor that drives the primary-key-index KV
// engine on the write path (design §6.1, C1). It is appended to the interceptor
// chain AFTER `shard`, so segment assignment and timetick are already set on the
// insert header by the time DoAppend runs.
//
// The interceptor is config-gated by streaming.pkindex.engine (pebble | slatedb;
// empty disables it). Per simplification A5 the Demo targets a fixed PK field
// schema rather than resolving the live collection schema at the WAL layer.
package pkindex

import (
	"context"
	"os"
	"path/filepath"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/pkindex/pkengine"
	// register the pure-Go Pebble engine; SlateDB is registered by the
	// build-tagged register_slatedb.go (-tags slatedb).
	_ "github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/pkindex/pkengine/pebble"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// NewInterceptorBuilder creates a new pkindex interceptor builder.
func NewInterceptorBuilder() interceptors.InterceptorBuilder {
	return &interceptorBuilder{}
}

type interceptorBuilder struct{}

// Build opens the configured KV engine for this pchannel. When pkindex is
// disabled (empty config) or the engine fails to open, it returns a pass-through
// interceptor so the WAL path is never blocked by the Demo.
func (b *interceptorBuilder) Build(param *interceptors.InterceptorBuildParam) interceptors.Interceptor {
	pchannel := param.ChannelInfo.Name
	logger := log.With()

	engineName := paramtable.Get().StreamingCfg.PKIndexEngine.GetValue()
	if engineName == "" {
		return &pkIndexInterceptor{pchannel: pchannel} // disabled pass-through
	}

	eng, _, err := pkengine.New(engineName)
	if err != nil {
		logger.Warn("pkindex disabled: unknown engine", zap.Error(err))
		return &pkIndexInterceptor{pchannel: pchannel}
	}
	localDir := filepath.Join(os.TempDir(), "milvus-pkindex", pchannel)
	if err := eng.Open(context.Background(), pkengine.OpenParam{
		Pchannel:    pchannel,
		ObjectStore: resource.Resource().ChunkManager(), // satisfies pkengine.ObjectStore
		LocalDir:    localDir,
	}); err != nil {
		logger.Warn("pkindex disabled: engine open failed", zap.Error(err))
		return &pkIndexInterceptor{pchannel: pchannel}
	}
	// Hand compaction control to the external indexnode compactor (C2).
	if err := eng.PauseBuiltinCompaction(context.Background()); err != nil {
		logger.Warn("pkindex: pause builtin compaction failed", zap.Error(err))
	}
	logger.Info("pkindex interceptor enabled", zap.String("engine", engineName), zap.String("pchannel", pchannel))
	return &pkIndexInterceptor{eng: eng, pchannel: pchannel}
}
