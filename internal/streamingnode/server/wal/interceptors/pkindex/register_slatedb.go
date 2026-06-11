//go:build slatedb

package pkindex

// Registers the SlateDB engine (cgo) so the interceptor can select it via
// streaming.pkindex.engine=slatedb. Only built with -tags slatedb (R-S1).
import _ "github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/pkindex/pkengine/slatedb"
