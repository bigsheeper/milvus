//go:build slatedb

package harness

// Registers the SlateDB engine for the parameterized UT suite. Only built with
// `-tags slatedb` (cgo); see design doc §13.2 / R-S1.
import _ "github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/pkindex/pkengine/slatedb"
