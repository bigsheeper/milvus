package pkindex

import (
	"context"
	"encoding/binary"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/pkindex/pkengine"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/pkindex/pkengine/sstcodec"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// A5 (Demo simplification): a fixed demo collection PK field schema instead of
// resolving the live collection schema at the WAL interceptor layer. Inserts /
// deletes whose field set does not contain this PK field are skipped (the Demo
// only indexes its own collection). The default id 100 matches Milvus's first
// auto-assigned field id.
var demoPKFieldSchema = &schemapb.FieldSchema{
	FieldID:      100,
	Name:         "pk",
	IsPrimaryKey: true,
	DataType:     schemapb.DataType_Int64,
}

type pkIndexInterceptor struct {
	eng      pkengine.Engine // nil when pkindex is disabled
	pchannel string
}

// DoAppend writes to WAL first; only on success does it apply put/delete to the
// KV engine, with the WAL checkpoint (TimeTick + returned MessageID) bound to
// the write (§6.1). On WAL failure it leaves the KV untouched, avoiding ghost
// entries.
func (it *pkIndexInterceptor) DoAppend(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	id, err := appendOp(ctx, msg)
	if err != nil || it.eng == nil {
		return id, err
	}
	var msgID []byte
	if id != nil {
		msgID = []byte(id.Marshal())
	}
	ckpt := pkengine.WALCheckpoint{TimeTick: msg.TimeTick(), MessageID: msgID}

	switch msg.MessageType() {
	case message.MessageTypeInsert:
		it.applyInsert(ctx, message.MustAsMutableInsertMessageV1(msg).MustBody(), ckpt)
	case message.MessageTypeDelete:
		it.applyDelete(ctx, message.MustAsMutableDeleteMessageV1(msg).MustBody(), ckpt)
	}
	return id, nil
}

func (it *pkIndexInterceptor) applyInsert(ctx context.Context, body *message.InsertRequest, ckpt pkengine.WALCheckpoint) {
	pkFD, err := typeutil.GetPrimaryFieldData(body.GetFieldsData(), demoPKFieldSchema)
	if err != nil {
		return // not the demo collection (A5): skip
	}
	pks, err := storage.ParseFieldData2PrimaryKeys(pkFD)
	if err != nil {
		log.Ctx(ctx).Warn("pkindex: parse insert pks failed", zap.Error(err))
		return
	}
	collID := body.GetCollectionID()
	for i, pk := range pks {
		key, ok := encodeKey(collID, pk)
		if !ok {
			continue
		}
		if err := it.eng.Put(ctx, key, encodeLocator(int64(i)), ckpt); err != nil {
			log.Ctx(ctx).Warn("pkindex: put failed", zap.Error(err))
		}
	}
}

func (it *pkIndexInterceptor) applyDelete(ctx context.Context, body *message.DeleteRequest, ckpt pkengine.WALCheckpoint) {
	ids := body.GetPrimaryKeys()
	if ids == nil {
		return
	}
	collID := body.GetCollectionID()
	for _, pk := range storage.ParseIDs2PrimaryKeys(ids) {
		key, ok := encodeKey(collID, pk)
		if !ok {
			continue
		}
		if err := it.eng.Delete(ctx, key, ckpt); err != nil {
			log.Ctx(ctx).Warn("pkindex: delete failed", zap.Error(err))
		}
	}
}

// encodeKey maps a storage.PrimaryKey to the shared sstcodec data key (§8).
func encodeKey(collID int64, pk storage.PrimaryKey) ([]byte, bool) {
	switch p := pk.(type) {
	case *storage.Int64PrimaryKey:
		return sstcodec.EncodeInt64Key(collID, p.Value), true
	case *storage.VarCharPrimaryKey:
		return sstcodec.EncodeVarCharKey(collID, p.Value), true
	default:
		return nil, false
	}
}

// encodeLocator stores a logical locator placeholder (§8: value is not validated
// by the Demo — point queries are a non-goal).
func encodeLocator(segID int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(segID))
	return b
}

// Close releases the engine.
func (it *pkIndexInterceptor) Close() {
	if it.eng != nil {
		if err := it.eng.Close(); err != nil {
			log.Warn("pkindex: engine close failed", zap.Error(err))
		}
	}
}
