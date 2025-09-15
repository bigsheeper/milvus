package rootcoord

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message/ce"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
	"go.uber.org/zap"
)

func (c *Core) broadcastDropDatabase(ctx context.Context, in *milvuspb.DropDatabaseRequest) error {
	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx, message.NewExclusiveDBNameResourceKey(in.GetDbName()))
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	if err := c.meta.CheckIfDatabaseDroppable(ctx, in.GetDbName()); err != nil {
		if errors.Is(err, merr.ErrDatabaseNotFound) {
			log.Ctx(ctx).Info("drop a database that not found, ignore it", zap.String("dbName", in.GetDbName()))
			return nil
		}
		return err
	}

	db, err := c.meta.GetDatabaseByName(ctx, in.GetDbName(), typeutil.MaxTimestamp)
	if err != nil {
		return err
	}

	msg := message.NewDropDatabaseMessageBuilderV2().
		WithHeader(&message.DropDatabaseMessageHeader{
			DbName: in.GetDbName(),
			DbId:   db.ID,
		}).
		WithBody(&message.DropDatabaseMessageBody{}).
		WithBroadcast([]string{streaming.WAL().ControlChannel()}).
		MustBuildBroadcast()

	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}

func (c *DDLCallback) dropDatabaseV1AckCallback(ctx context.Context, result message.BroadcastResultDropDatabaseMessageV2) error {
	header := result.Message.Header()
	if err := c.meta.DropDatabase(ctx, header.DbName, result.GetControlChannelResult().TimeTick); err != nil {
		return err
	}
	return c.ExpireCaches(ctx, ce.NewBuilder().
		WithLegacyProxyCollectionMetaCache(
			ce.OptLPCMDBName(header.DbName),
			ce.OptLPCMMsgType(commonpb.MsgType_DropDatabase),
		),
		result.GetControlChannelResult().TimeTick)
}
