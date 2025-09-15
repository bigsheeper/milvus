package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message/ce"
)

func (c *Core) broadcastCreateDatabase(ctx context.Context, in *milvuspb.CreateDatabaseRequest) error {
	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx, message.NewExclusiveDBNameResourceKey(in.GetDbName()))
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	if err := c.meta.CheckIfDatabaseCreatable(ctx, in.GetDbName()); err != nil {
		return err
	}

	dbID, err := c.idAllocator.AllocOne()
	if err != nil {
		return err
	}

	// Use dbID as ezID because the dbID is unqiue
	properties, err := hookutil.TidyDBCipherProperties(dbID, in.GetProperties())
	if err != nil {
		return err
	}

	msg := message.NewCreateDatabaseMessageBuilderV2().
		WithHeader(&message.CreateDatabaseMessageHeader{
			DbName: in.GetDbName(),
			DbId:   dbID,
		}).
		WithBody(&message.CreateDatabaseMessageBody{
			Properties: properties,
		}).
		WithBroadcast([]string{streaming.WAL().ControlChannel()}).
		MustBuildBroadcast()

	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}

func (c *DDLCallback) createDatabaseV1AckCallback(ctx context.Context, result message.BroadcastResultCreateDatabaseMessageV2) error {
	header := result.Message.Header()
	db := model.NewDatabase(header.DbId, header.DbName, etcdpb.DatabaseState_DatabaseCreated, result.Message.MustBody().Properties)
	if err := c.meta.CreateDatabase(ctx, db, result.GetControlChannelResult().TimeTick); err != nil {
		return err
	}
	return c.ExpireCaches(ctx, ce.NewBuilder().
		WithLegacyProxyCollectionMetaCache(
			ce.OptLPCMDBName(header.DbName),
			ce.OptLPCMMsgType(commonpb.MsgType_DropDatabase),
		),
		result.GetControlChannelResult().TimeTick)
}
