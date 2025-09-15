package rootcoord

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message/ce"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
	"github.com/samber/lo"
)

func (c *Core) broadcastAlterDatabase(ctx context.Context, in *rootcoordpb.AlterDatabaseRequest) error {
	if in.GetProperties() == nil && in.GetDeleteKeys() == nil {
		log.Warn("alter database with empty properties and delete keys, expected to set either properties or delete keys ")
		return errors.New("alter database with empty properties and delete keys, expected to set either properties or delete keys")
	}

	if len(in.GetProperties()) > 0 && len(in.GetDeleteKeys()) > 0 {
		return errors.New("alter database cannot modify properties and delete keys at the same time")
	}

	if hookutil.ContainsCipherProperties(in.GetProperties(), in.GetDeleteKeys()) {
		log.Info("skip to alter collection due to cipher properties were detected in the request properties")
		return errors.New("can not alter cipher related properties")
	}

	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx, message.NewExclusiveDBNameResourceKey(in.GetDbName()))
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	oldDB, err := c.meta.GetDatabaseByName(ctx, in.GetDbName(), typeutil.MaxTimestamp)
	if err != nil {
		log.Warn("get database failed during changing database props")
		return err
	}
	alterLoadConfig, err := c.getAlterLoadConfigOfAlterDatabase(ctx, in.GetDbName(), oldDB.Properties, in.GetProperties())
	if err != nil {
		return err
	}

	var newProperties []*commonpb.KeyValuePair
	if (len(in.GetProperties())) > 0 {
		if IsSubsetOfProperties(in.GetProperties(), oldDB.Properties) {
			log.Info("skip to alter database due to no changes were detected in the properties")
			return nil
		}
		newProperties = MergeProperties(oldDB.Properties, in.GetProperties())
	} else if (len(in.GetDeleteKeys())) > 0 {
		newProperties = DeleteProperties(oldDB.Properties, in.GetDeleteKeys())
	}

	msg := message.NewAlterDatabaseMessageBuilderV2().
		WithHeader(&message.AlterDatabaseMessageHeader{
			DbName: in.GetDbName(),
			DbId:   oldDB.ID,
		}).
		WithBody(&message.AlterDatabaseMessageBody{
			Properties:      newProperties,
			AlterLoadConfig: alterLoadConfig,
		}).
		WithBroadcast([]string{streaming.WAL().ControlChannel()}).
		MustBuildBroadcast()

	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}

// getAlterLoadConfigOfAlterDatabase gets the put load config of put database.
func (c *Core) getAlterLoadConfigOfAlterDatabase(ctx context.Context, dbName string, oldProps []*commonpb.KeyValuePair, newProps []*commonpb.KeyValuePair) (*message.AlterLoadConfigOfAlterDatabase, error) {
	oldReplicaNumber, _ := common.DatabaseLevelReplicaNumber(oldProps)
	oldResourceGroups, _ := common.DatabaseLevelResourceGroups(oldProps)
	newReplicaNumber, _ := common.DatabaseLevelReplicaNumber(newProps)
	newResourceGroups, _ := common.DatabaseLevelResourceGroups(newProps)
	left, right := lo.Difference(oldResourceGroups, newResourceGroups)
	rgChanged := len(left) > 0 || len(right) > 0
	replicaChanged := oldReplicaNumber != newReplicaNumber
	if !rgChanged && !replicaChanged {
		return nil, nil
	}

	colls, err := c.meta.ListCollections(ctx, dbName, typeutil.MaxTimestamp, true)
	if err != nil {
		return nil, err
	}
	if len(colls) == 0 {
		return nil, nil
	}
	return &message.AlterLoadConfigOfAlterDatabase{
		CollectionIds:  lo.Map(colls, func(coll *model.Collection, _ int) int64 { return coll.CollectionID }),
		ReplicaNumber:  int32(newReplicaNumber),
		ResourceGroups: newResourceGroups,
	}, nil
}

func (c *DDLCallback) alterDatabaseV1AckCallback(ctx context.Context, result message.BroadcastResultAlterDatabaseMessageV2) error {
	header := result.Message.Header()
	body := result.Message.MustBody()

	db := model.NewDatabase(header.DbId, header.DbName, etcdpb.DatabaseState_DatabaseCreated, result.Message.MustBody().Properties)
	if err := c.meta.AlterDatabase(ctx, db, result.GetControlChannelResult().TimeTick); err != nil {
		return err
	}
	if err := c.ExpireCaches(ctx, ce.NewBuilder().
		WithLegacyProxyCollectionMetaCache(
			ce.OptLPCMDBName(header.DbName),
			ce.OptLPCMMsgType(commonpb.MsgType_AlterDatabase),
		),
		result.GetControlChannelResult().TimeTick); err != nil {
		return err
	}
	if body.AlterLoadConfig != nil {
		resp, err := c.mixCoord.UpdateLoadConfig(ctx, &querypb.UpdateLoadConfigRequest{
			CollectionIDs:  body.AlterLoadConfig.CollectionIds,
			ReplicaNumber:  body.AlterLoadConfig.ReplicaNumber,
			ResourceGroups: body.AlterLoadConfig.ResourceGroups,
		})
		return merr.CheckRPCCall(resp, err)
	}
	return nil
}

func MergeProperties(oldProps, updatedProps []*commonpb.KeyValuePair) []*commonpb.KeyValuePair {
	_, existEndTS := common.GetReplicateEndTS(updatedProps)
	if existEndTS {
		updatedProps = append(updatedProps, &commonpb.KeyValuePair{
			Key:   common.ReplicateIDKey,
			Value: "",
		})
	}

	props := make(map[string]string)
	for _, prop := range oldProps {
		props[prop.Key] = prop.Value
	}

	for _, prop := range updatedProps {
		props[prop.Key] = prop.Value
	}

	propKV := make([]*commonpb.KeyValuePair, 0)

	for key, value := range props {
		propKV = append(propKV, &commonpb.KeyValuePair{
			Key:   key,
			Value: value,
		})
	}

	return propKV
}
