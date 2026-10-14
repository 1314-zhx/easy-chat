package immodels

import (
	"context"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// EnsureIndexes 按现有查询建立业务索引。重复执行安全（同名同规格幂等）。
func EnsureIndexes(ctx context.Context, uri, dbName string) error {
	if uri == "" || dbName == "" {
		return nil
	}
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
	}

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return err
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	if err := client.Ping(ctx, nil); err != nil {
		return err
	}

	db := client.Database(dbName)
	specs := []struct {
		coll    string
		indexes []mongo.IndexModel
	}{
		{
			coll: "chat_log",
			indexes: []mongo.IndexModel{
				{
					Keys:    bson.D{{Key: "conversationId", Value: 1}, {Key: "seq", Value: 1}},
					Options: options.Index().SetName("idx_conv_seq"),
				},
				{
					Keys:    bson.D{{Key: "conversationId", Value: 1}, {Key: "sendTime", Value: -1}},
					Options: options.Index().SetName("idx_conv_sendTime"),
				},
				{
					Keys:    bson.D{{Key: "clientMsgId", Value: 1}},
					Options: options.Index().SetName("uk_clientMsgId").SetUnique(true).SetSparse(true),
				},
			},
		},
		{
			coll: "conversation",
			indexes: []mongo.IndexModel{
				{
					Keys:    bson.D{{Key: "conversationId", Value: 1}},
					Options: options.Index().SetName("idx_conversationId"),
				},
			},
		},
		{
			coll: "conversations",
			indexes: []mongo.IndexModel{
				{
					Keys:    bson.D{{Key: "userId", Value: 1}},
					Options: options.Index().SetUnique(true).SetName("uk_userId"),
				},
			},
		},
		{
			coll: "user_conversation_read",
			indexes: []mongo.IndexModel{
				{
					Keys:    bson.D{{Key: "userId", Value: 1}, {Key: "conversationId", Value: 1}},
					Options: options.Index().SetUnique(true).SetName("uk_user_conv"),
				},
			},
		},
		{
			coll: "conversation_topics",
			indexes: []mongo.IndexModel{
				{
					Keys:    bson.D{{Key: "conversationId", Value: 1}, {Key: "status", Value: 1}},
					Options: options.Index().SetName("idx_conv_status"),
				},
				{
					Keys:    bson.D{{Key: "topicId", Value: 1}},
					Options: options.Index().SetUnique(true).SetName("uk_topicId"),
				},
			},
		},
		{
			coll: "message_vectors",
			indexes: []mongo.IndexModel{
				{
					Keys:    bson.D{{Key: "chatLogId", Value: 1}},
					Options: options.Index().SetUnique(true).SetName("uk_chatLogId"),
				},
				{
					Keys:    bson.D{{Key: "conversationId", Value: 1}, {Key: "sendTime", Value: -1}},
					Options: options.Index().SetName("idx_conv_sendTime"),
				},
			},
		},
		{
			coll: "user_memory",
			indexes: []mongo.IndexModel{
				{
					Keys:    bson.D{{Key: "userId", Value: 1}, {Key: "conversationId", Value: 1}},
					Options: options.Index().SetUnique(true).SetName("uk_user_conv"),
				},
			},
		},
	}

	var firstErr error
	for _, spec := range specs {
		for _, idx := range spec.indexes {
			name := ""
			if idx.Options != nil && idx.Options.Name != nil {
				name = *idx.Options.Name
			}
			if _, err := db.Collection(spec.coll).Indexes().CreateOne(ctx, idx); err != nil {
				logx.Errorf("mongo EnsureIndexes collection=%s index=%s err=%v", spec.coll, name, err)
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			logx.Infof("mongo EnsureIndexes collection=%s index=%s ok", spec.coll, name)
		}
	}
	return firstErr
}

// EnsureIndexesAsync 启动时建索引，不阻塞服务。
func EnsureIndexesAsync(uri, dbName string) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		if err := EnsureIndexes(ctx, uri, dbName); err != nil {
			logx.Errorf("mongo EnsureIndexes: %v", err)
		}
	}()
}
