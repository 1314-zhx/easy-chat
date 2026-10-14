package immodels

import (
	"context"
	"time"

	"github.com/zeromicro/go-zero/core/stores/mon"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MessageVectorModel interface {
	Upsert(ctx context.Context, data *MessageVector) error
	Exists(ctx context.Context, chatLogId string) (bool, error)
	ListByConversation(ctx context.Context, conversationId string, limit int64) ([]*MessageVector, error)
}

type defaultMessageVectorModel struct {
	conn *mon.Model
}

func NewMessageVectorModel(url, db, collection string) MessageVectorModel {
	return &defaultMessageVectorModel{
		conn: mon.MustNewModel(url, db, collection),
	}
}

func (m *defaultMessageVectorModel) Upsert(ctx context.Context, data *MessageVector) error {
	now := time.Now()
	createAt := data.CreateAt
	if createAt.IsZero() {
		createAt = now
	}
	filter := bson.M{"chatLogId": data.ChatLogId}
	update := bson.M{
		"$set": bson.M{
			"chatLogId":      data.ChatLogId,
			"conversationId": data.ConversationId,
			"sendId":         data.SendId,
			"msgContent":     data.MsgContent,
			"sendTime":       data.SendTime,
			"embedding":      data.Embedding,
			"updateAt":       now,
		},
		"$setOnInsert": bson.M{
			"createAt": createAt,
		},
	}
	opts := options.Update().SetUpsert(true)
	_, err := m.conn.UpdateOne(ctx, filter, update, opts)
	return err
}

func (m *defaultMessageVectorModel) Exists(ctx context.Context, chatLogId string) (bool, error) {
	count, err := m.conn.Collection.CountDocuments(ctx, bson.M{"chatLogId": chatLogId})
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (m *defaultMessageVectorModel) ListByConversation(ctx context.Context, conversationId string, limit int64) ([]*MessageVector, error) {
	opts := options.Find().SetSort(bson.M{"sendTime": -1})
	if limit > 0 {
		opts.SetLimit(limit)
	}
	var results []*MessageVector
	cursor, err := m.conn.Collection.Find(ctx, bson.M{"conversationId": conversationId}, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	if err = cursor.All(ctx, &results); err != nil {
		return nil, err
	}
	return results, nil
}
