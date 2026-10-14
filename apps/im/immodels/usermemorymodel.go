package immodels

import (
	"context"
	"time"

	"github.com/zeromicro/go-zero/core/stores/mon"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type UserMemoryModel interface {
	FindOne(ctx context.Context, userId, conversationId string) (*UserMemory, error)
	Upsert(ctx context.Context, data *UserMemory) error
}

type defaultUserMemoryModel struct {
	conn *mon.Model
}

func NewUserMemoryModel(url, db, collection string) UserMemoryModel {
	return &defaultUserMemoryModel{
		conn: mon.MustNewModel(url, db, collection),
	}
}

func (m *defaultUserMemoryModel) FindOne(ctx context.Context, userId, conversationId string) (*UserMemory, error) {
	var data UserMemory
	err := m.conn.FindOne(ctx, &data, bson.M{
		"userId":         userId,
		"conversationId": conversationId,
	})
	switch err {
	case nil:
		return &data, nil
	case mon.ErrNotFound:
		return nil, nil
	default:
		return nil, err
	}
}

func (m *defaultUserMemoryModel) Upsert(ctx context.Context, data *UserMemory) error {
	now := time.Now()
	data.UpdateAt = now
	if data.CreateAt.IsZero() {
		data.CreateAt = now
	}
	filter := bson.M{
		"userId":         data.UserId,
		"conversationId": data.ConversationId,
	}
	update := bson.M{"$set": data}
	opts := options.Update().SetUpsert(true)
	_, err := m.conn.UpdateOne(ctx, filter, update, opts)
	return err
}
