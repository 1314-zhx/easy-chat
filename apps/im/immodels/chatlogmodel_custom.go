package immodels

import (
	"context"
	"time"

	"github.com/zeromicro/go-zero/core/stores/mon"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// CustomChatLogModel 扩展接口
type CustomChatLogModel interface {
	ChatLogModel
	FindWithFilter(ctx context.Context, filter bson.M, limit int64, sort bson.M) ([]*ChatLog, error)
	InsertMany(ctx context.Context, docs []*ChatLog) error
	UpdateTopicTag(ctx context.Context, msgId, topicId, roundId string) error
	MongoClient() *mongo.Client
}

type customChatLogModelImpl struct {
	ChatLogModel
	model *defaultChatLogModel
}

// NewCustomChatLogModel 创建支持自定义查询的模型
func NewCustomChatLogModel(url, db, collection string) CustomChatLogModel {
	conn := mon.MustNewModel(url, db, collection)
	defaultModel := newDefaultChatLogModel(conn)
	return &customChatLogModelImpl{
		ChatLogModel: defaultModel,
		model:        defaultModel,
	}
}

func (m *customChatLogModelImpl) FindWithFilter(ctx context.Context, filter bson.M, limit int64, sort bson.M) ([]*ChatLog, error) {
	opts := options.Find()
	if limit > 0 {
		opts.SetLimit(limit)
	}
	if len(sort) > 0 {
		opts.SetSort(sort)
	}

	var results []*ChatLog
	cursor, err := m.model.conn.Collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	if err = cursor.All(ctx, &results); err != nil {
		return nil, err
	}

	return results, nil
}

// InsertMany 批量插入聊天记录
func (m *customChatLogModelImpl) InsertMany(ctx context.Context, docs []*ChatLog) error {
	if len(docs) == 0 {
		return nil
	}

	interfaceDocs := make([]interface{}, len(docs))
	for i, doc := range docs {
		interfaceDocs[i] = doc
	}

	opts := options.InsertMany().SetOrdered(false)
	_, err := m.model.conn.Collection.InsertMany(ctx, interfaceDocs, opts)
	return err
}

func (m *customChatLogModelImpl) MongoClient() *mongo.Client {
	return m.model.conn.Database().Client()
}

func (m *customChatLogModelImpl) UpdateTopicTag(ctx context.Context, msgId, topicId, roundId string) error {
	oid, err := primitive.ObjectIDFromHex(msgId)
	if err != nil {
		return err
	}
	_, err = m.model.conn.UpdateOne(ctx, bson.M{"_id": oid}, bson.M{"$set": bson.M{
		"topicId":  topicId,
		"roundId":  roundId,
		"updateAt": time.Now(),
	}})
	return err
}
