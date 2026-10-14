package immodels

import (
	"context"
	"time"

	"github.com/zeromicro/go-zero/core/stores/mon"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var _ ConversationModel = (*customConversationModel)(nil)

type (
	ConversationModel interface {
		conversationModel
		IncrSeqBy(ctx context.Context, conversationId string, n int64) (int64, error)
	}

	customConversationModel struct {
		*defaultConversationModel
	}
)

// IncrSeqBy 会话序号一次增加 n，返回增加后的 maxSeq。ctx 可为 mongo.SessionContext 以加入事务。
func (m *customConversationModel) IncrSeqBy(ctx context.Context, conversationId string, n int64) (int64, error) {
	if n <= 0 {
		n = 1
	}
	result, err := m.conn.Collection.FindOneAndUpdate(ctx,
		bson.M{"conversationId": conversationId},
		bson.M{
			"$inc": bson.M{"seq": n},
			"$set": bson.M{"updateAt": time.Now()},
		},
		options.FindOneAndUpdate().SetReturnDocument(options.After),
	)
	if err != nil {
		return 0, err
	}
	var data Conversation
	if err = result.Decode(&data); err != nil {
		return 0, err
	}
	return data.Seq, nil
}

// NewConversationModel returns a model for the mongo.
func NewConversationModel(url, db, collection string) ConversationModel {
	conn := mon.MustNewModel(url, db, collection)
	return &customConversationModel{
		defaultConversationModel: newDefaultConversationModel(conn),
	}
}

func MustConversationModel(url, db string) ConversationModel {
	return NewConversationModel(url, db, "conversation")
}
