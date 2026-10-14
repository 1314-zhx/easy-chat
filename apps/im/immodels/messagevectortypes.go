package immodels

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// MessageVector 聊天消息的向量索引，用于语义检索。
type MessageVector struct {
	ID             primitive.ObjectID `bson:"_id,omitempty"`
	ChatLogId      string             `bson:"chatLogId"`
	ConversationId string             `bson:"conversationId"`
	SendId         string             `bson:"sendId"`
	MsgContent     string             `bson:"msgContent"`
	SendTime       int64              `bson:"sendTime"`
	Embedding      []float32          `bson:"embedding"`
	CreateAt       time.Time          `bson:"createAt,omitempty"`
	UpdateAt       time.Time          `bson:"updateAt,omitempty"`
}
