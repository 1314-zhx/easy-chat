package immodels

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// UserConversationRead 用户对会话的已读/未读记录
// 统一存储单聊和群聊的已读游标，配合 Redis 实现双写
type UserConversationRead struct {
	ID             primitive.ObjectID `bson:"_id,omitempty"`
	UserId         string             `bson:"userId"`            // 用户ID
	ConversationId string             `bson:"conversationId"`    // 会话ID
	ReadSeq        int64              `bson:"readSeq"`           // 已读游标（用户已读到的消息序列号）

	UpdateAt time.Time `bson:"updateAt,omitempty"`
	CreateAt time.Time `bson:"createAt,omitempty"`
}
