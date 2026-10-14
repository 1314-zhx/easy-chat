package immodels

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// ConversationTopic 会话主题档案（Mongo 仅存摘要与元数据，不存向量）。
type ConversationTopic struct {
	ID primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`

	TopicId        string `bson:"topicId"`
	ConversationId string `bson:"conversationId"`
	UserId         string `bson:"userId"`
	Name           string `bson:"name"`
	Summary        string `bson:"summary"`
	SummaryVersion int    `bson:"summaryVersion"`
	Status         string `bson:"status"` // active | ended

	StartedAt     time.Time  `bson:"startedAt"`
	EndedAt       *time.Time `bson:"endedAt,omitempty"`
	TotalHitCount int64      `bson:"totalHitCount"`
	LastHitAt     time.Time  `bson:"lastHitAt"`

	UpdateAt time.Time `bson:"updateAt,omitempty"`
	CreateAt time.Time `bson:"createAt,omitempty"`
}
