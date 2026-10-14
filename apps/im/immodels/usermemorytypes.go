package immodels

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// UserMemory 用户长期记忆（按用户 + 会话维度）。
type UserMemory struct {
	ID             primitive.ObjectID `bson:"_id,omitempty"`         // MongoDB 自动生成的主键 ID
	UserId         string             `bson:"userId"`                // 用户 ID（关联具体的用户）
	ConversationId string             `bson:"conversationId"`        // 会话 ID（关联特定的对话场景，可为空则代表全局记忆）
	Name           string             `bson:"name,omitempty"`        // 用户姓名或昵称
	City           string             `bson:"city,omitempty"`        // 用户所在城市
	Preferences    []string           `bson:"preferences,omitempty"` // 用户偏好列表（例如：喜欢科幻电影、偏好简洁回答等）
	Facts          []string           `bson:"facts,omitempty"`       // 关于用户的关键事实集合（例如：养了一只猫、正在学习 Go 语言等）
	Summary        string             `bson:"summary,omitempty"`     // 用户画像或对话历史的综合摘要
	UpdateAt       time.Time          `bson:"updateAt,omitempty"`    // 记忆的最后更新时间
	CreateAt       time.Time          `bson:"createAt,omitempty"`    // 记忆的首次创建时间
}