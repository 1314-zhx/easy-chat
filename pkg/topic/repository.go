package topic

import (
	"context"
	"time"
)

// Repository Mongo 主题档案（仅摘要，无向量）。
type Repository interface {
	Insert(ctx context.Context, doc *TopicDoc) error
	EndTopic(ctx context.Context, topicId string, endedAt time.Time) error
	EndActive(ctx context.Context, conversationId string, endedAt time.Time) error
	UpdateSummary(ctx context.Context, topicId, summary string, version int) error
	IncHitCount(ctx context.Context, topicId string, lastHitAt time.Time) error
	FindActive(ctx context.Context, conversationId string) (*TopicDoc, error)
	ListEnded(ctx context.Context, conversationId string, limit int) ([]*TopicDoc, error)
}
