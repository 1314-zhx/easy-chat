package immodels

import (
	"context"
	"time"

	"easy-chat/pkg/topic"

	"github.com/zeromicro/go-zero/core/stores/mon"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type ConversationTopicModel interface {
	Insert(ctx context.Context, data *ConversationTopic) error
	EndTopic(ctx context.Context, topicId string, endedAt time.Time) error
	EndActive(ctx context.Context, conversationId string, endedAt time.Time) error
	UpdateSummary(ctx context.Context, topicId, summary string, version int) error
	IncHitCount(ctx context.Context, topicId string, lastHitAt time.Time) error
	FindActive(ctx context.Context, conversationId string) (*ConversationTopic, error)
	ListEnded(ctx context.Context, conversationId string, limit int) ([]*ConversationTopic, error)
}

type defaultConversationTopicModel struct {
	conn *mon.Model
}

func NewConversationTopicModel(url, db, collection string) ConversationTopicModel {
	return &defaultConversationTopicModel{conn: mon.MustNewModel(url, db, collection)}
}

func MustConversationTopicModel(url, db string) ConversationTopicModel {
	return NewConversationTopicModel(url, db, "conversation_topics")
}

func (m *defaultConversationTopicModel) Insert(ctx context.Context, data *ConversationTopic) error {
	now := time.Now()
	if data.ID.IsZero() {
		data.CreateAt = now
	}
	data.UpdateAt = now
	_, err := m.conn.InsertOne(ctx, data)
	return err
}

func (m *defaultConversationTopicModel) EndTopic(ctx context.Context, topicId string, endedAt time.Time) error {
	_, err := m.conn.UpdateOne(ctx,
		bson.M{"topicId": topicId, "status": topic.TopicStatusActive},
		bson.M{"$set": bson.M{
			"status":   topic.TopicStatusEnded,
			"endedAt":  endedAt,
			"updateAt": time.Now(),
		}},
	)
	return err
}

func (m *defaultConversationTopicModel) EndActive(ctx context.Context, conversationId string, endedAt time.Time) error {
	_, err := m.conn.UpdateMany(ctx,
		bson.M{"conversationId": conversationId, "status": topic.TopicStatusActive},
		bson.M{"$set": bson.M{
			"status":   topic.TopicStatusEnded,
			"endedAt":  endedAt,
			"updateAt": time.Now(),
		}},
	)
	return err
}

func (m *defaultConversationTopicModel) UpdateSummary(ctx context.Context, topicId, summary string, version int) error {
	_, err := m.conn.UpdateOne(ctx,
		bson.M{"topicId": topicId},
		bson.M{"$set": bson.M{
			"summary":        summary,
			"summaryVersion": version,
			"updateAt":       time.Now(),
		}},
	)
	return err
}

func (m *defaultConversationTopicModel) IncHitCount(ctx context.Context, topicId string, lastHitAt time.Time) error {
	_, err := m.conn.UpdateOne(ctx,
		bson.M{"topicId": topicId},
		bson.M{
			"$inc": bson.M{"totalHitCount": 1},
			"$set": bson.M{"lastHitAt": lastHitAt, "updateAt": time.Now()},
		},
	)
	return err
}

func (m *defaultConversationTopicModel) FindActive(ctx context.Context, conversationId string) (*ConversationTopic, error) {
	var data ConversationTopic
	err := m.conn.FindOne(ctx, &data, bson.M{
		"conversationId": conversationId,
		"status":         topic.TopicStatusActive,
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

func (m *defaultConversationTopicModel) ListEnded(ctx context.Context, conversationId string, limit int) ([]*ConversationTopic, error) {
	if limit <= 0 {
		limit = 20
	}
	var list []*ConversationTopic
	opt := options.Find().SetSort(bson.M{"lastHitAt": -1}).SetLimit(int64(limit))
	err := m.conn.Find(ctx, &list, bson.M{
		"conversationId": conversationId,
		"status":         topic.TopicStatusEnded,
	}, opt)
	if err == mon.ErrNotFound {
		return nil, nil
	}
	return list, err
}

// TopicRepository 适配 pkg/topic.Repository。
type TopicRepository struct {
	model ConversationTopicModel
}

func NewTopicRepository(model ConversationTopicModel) *TopicRepository {
	return &TopicRepository{model: model}
}

func (r *TopicRepository) Insert(ctx context.Context, doc *topic.TopicDoc) error {
	return r.model.Insert(ctx, &ConversationTopic{
		TopicId:        doc.TopicId,
		ConversationId: doc.ConversationId,
		UserId:         doc.UserId,
		Name:           doc.Name,
		Summary:        doc.Summary,
		SummaryVersion: doc.SummaryVersion,
		Status:         doc.Status,
		StartedAt:      doc.StartedAt,
		EndedAt:        doc.EndedAt,
		TotalHitCount:  doc.TotalHitCount,
		LastHitAt:      doc.LastHitAt,
	})
}

func (r *TopicRepository) EndTopic(ctx context.Context, topicId string, endedAt time.Time) error {
	return r.model.EndTopic(ctx, topicId, endedAt)
}

func (r *TopicRepository) EndActive(ctx context.Context, conversationId string, endedAt time.Time) error {
	return r.model.EndActive(ctx, conversationId, endedAt)
}

func (r *TopicRepository) UpdateSummary(ctx context.Context, topicId, summary string, version int) error {
	return r.model.UpdateSummary(ctx, topicId, summary, version)
}

func (r *TopicRepository) IncHitCount(ctx context.Context, topicId string, lastHitAt time.Time) error {
	return r.model.IncHitCount(ctx, topicId, lastHitAt)
}

func (r *TopicRepository) FindActive(ctx context.Context, conversationId string) (*topic.TopicDoc, error) {
	t, err := r.model.FindActive(ctx, conversationId)
	if err != nil || t == nil {
		return nil, err
	}
	return toTopicDoc(t), nil
}

func (r *TopicRepository) ListEnded(ctx context.Context, conversationId string, limit int) ([]*topic.TopicDoc, error) {
	list, err := r.model.ListEnded(ctx, conversationId, limit)
	if err != nil {
		return nil, err
	}
	out := make([]*topic.TopicDoc, 0, len(list))
	for _, t := range list {
		out = append(out, toTopicDoc(t))
	}
	return out, nil
}

func toTopicDoc(t *ConversationTopic) *topic.TopicDoc {
	return &topic.TopicDoc{
		TopicId:        t.TopicId,
		ConversationId: t.ConversationId,
		UserId:         t.UserId,
		Name:           t.Name,
		Summary:        t.Summary,
		SummaryVersion: t.SummaryVersion,
		Status:         t.Status,
		StartedAt:      t.StartedAt,
		EndedAt:        t.EndedAt,
		TotalHitCount:  t.TotalHitCount,
		LastHitAt:      t.LastHitAt,
	}
}

var _ topic.Repository = (*TopicRepository)(nil)
