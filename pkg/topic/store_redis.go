package topic

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/zeromicro/go-zero/core/stores/redis"
)

const redisKeyPrefix = "conv:topic:state:"

// RedisStore Redis 热状态存储（含向量，不含完整对话）。
type RedisStore struct {
	client *redis.Redis
	ttl    time.Duration
}

func NewRedisStore(client *redis.Redis, ttl time.Duration) *RedisStore {
	if ttl <= 0 {
		ttl = DefaultConfig().RedisTTL
	}
	return &RedisStore{client: client, ttl: ttl}
}

func (s *RedisStore) key(conversationId string) string {
	return redisKeyPrefix + conversationId
}

func (s *RedisStore) Get(ctx context.Context, conversationId string) (*State, error) {
	data, err := s.client.GetCtx(ctx, s.key(conversationId))
	if err != nil {
		return nil, err
	}
	if data == "" {
		return nil, nil
	}
	var st State
	if err := json.Unmarshal([]byte(data), &st); err != nil {
		return nil, err
	}
	return &st, nil
}

func (s *RedisStore) Save(ctx context.Context, st *State) error {
	if st == nil {
		return nil
	}
	st.UpdatedAt = time.Now().Unix()
	body, err := json.Marshal(st)
	if err != nil {
		return err
	}
	ttlSec := int(s.ttl.Seconds())
	if ttlSec <= 0 {
		ttlSec = int((7 * 24 * time.Hour).Seconds())
	}
	return s.client.SetexCtx(ctx, s.key(st.ConversationId), string(body), ttlSec)
}

func (s *RedisStore) Delete(ctx context.Context, conversationId string) error {
	_, err := s.client.DelCtx(ctx, s.key(conversationId))
	return err
}

// ApplySummaryRewrite CAS 更新正式主题摘要与向量。
func (s *RedisStore) ApplySummaryRewrite(ctx context.Context, conversationId, topicId, summary string, emb []float32, expectVersion int) error {
	st, err := s.Get(ctx, conversationId)
	if err != nil {
		return err
	}
	if st == nil || st.Formal == nil || st.Formal.TopicId != topicId {
		return fmt.Errorf("formal topic not found for rewrite")
	}
	if st.Formal.SummaryVersion != expectVersion {
		return fmt.Errorf("summary version mismatch")
	}
	st.Formal.Summary = summary
	st.Formal.Embedding = emb
	st.Formal.SummaryVersion++
	return s.Save(ctx, st)
}
