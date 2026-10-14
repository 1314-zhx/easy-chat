package redis

import (
	"fmt"
	"strconv"
	"time"

	"github.com/zeromicro/go-zero/core/stores/redis"
)

// ReadCursorCache 记录会话已读游标
// key: read:cursor:{userId}:{conversationId} -> lastReadSeq
// 双写到 Redis + Mongo 时，Redis 负责实时性，Mongo 负责持久化回放
 type ReadCursorCache struct {
	client *redis.Redis
	ttl    time.Duration
}

func NewReadCursorCache(client *redis.Redis, ttl time.Duration) *ReadCursorCache {
	if ttl <= 0 {
		ttl = 7 * 24 * time.Hour
	}
	return &ReadCursorCache{client: client, ttl: ttl}
}

func (c *ReadCursorCache) key(userId, conversationId string) string {
	return fmt.Sprintf("read:cursor:%s:%s", userId, conversationId)
}

func (c *ReadCursorCache) Set(userId, conversationId string, lastReadSeq int64) error {
	return c.client.Setex(c.key(userId, conversationId), strconv.FormatInt(lastReadSeq, 10), int(c.ttl.Seconds()))
}

func (c *ReadCursorCache) Get(userId, conversationId string) (int64, bool, error) {
	val, err := c.client.Get(c.key(userId, conversationId))
	if err != nil {
		return 0, false, err
	}
	if val == "" {
		return 0, false, nil
	}
	seq, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, false, err
	}
	return seq, true, nil
}

func (c *ReadCursorCache) Del(userId, conversationId string) error {
	_, err := c.client.Del(c.key(userId, conversationId))
	return err
}
