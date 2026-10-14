package redis

import (
	"fmt"
	"math/rand"
	"time"

	"easy-chat/pkg/metrics"

	"github.com/zeromicro/go-zero/core/stores/redis"
)

// UnreadCountCache 未读消息数缓存（过期抖动防止雪崩）
type UnreadCountCache struct {
	client *redis.Redis
	ttl    time.Duration // 缓存过期时间
}

// NewUnreadCountCache 创建未读消息数缓存
func NewUnreadCountCache(client *redis.Redis, ttl time.Duration) *UnreadCountCache {
	if ttl <= 0 {
		ttl = 5 * time.Minute // 默认 5 分钟
	}
	return &UnreadCountCache{
		client: client,
		ttl:    ttl,
	}
}

// key 生成缓存 key
func (c *UnreadCountCache) key(userId, conversationId string) string {
	return fmt.Sprintf("unread:count:%s:%s", userId, conversationId)
}

// jitter 添加过期时间抖动（±10%），防止缓存雪崩
func (c *UnreadCountCache) jitter() int {
	jitterRange := int(float64(c.ttl) * 0.1)
	if jitterRange <= 0 {
		jitterRange = int(c.ttl)
	}
	offset := rand.Intn(jitterRange*2+1) - jitterRange
	ttl := int(c.ttl) + offset
	if ttl <= 0 {
		ttl = 1
	}
	return ttl
}

// Get 获取未读消息数
// 返回未读数和一个布尔值表示是否命中
func (c *UnreadCountCache) Get(userId, conversationId string) (int64, bool, error) {
	start := time.Now()
	val, err := c.client.Get(c.key(userId, conversationId))

	// 埋点: Redis 操作延迟
	metrics.RedisOperationLatency.WithLabelValues("get", "unread").Observe(
		float64(time.Since(start).Milliseconds()),
	)

	if err != nil {
		metrics.RedisOperationTotal.WithLabelValues("get", "failed").Inc()
		return 0, false, err
	}

	if val == "" {
		// 埋点: 缓存未命中
		metrics.UnreadCountCacheMiss.Inc()
		return 0, false, nil // Miss
	}

	var count int64
	if _, err := fmt.Sscanf(val, "%d", &count); err != nil {
		metrics.RedisOperationTotal.WithLabelValues("get", "failed").Inc()
		return 0, false, err
	}

	// 埋点: 缓存命中
	metrics.UnreadCountCacheHit.Inc()
	return count, true, nil // Hit
}

// Incr 自增未读消息数
func (c *UnreadCountCache) Incr(userId, conversationId string) (int64, error) {
	start := time.Now()
	key := c.key(userId, conversationId)
	count, err := c.client.Incr(key)

	// 埋点: Redis 操作延迟
	metrics.RedisOperationLatency.WithLabelValues("incr", "unread").Observe(
		float64(time.Since(start).Milliseconds()),
	)

	if err != nil {
		metrics.RedisOperationTotal.WithLabelValues("incr", "failed").Inc()
		return 0, err
	}

	c.client.Expire(key, c.jitter())
	metrics.RedisOperationTotal.WithLabelValues("incr", "success").Inc()
	return count, nil
}

// Decr 自减未读消息数（已读时调用）
func (c *UnreadCountCache) Decr(userId, conversationId string, delta int64) (int64, error) {
	start := time.Now()
	key := c.key(userId, conversationId)
	count, err := c.client.Decrby(key, delta)

	// 埋点: Redis 操作延迟
	metrics.RedisOperationLatency.WithLabelValues("decr", "unread").Observe(
		float64(time.Since(start).Milliseconds()),
	)

	if err != nil {
		metrics.RedisOperationTotal.WithLabelValues("decr", "failed").Inc()
		return 0, err
	}

	if count < 0 {
		c.client.Setex(key, "0", c.jitter())
		return 0, nil
	}
	c.client.Expire(key, c.jitter())
	metrics.RedisOperationTotal.WithLabelValues("decr", "success").Inc()
	return count, nil
}

// Set 设置未读消息数（带过期抖动）
func (c *UnreadCountCache) Set(userId, conversationId string, count int64) error {
	start := time.Now()
	err := c.client.Setex(c.key(userId, conversationId), fmt.Sprintf("%d", count), c.jitter())

	// 埋点: Redis 操作延迟
	metrics.RedisOperationLatency.WithLabelValues("set", "unread").Observe(
		float64(time.Since(start).Milliseconds()),
	)

	if err != nil {
		metrics.RedisOperationTotal.WithLabelValues("set", "failed").Inc()
		return err
	}

	metrics.RedisOperationTotal.WithLabelValues("set", "success").Inc()
	return nil
}

// Del 删除未读消息数缓存
func (c *UnreadCountCache) Del(userId, conversationId string) error {
	_, err := c.client.Del(c.key(userId, conversationId))
	return err
}

// DelAsync 异步延迟删除（延迟双删策略）
func (c *UnreadCountCache) DelAsync(userId, conversationId string, delay time.Duration) {
	go func() {
		c.Del(userId, conversationId)
		time.Sleep(delay)
		c.Del(userId, conversationId)
	}()
}

// DelByUser 删除用户所有会话的未读缓存
func (c *UnreadCountCache) DelByUser(userId string) error {
	key := fmt.Sprintf("unread:count:%s:*", userId)
	keys, err := c.client.Keys(key)
	if err != nil {
		return err
	}
	for _, k := range keys {
		c.client.Del(k)
	}
	return nil
}
