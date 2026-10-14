package redis

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/zeromicro/go-zero/core/stores/redis"
)

// ConversationCache 会话列表缓存（旁路缓存模式 + 过期抖动防止雪崩）
type ConversationCache struct {
	client *redis.Redis
	ttl    time.Duration // 缓存过期时间
}

// NewConversationCache 创建会话缓存
func NewConversationCache(client *redis.Redis, ttl time.Duration) *ConversationCache {
	if ttl <= 0 {
		ttl = 5 * time.Minute // 默认 5 分钟
	}
	return &ConversationCache{
		client: client,
		ttl:    ttl,
	}
}

// key 生成缓存 key
func (c *ConversationCache) key(userId string) string {
	return fmt.Sprintf("conv:list:%s", userId)
}

// jitter 添加过期时间抖动（±10%），防止缓存雪崩
func (c *ConversationCache) jitter() int {
	// 基础抖动范围：ttl 的 10%
	jitterRange := int(float64(c.ttl) * 0.1)
	if jitterRange <= 0 {
		jitterRange = int(c.ttl)
	}
	// 随机偏移量 [-jitterRange, +jitterRange]
	offset := rand.Intn(jitterRange*2+1) - jitterRange
	ttl := int(c.ttl) + offset
	if ttl <= 0 {
		ttl = 1
	}
	return ttl
}

// Get 获取会话列表缓存
// 返回缓存数据和一个布尔值表示是否命中
func (c *ConversationCache) Get(userId string) ([]byte, bool, error) {
	data, err := c.client.Get(c.key(userId))
	if err != nil {
		return nil, false, err
	}
	if data == "" {
		return nil, false, nil // Miss
	}
	return []byte(data), true, nil // Hit
}

// Set 设置会话列表缓存（带过期抖动）
func (c *ConversationCache) Set(userId string, data interface{}) error {
	bytes, err := json.Marshal(data)
	if err != nil {
		return err
	}
	// 使用带抖动的 TTL
	return c.client.Setex(c.key(userId), string(bytes), c.jitter())
}

// Del 删除会话列表缓存
func (c *ConversationCache) Del(userId string) error {
	_, err := c.client.Del(c.key(userId))
	return err
}

const defaultDoubleDeleteDelay = 500 * time.Millisecond

// ScheduleSecondDelete 延迟双删的「第二次删除」：写库成功后调用，sleep 后再删一次，
// 清掉并发读回源写回的脏缓存。
func (c *ConversationCache) ScheduleSecondDelete(userId string, delay time.Duration) {
	if delay <= 0 {
		delay = defaultDoubleDeleteDelay
	}
	go func() {
		time.Sleep(delay)
		_ = c.Del(userId)
	}()
}

// DelayedDoubleDelete 标准延迟双删：先删 → writeFn 写 DB → 延迟再删。
func (c *ConversationCache) DelayedDoubleDelete(userId string, delay time.Duration, writeFn func() error) error {
	_ = c.Del(userId)
	if err := writeFn(); err != nil {
		return err
	}
	c.ScheduleSecondDelete(userId, delay)
	return nil
}

// DelAsync 兼容旧调用：等价于先删一次 + 再调度第二次延迟删除（两次删除均异步）。
// 写路径请优先用 DelayedDoubleDelete / Del + ScheduleSecondDelete。
func (c *ConversationCache) DelAsync(userId string, delay time.Duration) {
	go func() {
		_ = c.Del(userId)
		if delay <= 0 {
			delay = defaultDoubleDeleteDelay
		}
		time.Sleep(delay)
		_ = c.Del(userId)
	}()
}

// Exists 检查缓存是否存在
func (c *ConversationCache) Exists(userId string) (bool, error) {
	data, err := c.client.Get(c.key(userId))
	if err != nil {
		return false, err
	}
	return data != "", nil
}
