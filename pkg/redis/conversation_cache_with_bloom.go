package redis

import (
	"context"
	"encoding/json"
	"time"

	"github.com/zeromicro/go-zero/core/stores/redis"
)

// ConversationCacheWithBloom 会话缓存（旁路缓存 + 延迟双删 + 布隆过滤器）
// 防止缓存穿透、缓存击穿、缓存雪崩
type ConversationCacheWithBloom struct {
	cache *ConversationCache
	bloom *BloomFilter
	ttl   time.Duration
}

// NewConversationCacheWithBloom 创建带布隆过滤器的会话缓存
func NewConversationCacheWithBloom(client *redis.Redis, ttl time.Duration, bloomSize uint64) *ConversationCacheWithBloom {
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	if bloomSize == 0 {
		bloomSize = 1 << 20 // 默认 1M 位
	}
	return &ConversationCacheWithBloom{
		cache: NewConversationCache(client, ttl),
		bloom: NewBloomFilter(client, "conversation", bloomSize, 7),
		ttl:   ttl,
	}
}

// Get 获取会话列表（防止缓存穿透）
// 流程：
// 1. 先检查布隆过滤器，元素一定不存在则直接返回空
// 2. 元素可能存在则查询缓存
// 3. 缓存未命中则查询数据库并写入缓存
func (c *ConversationCacheWithBloom) Get(ctx context.Context, userId string, dbFunc func() ([]byte, error)) ([]byte, bool, error) {
	// 1. 检查布隆过滤器
	mayExist, err := c.bloom.MayExist(userId)
	if err != nil {
		// 布隆过滤器错误，不影响主流程
	}

	// 2. 布隆过滤器判断一定不存在，直接返回空（防止缓存穿透）
	if !mayExist {
		return nil, false, nil
	}

	// 3. 查询缓存
	cached, hit, err := c.cache.Get(userId)
	if err != nil {
		// 缓存错误，不影响主流程
	}
	if hit {
		return cached, true, nil
	}

	// 4. 缓存未命中，查询数据库
	data, err := dbFunc()
	if err != nil {
		return nil, false, err
	}

	if data == nil || len(data) == 0 {
		// 数据库也为空，将 userId 加入布隆过滤器
		// 这样下次查询可以直接过滤掉
		_ = c.bloom.Add(userId)
		return nil, false, nil
	}

	// 5. 写入缓存
	_ = c.cache.Set(userId, json.RawMessage(data))

	return data, false, nil
}

// SimpleGet 简单获取缓存（不查数据库，用于兼容旧接口）
func (c *ConversationCacheWithBloom) SimpleGet(userId string) ([]byte, bool, error) {
	// 1. 检查布隆过滤器
	mayExist, err := c.bloom.MayExist(userId)
	if err != nil {
		// 布隆过滤器错误，不影响主流程
	}

	// 2. 一定不存在，直接返回空
	if !mayExist {
		return nil, false, nil
	}

	// 3. 查询缓存
	return c.cache.Get(userId)
}

// Set 设置缓存（同时更新布隆过滤器）
func (c *ConversationCacheWithBloom) Set(userId string, data interface{}) error {
	// 先添加布隆过滤器
	_ = c.bloom.Add(userId)
	return c.cache.Set(userId, data)
}

// Del 删除缓存（延迟双删策略）
func (c *ConversationCacheWithBloom) Del(userId string) error {
	return c.cache.Del(userId)
}

// DelWithDelay 延迟双删
func (c *ConversationCacheWithBloom) DelWithDelay(userId string, delay time.Duration) {
	c.cache.DelAsync(userId, delay)
}

// MayExist 检查元素是否可能在过滤器中
func (c *ConversationCacheWithBloom) MayExist(userId string) (bool, error) {
	return c.bloom.MayExist(userId)
}
