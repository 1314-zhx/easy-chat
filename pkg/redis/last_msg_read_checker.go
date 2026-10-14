package redis

import (
	"github.com/zeromicro/go-zero/core/stores/redis"
)

// LastMsgReadChecker 只记录最后一条消息的已读状态
// 设计思路：只记录最后一条消息的已读用户，读取最后一条 = 读取所有
// 内存：整个群只用一个 Redis key（Set），极大节省内存
type LastMsgReadChecker struct {
	client *redis.Redis
	ttl    int // 过期时间（秒）
}

// NewLastMsgReadChecker 创建最后一条消息已读检查器（默认 7 天过期）
func NewLastMsgReadChecker(client *redis.Redis) *LastMsgReadChecker {
	return &LastMsgReadChecker{
		client: client,
		ttl:    7 * 24 * 3600, // 7 天
	}
}

// NewLastMsgReadCheckerWithTTL 创建最后一条消息已读检查器（自定义过期时间）
func NewLastMsgReadCheckerWithTTL(client *redis.Redis, ttlSeconds int) *LastMsgReadChecker {
	return &LastMsgReadChecker{
		client: client,
		ttl:    ttlSeconds,
	}
}

// key 生成已读集合的 key（群级别，一个群一个 key）
func (c *LastMsgReadChecker) key(conversationId string) string {
	return "msg:read:last:" + conversationId
}

// MarkRead 标记用户已读最后一条消息
// 返回值：是否为新增（true=首次已读，false=重复已读）
func (c *LastMsgReadChecker) MarkRead(conversationId, userId string) (bool, error) {
	result, err := c.client.Sadd(c.key(conversationId), userId)
	if err != nil {
		return false, err
	}
	// SADD 返回新增元素数量，1=新增，0=已存在
	return result == 1, nil
}

// HasReadLastMsg 检查用户是否已读最后一条消息
func (c *LastMsgReadChecker) HasReadLastMsg(conversationId, userId string) (bool, error) {
	return c.client.Sismember(c.key(conversationId), userId)
}

// GetReadCount 获取已读最后一条消息的用户数
func (c *LastMsgReadChecker) GetReadCount(conversationId string) (int64, error) {
	result, err := c.client.Scard(c.key(conversationId))
	if err != nil {
		return 0, err
	}
	return result, nil
}

// CardIfExists 去重集合还在时返回 SCARD；key 不存在则 exists=false（与空集 SCARD=0 区分）。
func (c *LastMsgReadChecker) CardIfExists(conversationId string) (count int64, exists bool, err error) {
	ok, err := c.client.Exists(c.key(conversationId))
	if err != nil || !ok {
		return 0, false, err
	}
	result, err := c.client.Scard(c.key(conversationId))
	if err != nil {
		return 0, true, err
	}
	return result, true, nil
}

// GetReadUsers 获取所有已读最后一条消息的用户
func (c *LastMsgReadChecker) GetReadUsers(conversationId string) ([]string, error) {
	result, err := c.client.Smembers(c.key(conversationId))
	if err != nil {
		return nil, err
	}
	return result, nil
}

// UpdateLastMsg 当群有新消息时，原子更新已读标记
// 1. 删除旧的已读记录（用户需要重新已读新消息）
// 2. 设置新的过期时间
// 注意：需要配合消息序列号来判断是否真的是"新消息"
func (c *LastMsgReadChecker) UpdateLastMsg(conversationId string) error {
	key := c.key(conversationId)

	// 1. 删除旧记录
	if _, err := c.client.Del(key); err != nil {
		return err
	}

	// 2. 设置过期时间
	return c.client.Expire(key, c.ttl)
}

// Del 删除已读记录（群解散或清理时调用）
func (c *LastMsgReadChecker) Del(conversationId string) error {
	_, err := c.client.Del(c.key(conversationId))
	return err
}

// Expire 设置过期时间
func (c *LastMsgReadChecker) Expire(conversationId string) error {
	return c.client.Expire(c.key(conversationId), c.ttl)
}
