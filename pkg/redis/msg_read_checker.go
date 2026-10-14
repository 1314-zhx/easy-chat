package redis

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/zeromicro/go-zero/core/stores/redis"
)

// MsgReadChecker 消息已读状态检查器（使用 Redis Set 精确存储 + 过期抖动）
type MsgReadChecker struct {
	client *redis.Redis
	ttl    time.Duration // 默认过期时间
}

// NewMsgReadChecker 创建已读检查器
func NewMsgReadChecker(client *redis.Redis) *MsgReadChecker {
	return &MsgReadChecker{
		client: client,
		ttl:    7 * 24 * time.Hour, // 默认 7 天
	}
}

// NewMsgReadCheckerWithTTL 创建已读检查器（自定义过期时间）
func NewMsgReadCheckerWithTTL(client *redis.Redis, ttl time.Duration) *MsgReadChecker {
	return &MsgReadChecker{
		client: client,
		ttl:    ttl,
	}
}

// key 生成已读集合的 key
func (c *MsgReadChecker) key(msgId string) string {
	return fmt.Sprintf("msg:read:%s", msgId)
}

// jitter 添加过期时间抖动（±10%），防止缓存雪崩
func (c *MsgReadChecker) jitter(base int) int {
	jitterRange := int(float64(base) * 0.1)
	if jitterRange <= 0 {
		jitterRange = base
	}
	offset := rand.Intn(jitterRange*2+1) - jitterRange
	ttl := base + offset
	if ttl <= 0 {
		ttl = 1
	}
	return ttl
}

// MarkRead 标记用户已读
func (c *MsgReadChecker) MarkRead(msgId, userId string) error {
	_, err := c.client.Sadd(c.key(msgId), userId)
	return err
}

// IsRead 检查用户是否已读
func (c *MsgReadChecker) IsRead(msgId, userId string) (bool, error) {
	return c.client.Sismember(c.key(msgId), userId)
}

// GetReadUsers 获取所有已读用户
func (c *MsgReadChecker) GetReadUsers(msgId string) ([]string, error) {
	return c.client.Smembers(c.key(msgId))
}

// GetReadCount 获取已读人数
func (c *MsgReadChecker) GetReadCount(msgId string) (int64, error) {
	return c.client.Scard(c.key(msgId))
}

// IsUnRead 检查用户是否未读（精确）
func (c *MsgReadChecker) IsUnRead(msgId, userId string) (bool, error) {
	isRead, err := c.IsRead(msgId, userId)
	if err != nil {
		return false, err
	}
	return !isRead, nil
}

// BatchIsRead 批量检查用户是否已读
func (c *MsgReadChecker) BatchIsRead(msgId string, userIds []string) (map[string]bool, error) {
	result := make(map[string]bool, len(userIds))
	for _, userId := range userIds {
		isRead, err := c.IsRead(msgId, userId)
		if err != nil {
			return nil, err
		}
		result[userId] = isRead
	}
	return result, nil
}

// Del 删除已读记录（消息过期时清理）
func (c *MsgReadChecker) Del(msgId string) error {
	_, err := c.client.Del(c.key(msgId))
	return err
}

// DelAsync 异步延迟删除已读记录（延迟双删策略）
// 先删除缓存，延迟后再删除一次，防止并发导致的数据不一致
func (c *MsgReadChecker) DelAsync(msgId string, delay time.Duration) {
	go func() {
		// 第一次删除
		c.Del(msgId)
		// 延迟后再删除
		time.Sleep(delay)
		c.Del(msgId)
	}()
}

// MarkReadWithDel 标记已读前先删除缓存（延迟双删策略）
// 适用于写入操作，确保缓存一致性
func (c *MsgReadChecker) MarkReadWithDel(msgId, userId string, delay time.Duration) error {
	// 1. 先删除缓存
	if err := c.Del(msgId); err != nil {
		return err
	}
	// 2. 异步延迟删除（延迟双删）
	c.DelAsync(msgId, delay)
	// 3. 写入数据（这里不直接写，由调用方控制）
	return nil
}

// UnreadCount 计算未读人数
func (c *MsgReadChecker) UnreadCount(msgId string, totalMembers int64) (int64, error) {
	readCount, err := c.GetReadCount(msgId)
	if err != nil {
		return 0, err
	}
	return totalMembers - readCount, nil
}

// Expire 设置过期时间（带抖动，防止雪崩）
func (c *MsgReadChecker) Expire(msgId string, seconds int) error {
	if seconds <= 0 {
		seconds = int(c.ttl.Seconds())
	}
	return c.client.Expire(c.key(msgId), c.jitter(seconds))
}

// EnsureFromMongo 从 MongoDB 备份恢复数据到 Redis
// 如果 Redis 中没有数据，则从 MongoDB 的已读记录解析并写入 Redis
func (c *MsgReadChecker) EnsureFromMongo(msgId, mongoReadRecords string) error {
	// 检查 Redis 是否已有数据
	exists, err := c.client.Exists(c.key(msgId))
	if err != nil {
		return err
	}

	// Redis 已有数据，无需恢复
	if exists {
		return nil
	}

	// 从 MongoDB 备份解析已读用户列表
	if mongoReadRecords == "" {
		return nil
	}

	users := strings.Split(mongoReadRecords, ",")
	if len(users) == 0 || users[0] == "" {
		return nil
	}

	// 批量写入 Redis（将用户列表作为多个元素添加到 Set）
	_, err = c.client.Sadd(c.key(msgId), users)
	return err
}
