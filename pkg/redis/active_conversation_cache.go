package redis

import (
	"fmt"
	"time"

	"easy-chat/pkg/metrics"

	"github.com/zeromicro/go-zero/core/stores/redis"
)

// ActiveConversations 用户活跃会话的逻辑模型。
// Redis 中不存该 JSON，而是拆成 Set：key=active:conv:{userId}，member=conversationId。
type ActiveConversations struct {
	UserId          string   `json:"userId"`
	ConversationIds []string `json:"conversationIds"`
}

// ActiveConversationCache 记录用户当前打开中的会话集合（推送降噪 / 前台已读判断）
//
// 数据结构：
//
//	Go:     ActiveConversations{UserId, ConversationIds}
//	Redis:  Key = active:conv:{userId}   Type = Set
//	        Member = conversationId（可多个）
//	        TTL = 默认 10m（每次 Add/Touch 续期整 key，防客户端异常退出残留）
//
// 语义：
//   - 进房 Activate → SADD + EXPIRE
//   - 退房 Deactivate → SREM（只删该会话，不影响其它打开中的会话）
//   - 判断是否前台 → SISMEMBER
type ActiveConversationCache struct {
	client *redis.Redis
	ttl    time.Duration
}

func NewActiveConversationCache(client *redis.Redis, ttl time.Duration) *ActiveConversationCache {
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}
	return &ActiveConversationCache{client: client, ttl: ttl}
}

func (c *ActiveConversationCache) key(userId string) string {
	return fmt.Sprintf("active:conv:%s", userId)
}

func (c *ActiveConversationCache) ttlSeconds() int {
	sec := int(c.ttl.Seconds())
	if sec <= 0 {
		return 600
	}
	return sec
}

// Add 将会话加入用户的活跃集合，并续期整 key TTL
func (c *ActiveConversationCache) Add(userId, conversationId string) error {
	if userId == "" || conversationId == "" {
		return nil
	}
	start := time.Now()
	script := `
		redis.call('SADD', KEYS[1], ARGV[1])
		redis.call('EXPIRE', KEYS[1], ARGV[2])
		return 1
	`
	_, err := c.client.Eval(script, []string{c.key(userId)}, conversationId, c.ttlSeconds())
	c.observe("sadd", start, err)
	return err
}

// Remove 从活跃集合移除指定会话；集合空则删除 key
func (c *ActiveConversationCache) Remove(userId, conversationId string) error {
	if userId == "" || conversationId == "" {
		return nil
	}
	start := time.Now()
	script := `
		redis.call('SREM', KEYS[1], ARGV[1])
		local n = redis.call('SCARD', KEYS[1])
		if n == 0 then
			redis.call('DEL', KEYS[1])
		else
			redis.call('EXPIRE', KEYS[1], ARGV[2])
		end
		return n
	`
	_, err := c.client.Eval(script, []string{c.key(userId)}, conversationId, c.ttlSeconds())
	c.observe("srem", start, err)
	return err
}

// IsMember 判断会话是否在用户活跃集合中
func (c *ActiveConversationCache) IsMember(userId, conversationId string) (bool, error) {
	if userId == "" || conversationId == "" {
		return false, nil
	}
	start := time.Now()
	ok, err := c.client.Sismember(c.key(userId), conversationId)
	c.observe("sismember", start, err)
	return ok, err
}

// Members 返回用户当前全部活跃会话 ID
func (c *ActiveConversationCache) Members(userId string) ([]string, error) {
	start := time.Now()
	vals, err := c.client.Smembers(c.key(userId))
	c.observe("smembers", start, err)
	return vals, err
}

// GetActive 读取并组装为 Go 结构体（Redis 仍是 Set，非 JSON 整包）
func (c *ActiveConversationCache) GetActive(userId string) (*ActiveConversations, error) {
	ids, err := c.Members(userId)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	return &ActiveConversations{
		UserId:          userId,
		ConversationIds: ids,
	}, nil
}

// Contains 等价于 IsMember，便于按结构体语义表达
func (a *ActiveConversations) Contains(conversationId string) bool {
	if a == nil {
		return false
	}
	for _, id := range a.ConversationIds {
		if id == conversationId {
			return true
		}
	}
	return false
}

// Touch 仅续期（心跳），不改变成员
func (c *ActiveConversationCache) Touch(userId string) error {
	if userId == "" {
		return nil
	}
	start := time.Now()
	err := c.client.Expire(c.key(userId), c.ttlSeconds())
	c.observe("expire", start, err)
	return err
}

// Clear 清空用户全部活跃会话（慎用：多端共享同一 key）
func (c *ActiveConversationCache) Clear(userId string) error {
	if userId == "" {
		return nil
	}
	start := time.Now()
	_, err := c.client.Del(c.key(userId))
	c.observe("del", start, err)
	return err
}

// Set 兼容旧调用：等同 Add
func (c *ActiveConversationCache) Set(userId, conversationId string) error {
	return c.Add(userId, conversationId)
}

// Get 兼容旧调用：任意返回集合中一个会话；空集合返回 ""
// 新逻辑请用 IsMember / Members。
func (c *ActiveConversationCache) Get(userId string) (string, error) {
	vals, err := c.Members(userId)
	if err != nil {
		return "", err
	}
	if len(vals) == 0 {
		return "", nil
	}
	return vals[0], nil
}

// Del 兼容旧调用：等同 Clear
func (c *ActiveConversationCache) Del(userId string) error {
	return c.Clear(userId)
}

func (c *ActiveConversationCache) observe(op string, start time.Time, err error) {
	metrics.RedisOperationLatency.WithLabelValues(op, "active").Observe(
		float64(time.Since(start).Milliseconds()),
	)
	if err != nil {
		metrics.RedisOperationTotal.WithLabelValues(op, "failed").Inc()
		return
	}
	metrics.RedisOperationTotal.WithLabelValues(op, "success").Inc()
}
