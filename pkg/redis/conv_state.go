package redis

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/zeromicro/go-zero/core/stores/redis"
)

// =====================================================================
// 用户会话水位线（独立层）: conv:state:{convId}:{userId}
// =====================================================================

// ConvStateFields
const (
	ConvStateFieldReadSeq        = "readSeq"
	ConvStateFieldUnread         = "unread"
	ConvStateFieldIsShow         = "isShow"
	ConvStateFieldCacheSeq       = "cacheSeq"
	ConvStateFieldCacheTimestamp = "cacheTimestamp"
	ConvStateFieldLastActive     = "lastActiveTime"
)

// ConvState 用户在指定会话的状态
type ConvState struct {
	ReadSeq        int64 `json:"readSeq"`
	Unread         int64 `json:"unread"`
	IsShow         bool  `json:"isShow"`
	CacheSeq       int64 `json:"cacheSeq"`
	CacheTimestamp int64 `json:"cacheTimestamp"`
	LastActiveTime int64 `json:"lastActiveTime"`
}

// ConvStateCache 用户会话状态缓存
type ConvStateCache struct {
	client *redis.Redis
	ttl    time.Duration
}

// NewConvStateCache 创建用户会话状态缓存
func NewConvStateCache(client *redis.Redis, ttl time.Duration) *ConvStateCache {
	if ttl <= 0 {
		ttl = ConvStateTTL
	}
	return &ConvStateCache{client: client, ttl: ttl}
}

func (c *ConvStateCache) key(conversationId, userId string) string {
	return fmt.Sprintf("conv:state:%s:%s", conversationId, userId)
}

func (c *ConvStateCache) jitter() int {
	base := int(c.ttl.Seconds())
	scope := base / 10
	if scope <= 0 {
		scope = base
	}
	offset := rand.Intn(scope*2+1) - scope
	ttl := base + offset
	if ttl <= 0 {
		ttl = 1
	}
	return ttl
}

// Get 获取单个用户的状态
func (c *ConvStateCache) Get(conversationId, userId string) (*ConvState, error) {
	data, err := c.client.Hgetall(c.key(conversationId, userId))
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}
	s := &ConvState{}
	if v, ok := data[ConvStateFieldReadSeq]; ok && v != "" {
		s.ReadSeq, _ = strconv.ParseInt(v, 10, 64)
	}
	if v, ok := data[ConvStateFieldUnread]; ok && v != "" {
		s.Unread, _ = strconv.ParseInt(v, 10, 64)
	}
	if v, ok := data[ConvStateFieldIsShow]; ok {
		s.IsShow = v == "1"
	}
	if v, ok := data[ConvStateFieldCacheSeq]; ok && v != "" {
		s.CacheSeq, _ = strconv.ParseInt(v, 10, 64)
	}
	if v, ok := data[ConvStateFieldCacheTimestamp]; ok && v != "" {
		s.CacheTimestamp, _ = strconv.ParseInt(v, 10, 64)
	}
	if v, ok := data[ConvStateFieldLastActive]; ok && v != "" {
		s.LastActiveTime, _ = strconv.ParseInt(v, 10, 64)
	}
	return s, nil
}

// Set 整体设置状态
func (c *ConvStateCache) Set(conversationId, userId string, state *ConvState) error {
	fields := map[string]string{
		ConvStateFieldReadSeq:        strconv.FormatInt(state.ReadSeq, 10),
		ConvStateFieldUnread:         strconv.FormatInt(state.Unread, 10),
		ConvStateFieldIsShow:         boolToStr(state.IsShow),
		ConvStateFieldCacheSeq:       strconv.FormatInt(state.CacheSeq, 10),
		ConvStateFieldCacheTimestamp: strconv.FormatInt(state.CacheTimestamp, 10),
		ConvStateFieldLastActive:     strconv.FormatInt(state.LastActiveTime, 10),
	}
	return c.SetFields(conversationId, userId, fields)
}

// SetFields 批量设置
func (c *ConvStateCache) SetFields(conversationId, userId string, fields map[string]string) error {
	if len(fields) == 0 {
		return nil
	}
	args := make([]interface{}, 0, len(fields)*2)
	for k, v := range fields {
		args = append(args, k, v)
	}
	script := `
		local key = KEYS[1]
		local ttl = tonumber(ARGV[1])
		for i = 2, #ARGV, 2 do
			redis.call('HSET', key, ARGV[i], ARGV[i+1])
		end
		redis.call('EXPIRE', key, ttl)
		return 'OK'
	`
	argsKV := append([]interface{}{c.jitter()}, args...)
	_, err := c.client.Eval(script, []string{c.key(conversationId, userId)}, argsKV...)
	return err
}

// IncrUnread 未读数 +1（群发消息用）
func (c *ConvStateCache) IncrUnread(conversationId, userId string) (int64, error) {
	script := `
		local key = KEYS[1]
		local ttl = tonumber(ARGV[1])
		local v = redis.call('HINCRBY', key, 'unread', 1)
		redis.call('EXPIRE', key, ttl)
		return v
	`
	res, err := c.client.Eval(script, []string{c.key(conversationId, userId)}, c.jitter())
	if err != nil {
		return 0, err
	}
	return toInt64(res)
}

// BatchIncrUnread 批量给群成员 Incr 未读数（Lua 单 RTT）
// 400 人群从 800 命令 + 1 次 Pipeline 降为 1 次 EVAL
func (c *ConvStateCache) BatchIncrUnread(conversationId string, userIds []string) error {
	if len(userIds) == 0 {
		return nil
	}
	script := `
		local ttl = tonumber(ARGV[1])
		for i = 2, #ARGV do
			local key = KEYS[i - 1]
			redis.call('HINCRBY', key, 'unread', 1)
			redis.call('EXPIRE', key, ttl)
		end
		return 'OK'
	`
	ttl := c.jitter()
	keys := make([]string, len(userIds))
	args := make([]interface{}, 0, len(userIds)+1)
	args = append(args, ttl)
	for i, uid := range userIds {
		keys[i] = c.key(conversationId, uid)
	}
	_, err := c.client.Eval(script, keys, args...)
	return err
}

// ResetUnread 用户进入会话 / 已读时清零未读
func (c *ConvStateCache) ResetUnread(conversationId, userId string) error {
	return c.SetFields(conversationId, userId, map[string]string{
		ConvStateFieldUnread:    "0",
		ConvStateFieldLastActive: strconv.FormatInt(time.Now().Unix(), 10),
	})
}

// UpdateReadSeq 更新已读游标（仅当 newSeq 更大，避免回退）
func (c *ConvStateCache) UpdateReadSeq(conversationId, userId string, seq int64) error {
	_, err := c.AdvanceReadSeq(conversationId, userId, seq)
	return err
}

// AdvanceReadSeq 将 readSeq 推进到 max(old, seq)，30 天过期，不删 key。
func (c *ConvStateCache) AdvanceReadSeq(conversationId, userId string, seq int64) (int64, error) {
	if seq < 0 {
		seq = 0
	}
	ttl := int(ConvReadSeqTTL.Seconds())
	if ttl <= 0 {
		ttl = 30 * 24 * 3600
	}
	script := `
		local key = KEYS[1]
		local newSeq = tonumber(ARGV[1])
		local ttl = tonumber(ARGV[2])
		local old = tonumber(redis.call('HGET', key, 'readSeq') or '0')
		if newSeq > old then
			redis.call('HSET', key, 'readSeq', newSeq)
			old = newSeq
		end
		redis.call('HSET', key, 'lastActiveTime', ARGV[3])
		redis.call('EXPIRE', key, ttl)
		return old
	`
	res, err := c.client.Eval(script, []string{c.key(conversationId, userId)},
		seq, ttl, strconv.FormatInt(time.Now().Unix(), 10))
	if err != nil {
		return 0, err
	}
	return toInt64(res)
}

// UpdateCacheSeq 更新用户在共享池的水位线
func (c *ConvStateCache) UpdateCacheSeq(conversationId, userId string, cacheSeq int64) error {
	return c.SetFields(conversationId, userId, map[string]string{
		ConvStateFieldCacheSeq:       strconv.FormatInt(cacheSeq, 10),
		ConvStateFieldCacheTimestamp: strconv.FormatInt(time.Now().Unix(), 10),
	})
}

// Del 删除用户状态
func (c *ConvStateCache) Del(conversationId, userId string) error {
	_, err := c.client.Del(c.key(conversationId, userId))
	return err
}

// DelByConversation 删除整个会话下所有用户的状态（解散/清空时用）
func (c *ConvStateCache) DelByConversation(conversationId string) error {
	pattern := fmt.Sprintf("conv:state:%s:*", conversationId)
	keys, err := c.client.Keys(pattern)
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		return nil
	}
	_, err = c.client.Del(keys...)
	return err
}

// ListByConversation 通过 SCAN 列出某会话下所有用户的 state
// （会话列表页用，返回每用户 readSeq / unread）
func (c *ConvStateCache) ListByConversation(conversationId string) (map[string]*ConvState, error) {
	pattern := fmt.Sprintf("conv:state:%s:*", conversationId)
	keys, err := c.client.Keys(pattern)
	if err != nil {
		return nil, err
	}
	out := make(map[string]*ConvState, len(keys))
	if len(keys) == 0 {
		return out, nil
	}
	// 逐个 HGETALL（也可考虑 Pipeline）
	for _, k := range keys {
		// 提取 userId
		prefix := fmt.Sprintf("conv:state:%s:", conversationId)
		if len(k) <= len(prefix) {
			continue
		}
		uid := k[len(prefix):]
		data, err := c.client.Hgetall(k)
		if err != nil {
			continue
		}
		if len(data) == 0 {
			continue
		}
		s := &ConvState{}
		if v, ok := data[ConvStateFieldReadSeq]; ok && v != "" {
			s.ReadSeq, _ = strconv.ParseInt(v, 10, 64)
		}
		if v, ok := data[ConvStateFieldUnread]; ok && v != "" {
			s.Unread, _ = strconv.ParseInt(v, 10, 64)
		}
		if v, ok := data[ConvStateFieldIsShow]; ok {
			s.IsShow = v == "1"
		}
		out[uid] = s
	}
	return out, nil
}

func boolToStr(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

// toInt64 兼容 go-zero 返回的不同数值类型
func toInt64(v interface{}) (int64, error) {
	switch n := v.(type) {
	case int64:
		return n, nil
	case int:
		return int64(n), nil
	case string:
		return strconv.ParseInt(n, 10, 64)
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return 0, err
		}
		return strconv.ParseInt(string(b), 10, 64)
	}
}
