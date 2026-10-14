package redis

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/zeromicro/go-zero/core/stores/redis"
)

const (
	// MaxMessagesPerConversation 单个会话缓存的最大消息条数
	MaxMessagesPerConversation = 50
)

// ConversationData 单个会话的缓存数据结构（包含会话信息和最近消息）
type ConversationData struct {
	ConversationId string      `json:"conversationId"`             // 会话ID
	ChatType       int32      `json:"chatType"`                   // 聊天类型：1=单聊, 2=群聊
	TargetId       string     `json:"targetId"`                   // 对方ID
	TargetName     string     `json:"targetName,omitempty"`       // 对方名称（从联系人缓存填充）
	TargetIcon     string     `json:"targetIcon,omitempty"`       // 对方头像
	IsShow         bool       `json:"isShow"`                     // 是否显示
	Seq            int64      `json:"seq"`                        // 会话序号
	Read           int32      `json:"read"`                       // 已读消息数
	Total          int32      `json:"total"`                     // 总消息数
	Unread         int32      `json:"unread"`                    // 未读消息数
	Messages       []*MsgData `json:"messages,omitempty"`        // 最近的消息列表（内嵌）
}

// MsgData 消息数据结构（精简版，仅缓存需要展示的字段）
type MsgData struct {
	Id       string `json:"id,omitempty"`  // 消息ID
	SendId   string `json:"sendId"`        // 发送者ID
	SendName string `json:"sendName,omitempty"` // 发送者名称
	Content  string `json:"content"`       // 消息内容
	Time     int64  `json:"time"`         // 发送时间
	Type     int32  `json:"type"`         // 消息类型
}

// ConversationCacheV2 会话缓存 V2（Hash 结构 + 内嵌消息）
// 改进点：
// 1. 使用 Hash 结构，每个会话独立存储
// 2. 消息内嵌到会话中，一次操作搞定
// 3. 原子操作，支持并发更新
type ConversationCacheV2 struct {
	client *redis.Redis
	ttl    time.Duration
}

// NewConversationCacheV2 创建会话缓存 V2
func NewConversationCacheV2(client *redis.Redis, ttl time.Duration) *ConversationCacheV2 {
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	return &ConversationCacheV2{
		client: client,
		ttl:    ttl,
	}
}

// key 生成缓存 key
func (c *ConversationCacheV2) key(userId string) string {
	return fmt.Sprintf("conv:list:%s", userId)
}

// jitter 添加过期时间抖动（±10%），防止缓存雪崩
func (c *ConversationCacheV2) jitter() int {
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

// HGet 获取单个会话缓存
func (c *ConversationCacheV2) HGet(userId, conversationId string) (*ConversationData, error) {
	data, err := c.client.Hget(c.key(userId), conversationId)
	if err != nil {
		return nil, err
	}
	if data == "" {
		return nil, nil
	}

	var conv ConversationData
	if err := json.Unmarshal([]byte(data), &conv); err != nil {
		return nil, err
	}
	return &conv, nil
}

// HGetAll 获取用户所有会话缓存
func (c *ConversationCacheV2) HGetAll(userId string) (map[string]*ConversationData, error) {
	data, err := c.client.Hgetall(c.key(userId))
	if err != nil {
		return nil, err
	}

	result := make(map[string]*ConversationData)
	for convId, jsonStr := range data {
		var conv ConversationData
		if err := json.Unmarshal([]byte(jsonStr), &conv); err != nil {
			continue // 跳过解析失败的
		}
		result[convId] = &conv
	}
	return result, nil
}

// HSet 设置单个会话缓存
func (c *ConversationCacheV2) HSet(userId string, conv *ConversationData) error {
	data, err := json.Marshal(conv)
	if err != nil {
		return err
	}
	return c.client.Hset(c.key(userId), conv.ConversationId, string(data))
}

// HSetWithTTL 设置单个会话缓存并刷新 TTL
func (c *ConversationCacheV2) HSetWithTTL(userId string, conv *ConversationData) error {
	data, err := json.Marshal(conv)
	if err != nil {
		return err
	}
	key := c.key(userId)
	field := conv.ConversationId
	ttl := c.jitter()

	// 使用 Lua 脚本原子操作：HSET + EXPIRE
	script := `
		redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
		redis.call('EXPIRE', KEYS[1], ARGV[3])
		return 'OK'
	`
	_, err = c.client.Eval(script, []string{key}, field, string(data), ttl)
	return err
}

// HDel 删除单个会话缓存
func (c *ConversationCacheV2) HDel(userId, conversationId string) error {
	_, err := c.client.Hdel(c.key(userId), conversationId)
	return err
}

// HDelAll 删除用户所有会话缓存
func (c *ConversationCacheV2) HDelAll(userId string) error {
	_, err := c.client.Del(c.key(userId))
	return err
}

// HExists 检查会话是否存在
func (c *ConversationCacheV2) HExists(userId, conversationId string) (bool, error) {
	exists, err := c.client.Hexists(c.key(userId), conversationId)
	return exists, err
}

// HLen 获取会话数量
func (c *ConversationCacheV2) HLen(userId string) (int, error) {
	return c.client.Hlen(c.key(userId))
}

// AppendMessage 追加消息到会话（Lua 原子操作）
// 会自动截取保留最近 MaxMessagesPerConversation 条
func (c *ConversationCacheV2) AppendMessage(userId, conversationId string, msg *MsgData) error {
	script := `
		local key = KEYS[1]
		local field = ARGV[1]
		local new_msg = ARGV[2]
		local max_messages = tonumber(ARGV[3])
		local ttl = tonumber(ARGV[4])

		-- 获取现有数据
		local data = redis.call('HGET', key, field)
		local conv

		if data and data ~= false then
			conv = cjson.decode(data)
			if not conv.messages then
				conv.messages = {}
			end
			-- 追加新消息
			local msg_obj = cjson.decode(new_msg)
			table.insert(conv.messages, msg_obj)
			-- 截取保留最近的消息
			while #conv.messages > max_messages do
				table.remove(conv.messages, 1)
			end
		else
			-- 新建会话数据
			conv = {
				conversationId = field,
				messages = { cjson.decode(new_msg) }
			}
		end

		-- 写回
		redis.call('HSET', key, field, cjson.encode(conv))
		-- 刷新 TTL
		redis.call('EXPIRE', key, ttl)

		return 'OK'
	`

	msgJson, _ := json.Marshal(msg)
	_, err := c.client.Eval(
		script,
		[]string{c.key(userId)},
		conversationId,
		string(msgJson),
		MaxMessagesPerConversation,
		c.jitter(),
	)
	return err
}

// AppendMessageWithMeta 追加消息并更新会话元信息（Lua 原子操作）
func (c *ConversationCacheV2) AppendMessageWithMeta(userId, conversationId string, msg *MsgData, meta *ConversationMeta) error {
	script := `
		local key = KEYS[1]
		local field = ARGV[1]
		local new_msg = ARGV[2]
		local meta_str = ARGV[3]
		local max_messages = tonumber(ARGV[4])
		local ttl = tonumber(ARGV[5])

		-- 获取现有数据
		local data = redis.call('HGET', key, field)
		local conv
		local meta_obj = cjson.decode(meta_str)

		if data and data ~= false then
			conv = cjson.decode(data)
			if not conv.messages then
				conv.messages = {}
			end
			-- 追加新消息
			local msg_obj = cjson.decode(new_msg)
			table.insert(conv.messages, msg_obj)
			-- 截取保留最近的消息
			while #conv.messages > max_messages do
				table.remove(conv.messages, 1)
			end
			-- 更新元信息
			for k, v in pairs(meta_obj) do
				conv[k] = v
			end
		else
			-- 新建会话数据
			conv = meta_obj
			conv.conversationId = field
			conv.messages = { cjson.decode(new_msg) }
		end

		-- 写回
		redis.call('HSET', key, field, cjson.encode(conv))
		-- 刷新 TTL
		redis.call('EXPIRE', key, ttl)

		return 'OK'
	`

	msgJson, _ := json.Marshal(msg)
	metaJson, _ := json.Marshal(meta)
	_, err := c.client.Eval(
		script,
		[]string{c.key(userId)},
		conversationId,
		string(msgJson),
		string(metaJson),
		MaxMessagesPerConversation,
		c.jitter(),
	)
	return err
}

// ConversationMeta 会话元信息（用于更新）
type ConversationMeta struct {
	ChatType   int32  `json:"chatType"`
	TargetId   string `json:"targetId"`
	TargetName string `json:"targetName,omitempty"`
	TargetIcon string `json:"targetIcon,omitempty"`
	IsShow     bool   `json:"isShow"`
	Seq        int64  `json:"seq"`
	Read       int32  `json:"read"`
	Total      int32  `json:"total"`
	Unread     int32  `json:"unread"`
}

// UpdateMeta 更新会话元信息
func (c *ConversationCacheV2) UpdateMeta(userId, conversationId string, meta *ConversationMeta) error {
	data, err := c.client.Hget(c.key(userId), conversationId)
	if err != nil {
		return err
	}
	if data == "" {
		return nil // 会话不存在，不更新
	}

	var conv ConversationData
	if err := json.Unmarshal([]byte(data), &conv); err != nil {
		return err
	}

	// 更新元信息
	conv.ChatType = meta.ChatType
	conv.TargetId = meta.TargetId
	conv.TargetName = meta.TargetName
	conv.TargetIcon = meta.TargetIcon
	conv.IsShow = meta.IsShow
	conv.Seq = meta.Seq
	conv.Read = meta.Read
	conv.Total = meta.Total
	conv.Unread = meta.Unread

	return c.HSetWithTTL(userId, &conv)
}

// UpdateUnread 更新未读数
func (c *ConversationCacheV2) UpdateUnread(userId, conversationId string, unread int32) error {
	data, err := c.client.Hget(c.key(userId), conversationId)
	if err != nil {
		return err
	}
	if data == "" {
		return nil
	}

	var conv ConversationData
	if err := json.Unmarshal([]byte(data), &conv); err != nil {
		return err
	}

	conv.Unread = unread
	return c.HSetWithTTL(userId, &conv)
}

// DelAsync 异步延迟删除会话缓存（延迟双删策略）
func (c *ConversationCacheV2) DelAsync(userId string, delay time.Duration) {
	go func() {
		c.HDelAll(userId)
		time.Sleep(delay)
		c.HDelAll(userId)
	}()
}
