package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/zeromicro/go-zero/core/stores/redis"
)

// 会话静态元信息: conv:meta:{bucket}，field=conversationId，value=JSON。
const (
	ConvMetaFieldChatType    = "chatType"
	ConvMetaFieldMemberCount = "memberCount"
	ConvMetaFieldMembers     = "members"
)

// ConvMeta 只保存低频变化的会话公共信息；动态消息字段统一放入 ConvHot。
type ConvMeta struct {
	ChatType    int32    `json:"chatType"`              // 聊天类型：1=单聊，2=群聊
	MemberCount int      `json:"memberCount,omitempty"` // 群成员数量
	Members     []string `json:"members,omitempty"`     // 群成员 ID 列表
}

type ConvMetaCache struct {
	client  *redis.Redis
	ttl     time.Duration
	buckets int
}

func NewConvMetaCache(client *redis.Redis, ttl time.Duration) *ConvMetaCache {
	if ttl <= 0 {
		ttl = ConvMetaTTL
	}
	return &ConvMetaCache{client: client, ttl: ttl, buckets: 32}
}

func (c *ConvMetaCache) bucket(conversationId string) int {
	return hashBucket(conversationId, c.buckets)
}
func (c *ConvMetaCache) key(bucket int) string              { return fmt.Sprintf("conv:meta:%d", bucket) }
func (c *ConvMetaCache) field(conversationId string) string { return conversationId }

func (c *ConvMetaCache) jitter() int {
	base := int(c.ttl.Seconds())
	scope := base / 10
	if scope <= 0 {
		scope = base
	}
	ttl := base + rand.Intn(scope*2+1) - scope
	if ttl <= 0 {
		return 1
	}
	return ttl
}

func (c *ConvMetaCache) Get(conversationId string) (*ConvMeta, error) {
	data, err := c.client.Hget(c.key(c.bucket(conversationId)), c.field(conversationId))
	if err != nil {
		return nil, err
	}
	if data == "" {
		return nil, nil
	}
	var meta ConvMeta
	if err := json.Unmarshal([]byte(data), &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

// Set 原子执行静态元信息 HSET 和 bucket TTL 刷新。
func (c *ConvMetaCache) Set(conversationId string, meta *ConvMeta) error {
	if meta == nil {
		return nil
	}
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	script := `
		redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
		redis.call('EXPIRE', KEYS[1], ARGV[3])
		return 'OK'
	`
	_, err = c.client.Eval(script, []string{c.key(c.bucket(conversationId))}, c.field(conversationId), string(data), c.jitter())
	return err
}

func (c *ConvMetaCache) SetMembers(conversationId string, members []string) error {
	meta, err := c.Get(conversationId)
	if err != nil {
		return err
	}
	if meta == nil {
		meta = &ConvMeta{}
	}
	meta.Members = members
	meta.MemberCount = len(members)
	return c.Set(conversationId, meta)
}

// SetFields 仅允许修改静态字段，动态字段必须通过 ConvHotCache 更新。
func (c *ConvMetaCache) SetFields(conversationId string, fields map[string]string) error {
	if len(fields) == 0 {
		return nil
	}
	meta, err := c.Get(conversationId)
	if err != nil {
		return err
	}
	if meta == nil {
		meta = &ConvMeta{}
	}
	if v, ok := fields[ConvMetaFieldChatType]; ok {
		n, _ := strconv.ParseInt(v, 10, 32)
		meta.ChatType = int32(n)
	}
	if v, ok := fields[ConvMetaFieldMemberCount]; ok {
		meta.MemberCount, _ = strconv.Atoi(v)
	}
	if v, ok := fields[ConvMetaFieldMembers]; ok {
		_ = json.Unmarshal([]byte(v), &meta.Members)
	}
	return c.Set(conversationId, meta)
}

func (c *ConvMetaCache) Del(conversationId string) error {
	_, err := c.client.Hdel(c.key(c.bucket(conversationId)), c.field(conversationId))
	return err
}

// PipelineSet 分批 Pipeline 发送多个 Lua；每个会话的 HSET+EXPIRE 独立原子。
func (c *ConvMetaCache) PipelineSet(ctx context.Context, updates map[string]*ConvMeta) error {
	if len(updates) == 0 {
		return nil
	}
	const batchSize = 200
	type update struct {
		conversationId string
		meta           *ConvMeta
	}
	batch := make([]update, 0, batchSize)
	flush := func(items []update) error {
		if len(items) == 0 {
			return nil
		}
		script := `
			redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
			redis.call('EXPIRE', KEYS[1], ARGV[3])
			return 'OK'
		`
		return c.client.PipelinedCtx(ctx, func(pipe redis.Pipeliner) error {
			for _, item := range items {
				data, err := json.Marshal(item.meta)
				if err != nil {
					return err
				}
				pipe.Eval(ctx, script, []string{c.key(c.bucket(item.conversationId))}, c.field(item.conversationId), string(data), c.jitter())
			}
			return nil
		})
	}
	for conversationId, meta := range updates {
		if meta == nil {
			continue
		}
		batch = append(batch, update{conversationId: conversationId, meta: meta})
		if len(batch) == batchSize {
			if err := flush(batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
	return flush(batch)
}

func previewOf(msg *MsgData) string {
	if msg == nil {
		return ""
	}
	if len(msg.Content) <= 64 {
		return msg.Content
	}
	return msg.Content[:64]
}
