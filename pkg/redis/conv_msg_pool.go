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
// 共享消息池: conv:msg:{convId}  (List, 按插入顺序)
// 仅缓存最近 ConvMsgPoolMaxSize 条，超出走 MongoDB。
// 每用户用自己的水位线 cacheSeq 在池中裁剪。
// =====================================================================

// MsgPoolItem 池中元素
type MsgPoolItem struct {
	Seq      int64  `json:"seq"`
	MsgId    string `json:"msgId"`
	SendId   string `json:"sendId"`
	SendName string `json:"sendName,omitempty"`
	Content  string `json:"content"`
	Type     int32  `json:"type"`
	Time     int64  `json:"time"`
}

// ConvMsgPool 共享消息池
type ConvMsgPool struct {
	client *redis.Redis
	ttl    time.Duration
}

// NewConvMsgPool 创建共享消息池
func NewConvMsgPool(client *redis.Redis, ttl time.Duration) *ConvMsgPool {
	if ttl <= 0 {
		ttl = ConvMsgPoolTTL
	}
	return &ConvMsgPool{client: client, ttl: ttl}
}

func (c *ConvMsgPool) key(conversationId string) string {
	return fmt.Sprintf("conv:msg:%s", conversationId)
}

func (c *ConvMsgPool) jitter() int {
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

// PoolMeta 池元信息
type PoolMeta struct {
	MinSeq int64
	MaxSeq int64
	Size   int64
}

// GetMeta 获取池的范围元信息（min/max seq, size）
func (c *ConvMsgPool) GetMeta(conversationId string) (*PoolMeta, error) {
	meta := &PoolMeta{}
	card, err := c.client.Llen(c.key(conversationId))
	if err != nil {
		return nil, err
	}
	if card == 0 {
		return meta, nil
	}
	meta.Size = int64(card)

	// 取头部和尾部来获取 min/max seq
	head, err := c.client.Lindex(c.key(conversationId), 0)
	if err != nil || head == "" {
		return meta, nil
	}
	if item, _ := parsePoolItem(head); item != nil {
		meta.MinSeq = item.Seq
	}

	tail, err := c.client.Lindex(c.key(conversationId), -1)
	if err != nil || tail == "" {
		return meta, nil
	}
	if item, _ := parsePoolItem(tail); item != nil {
		meta.MaxSeq = item.Seq
	}
	return meta, nil
}

// Append 追加一条消息到池（RPUSH + LTRIM 保持最近 N 条）
func (c *ConvMsgPool) Append(conversationId string, item *MsgPoolItem) error {
	if item == nil {
		return nil
	}
	b, err := json.Marshal(item)
	if err != nil {
		return err
	}
	key := c.key(conversationId)
	ttl := c.jitter()
	script := `
		redis.call('RPUSH', KEYS[1], ARGV[1])
		redis.call('LTRIM', KEYS[1], -tonumber(ARGV[2]), -1)
		if redis.call('TTL', KEYS[1]) < 0 then
			redis.call('EXPIRE', KEYS[1], ARGV[3])
		end
		return 'OK'
	`
	_, err = c.client.Eval(
		script,
		[]string{key},
		string(b),
		ConvMsgPoolMaxSize,
		ttl,
	)
	return err
}

// BatchAppend 批量追加（Lua 原子操作）
func (c *ConvMsgPool) BatchAppend(conversationId string, items []*MsgPoolItem) error {
	if len(items) == 0 {
		return nil
	}
	key := c.key(conversationId)
	ttl := c.jitter()

	// 构建 Lua 参数：所有消息 JSON
	args := make([]interface{}, 0, len(items)+2)
	for _, item := range items {
		b, err := json.Marshal(item)
		if err != nil {
			continue
		}
		args = append(args, string(b))
	}
	args = append(args, ConvMsgPoolMaxSize)
	args = append(args, ttl)

	script := `
		local key = KEYS[1]
		local max = tonumber(ARGV[#ARGV - 1])
		local ttl = tonumber(ARGV[#ARGV])
		for i = 1, #ARGV - 2 do
			redis.call('RPUSH', key, ARGV[i])
		end
		redis.call('LTRIM', key, -max, -1)
		if redis.call('TTL', key) < 0 then
			redis.call('EXPIRE', key, ttl)
		end
		return 'OK'
	`
	_, err := c.client.Eval(script, []string{key}, args...)
	return err
}

func parsePoolItem(raw string) (*MsgPoolItem, error) {
	if raw == "" {
		return nil, nil
	}
	var item MsgPoolItem
	if err := json.Unmarshal([]byte(raw), &item); err != nil {
		return nil, err
	}
	return &item, nil
}

// RangeN 取最近 N 条消息（从尾部取，返回升序）
func (c *ConvMsgPool) RangeN(conversationId string, n int) ([]*MsgPoolItem, error) {
	if n <= 0 {
		n = ConvMsgPoolMaxSize
	}
	raw, err := c.client.Lrange(c.key(conversationId), -n, -1)
	if err != nil {
		return nil, err
	}
	out := make([]*MsgPoolItem, 0, len(raw))
	for _, r := range raw {
		item, _ := parsePoolItem(r)
		if item != nil {
			out = append(out, item)
		}
	}
	return out, nil
}

// RangeAbove 从某 seq 之上的所有元素（升序），最多取 limit 条
func (c *ConvMsgPool) RangeAbove(conversationId string, fromSeq int64, limit int) ([]*MsgPoolItem, error) {
	if limit <= 0 {
		limit = ConvMsgPoolMaxSize
	}
	// List 是按插入顺序（也即 seq 递增顺序）存储的
	// 取最近 limit 条，然后过滤出 seq > fromSeq 的
	raw, err := c.client.Lrange(c.key(conversationId), -limit, -1)
	if err != nil {
		return nil, err
	}
	out := make([]*MsgPoolItem, 0, len(raw))
	for _, r := range raw {
		item, _ := parsePoolItem(r)
		if item == nil {
			continue
		}
		if fromSeq <= 0 || item.Seq > fromSeq {
			out = append(out, item)
		}
	}
	return out, nil
}

// RangeBelow 从某 seq 之下取 N 条（升序）
func (c *ConvMsgPool) RangeBelow(conversationId string, toSeq int64, limit int) ([]*MsgPoolItem, error) {
	if limit <= 0 {
		limit = ConvMsgPoolMaxSize
	}
	// 取最近 limit*2 条（多取一些确保过滤后有足够数量），然后过滤出 seq < toSeq 的
	raw, err := c.client.Lrange(c.key(conversationId), -limit*2, -1)
	if err != nil {
		return nil, err
	}
	var filtered []*MsgPoolItem
	for _, r := range raw {
		item, _ := parsePoolItem(r)
		if item == nil {
			continue
		}
		if item.Seq < toSeq {
			filtered = append(filtered, item)
		}
	}
	// 取最后 limit 条
	start := 0
	if len(filtered) > limit {
		start = len(filtered) - limit
	}
	return filtered[start:], nil
}

// FetchByUserWaterLine 根据用户水位线在共享池中裁剪可见消息
func (c *ConvMsgPool) FetchByUserWaterLine(conversationId string, cacheSeq int64, n int) ([]*MsgPoolItem, int64, error) {
	if n <= 0 {
		n = ConvMsgPoolMaxSize
	}
	items, err := c.RangeAbove(conversationId, cacheSeq, n)
	if err != nil {
		return nil, 0, err
	}
	var newCacheSeq int64
	if len(items) > 0 {
		newCacheSeq = items[0].Seq - 1
	}
	return items, newCacheSeq, nil
}

// HasSeq 判断池里是否有某条 seq
func (c *ConvMsgPool) HasSeq(conversationId string, seq int64) (bool, error) {
	items, err := c.RangeN(conversationId, ConvMsgPoolMaxSize)
	if err != nil {
		return false, err
	}
	for _, item := range items {
		if item.Seq == seq {
			return true, nil
		}
	}
	return false, nil
}

// Del 删除共享池
func (c *ConvMsgPool) Del(conversationId string) error {
	_, err := c.client.Del(c.key(conversationId))
	return err
}

// TrimToSeq 删除池中所有 seq < minSeq 的消息
func (c *ConvMsgPool) TrimToSeq(conversationId string, minSeq int64) error {
	items, err := c.RangeN(conversationId, ConvMsgPoolMaxSize)
	if err != nil {
		return err
	}
	// 找到第一个 seq >= minSeq 的位置
	cutoff := -1
	for i, item := range items {
		if item.Seq >= minSeq {
			cutoff = i
			break
		}
	}
	if cutoff <= 0 {
		return nil // 没有需要裁剪的
	}
	// LTRIM 保留从 cutoff 开始的元素（转换为从尾部计算的索引）
	key := c.key(conversationId)
	keepFrom := cutoff - len(items)
	script := `
		redis.call('LTRIM', KEYS[1], ARGV[1], -1)
		redis.call('EXPIRE', KEYS[1], ARGV[2])
		return 'OK'
	`
	_, err = c.client.Eval(script, []string{key}, keepFrom, c.jitter())
	return err
}

// unused but kept for compatibility
var _ = strconv.FormatInt
