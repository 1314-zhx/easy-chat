package redis

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/zeromicro/go-zero/core/stores/redis"
)

// =====================================================================
// 会话热字段（共享层）: conv:hot:{convId}
// 两层 Hash 结构：
//   外层 key = conv:hot:{convId}，每个会话独立一个 Redis Hash key
//   内层 field = 热字段名，value = 字段值
// 每个热字段独立存储，更新单字段只需 HSET/HINCRBY，无需 JSON 序列化。
// =====================================================================

// ConvHotField 热字段名常量
const (
	// ConvHotPipelineBatchSize 单次 Pipeline 最多携带的会话更新数。
	ConvHotPipelineBatchSize = 200

	ConvHotFieldSeq              = "seq"
	ConvHotFieldTotal            = "total"
	ConvHotFieldLastMsgId        = "lastMsgId"
	ConvHotFieldLastMsgTime      = "lastMsgTime"
	ConvHotFieldLastMsgPreview   = "lastMsgPreview"
	ConvHotFieldLastMsgReadCount = "lastMsgReadCount"
	ConvHotFieldHotLeaseAt       = "hotLeaseAt" // 上次热点续约时间戳；空=非热点。重建不得覆盖
)

// ConvHot 会话热字段（Go 侧聚合结构体，Redis 里是独立 hash field）
// 存储: conv:hot:{convId} → Hash → field=字段名 → value=字段值
// 每个字段独立读写，更新单字段只需 HSET/HINCRBY，无需 JSON 序列化
type ConvHot struct {
	Seq              int64  `json:"seq"`              // 会话当前递增序号
	Total            int64  `json:"total"`            // 会话消息总数
	LastMsgId        string `json:"lastMsgId"`        // 当前最后一条消息ID（用于已读回执命中判断）
	LastMsgTime      int64  `json:"lastMsgTime"`      // 最后一条消息发送时间
	LastMsgPreview      string `json:"lastMsgPreview"`
	LastMsgReadCount    int64  `json:"lastMsgReadCount"`
	HasLastMsgReadCount bool   `json:"-"`
	HotLeaseAt          int64  `json:"hotLeaseAt,omitempty"`
}

// ConvHotCache 会话热字段缓存（两层 Hash）
type ConvHotCache struct {
	client *redis.Redis
	ttl    time.Duration
}

// NewConvHotCache 创建会话热字段缓存
func NewConvHotCache(client *redis.Redis, ttl time.Duration) *ConvHotCache {
	if ttl <= 0 {
		ttl = ConvMetaTTL
	}
	return &ConvHotCache{client: client, ttl: ttl}
}

// key 每个会话独立一个 Redis Hash key
func (c *ConvHotCache) key(conversationId string) string {
	return fmt.Sprintf("conv:hot:%s", conversationId)
}

func (c *ConvHotCache) jitter() int {
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

// Get 获取会话所有热字段（HGETALL → 结构体）
func (c *ConvHotCache) Get(conversationId string) (*ConvHot, error) {
	data, err := c.client.Hgetall(c.key(conversationId))
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}
	hot := &ConvHot{}
	if v, ok := data[ConvHotFieldSeq]; ok && v != "" {
		hot.Seq, _ = strconv.ParseInt(v, 10, 64)
	}
	if v, ok := data[ConvHotFieldTotal]; ok && v != "" {
		hot.Total, _ = strconv.ParseInt(v, 10, 64)
	}
	if v, ok := data[ConvHotFieldLastMsgId]; ok {
		hot.LastMsgId = v
	}
	if v, ok := data[ConvHotFieldLastMsgTime]; ok && v != "" {
		hot.LastMsgTime, _ = strconv.ParseInt(v, 10, 64)
	}
	if v, ok := data[ConvHotFieldLastMsgPreview]; ok {
		hot.LastMsgPreview = v
	}
	if v, ok := data[ConvHotFieldLastMsgReadCount]; ok && v != "" {
		hot.LastMsgReadCount, _ = strconv.ParseInt(v, 10, 64)
		hot.HasLastMsgReadCount = true
	}
	if v, ok := data[ConvHotFieldHotLeaseAt]; ok && v != "" {
		hot.HotLeaseAt, _ = strconv.ParseInt(v, 10, 64)
	}
	return hot, nil
}

// Set 写入业务热字段；不写 hotLeaseAt。仅新建 key 时带冷缓存 TTL。
func (c *ConvHotCache) Set(conversationId string, hot *ConvHot) error {
	if hot == nil {
		return nil
	}
	fields := map[string]string{
		ConvHotFieldSeq:            strconv.FormatInt(hot.Seq, 10),
		ConvHotFieldTotal:          strconv.FormatInt(hot.Total, 10),
		ConvHotFieldLastMsgId:      hot.LastMsgId,
		ConvHotFieldLastMsgTime:    strconv.FormatInt(hot.LastMsgTime, 10),
		ConvHotFieldLastMsgPreview: hot.LastMsgPreview,
	}
	// Mongo 回填没有已读人数时不要 HSET 0，否则会把「未知」写成「没人读」
	if hot.HasLastMsgReadCount {
		fields[ConvHotFieldLastMsgReadCount] = strconv.FormatInt(hot.LastMsgReadCount, 10)
	}
	// 重建不得写入 hotLeaseAt，避免覆盖探测结果
	return c.SetFields(conversationId, fields)
}

const convHotSetFieldsScript = `
		local key = KEYS[1]
		local ttl = tonumber(ARGV[1])
		for i = 2, #ARGV, 2 do
			if ARGV[i] ~= 'hotLeaseAt' then
				redis.call('HSET', key, ARGV[i], ARGV[i+1])
			end
		end
		if ttl > 0 and redis.call('TTL', key) < 0 then
			redis.call('EXPIRE', key, ttl)
		end
		return 'OK'
	`

// SetFields 写业务热字段。不改 hotLeaseAt；仅当 key 尚无 TTL 时套冷缓存过期。
func (c *ConvHotCache) SetFields(conversationId string, fields map[string]string) error {
	if len(fields) == 0 {
		return nil
	}
	delete(fields, ConvHotFieldHotLeaseAt)
	args := make([]interface{}, 0, len(fields)*2+1)
	args = append(args, c.jitter())
	for k, v := range fields {
		args = append(args, k, v)
	}
	_, err := c.client.Eval(convHotSetFieldsScript, []string{c.key(conversationId)}, args...)
	return err
}

// UpdateLastMsg 更新最后一条消息信息（3 个 field 一次写入）
func (c *ConvHotCache) UpdateLastMsg(conversationId, msgId, preview string, msgTime int64) error {
	return c.SetFields(conversationId, map[string]string{
		ConvHotFieldLastMsgId:      msgId,
		ConvHotFieldLastMsgPreview: preview,
		ConvHotFieldLastMsgTime:    strconv.FormatInt(msgTime, 10),
	})
}

// UpdateSeqTotal 更新 seq 和 total
func (c *ConvHotCache) UpdateSeqTotal(conversationId string, seq, total int64) error {
	return c.SetFields(conversationId, map[string]string{
		ConvHotFieldSeq:   strconv.FormatInt(seq, 10),
		ConvHotFieldTotal: strconv.FormatInt(total, 10),
	})
}

// UpdateReadCount 直接设置已读数
func (c *ConvHotCache) UpdateReadCount(conversationId string, count int64) error {
	return c.SetFields(conversationId, map[string]string{
		ConvHotFieldLastMsgReadCount: strconv.FormatInt(count, 10),
	})
}

// IncrReadCount 最后一条消息已读数原子 +1
// 直接 HINCRBY 单字段，无需读出 JSON 再写回
func (c *ConvHotCache) IncrReadCount(conversationId string) (int64, error) {
	script := `
		local key = KEYS[1]
		local v = redis.call('HINCRBY', key, 'lastMsgReadCount', 1)
		return v
	`
	res, err := c.client.Eval(script, []string{c.key(conversationId)})
	if err != nil {
		return 0, err
	}
	return toInt64(res)
}

// UpdateLastMessageAtomic 仅接受更大的 seq，并原子覆盖最后消息字段、递增 total、清理旧回执集合。
// 返回 updated=false 表示消息过旧或重复乱序，不会覆盖当前热数据。
func (c *ConvHotCache) UpdateLastMessageAtomic(conversationId string, msg *MsgPoolItem, dedup *LastMsgReadChecker) (*ConvHot, bool, error) {
	if msg == nil || msg.MsgId == "" || dedup == nil {
		return nil, false, nil
	}
	script := `
		local hotKey = KEYS[1]
		local dedupKey = KEYS[2]
		local incomingSeq = tonumber(ARGV[1])
		local currentSeq = tonumber(redis.call('HGET', hotKey, 'seq') or '0')
		if incomingSeq <= currentSeq then
			return {0, currentSeq, tonumber(redis.call('HGET', hotKey, 'total') or '0')}
		end
		local total = redis.call('HINCRBY', hotKey, 'total', 1)
		redis.call('HSET', hotKey,
			'seq', ARGV[1],
			'lastMsgId', ARGV[2],
			'lastMsgTime', ARGV[3],
			'lastMsgPreview', ARGV[4],
			'lastMsgReadCount', 0)
		redis.call('DEL', dedupKey)
		return {1, incomingSeq, total}
	`
	res, err := c.client.Eval(script, []string{c.key(conversationId), dedup.key(conversationId)}, msg.Seq, msg.MsgId, msg.Time, previewOf(&MsgData{Content: msg.Content}))
	if err != nil {
		return nil, false, err
	}
	values, ok := res.([]interface{})
	if !ok || len(values) != 3 {
		return nil, false, nil
	}
	updated, err := toInt64(values[0])
	if err != nil {
		return nil, false, err
	}
	seq, err := toInt64(values[1])
	if err != nil {
		return nil, false, err
	}
	total, err := toInt64(values[2])
	if err != nil {
		return nil, false, err
	}
	return &ConvHot{Seq: seq, Total: total, LastMsgId: msg.MsgId, LastMsgTime: msg.Time, LastMsgPreview: previewOf(&MsgData{Content: msg.Content})}, updated == 1, nil
}

// MarkLastMsgReadAtomic 原子完成最后一条消息校验、用户去重和已读数递增。
// 只有用户已读游标覆盖当前 hot.Seq 时才参与计数。
func (c *ConvHotCache) MarkLastMsgReadAtomic(conversationId string, readSeq int64, userId string, dedup *LastMsgReadChecker) (bool, int64, error) {
	if userId == "" || dedup == nil {
		return false, 0, nil
	}
	script := `
		local hotKey = KEYS[1]
		local dedupKey = KEYS[2]
		local dedupTTL = tonumber(ARGV[1])
		local userId = ARGV[2]
		local readSeq = tonumber(ARGV[3])
		local currentSeq = tonumber(redis.call('HGET', hotKey, 'seq') or '0')
		if readSeq < currentSeq then
			return {0, tonumber(redis.call('HGET', hotKey, 'lastMsgReadCount') or '0')}
		end
		local added = redis.call('SADD', dedupKey, userId)
		if added == 0 then
			return {0, redis.call('HGET', hotKey, 'lastMsgReadCount') or 0}
		end
		local count = redis.call('HINCRBY', hotKey, 'lastMsgReadCount', 1)
		redis.call('EXPIRE', dedupKey, dedupTTL)
		return {1, count}
	`
	res, err := c.client.Eval(script, []string{c.key(conversationId), dedup.key(conversationId)}, dedup.ttl, userId, readSeq)
	if err != nil {
		return false, 0, err
	}
	values, ok := res.([]interface{})
	if !ok || len(values) != 2 {
		return false, 0, nil
	}
	added, err := toInt64(values[0])
	if err != nil {
		return false, 0, err
	}
	count, err := toInt64(values[1])
	if err != nil {
		return false, 0, err
	}
	return added == 1, count, nil
}

// PipelineSetFields 使用有界 Pipeline 批量发送多个 Lua 原子写操作。
// 每批最多 ConvHotPipelineBatchSize 个会话，批次之间立即发送，不按时间等待。
func (c *ConvHotCache) PipelineSetFields(ctx context.Context, updates map[string]map[string]string) error {
	if len(updates) == 0 {
		return nil
	}
	script := convHotSetFieldsScript
	batch := make([]struct {
		conversationId string
		fields         map[string]string
	}, 0, ConvHotPipelineBatchSize)
	flush := func(items []struct {
		conversationId string
		fields         map[string]string
	}) error {
		return c.client.PipelinedCtx(ctx, func(pipe redis.Pipeliner) error {
			for _, item := range items {
				args := make([]interface{}, 0, len(item.fields)*2+1)
				args = append(args, c.jitter())
				for field, value := range item.fields {
					if field == ConvHotFieldHotLeaseAt {
						continue
					}
					args = append(args, field, value)
				}
				pipe.Eval(ctx, script, []string{c.key(item.conversationId)}, args...)
			}
			return nil
		})
	}
	for conversationId, fields := range updates {
		if len(fields) == 0 {
			continue
		}
		batch = append(batch, struct {
			conversationId string
			fields         map[string]string
		}{conversationId: conversationId, fields: fields})
		if len(batch) == ConvHotPipelineBatchSize {
			if err := flush(batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
	return flush(batch)
}

// GetLastMsgId 只读 lastMsgId 一个字段（HGET）
func (c *ConvHotCache) GetLastMsgId(conversationId string) (string, error) {
	return c.client.Hget(c.key(conversationId), ConvHotFieldLastMsgId)
}

// Del 删除整个会话的热字段 key
func (c *ConvHotCache) Del(conversationId string) error {
	_, err := c.client.Del(c.key(conversationId))
	return err
}
