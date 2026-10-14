package redis

import (
	"fmt"
	"time"

	"github.com/zeromicro/go-zero/core/stores/redis"
)

// ConversationHeatConfig 会话级热点（普通会话 → 热点会话）配置
//
// 时钟对齐一次续约：T=0 写入 conv:hot.hotLeaseAt 并 EXPIRE 15m
//
//	0~10m   逻辑未到期，不改租约、不续物理 TTL
//	10~14m  逻辑到期，探测：仍热则覆盖时间戳并 EXPIRE 15m；不热则 hotLeaseAt 置空
//	14~15m  探测与 TTL 可能并发，后写覆盖
//	15m+    Hash 物理过期，下次 miss 重建（重建不得覆盖 hotLeaseAt）
type ConversationHeatConfig struct {
	FrequencyThreshold  int64         // 窗口内访问次数阈值，默认 100
	WindowSize          int64         // 窗口秒数，默认 60
	LogicExpireDuration time.Duration // 逻辑过期，默认 10m（相对 hotLeaseAt）
	DetectUntilDuration time.Duration // 探测窗口结束，默认 14m
	HotTTL              time.Duration // 物理 TTL，默认 15m（仅写/刷新租约时设置）
}

func (c *ConversationHeatConfig) withDefaults() ConversationHeatConfig {
	out := *c
	if out.FrequencyThreshold == 0 {
		out.FrequencyThreshold = 100
	}
	if out.WindowSize == 0 {
		out.WindowSize = 60
	}
	if out.LogicExpireDuration == 0 {
		out.LogicExpireDuration = 10 * time.Minute
	}
	if out.DetectUntilDuration == 0 {
		out.DetectUntilDuration = 14 * time.Minute
	}
	if out.HotTTL == 0 {
		out.HotTTL = 15 * time.Minute
	}
	return out
}

// ConversationHeatTracker 按 conversationId 统计访问频率。
// 热点身份存在 conv:hot Hash 的 hotLeaseAt：非空时间戳=热，空=非热。
type ConversationHeatTracker struct {
	client *redis.Redis
	config ConversationHeatConfig
}

func NewConversationHeatTracker(client *redis.Redis, cfg ConversationHeatConfig) *ConversationHeatTracker {
	return &ConversationHeatTracker{
		client: client,
		config: cfg.withDefaults(),
	}
}

func (t *ConversationHeatTracker) freqKey(conversationId string, window int64) string {
	return fmt.Sprintf("hot:freq:conv:%s:%d", conversationId, window)
}

func (t *ConversationHeatTracker) hotKey(conversationId string) string {
	return fmt.Sprintf("conv:hot:%s", conversationId)
}

func (t *ConversationHeatTracker) msgKey(conversationId string) string {
	return fmt.Sprintf("conv:msg:%s", conversationId)
}

// IsHot 以 Redis hotLeaseAt 为准：有时间戳即为热点（含 10~14m 探测期）。
func (t *ConversationHeatTracker) IsHot(conversationId string) bool {
	if conversationId == "" || t == nil || t.client == nil {
		return false
	}
	v, err := t.client.Hget(t.hotKey(conversationId), ConvHotFieldHotLeaseAt)
	if err != nil || v == "" {
		return false
	}
	return true
}

// Touch 会话被访问时调用（进房 / 发消息 / 翻页等）
func (t *ConversationHeatTracker) Touch(conversationId string) error {
	if conversationId == "" || t == nil {
		return nil
	}
	count, err := t.incrFreq(conversationId)
	if err != nil {
		return err
	}
	if err := t.tryPromote(conversationId, count); err != nil {
		return err
	}
	return t.detectAndRenew(conversationId, count)
}

func (t *ConversationHeatTracker) incrFreq(conversationId string) (int64, error) {
	now := time.Now().Unix()
	window := now / t.config.WindowSize
	ttl := int(t.config.WindowSize * 2)
	if ttl <= 0 {
		ttl = 120
	}
	script := `
		local n = redis.call('INCR', KEYS[1])
		if n == 1 then
			redis.call('EXPIRE', KEYS[1], ARGV[1])
		end
		return n
	`
	res, err := t.client.Eval(script, []string{t.freqKey(conversationId, window)}, ttl)
	if err != nil {
		return 0, err
	}
	return toInt64(res)
}

func (t *ConversationHeatTracker) tryPromote(conversationId string, count int64) error {
	if count < t.config.FrequencyThreshold {
		return nil
	}
	now := time.Now().Unix()
	logicSec := int64(t.config.LogicExpireDuration.Seconds())
	ttlSec := int(t.config.HotTTL.Seconds())
	script := `
		local hotKey = KEYS[1]
		local msgKey = KEYS[2]
		local now = tonumber(ARGV[1])
		local logicSec = tonumber(ARGV[2])
		local ttl = tonumber(ARGV[3])
		if redis.call('EXISTS', hotKey) == 0 then
			return 0
		end
		local lease = redis.call('HGET', hotKey, 'hotLeaseAt')
		if lease and lease ~= '' then
			local age = now - tonumber(lease)
			if age < logicSec then
				return 1
			end
			return 2
		end
		redis.call('HSET', hotKey, 'hotLeaseAt', ARGV[1])
		redis.call('EXPIRE', hotKey, ttl)
		if redis.call('EXISTS', msgKey) == 1 then
			redis.call('EXPIRE', msgKey, ttl)
		end
		return 3
	`
	_, err := t.client.Eval(script, []string{t.hotKey(conversationId), t.msgKey(conversationId)}, now, logicSec, ttlSec)
	return err
}

func (t *ConversationHeatTracker) detectAndRenew(conversationId string, count int64) error {
	now := time.Now().Unix()
	logicSec := int64(t.config.LogicExpireDuration.Seconds())
	ttlSec := int(t.config.HotTTL.Seconds())
	script := `
		local hotKey = KEYS[1]
		local msgKey = KEYS[2]
		local now = tonumber(ARGV[1])
		local logicSec = tonumber(ARGV[2])
		local ttl = tonumber(ARGV[3])
		local count = tonumber(ARGV[4])
		local threshold = tonumber(ARGV[5])
		if redis.call('EXISTS', hotKey) == 0 then
			return 0
		end
		local lease = redis.call('HGET', hotKey, 'hotLeaseAt')
		if not lease or lease == '' then
			return 0
		end
		local age = now - tonumber(lease)
		if age < logicSec then
			return 1
		end
		if count < threshold then
			redis.call('HSET', hotKey, 'hotLeaseAt', '')
			return 2
		end
		redis.call('HSET', hotKey, 'hotLeaseAt', ARGV[1])
		redis.call('EXPIRE', hotKey, ttl)
		if redis.call('EXISTS', msgKey) == 1 then
			redis.call('EXPIRE', msgKey, ttl)
		end
		return 3
	`
	_, err := t.client.Eval(
		script,
		[]string{t.hotKey(conversationId), t.msgKey(conversationId)},
		now, logicSec, ttlSec, count, t.config.FrequencyThreshold,
	)
	return err
}

// Snapshot 调试：当前会话是否带租约（读 Redis）。
func (t *ConversationHeatTracker) Snapshot() map[string]HotKeyMeta {
	return map[string]HotKeyMeta{}
}
