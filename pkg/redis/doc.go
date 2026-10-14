// Package redis 提供 IM 系统的核心缓存抽象。
//
// 三层缓存模型（精简版）：
//
//	┌──────────────────────────────────────────────────────────────┐
//	│ 1. conv:meta:{bucket}                   [Hash]  会话静态元信息 │
//	│    - field = conversationId                                   │
//	│    - chatType / members / memberCount / targetId              │
//	│    - targetName / targetIcon / isShow                         │
//	└──────────────────────────────────────────────────────────────┘
//	┌──────────────────────────────────────────────────────────────┐
//	│ 2. conv:hot:{convId}                    [Hash]  会话热字段     │
//	│    - 每个会话独立一个 Hash key                                │
//	│    - field: seq / total / lastMsgId / lastMsgTime             │
//	│    - field: lastMsgPreview / lastMsgReadCount / hotLeaseAt     │
//	│    - hotLeaseAt: 续约时间戳=热点，空=非热；重建不得覆盖        │
//	│    - 业务 HSET 不续 TTL；仅晋升/探测成功时 EXPIRE 15m         │
//	└──────────────────────────────────────────────────────────────┘
//	┌──────────────────────────────────────────────────────────────┐
//	│ 3. conv:msg:{convId}                    [List]  共享消息池     │
//	│    - 按插入顺序保存最近 N=50 条                              │
//	│    - RPUSH + LTRIM 保留尾部消息                               │
//	│    - 超出范围由调用方走 MongoDB                               │
//	└──────────────────────────────────────────────────────────────┘
//	┌──────────────────────────────────────────────────────────────┐
//	│ 4. conv:state:{convId}:{userId}         [Hash]  用户状态       │
//	│    - readSeq          已读游标（高频更新）                   │
//	│    - unread           未读数                                 │
//	│    - isShow           是否显示                               │
//	│    - cacheSeq         用户在共享池能看到的最低 seq            │
//	│    - cacheTimestamp   缓存命中/重建时间                      │
//	└──────────────────────────────────────────────────────────────┘
//	┌──────────────────────────────────────────────────────────────┐
//	│ 5. active:conv:{userId}                 [Set]   用户活跃会话   │
//	│    - member = conversationId（可同时打开多个）               │
//	│    - 进房 SADD、退房 SREM、判断 SISMEMBER                   │
//	│    - 整 key TTL（默认 10m），Add/Remove 时续期               │
//	└──────────────────────────────────────────────────────────────┘
//
//	┌──────────────────────────────────────────────────────────────┐
//	│ 6. 会话热点 ConversationHeatTracker                           │
//	│    - 频率：hot:freq:conv:{convId}:{window}（1 分钟窗）        │
//	│    - ≥100 且 conv:hot 存在 → Hash.hotLeaseAt=now，TTL 15m    │
//	│    - 0~10m 不管；10~14m 探测；15m 物理过期后旁路重建          │
//	│    - 触发：进房 / 发消息 / 翻页；热会话新消息双写 hot+msg     │
//	└──────────────────────────────────────────────────────────────┘
//
// 设计要点：
//   - 静态元信息与热字段分离，避免整块 JSON 重写
//   - 用户状态独立（高频更新互不干扰）
//   - 消息池用 List 严格控制最近 N 条
//   - 每用户水位线不同，所以同一个池子"裁"出不同长度的列表
//   - 活跃会话用 Set 支持多窗口，避免「在线即已读」误判
//   - 热点会话按访问/消息频率升级，续期对象是 conv:hot / conv:msg
package redis

import "time"

const (
	// ConvMetaTTL 会话元信息 TTL（含抖动）
	ConvMetaTTL = time.Hour
	// ConvMsgPoolTTL 共享消息池 TTL
	ConvMsgPoolTTL = 30 * time.Minute
	// ConvStateTTL 用户会话状态默认 TTL（cacheSeq 等）
	ConvStateTTL = 2 * time.Hour
	// ConvReadSeqTTL 已读游标 TTL（仅 Redis 会话态，退房才落 Mongo）
	ConvReadSeqTTL = 30 * 24 * time.Hour
	// ConvMsgPoolMaxSize 共享消息池最大消息条数（超出走 MongoDB）
	ConvMsgPoolMaxSize = 50
)

// hashBucket 使用 FNV-1a 将 conversationId 映射到 [0, buckets) 分桶
func hashBucket(conversationId string, buckets int) int {
	if buckets <= 0 {
		buckets = 1
	}
	h := uint32(2166136261)
	for i := 0; i < len(conversationId); i++ {
		h ^= uint32(conversationId[i])
		h *= 16777619
	}
	return int(h % uint32(buckets))
}