package redis

import (
	"fmt"
	"sync"
	"time"

	"github.com/zeromicro/go-zero/core/stores/redis"
)

// =====================================================================
// ConvSession 高层聚合 API
// 把 conv:meta / conv:msg / conv:state 三层封装为一个面向业务的会话对象。
// =====================================================================

// ConvSession 聚合四层缓存 + 会话热点追踪
type ConvSession struct {
	client *redis.Redis

	meta     *ConvMetaCache
	hot      *ConvHotCache
	pool     *ConvMsgPool
	state    *ConvStateCache
	lastRead *LastMsgReadChecker
	heat     *ConversationHeatTracker

	// 上层依赖（如 MongoDB loader，由调用方注入）
	loader     MsgLoader
	readSeqStore ReadSeqStore
}

// ReadSeqStore 已读游标落库（进房加载 / 退房写回）
type ReadSeqStore interface {
	Load(userId, conversationId string) (int64, error)
	Save(userId, conversationId string, seq int64) error
}

// MsgLoader 从 MongoDB 加载消息的回调
// - conversationId: 会话ID
// - anchorSeq:    锚点 seq（含此 seq 之下的 n 条）
// - direction:    "up"=向上（取更老） / "down"=向下（取更新）
// - limit:        取多少条
type MsgLoader func(conversationId string, anchorSeq int64, direction string, limit int) ([]*MsgPoolItem, error)

// ConvSessionOption 构造选项
type ConvSessionOption func(*ConvSession)

// WithMsgLoader 注入 MongoDB loader
func WithMsgLoader(loader MsgLoader) ConvSessionOption {
	return func(s *ConvSession) {
		s.loader = loader
	}
}

// WithReadSeqStore 注入已读游标持久化（进房加载 / 退房写回）
func WithReadSeqStore(store ReadSeqStore) ConvSessionOption {
	return func(s *ConvSession) {
		s.readSeqStore = store
	}
}

// NewConvSession 创建聚合会话对象
func NewConvSession(client *redis.Redis, opts ...ConvSessionOption) *ConvSession {
	s := &ConvSession{
		client:   client,
		meta:     NewConvMetaCache(client, 0),
		hot:      NewConvHotCache(client, 0),
		pool:     NewConvMsgPool(client, 0),
		state:    NewConvStateCache(client, 0),
		lastRead: NewLastMsgReadChecker(client),
		heat:     NewConversationHeatTracker(client, ConversationHeatConfig{}),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Meta 暴露元信息层
func (s *ConvSession) Meta() *ConvMetaCache { return s.meta }

// Hot 暴露会话热字段层
func (s *ConvSession) Hot() *ConvHotCache { return s.hot }

// Pool 暴露消息池层
func (s *ConvSession) Pool() *ConvMsgPool { return s.pool }

// State 暴露用户状态层
func (s *ConvSession) State() *ConvStateCache { return s.state }

// LastRead 暴露最后一条消息已读集合
func (s *ConvSession) LastRead() *LastMsgReadChecker { return s.lastRead }

// Heat 暴露会话热点追踪（普通会话 → 热点会话）
func (s *ConvSession) Heat() *ConversationHeatTracker { return s.heat }

// TouchHeat 记录一次会话访问（进房/发消息/翻页）；达标升热点并负责续期
func (s *ConvSession) TouchHeat(conversationId string) {
	if s == nil || s.heat == nil {
		return
	}
	_ = s.heat.Touch(conversationId)
}

// IsHotConversation 是否为热点会话
func (s *ConvSession) IsHotConversation(conversationId string) bool {
	if s == nil || s.heat == nil {
		return false
	}
	return s.heat.IsHot(conversationId)
}

// =====================================================================
// 高层 API（面向业务）
// =====================================================================

// OnNewMessage 新消息落库后调用。
func (s *ConvSession) OnNewMessage(conversationId string, chatType int32, msg *MsgPoolItem, recvIds []string) error {
	return s.InvalidateOnNewMessage(conversationId, msg)
}

// InvalidateOnNewMessage 热点会话双写 hot/msg；冷会话删除缓存。发送者 readSeq 推到本条 seq。
func (s *ConvSession) InvalidateOnNewMessage(conversationId string, msg *MsgPoolItem) error {
	s.TouchHeat(conversationId)
	if msg != nil && s.IsHotConversation(conversationId) {
		if err := s.dualWriteHotWithRetry(conversationId, msg); err != nil {
			_ = s.hot.Del(conversationId)
			_ = s.pool.Del(conversationId)
			return err
		}
	} else {
		if err := s.hot.Del(conversationId); err != nil {
			return err
		}
		if err := s.pool.Del(conversationId); err != nil {
			return err
		}
	}
	if msg != nil && msg.SendId != "" && msg.Seq > 0 {
		_, _ = s.state.AdvanceReadSeq(conversationId, msg.SendId, msg.Seq)
	}
	return nil
}

func (s *ConvSession) dualWriteHotWithRetry(conversationId string, msg *MsgPoolItem) error {
	var lastErr error
	delay := 50 * time.Millisecond
	for attempt := 0; attempt < 4; attempt++ {
		if attempt > 0 {
			time.Sleep(delay)
			delay *= 2
		}
		_, _, err := s.hot.UpdateLastMessageAtomic(conversationId, msg, s.lastRead)
		if err != nil {
			lastErr = err
			continue
		}
		meta, err := s.pool.GetMeta(conversationId)
		if err != nil {
			lastErr = err
			continue
		}
		if meta != nil && meta.MaxSeq >= msg.Seq {
			return nil
		}
		// 空池不从单条双写冷启动，避免只有 1 条的残缺窗口
		if meta == nil || meta.Size == 0 {
			return nil
		}
		if err = s.pool.Append(conversationId, msg); err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	return lastErr
}

// LoadCursorOnEnter 进房：Mongo 游标与 Redis 取 max，不覆盖更大的 Redis 值。
func (s *ConvSession) LoadCursorOnEnter(conversationId, userId string) error {
	_, err := s.EnsureReadSeq(conversationId, userId)
	return err
}

// EnsureReadSeq 已读游标：Redis 命中直接返回；miss 才读 Mongo 并回填。
func (s *ConvSession) EnsureReadSeq(conversationId, userId string) (int64, error) {
	st, err := s.state.Get(conversationId, userId)
	if err != nil {
		return 0, err
	}
	if st != nil {
		return st.ReadSeq, nil
	}
	if s.readSeqStore == nil {
		return 0, nil
	}
	seq, err := s.readSeqStore.Load(userId, conversationId)
	if err != nil {
		return 0, err
	}
	_, err = s.state.AdvanceReadSeq(conversationId, userId, seq)
	return seq, err
}

// FlushCursor 退房/断线：把 Redis readSeq 写回 Mongo。
func (s *ConvSession) FlushCursor(conversationId, userId string) error {
	if s.readSeqStore == nil {
		return nil
	}
	st, err := s.state.Get(conversationId, userId)
	if err != nil || st == nil {
		return err
	}
	return s.readSeqStore.Save(userId, conversationId, st.ReadSeq)
}

// FlushCursors 批量写回已读游标
func (s *ConvSession) FlushCursors(userId string, conversationIds []string) {
	for _, cid := range conversationIds {
		if cid == "" {
			continue
		}
		_ = s.FlushCursor(cid, userId)
	}
}

// EnterConversation 用户进入会话（重置未读、更新水位线）
func (s *ConvSession) EnterConversation(conversationId, userId string) (visibleMsgs []*MsgPoolItem, err error) {
	s.TouchHeat(conversationId)

	// 1. 重置未读
	if err := s.state.ResetUnread(conversationId, userId); err != nil {
		return nil, err
	}

	// 2. 拿用户在池中的水位线
	state, err := s.state.Get(conversationId, userId)
	if err != nil {
		return nil, err
	}

	// 3. 从共享池裁剪可见消息
	cacheSeq := int64(0)
	if state != nil {
		cacheSeq = state.CacheSeq
	}
	items, _, err := s.pool.FetchByUserWaterLine(conversationId, cacheSeq, ConvMsgPoolMaxSize)
	if err != nil {
		return nil, err
	}

	// 4. 更新用户水位线（最低 seq-1）
	if len(items) > 0 {
		newWaterLine := items[0].Seq - 1
		_ = s.state.UpdateCacheSeq(conversationId, userId, newWaterLine)
	}
	return items, nil
}

// MarkRead 用户标记已读到 seq
func (s *ConvSession) MarkRead(conversationId, userId string, readSeq int64) error {
	return s.state.UpdateReadSeq(conversationId, userId, readSeq)
}

// ScrollUp 用户向上翻页
// - anchorSeq: 当前页最顶 seq（用户拉到这个 seq 想要看更早的）
// 返回：可见消息列表 + 是否需要继续走 MongoDB
func (s *ConvSession) ScrollUp(conversationId, userId string, anchorSeq int64, limit int) ([]*MsgPoolItem, bool, error) {
	s.TouchHeat(conversationId)

	if limit <= 0 {
		limit = ConvMsgPoolMaxSize
	}

	// 1. 先看共享池里能拿到多少
	poolItems, err := s.pool.RangeBelow(conversationId, anchorSeq, limit)
	if err != nil {
		return nil, false, err
	}

	// 2. 池里够直接返回
	if len(poolItems) >= limit {
		// 更新水位线
		if len(poolItems) > 0 {
			_ = s.state.UpdateCacheSeq(conversationId, userId, poolItems[0].Seq-1)
		}
		return poolItems, false, nil
	}

	// 3. 不够 → 池里有多少用多少 + 剩余走 MongoDB
	if s.loader == nil {
		// 没有 loader，只能返回池里的部分
		if len(poolItems) > 0 {
			_ = s.state.UpdateCacheSeq(conversationId, userId, poolItems[0].Seq-1)
		}
		return poolItems, false, nil
	}

	// 从池里最后一条的 seq 向下找（如果有）；否则从 anchorSeq 向下
	var loadAnchor int64 = anchorSeq
	if len(poolItems) > 0 {
		loadAnchor = poolItems[0].Seq
	}
	dbItems, err := s.loader(conversationId, loadAnchor, "up", limit-len(poolItems))
	if err != nil {
		// 降级：池里能拿多少拿多少
		if len(poolItems) > 0 {
			_ = s.state.UpdateCacheSeq(conversationId, userId, poolItems[0].Seq-1)
		}
		return poolItems, false, err
	}

	// 4. 把 MongoDB 拿到的写入共享池
	if len(dbItems) > 0 {
		_ = s.pool.BatchAppend(conversationId, dbItems)
	}

	// 5. 拼接
	out := append(dbItems, poolItems...)
	if len(out) > limit {
		out = out[:limit]
	}

	// 6. 更新水位线
	if len(out) > 0 {
		_ = s.state.UpdateCacheSeq(conversationId, userId, out[0].Seq-1)
	}

	return out, true, nil // true=本次走了 MongoDB
}

// GetUnread 未读 = max(0, 会话 seq - readSeq)
func (s *ConvSession) GetUnread(conversationId, userId string) (int64, error) {
	hot, err := s.hot.Get(conversationId)
	if err != nil {
		return 0, err
	}
	state, err := s.state.Get(conversationId, userId)
	if err != nil {
		return 0, err
	}
	var seq, readSeq int64
	if hot != nil {
		seq = hot.Seq
	}
	if state != nil {
		readSeq = state.ReadSeq
	}
	if seq <= readSeq {
		return 0, nil
	}
	return seq - readSeq, nil
}

// GetLastSeq 获取会话最新 seq（从 meta 读）
func (s *ConvSession) GetLastSeq(conversationId string) (int64, error) {
	hot, err := s.hot.Get(conversationId)
	if err != nil {
		return 0, err
	}
	if hot == nil {
		return 0, nil
	}
	return hot.Seq, nil
}

// EnsureConvMeta 确保元信息存在（创建会话/群时调用）
func (s *ConvSession) EnsureConvMeta(conversationId string, chatType int32, members []string) error {
	meta, err := s.meta.Get(conversationId)
	if err != nil {
		return err
	}
	if meta != nil {
		return nil
	}
	return s.meta.Set(conversationId, &ConvMeta{
		ChatType:    chatType,
		MemberCount: len(members),
		Members:     members,
	})
}

// RecentOnEnter 进房拉最近消息：池命中直接返回；空池则 Mongo 回填后再读。
func (s *ConvSession) RecentOnEnter(conversationId, userId string) (items []*MsgPoolItem, fromCache bool, err error) {
	items, err = s.EnterConversation(conversationId, userId)
	if err != nil {
		return nil, false, err
	}
	if len(items) > 0 {
		return items, true, nil
	}
	latest, _ := s.GetLastSeq(conversationId)
	loader := s.loader
	filled, err := s.BuildFromMongo(conversationId, userId, latest, ConvMsgPoolMaxSize, loader)
	if err != nil {
		return nil, false, err
	}
	return filled, false, nil
}

// BuildFromMongo 从 MongoDB 一次性填充（冷启动/池 miss）
func (s *ConvSession) BuildFromMongo(conversationId, userId string, latestSeq int64, topN int, loader MsgLoader) ([]*MsgPoolItem, error) {
	if topN <= 0 {
		topN = ConvMsgPoolMaxSize
	}
	items := make([]*MsgPoolItem, 0)
	if loader != nil {
		anchor := latestSeq + 1
		if anchor <= 1 {
			anchor = 1 << 62
		}
		dbItems, err := loader(conversationId, anchor, "up", topN)
		if err == nil && len(dbItems) > 0 {
			items = dbItems
			_ = s.pool.BatchAppend(conversationId, dbItems)
		}
	}
	// 设置用户水位线
	if len(items) > 0 {
		_ = s.state.UpdateCacheSeq(conversationId, userId, items[0].Seq-1)
	}
	return items, nil
}

// =====================================================================
// 简易统计（用于运维/调试）
// =====================================================================

// ConvCacheStats 三层缓存统计
type ConvCacheStats struct {
	MetaExists bool
	PoolSize   int64
	PoolMinSeq int64
	PoolMaxSeq int64
	StateSet   bool
}

// Stats 获取缓存状态
func (s *ConvSession) Stats(conversationId, userId string) (*ConvCacheStats, error) {
	stats := &ConvCacheStats{}

	meta, err := s.meta.Get(conversationId)
	if err != nil {
		return nil, err
	}
	stats.MetaExists = meta != nil

	poolMeta, err := s.pool.GetMeta(conversationId)
	if err != nil {
		return nil, err
	}
	stats.PoolSize = poolMeta.Size
	stats.PoolMinSeq = poolMeta.MinSeq
	stats.PoolMaxSeq = poolMeta.MaxSeq

	state, err := s.state.Get(conversationId, userId)
	if err != nil {
		return nil, err
	}
	stats.StateSet = state != nil

	return stats, nil
}

// =====================================================================
// 时间辅助
// =====================================================================

var _ = time.Now
var _ = fmt.Sprintf
var _ sync.Mutex
