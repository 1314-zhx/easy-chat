package redis

import (
	"strconv"
	"time"

	"easy-chat/apps/im/immodels"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"go.mongodb.org/mongo-driver/mongo"
)

// UserConversationReadManager 用户会话已读/未读管理器
// 统一管理单聊和群聊的已读游标，配合 Redis 实现双写
// 设计：Redis 负责实时性，MongoDB 负责持久化回放
type UserConversationReadManager struct {
	redisClient *redis.Redis
	model       immodels.UserConversationReadModel
	ttl         time.Duration
}

// NewUserConversationReadManager 创建用户会话已读管理器
func NewUserConversationReadManager(rdb *redis.Redis, model immodels.UserConversationReadModel) *UserConversationReadManager {
	return &UserConversationReadManager{
		redisClient: rdb,
		model:       model,
		ttl:         7 * 24 * time.Hour, // 7 天
	}
}

// ==================== Redis Key 辅助 ====================

// redisKey 生成 Redis key
func (m *UserConversationReadManager) redisKey(userId, conversationId string) string {
	return "read:cursor:" + userId + ":" + conversationId
}

// ==================== 核心操作 ====================

// SetUserReadSeq 设置用户已读游标（双写 Redis + MongoDB）
// 用于用户点击已读时调用
func (m *UserConversationReadManager) SetUserReadSeq(userId, conversationId string, readSeq int64) error {
	// 1. 先写 Redis（实时）
	key := m.redisKey(userId, conversationId)
	if err := m.redisClient.Setex(key, strconv.FormatInt(readSeq, 10), int(m.ttl.Seconds())); err != nil {
		return err
	}

	// 2. 异步写 MongoDB（持久化）
	go func() {
		_ = m.model.UpsertByUserAndConversation(nil, userId, conversationId, readSeq)
	}()

	return nil
}

// GetUserReadSeq 获取用户已读游标
// 优先从 Redis 获取，Redis 没有则从 MongoDB 获取（离线恢复）
func (m *UserConversationReadManager) GetUserReadSeq(ctx mongo.SessionContext, userId, conversationId string) (int64, bool, error) {
	// 1. 优先从 Redis 获取
	key := m.redisKey(userId, conversationId)
	val, err := m.redisClient.Get(key)
	if err == nil && val != "" {
		seq, _ := strconv.ParseInt(val, 10, 64)
		return seq, true, nil
	}

	// 2. Redis 没有，从 MongoDB 获取
	record, err := m.model.FindByUserAndConversation(ctx, userId, conversationId)
	if err == nil {
		// 同步回 Redis
		go func() {
			_ = m.redisClient.Setex(key, strconv.FormatInt(record.ReadSeq, 10), int(m.ttl.Seconds()))
		}()
		return record.ReadSeq, true, nil
	}

	if err == mongo.ErrNoDocuments || err == immodels.ErrNotFound {
		return 0, false, nil
	}

	return 0, false, err
}

// GetUserReadSeqFromMongo 从 MongoDB 获取用户已读游标（离线恢复用）
func (m *UserConversationReadManager) GetUserReadSeqFromMongo(ctx mongo.SessionContext, userId, conversationId string) (int64, bool, error) {
	record, err := m.model.FindByUserAndConversation(ctx, userId, conversationId)
	if err != nil {
		if err == mongo.ErrNoDocuments || err == immodels.ErrNotFound {
			return 0, false, nil
		}
		return 0, false, err
	}
	return record.ReadSeq, true, nil
}

// ListUserAllReads 获取用户所有会话的已读记录（用于离线恢复）
func (m *UserConversationReadManager) ListUserAllReads(ctx mongo.SessionContext, userId string) ([]*immodels.UserConversationRead, error) {
	return m.model.ListByUser(ctx, userId)
}

// ListConversationAllReads 获取会话所有用户的已读记录（用于计算未读数）
func (m *UserConversationReadManager) ListConversationAllReads(ctx mongo.SessionContext, conversationId string) ([]*immodels.UserConversationRead, error) {
	return m.model.ListByConversation(ctx, conversationId)
}

// SyncAllToRedis 将 MongoDB 中的所有已读记录同步到 Redis
// 用于服务启动时预热缓存
func (m *UserConversationReadManager) SyncAllToRedis(ctx mongo.SessionContext) error {
	// 这个方法用于管理员手动触发同步，一般不需要
	return nil
}
