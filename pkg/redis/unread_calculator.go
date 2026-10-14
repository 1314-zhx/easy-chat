package redis

import (
	"github.com/zeromicro/go-zero/core/stores/redis"
)

// UnreadCalculator 未读数计算器
// 结合游标和最后一条消息已读状态，计算用户未读数
type UnreadCalculator struct {
	readCursor *ReadCursorCache      // 用户已读游标
	lastRead   *LastMsgReadChecker   // 最后一条消息已读检查器
}

// NewUnreadCalculator 创建未读数计算器
func NewUnreadCalculator(redisClient *redis.Redis) *UnreadCalculator {
	return &UnreadCalculator{
		readCursor: NewReadCursorCache(redisClient, 0),
		lastRead:   NewLastMsgReadChecker(redisClient),
	}
}

// CalcUnreadCount 计算用户未读消息数
//
// 参数：
//   - conversationId: 会话 ID
//   - userId: 用户 ID
//   - lastSeq: 会话最新消息序列号
//
// 逻辑：
//   1. 获取用户已读游标（用户上次看到哪条消息）
//   2. 检查用户是否已读最后一条消息
//   3. 如果已读最后一条 → 未读数 = 0
//   4. 如果未读最后一条 → 未读数 = lastSeq - 已读游标
func (u *UnreadCalculator) CalcUnreadCount(conversationId, userId string, lastSeq int64) (int64, error) {
	// 1. 获取用户已读游标
	readSeq, exists, err := u.readCursor.Get(userId, conversationId)
	if err != nil {
		return 0, err
	}
	
	// 2. 检查是否已读最后一条消息
	hasRead, err := u.lastRead.HasReadLastMsg(conversationId, userId)
	if err != nil {
		return 0, err
	}
	
	// 3. 判断未读数
	if hasRead {
		// 已读最后一条消息，未读数为 0
		return 0, nil
	}
	
	// 4. 未读最后一条消息
	if !exists {
		// 没有已读游标，说明从未读过，未读数 = 全部消息
		return lastSeq, nil
	}
	
	// 有游标但没读最后一条，未读数 = 最后seq - 已读游标
	unread := lastSeq - readSeq
	if unread < 0 {
		unread = 0
	}
	return unread, nil
}

// MarkRead 标记用户已读
// 1. 更新已读游标
// 2. 标记已读最后一条消息
func (u *UnreadCalculator) MarkRead(conversationId, userId string, readSeq int64) error {
	// 1. 更新已读游标
	if err := u.readCursor.Set(userId, conversationId, readSeq); err != nil {
		return err
	}
	
	// 2. 标记已读最后一条消息
	_, err := u.lastRead.MarkRead(conversationId, userId)
	return err
}

// OnNewMessage 当群有新消息时调用
// 删除旧的"最后一条已读"记录，强制用户重新已读新消息
func (u *UnreadCalculator) OnNewMessage(conversationId string) error {
	return u.lastRead.UpdateLastMsg(conversationId)
}

// IsLastRead 检查用户是否已读最后一条消息
func (u *UnreadCalculator) IsLastRead(conversationId, userId string) (bool, error) {
	return u.lastRead.HasReadLastMsg(conversationId, userId)
}

// GetLastReadCount 获取已读最后一条消息的用户数
func (u *UnreadCalculator) GetLastReadCount(conversationId string) (int64, error) {
	return u.lastRead.GetReadCount(conversationId)
}
