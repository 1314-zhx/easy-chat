package redis

import (
	"strconv"

	"github.com/zeromicro/go-zero/core/stores/redis"
)

// MsgReadBitmap 消息已读状态检查器（使用 Redis Bitmap 精确存储）
type MsgReadBitmap struct {
	client *redis.Redis
}

// NewMsgReadBitmap 创建已读 Bitmap 检查器
func NewMsgReadBitmap(client *redis.Redis) *MsgReadBitmap {
	return &MsgReadBitmap{client: client}
}

// key 生成已读 Bitmap 的 key
func (c *MsgReadBitmap) key(msgId string) string {
	return "msg:read:" + msgId
}

// userToOffset 将 userId 转换为 bit offset
// userId 格式如 "0000001000000002"，直接转换为数字作为 offset
func (c *MsgReadBitmap) userToOffset(userId string) (int64, error) {
	return strconv.ParseInt(userId, 10, 64)
}

// MarkRead 标记用户已读
func (c *MsgReadBitmap) MarkRead(msgId, userId string) error {
	offset, err := c.userToOffset(userId)
	if err != nil {
		return err
	}
	_, err = c.client.SetBit(c.key(msgId), offset, 1)
	return err
}

// IsRead 检查用户是否已读（精确）
func (c *MsgReadBitmap) IsRead(msgId, userId string) (bool, error) {
	offset, err := c.userToOffset(userId)
	if err != nil {
		return false, err
	}
	val, err := c.client.GetBit(c.key(msgId), offset)
	if err != nil {
		return false, err
	}
	return val == 1, nil
}

// BatchMarkRead 批量标记已读
func (c *MsgReadBitmap) BatchMarkRead(msgId string, userIds []string) error {
	for _, userId := range userIds {
		if err := c.MarkRead(msgId, userId); err != nil {
			return err
		}
	}
	return nil
}

// BatchIsRead 批量检查用户是否已读
func (c *MsgReadBitmap) BatchIsRead(msgId string, userIds []string) (map[string]bool, error) {
	result := make(map[string]bool, len(userIds))
	for _, userId := range userIds {
		isRead, err := c.IsRead(msgId, userId)
		if err != nil {
			return nil, err
		}
		result[userId] = isRead
	}
	return result, nil
}

// GetBitCount 获取 Bitmap 中被设置为 1 的位数（已读用户数）
func (c *MsgReadBitmap) GetBitCount(msgId string) (int64, error) {
	return c.client.BitCount(c.key(msgId), 0, -1)
}

// GetReadUsersCount 获取已读用户数量（用于统计）
func (c *MsgReadBitmap) GetReadUsersCount(msgId string) (int64, error) {
	return c.GetBitCount(msgId)
}

// GetReadCount 获取已读用户数
func (c *MsgReadBitmap) GetReadCount(msgId string) (int64, error) {
	return c.GetBitCount(msgId)
}

// Del 删除已读记录（消息过期时清理）
func (c *MsgReadBitmap) Del(msgId string) error {
	_, err := c.client.Del(c.key(msgId))
	return err
}

// Expire 设置过期时间
func (c *MsgReadBitmap) Expire(msgId string, seconds int) error {
	return c.client.Expire(c.key(msgId), seconds)
}

// UnreadCount 计算未读人数（需传入群总人数）
func (c *MsgReadBitmap) UnreadCount(msgId string, totalMembers int64) (int64, error) {
	readCount, err := c.GetReadCount(msgId)
	if err != nil {
		return 0, err
	}
	return totalMembers - readCount, nil
}
