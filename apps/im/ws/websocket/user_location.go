package websocket

import (
	"fmt"
	"time"

	"github.com/zeromicro/go-zero/core/stores/redis"
)

// UserLocationManager 用户位置管理器
// 用于分布式 WebSocket：记录用户当前连接的实例地址
type UserLocationManager struct {
	client *redis.Redis
	ttl    time.Duration // 用户位置过期时间
}

// NewUserLocationManager 创建用户位置管理器
func NewUserLocationManager(client *redis.Redis, ttl time.Duration) *UserLocationManager {
	if ttl <= 0 {
		ttl = 24 * time.Hour // 默认 24 小时
	}
	return &UserLocationManager{
		client: client,
		ttl:    ttl,
	}
}

// key 生成 Redis key
func (m *UserLocationManager) key(userId string) string {
	return fmt.Sprintf("ws:user:instance:%s", userId)
}

// SetUserInstance 设置用户所在实例
// 用户上线时调用，记录用户当前连接的 WS 实例地址
func (m *UserLocationManager) SetUserInstance(userId, instanceAddr string) error {
	key := m.key(userId)
	return m.client.Setex(key, instanceAddr, int(m.ttl.Seconds()))
}

// GetUserInstance 获取用户所在实例
// 返回 instanceAddr，如果用户不在线返回空字符串
func (m *UserLocationManager) GetUserInstance(userId string) (string, error) {
	key := m.key(userId)
	val, err := m.client.Get(key)
	if err != nil {
		return "", err
	}
	return val, nil
}

// DelUserInstance 删除用户位置
// 用户下线时调用
func (m *UserLocationManager) DelUserInstance(userId string) error {
	key := m.key(userId)
	_, err := m.client.Del(key)
	return err
}

// GetUsersInstance 批量获取用户所在实例
// 使用 MGET 一次 RTT 获取所有用户位置，避免 N 次串行 Get
func (m *UserLocationManager) GetUsersInstance(userIds []string) (map[string]string, error) {
	if len(userIds) == 0 {
		return make(map[string]string), nil
	}

	keys := make([]string, len(userIds))
	for i, uid := range userIds {
		keys[i] = m.key(uid)
	}

	vals, err := m.client.Mget(keys...)
	if err != nil {
		return nil, err
	}

	result := make(map[string]string, len(userIds))
	for i, val := range vals {
		if val != "" {
			result[userIds[i]] = val
		}
	}
	return result, nil
}

// KeepAlive 续期用户位置
// 定时调用，防止用户在线但位置过期
func (m *UserLocationManager) KeepAlive(userId, instanceAddr string) error {
	return m.SetUserInstance(userId, instanceAddr)
}
