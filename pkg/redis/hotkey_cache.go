package redis

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"golang.org/x/sync/singleflight"
)

// HotKeyConfig 热点 key 配置
type HotKeyConfig struct {
	// 热点 key 前缀（业务标识，配置级热点永不过期）
	HotPrefixes []string
	// 运行时热点的 Redis TTL（物理租约）
	HotKeyTTL time.Duration
	// 普通 key 默认 TTL
	DefaultTTL time.Duration
	// 访问频率阈值（WindowSize 内超过这个数标记为热点）
	FrequencyThreshold int64
	// 统计窗口大小（秒）；逻辑到期后的热点复检也使用该窗口
	WindowSize int64
	// 热点身份逻辑过期时间（本地租约）
	LogicExpireDuration time.Duration
}

// DefaultHotKeyConfig 默认配置
// 普通 5m；热点 Redis TTL 15m + 本地逻辑 10m；到期进入 Detecting，用 Redis 计数复检后续约
var DefaultHotKeyConfig = HotKeyConfig{
	HotPrefixes: []string{
		"announcement:",
		"global:config:",
	},
	HotKeyTTL:           15 * time.Minute,
	DefaultTTL:          5 * time.Minute,
	FrequencyThreshold:  100,
	WindowSize:          60,
	LogicExpireDuration: 10 * time.Minute,
}

// hotLease 本地逻辑过期表条目（进程内 map，不写入业务 value）
// hotKeys: map[string]*hotLease  // key = 业务缓存 key
type hotLease struct {
	LogicExpire int64 // 逻辑过期时间戳（秒）；0 = 配置级永不过期
	RedisExpire int64 // 本地估计的 Redis TTL 截止；0 = 未知
	Detecting   bool  // 正在热点复检；为 true 时定时任务不得删除
	DetectStart int64 // 进入 Detecting 的时间戳（秒）
}

// HotKeyCache 热点 key 缓存（防止缓存击穿）
type HotKeyCache struct {
	client *redis.Redis
	config HotKeyConfig
	// 本地逻辑过期 map：业务 key → 热点租约（含 Detecting）
	hotKeys map[string]*hotLease
	hotMu   sync.RWMutex
	// miss 时同 key 并发重建合并为一次（防击穿）
	sf singleflight.Group
}

// HotKeyMeta 热点 key 元信息（文档/对外说明用）
type HotKeyMeta struct {
	ExpireTime  int64 `json:"expire_time"` // 逻辑过期时间戳（秒）
	Detecting   bool  `json:"detecting"`   // 是否正在热点检测
	DetectStart int64 `json:"detect_start,omitempty"`
}

// NewHotKeyCache 创建热点 key 缓存
func NewHotKeyCache(client *redis.Redis, config HotKeyConfig) *HotKeyCache {
	if config.DefaultTTL == 0 {
		config.DefaultTTL = DefaultHotKeyConfig.DefaultTTL
	}
	if config.FrequencyThreshold == 0 {
		config.FrequencyThreshold = DefaultHotKeyConfig.FrequencyThreshold
	}
	if config.WindowSize == 0 {
		config.WindowSize = DefaultHotKeyConfig.WindowSize
	}
	if config.LogicExpireDuration == 0 {
		config.LogicExpireDuration = DefaultHotKeyConfig.LogicExpireDuration
	}
	if config.HotKeyTTL == 0 {
		config.HotKeyTTL = DefaultHotKeyConfig.HotKeyTTL
	}

	h := &HotKeyCache{
		client:  client,
		config:  config,
		hotKeys: make(map[string]*hotLease),
	}

	for _, prefix := range config.HotPrefixes {
		h.hotKeys[prefix] = &hotLease{} // LogicExpire=0 永不过期
	}

	go h.cleanupHotKeys()
	return h
}

// IsHotKey 配置级热点，或运行时热点（租约未过期 / 正在 Detecting）
func (h *HotKeyCache) IsHotKey(key string) bool {
	now := time.Now().Unix()

	h.hotMu.RLock()
	lease, ok := h.hotKeys[key]
	h.hotMu.RUnlock()

	if ok && lease != nil {
		if lease.LogicExpire == 0 {
			return true
		}
		if lease.Detecting {
			return true
		}
		if lease.LogicExpire > now {
			return true
		}
	}
	return h.IsConfigHotKey(key)
}

// MarkAsHot 标记为热点（只写本地逻辑租约）
func (h *HotKeyCache) MarkAsHot(key string) {
	if h.IsConfigHotKey(key) {
		return
	}
	now := time.Now().Unix()
	h.hotMu.Lock()
	h.hotKeys[key] = &hotLease{
		LogicExpire: now + int64(h.config.LogicExpireDuration.Seconds()),
	}
	h.hotMu.Unlock()
}

// IsConfigHotKey 判断是否为配置级热点（永不过期）
func (h *HotKeyCache) IsConfigHotKey(key string) bool {
	for _, prefix := range h.config.HotPrefixes {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			return true
		}
	}
	return false
}

// Get 获取缓存值：Redis 计数 → 升热点 → 逻辑到期则 Detecting 复检续约 → 读业务 key
func (h *HotKeyCache) Get(key string) (string, error) {
	count, _ := h.incrFreq(key)
	h.tryPromoteHot(key, count)
	_ = h.detectAndRenewIfNeeded(key, count)
	return h.client.Get(key)
}

// GetOrLoad 读缓存；miss 时 singleflight 回源
func (h *HotKeyCache) GetOrLoad(key string, loader func() (string, error)) (string, error) {
	count, _ := h.incrFreq(key)
	h.tryPromoteHot(key, count)
	_ = h.detectAndRenewIfNeeded(key, count)

	val, err := h.client.Get(key)
	if err != nil && err != redis.Nil {
		return "", err
	}
	if val != "" {
		return val, nil
	}

	v, err, _ := h.sf.Do(key, func() (interface{}, error) {
		if val, err := h.client.Get(key); err == nil && val != "" {
			return val, nil
		} else if err != nil && err != redis.Nil {
			return nil, err
		}
		raw, err := loader()
		if err != nil {
			return nil, err
		}
		if raw == "" {
			return "", nil
		}
		if err := h.Set(key, raw); err != nil {
			return raw, nil
		}
		return raw, nil
	})
	if err != nil {
		return "", err
	}
	s, _ := v.(string)
	return s, nil
}

// Set 设置缓存值。
// 流程：配置级永不过期；其余一律先按普通 TTL（约 5m）写入，再做热点检测，
// 若已是热点或当前窗口频次达标，则升级为热点 TTL（约 15m）并写入本地逻辑租约；否则保持 5m。
func (h *HotKeyCache) Set(key string, value interface{}) error {
	if h.IsConfigHotKey(key) {
		return h.client.Set(key, value.(string))
	}

	// 1. 首次/重新设置：先按普通缓存 TTL 写入
	ttl := jitterTTL(h.config.DefaultTTL)
	if err := h.client.Setex(key, value.(string), ttl); err != nil {
		return err
	}
	now := time.Now().Unix()
	h.bindRedisExpire(key, now+int64(ttl))

	// 2. 热点检测：符合条件则升级，否则保持 5m 不变
	h.detectAndUpgradeAfterSet(key)
	return nil
}

// detectAndUpgradeAfterSet 写后热点检测：已是热点或窗口计数 ≥ 阈值 → 升热点 TTL
func (h *HotKeyCache) detectAndUpgradeAfterSet(key string) {
	count, _ := h.currentFreq(key)
	meets := count >= h.config.FrequencyThreshold || h.IsHotKey(key)
	if !meets {
		return
	}
	// 确保本地热点租约存在
	h.tryPromoteHot(key, h.config.FrequencyThreshold)
	// 将刚写入的普通 TTL 升级为热点 TTL
	ttlSec := int(h.config.HotKeyTTL.Seconds())
	if ttlSec <= 0 {
		return
	}
	if ok, err := h.client.Exists(key); err != nil || !ok {
		return
	}
	_ = h.client.Expire(key, ttlSec)
	h.bindRedisExpire(key, time.Now().Unix()+int64(ttlSec))
}

// currentFreq 读取当前窗口计数（不 INCR）
func (h *HotKeyCache) currentFreq(key string) (int64, error) {
	if h.IsConfigHotKey(key) {
		return 0, nil
	}
	now := time.Now().Unix()
	window := now / h.config.WindowSize
	freqKey := fmt.Sprintf("hot:freq:%s:%d", key, window)
	val, err := h.client.Get(freqKey)
	if err != nil || val == "" {
		return 0, err
	}
	n, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, err
	}
	return n, nil
}

// SetWithTTL 设置缓存值（强制 TTL）
func (h *HotKeyCache) SetWithTTL(key string, value interface{}, ttl time.Duration) error {
	sec := int(ttl.Seconds())
	if err := h.client.Setex(key, value.(string), sec); err != nil {
		return err
	}
	if h.IsHotKey(key) && !h.IsConfigHotKey(key) && sec > 0 {
		h.bindRedisExpire(key, time.Now().Unix()+int64(sec))
	}
	return nil
}

// Del 删除缓存
func (h *HotKeyCache) Del(key string) error {
	h.hotMu.Lock()
	if lease, ok := h.hotKeys[key]; ok && lease != nil && lease.LogicExpire != 0 {
		delete(h.hotKeys, key)
	}
	h.hotMu.Unlock()
	_, err := h.client.Del(key)
	return err
}

// incrFreq 在 Redis 记录 Get 次数：hot:freq:{key}:{window} → INCR，窗口 = WindowSize 秒
func (h *HotKeyCache) incrFreq(key string) (int64, error) {
	if h.IsConfigHotKey(key) {
		return 0, nil
	}
	now := time.Now().Unix()
	window := now / h.config.WindowSize
	freqKey := fmt.Sprintf("hot:freq:%s:%d", key, window)
	ttl := int(h.config.WindowSize * 2)
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
	res, err := h.client.Eval(script, []string{freqKey}, ttl)
	if err != nil {
		return 0, err
	}
	return toInt64(res)
}

// tryPromoteHot 窗口计数达标则晋升运行时热点
func (h *HotKeyCache) tryPromoteHot(key string, count int64) {
	if count < h.config.FrequencyThreshold || h.IsConfigHotKey(key) {
		return
	}
	now := time.Now().Unix()
	logicDur := int64(h.config.LogicExpireDuration.Seconds())

	h.hotMu.Lock()
	lease, ok := h.hotKeys[key]
	if ok && lease != nil {
		if lease.LogicExpire == 0 {
			h.hotMu.Unlock()
			return
		}
		// 租约期内或正在检测：不在这里刷新（续约走 detectAndRenew）
		if lease.Detecting || lease.LogicExpire > now {
			h.hotMu.Unlock()
			return
		}
	}
	h.hotKeys[key] = &hotLease{
		LogicExpire: now + logicDur,
	}
	h.hotMu.Unlock()

	// 晋升时若业务 key 仍在，立刻提到热点 TTL
	if ok, err := h.client.Exists(key); err == nil && ok {
		ttlSec := int(h.config.HotKeyTTL.Seconds())
		if ttlSec > 0 {
			_ = h.client.Expire(key, ttlSec)
			h.bindRedisExpire(key, now+int64(ttlSec))
		}
	}
}

// detectAndRenewIfNeeded 逻辑过期到期/临近时进入 Detecting，用 Redis 计数复检后续约
//
// 规则：
//  1. 标记 Detecting，避免定时任务误删本地租约
//  2. 当前 1 分钟窗口计数 ≥ 阈值 → 仍热
//  3. 仍热则 Exists(业务 key)：在则双续期；不在则不续（检测期 TTL 已空，说明中段已无访问）
//  4. 不热则结束 Detecting，不续期，交给定时任务清理
func (h *HotKeyCache) detectAndRenewIfNeeded(key string, count int64) error {
	if h.IsConfigHotKey(key) {
		return nil
	}
	now := time.Now().Unix()
	window := h.config.WindowSize

	h.hotMu.Lock()
	lease, ok := h.hotKeys[key]
	if !ok || lease == nil || lease.LogicExpire == 0 {
		h.hotMu.Unlock()
		return nil
	}

	logicLeft := lease.LogicExpire - now
	due := logicLeft <= window // 已到期或进入最后 1 个检测窗口
	if !due && !lease.Detecting {
		h.hotMu.Unlock()
		return nil
	}

	if !lease.Detecting {
		lease.Detecting = true
		lease.DetectStart = now
	}
	detectStart := lease.DetectStart
	h.hotMu.Unlock()

	// 检测窗口结束仍未达标 → 结束检测，等定时清理
	if count < h.config.FrequencyThreshold {
		if now-detectStart >= window {
			h.hotMu.Lock()
			if l, ok := h.hotKeys[key]; ok && l != nil {
				l.Detecting = false
				l.DetectStart = 0
			}
			h.hotMu.Unlock()
		}
		return nil
	}

	// 仍热：业务 key 还在才续期（TTL 检测期已过期则不再 Set）
	exists, err := h.client.Exists(key)
	if err != nil {
		return err
	}
	if !exists {
		h.hotMu.Lock()
		if l, ok := h.hotKeys[key]; ok && l != nil {
			l.Detecting = false
			l.DetectStart = 0
			// 逻辑过期保持，定时任务稍后删除本地标记
		}
		h.hotMu.Unlock()
		return nil
	}

	if err := h.renewLease(key, now); err != nil {
		return err
	}
	h.hotMu.Lock()
	if l, ok := h.hotKeys[key]; ok && l != nil {
		l.Detecting = false
		l.DetectStart = 0
	}
	h.hotMu.Unlock()
	return nil
}

// renewLease 同时续逻辑过期与 Redis TTL
func (h *HotKeyCache) renewLease(key string, now int64) error {
	logicDur := int64(h.config.LogicExpireDuration.Seconds())
	ttlSec := int(h.config.HotKeyTTL.Seconds())
	if ttlSec <= 0 {
		h.hotMu.Lock()
		if lease, ok := h.hotKeys[key]; ok && lease != nil && lease.LogicExpire != 0 {
			lease.LogicExpire = now + logicDur
			lease.Detecting = false
			lease.DetectStart = 0
		}
		h.hotMu.Unlock()
		return nil
	}

	if err := h.client.Expire(key, ttlSec); err != nil {
		return err
	}

	h.hotMu.Lock()
	if lease, ok := h.hotKeys[key]; ok && lease != nil && lease.LogicExpire != 0 {
		lease.LogicExpire = now + logicDur
		lease.RedisExpire = now + int64(ttlSec)
		lease.Detecting = false
		lease.DetectStart = 0
	}
	h.hotMu.Unlock()
	return nil
}

func (h *HotKeyCache) bindRedisExpire(key string, redisExpireAt int64) {
	h.hotMu.Lock()
	defer h.hotMu.Unlock()
	if lease, ok := h.hotKeys[key]; ok && lease != nil && lease.LogicExpire != 0 {
		lease.RedisExpire = redisExpireAt
	}
}

// cleanupHotKeys 定时清理：只删「逻辑已过期且未在 Detecting」的本地租约
func (h *HotKeyCache) cleanupHotKeys() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		h.cleanupExpiredHotKeys()
	}
}

func (h *HotKeyCache) cleanupExpiredHotKeys() {
	now := time.Now().Unix()
	window := h.config.WindowSize

	h.hotMu.Lock()
	defer h.hotMu.Unlock()

	for key, lease := range h.hotKeys {
		if lease == nil || lease.LogicExpire == 0 {
			continue
		}
		// 检测超时兜底：Detecting 超过 2 个窗口仍未结束，强制结束以免永久占坑
		if lease.Detecting && lease.DetectStart > 0 && now-lease.DetectStart >= window*2 {
			lease.Detecting = false
			lease.DetectStart = 0
		}
		if lease.Detecting {
			continue
		}
		if lease.LogicExpire < now {
			delete(h.hotKeys, key)
		}
	}
}

// RefreshHotKey 强制双续期
func (h *HotKeyCache) RefreshHotKey(key string) error {
	if h.IsConfigHotKey(key) {
		return nil
	}
	now := time.Now().Unix()
	h.hotMu.Lock()
	if lease, ok := h.hotKeys[key]; !ok || lease == nil || lease.LogicExpire == 0 {
		h.hotKeys[key] = &hotLease{
			LogicExpire: now + int64(h.config.LogicExpireDuration.Seconds()),
		}
	}
	h.hotMu.Unlock()
	return h.renewLease(key, now)
}

// RefreshHotKeyWithoutTTL 只续逻辑过期
func (h *HotKeyCache) RefreshHotKeyWithoutTTL(key string) {
	if h.IsConfigHotKey(key) {
		return
	}
	now := time.Now().Unix()
	h.hotMu.Lock()
	defer h.hotMu.Unlock()
	if lease, ok := h.hotKeys[key]; ok && lease != nil && lease.LogicExpire != 0 {
		lease.LogicExpire = now + int64(h.config.LogicExpireDuration.Seconds())
		lease.Detecting = false
		lease.DetectStart = 0
		return
	}
	h.hotKeys[key] = &hotLease{
		LogicExpire: now + int64(h.config.LogicExpireDuration.Seconds()),
	}
}

// LocalHotLeases 返回本地逻辑过期 map 快照（运维/调试）
func (h *HotKeyCache) LocalHotLeases() map[string]HotKeyMeta {
	h.hotMu.RLock()
	defer h.hotMu.RUnlock()
	out := make(map[string]HotKeyMeta, len(h.hotKeys))
	for k, lease := range h.hotKeys {
		if lease == nil {
			continue
		}
		out[k] = HotKeyMeta{
			ExpireTime:  lease.LogicExpire,
			Detecting:   lease.Detecting,
			DetectStart: lease.DetectStart,
		}
	}
	return out
}

// jitterTTL 计算带抖动的 TTL
func jitterTTL(baseTTL time.Duration) int {
	if baseTTL <= 0 {
		return int(baseTTL.Seconds())
	}
	jitterRange := int(float64(baseTTL) * 0.1)
	if jitterRange <= 0 {
		jitterRange = int(baseTTL)
	}
	offset := rand.Intn(jitterRange*2+1) - jitterRange
	ttl := int(baseTTL.Seconds()) + offset
	if ttl <= 0 {
		ttl = 1
	}
	return ttl
}

// ==================== 群元信息缓存 ====================

// GroupInfo 群元信息
type GroupInfo struct {
	Id           string `json:"id"`
	Name         string `json:"name"`
	Icon         string `json:"icon"` // 群头像
	Notification string `json:"notification,omitempty"`
	MemberCount  int64  `json:"member_count"`
	CreatorUid   string `json:"creator_uid,omitempty"`
}

// SetGroupInfo 设置群元信息（永不过期）
func (h *HotKeyCache) SetGroupInfo(groupId string, info *GroupInfo) error {
	key := h.GroupInfoKey(groupId)
	data, err := json.Marshal(info)
	if err != nil {
		return err
	}
	// 群元信息永不过期
	return h.client.Set(key, string(data))
}

// GetGroupInfo 获取群元信息
func (h *HotKeyCache) GetGroupInfo(groupId string) (*GroupInfo, error) {
	data, err := h.Get(h.GroupInfoKey(groupId))
	if err != nil {
		return nil, err
	}
	if data == "" {
		return nil, nil
	}
	var info GroupInfo
	if err := json.Unmarshal([]byte(data), &info); err != nil {
		return nil, err
	}
	return &info, nil
}

// GetGroupInfoOrLoad miss 时 singleflight 加载并回填
func (h *HotKeyCache) GetGroupInfoOrLoad(groupId string, loader func() (*GroupInfo, error)) (*GroupInfo, error) {
	key := h.GroupInfoKey(groupId)
	data, err := h.GetOrLoad(key, func() (string, error) {
		info, err := loader()
		if err != nil {
			return "", err
		}
		if info == nil {
			return "", nil
		}
		b, err := json.Marshal(info)
		if err != nil {
			return "", err
		}
		return string(b), nil
	})
	if err != nil {
		return nil, err
	}
	if data == "" {
		return nil, nil
	}
	var info GroupInfo
	if err := json.Unmarshal([]byte(data), &info); err != nil {
		return nil, err
	}
	return &info, nil
}

// DelGroupInfo 删除群元信息缓存
func (h *HotKeyCache) DelGroupInfo(groupId string) error {
	key := h.GroupInfoKey(groupId)
	_, err := h.client.Del(key)
	return err
}

// GroupInfoKey 生成群元信息缓存 key
func (h *HotKeyCache) GroupInfoKey(groupId string) string {
	return fmt.Sprintf("hot:group:%s:info", groupId)
}

// IsHotGroup 判断是否为热点群
func (h *HotKeyCache) IsHotGroup(groupId string) bool {
	key := h.GroupInfoKey(groupId)
	return h.IsHotKey(key)
}

// ==================== 系统公告缓存 ====================

// Announcement 系统公告
type Announcement struct {
	Id      string    `json:"id"`
	Title   string    `json:"title"`
	Content string    `json:"content"`
	Time    time.Time `json:"time"`
}

// SetAnnouncement 设置系统公告（永不过期）
func (h *HotKeyCache) SetAnnouncement(id string, ann *Announcement) error {
	key := h.AnnouncementKey(id)
	data, err := json.Marshal(ann)
	if err != nil {
		return err
	}
	return h.client.Set(key, string(data))
}

// GetAnnouncement 获取系统公告
func (h *HotKeyCache) GetAnnouncement(id string) (*Announcement, error) {
	data, err := h.Get(h.AnnouncementKey(id))
	if err != nil {
		return nil, err
	}
	if data == "" {
		return nil, nil
	}
	var ann Announcement
	if err := json.Unmarshal([]byte(data), &ann); err != nil {
		return nil, err
	}
	return &ann, nil
}

// GetAnnouncementOrLoad miss 时 singleflight 加载并回填
func (h *HotKeyCache) GetAnnouncementOrLoad(id string, loader func() (*Announcement, error)) (*Announcement, error) {
	key := h.AnnouncementKey(id)
	data, err := h.GetOrLoad(key, func() (string, error) {
		ann, err := loader()
		if err != nil {
			return "", err
		}
		if ann == nil {
			return "", nil
		}
		b, err := json.Marshal(ann)
		if err != nil {
			return "", err
		}
		return string(b), nil
	})
	if err != nil {
		return nil, err
	}
	if data == "" {
		return nil, nil
	}
	var ann Announcement
	if err := json.Unmarshal([]byte(data), &ann); err != nil {
		return nil, err
	}
	return &ann, nil
}

// DelAnnouncement 删除系统公告缓存
func (h *HotKeyCache) DelAnnouncement(id string) error {
	key := h.AnnouncementKey(id)
	_, err := h.client.Del(key)
	return err
}

// AnnouncementKey 生成系统公告缓存 key
func (h *HotKeyCache) AnnouncementKey(id string) string {
	return fmt.Sprintf("announcement:%s", id)
}

// ==================== 联系人元信息缓存 ====================

// ContactInfo 联系人元信息（用户或群组）
type ContactInfo struct {
	Id   string `json:"id"`
	Name string `json:"name"`
	Icon string `json:"icon,omitempty"`
	Type int32  `json:"type,omitempty"` // 1=用户, 2=群组
}

// SetContactInfo 设置联系人元信息（永不过期）
func (h *HotKeyCache) SetContactInfo(id string, info *ContactInfo) error {
	key := h.ContactInfoKey(id)
	data, err := json.Marshal(info)
	if err != nil {
		return err
	}
	return h.client.Set(key, string(data))
}

// GetContactInfo 获取联系人元信息
func (h *HotKeyCache) GetContactInfo(id string) (*ContactInfo, error) {
	data, err := h.Get(h.ContactInfoKey(id))
	if err != nil {
		return nil, err
	}
	if data == "" {
		return nil, nil
	}
	var info ContactInfo
	if err := json.Unmarshal([]byte(data), &info); err != nil {
		return nil, err
	}
	return &info, nil
}

// GetContactInfoOrLoad miss 时 singleflight 加载并回填
func (h *HotKeyCache) GetContactInfoOrLoad(id string, loader func() (*ContactInfo, error)) (*ContactInfo, error) {
	key := h.ContactInfoKey(id)
	data, err := h.GetOrLoad(key, func() (string, error) {
		info, err := loader()
		if err != nil {
			return "", err
		}
		if info == nil {
			return "", nil
		}
		b, err := json.Marshal(info)
		if err != nil {
			return "", err
		}
		return string(b), nil
	})
	if err != nil {
		return nil, err
	}
	if data == "" {
		return nil, nil
	}
	var info ContactInfo
	if err := json.Unmarshal([]byte(data), &info); err != nil {
		return nil, err
	}
	return &info, nil
}

// DelContactInfo 删除联系人元信息缓存
func (h *HotKeyCache) DelContactInfo(id string) error {
	key := h.ContactInfoKey(id)
	_, err := h.client.Del(key)
	return err
}

// ContactInfoKey 生成联系人元信息缓存 key
func (h *HotKeyCache) ContactInfoKey(id string) string {
	return fmt.Sprintf("contact:info:%s", id)
}

// BatchGetContactInfo 批量获取联系人元信息
func (h *HotKeyCache) BatchGetContactInfo(ids []string) (map[string]*ContactInfo, []string) {
	result := make(map[string]*ContactInfo)
	var missed []string

	for _, id := range ids {
		info, err := h.GetContactInfo(id)
		if err != nil || info == nil {
			missed = append(missed, id)
			continue
		}
		result[id] = info
	}

	return result, missed
}

// BatchGetContactInfoOrLoad 批量获取；miss 时对每个 id 走 singleflight 回源并回填
func (h *HotKeyCache) BatchGetContactInfoOrLoad(ids []string, loader func(id string) (*ContactInfo, error)) map[string]*ContactInfo {
	result := make(map[string]*ContactInfo, len(ids))
	seen := make(map[string]struct{}, len(ids))

	for _, id := range ids {
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}

		info, err := h.GetContactInfoOrLoad(id, func() (*ContactInfo, error) {
			return loader(id)
		})
		if err != nil || info == nil {
			continue
		}
		result[id] = info
	}
	return result
}

// SetContactInfoBatch 批量设置联系人元信息
func (h *HotKeyCache) SetContactInfoBatch(infos []*ContactInfo) error {
	for _, info := range infos {
		if err := h.SetContactInfo(info.Id, info); err != nil {
			return err
		}
	}
	return nil
}
