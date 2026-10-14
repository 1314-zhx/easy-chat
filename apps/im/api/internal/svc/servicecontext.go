// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package svc

import (
	"easy-chat/apps/im/api/internal/config"
	"easy-chat/apps/im/rpc/imclient"
	"easy-chat/apps/social/rpc/socialclient"
	"easy-chat/apps/user/rpc/userclient"
	msgredis "easy-chat/pkg/redis"
	"time"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
)

type ServiceContext struct {
	Config config.Config
	Im     imclient.Im
	Social socialclient.Social
	User   userclient.User
	Redis  *redis.Redis

	// v3 聚合会话缓存（meta + msg pool + state 三层）
	ConvSession *msgredis.ConvSession

	// 旧缓存保留（用于联系人等无关路径的兼容性，迁移完成后再删除）
	HotKeyCache *msgredis.HotKeyCache
}

func NewServiceContext(c config.Config) *ServiceContext {
	rdb := redis.MustNewRedis(c.Redisx)
	return &ServiceContext{
		Config: c,
		Im:     imclient.NewIm(zrpc.MustNewClient(c.ImRpc)),
		Social: socialclient.NewSocial(zrpc.MustNewClient(c.SocialRpc)),
		User:   userclient.NewUser(zrpc.MustNewClient(c.UserRpc)),
		Redis:  rdb,
		ConvSession: msgredis.NewConvSession(rdb),
		HotKeyCache: msgredis.NewHotKeyCache(rdb, msgredis.HotKeyConfig{
			HotPrefixes: []string{
				"contact:info:", // 配置级前缀，永不过期
			},
			HotKeyTTL:          15 * time.Minute,
			DefaultTTL:         5 * time.Minute,
			FrequencyThreshold: 100,
			WindowSize:         60,
		}),
	}
}
