package svc

import (
	"easy-chat/apps/user/models"
	"easy-chat/apps/user/rpc/internal/config"
	"easy-chat/pkg/constants"
	"easy-chat/pkg/ctxdata"
	msgredis "easy-chat/pkg/redis"
	"time"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

type ServiceContext struct {
	Config config.Config
	*redis.Redis
	models.UsersModel
	// 热点 key 缓存（防止缓存击穿）
	HotKeyCache *msgredis.HotKeyCache
}

func NewServiceContext(c config.Config) *ServiceContext {
	sqlConn := sqlx.NewMysql(c.Mysql.DataSource)
	rdb := redis.MustNewRedis(c.Redisx)

	return &ServiceContext{
		Config:     c,
		Redis:      rdb,
		UsersModel: models.NewUsersModel(sqlConn, c.Cache),
		HotKeyCache: msgredis.NewHotKeyCache(rdb, msgredis.HotKeyConfig{
			HotPrefixes: []string{
				"system:",       // 系统 key（配置级，永不过期）
				"global:",       // 全局配置
				"announcement:", // 系统公告
				"hot:group:",    // 热门群聊
			},
			// 运行时热点：逻辑 10m + Redis TTL 15m，临期 KeepAlive（默认值）
			HotKeyTTL:          15 * time.Minute,
			DefaultTTL:         5 * time.Minute,
			FrequencyThreshold: 100,
			WindowSize:         60,
		}),
	}
}

// SetRootToken 将 root token 设置到 redis 中（使用热点缓存）
func (svc *ServiceContext) SetRootToken() error {
	// 生成jwt
	systemToken, err := ctxdata.GetJwtToken(svc.Config.Jwt.AccessSecret, time.Now().Unix(), 999999999, constants.SYSTEM_ROOT_UID)
	if err != nil {
		return err
	}
	// 写入到redis（使用热点缓存，永不过期）
	return svc.HotKeyCache.Set(constants.REDIS_SYSTEM_ROOT_TOKEN, systemToken)
}
