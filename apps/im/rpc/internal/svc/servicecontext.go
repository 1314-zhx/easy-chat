package svc

import (
	"easy-chat/apps/im/immodels"
	"easy-chat/apps/im/rpc/internal/config"
	msgredis "easy-chat/pkg/redis"
	"time"

	"github.com/zeromicro/go-zero/core/stores/redis"
)

type ServiceContext struct {
	Config config.Config

	immodels.ChatLogModel
	immodels.ConversationsModel
	immodels.ConversationModel
	immodels.UserConversationReadModel

	Redis             *redis.Redis
	ConvSession       *msgredis.ConvSession
	ConversationCache *msgredis.ConversationCache // 用户会话清单旁路缓存（延迟双删）
}

func NewServiceContext(c config.Config) *ServiceContext {
	rdb := redis.MustNewRedis(c.Redisx)
	immodels.EnsureIndexesAsync(c.Mongo.Url, c.Mongo.Db)
	return &ServiceContext{
		Config:                    c,
		ChatLogModel:              immodels.MustChatLogModel(c.Mongo.Url, c.Mongo.Db),
		ConversationsModel:        immodels.MustConversationsModel(c.Mongo.Url, c.Mongo.Db),
		ConversationModel:         immodels.MustConversationModel(c.Mongo.Url, c.Mongo.Db),
		UserConversationReadModel: immodels.MustUserConversationReadModel(c.Mongo.Url, c.Mongo.Db),
		Redis:                     rdb,
		ConvSession:               msgredis.NewConvSession(rdb),
		ConversationCache:         msgredis.NewConversationCache(rdb, 5*time.Minute),
	}
}
