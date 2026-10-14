/**
 * @author: dn-jinmin/dn-jinmin
 * @doc:
 */

package svc

import (
	"sync/atomic"
	"time"

	"easy-chat/apps/im/convload"
	"easy-chat/apps/im/immodels"
	"easy-chat/apps/im/ws/internal/config"
	"easy-chat/apps/task/mq/mqclient"
	msgredis "easy-chat/pkg/redis"

	"github.com/zeromicro/go-zero/core/stores/redis"
)

type ServiceContext struct {
	Config config.Config

	immodels.ChatLogModel
	immodels.UserConversationReadModel
	mqclient.MsgChatTransferClient
	mqclient.MsgReadTransferClient

	// v3 聚合会话缓存
	ConvSession *msgredis.ConvSession

	// 热点 key 缓存（防止缓存击穿）
	HotKeyCache *msgredis.HotKeyCache
	// 活跃会话缓存（推送降噪）
	ActiveConversationCache *msgredis.ActiveConversationCache
	// 用户会话已读游标管理器（双写 Redis + MongoDB）
	UserConversationReadManager *msgredis.UserConversationReadManager
	Redis                       *redis.Redis

	runtimeCfg atomic.Value
}

func NewServiceContext(c config.Config) *ServiceContext {
	rdb := redis.MustNewRedis(c.Redis)

	chatLogModel := immodels.MustChatLogModel(c.Mongo.Url, c.Mongo.Db)
	userConvReadModel := immodels.MustUserConversationReadModel(c.Mongo.Url, c.Mongo.Db)
	immodels.EnsureIndexesAsync(c.Mongo.Url, c.Mongo.Db)

	s := &ServiceContext{
		Config:                      c,
		MsgChatTransferClient:       mqclient.NewMsgChatTransferClient(c.MsgChatTransfer.Addrs, c.MsgChatTransfer.Topic),
		MsgReadTransferClient:       mqclient.NewMsgReadTransferClient(c.MsgReadTransfer.Addrs, c.MsgReadTransfer.Topic),
		ChatLogModel:                chatLogModel,
		UserConversationReadModel:   userConvReadModel,
		ConvSession: msgredis.NewConvSession(rdb,
			msgredis.WithMsgLoader(convload.BuildMsgLoader(chatLogModel)),
			msgredis.WithReadSeqStore(convload.NewReadSeqStore(userConvReadModel)),
		),
		HotKeyCache: msgredis.NewHotKeyCache(rdb, msgredis.HotKeyConfig{
			HotPrefixes: []string{
				"system:",
				"global:",
				"announcement:",
			},
			HotKeyTTL:          15 * time.Minute,
			DefaultTTL:         5 * time.Minute,
			FrequencyThreshold: 100,
			WindowSize:         60,
		}),
		ActiveConversationCache:     msgredis.NewActiveConversationCache(rdb, 10*time.Minute),
		UserConversationReadManager: msgredis.NewUserConversationReadManager(rdb, userConvReadModel),
		Redis:                       rdb,
	}
	s.runtimeCfg.Store(c)
	return s
}

func (s *ServiceContext) ApplyRuntimeConfig(c config.Config) {
	s.runtimeCfg.Store(c)
}

func (s *ServiceContext) RuntimeConfig() config.Config {
	if v := s.runtimeCfg.Load(); v != nil {
		if cfg, ok := v.(config.Config); ok {
			return cfg
		}
	}
	return s.Config
}
