package svc

import (
	"easy-chat/apps/im/convload"
	"easy-chat/apps/im/immodels"
	"easy-chat/apps/im/ws/websocket"
	"easy-chat/apps/social/rpc/socialclient"
	"easy-chat/apps/task/mq/internal/config"
	"easy-chat/apps/task/mq/mqclient"
	"easy-chat/pkg/constants"
	"easy-chat/pkg/qwen"
	"easy-chat/pkg/rag"
	msgredis "easy-chat/pkg/redis"
	"easy-chat/pkg/topic"
	"net/http"
	"time"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
)

type ServiceContext struct {
	config.Config

	// KafkaBrokers 用于 DLQ 生产者
	KafkaBrokers []string

	WsClient websocket.Client
	*redis.Redis

	// v3 聚合会话缓存（meta + msg pool + state 三层）
	ConvSession *msgredis.ConvSession
	// 活跃会话缓存（保留：用于推送降噪判断）
	ActiveConversationCache *msgredis.ActiveConversationCache
	// 用户已读游标 Mongo 持久化（保留）
	UserConversationReadManager *msgredis.UserConversationReadManager

	socialclient.Social
	immodels.ChatLogModel
	ChatLogCustom immodels.CustomChatLogModel
	immodels.ConversationModel
	immodels.UserConversationReadModel
	MessageIndexer *rag.Indexer

	TopicEngine      *topic.Engine
	TopicRoundClient mqclient.TopicRoundClient
}

func NewServiceContext(c config.Config) *ServiceContext {
	vectorModel := immodels.NewMessageVectorModel(c.Mongo.Url, c.Mongo.Db, "message_vectors")
	rdb := redis.MustNewRedis(c.Redisx)
	immodels.EnsureIndexesAsync(c.Mongo.Url, c.Mongo.Db)
	userConversationReadModel := immodels.MustUserConversationReadModel(c.Mongo.Url, c.Mongo.Db)

	var brokers []string
	if len(c.MsgChatTransfer.Brokers) > 0 {
		brokers = c.MsgChatTransfer.Brokers
	}

	svc := &ServiceContext{
		Config:                        c,
		KafkaBrokers:                  brokers,
		Redis:                         rdb,
		ConvSession: msgredis.NewConvSession(rdb,
			msgredis.WithReadSeqStore(convload.NewReadSeqStore(userConversationReadModel)),
		),
		ActiveConversationCache:       msgredis.NewActiveConversationCache(rdb, 10*time.Minute),
		UserConversationReadManager:   msgredis.NewUserConversationReadManager(rdb, userConversationReadModel),
		UserConversationReadModel:     userConversationReadModel,
		ChatLogModel:                  immodels.MustChatLogModel(c.Mongo.Url, c.Mongo.Db),
		ChatLogCustom:                 immodels.NewCustomChatLogModel(c.Mongo.Url, c.Mongo.Db, "chat_log"),
		ConversationModel:             immodels.MustConversationModel(c.Mongo.Url, c.Mongo.Db),
		Social:                        socialclient.NewSocial(zrpc.MustNewClient(c.SocialRpc)),
		MessageIndexer:                rag.NewIndexer(vectorModel, qwenConfigFromTask(c)),
	}

	token, err := svc.GetSystemToken()
	if err != nil {
		panic(err)
	}
	header := http.Header{}
	header.Set("Authorization", token)
	wsClient := websocket.NewClient(c.Ws.Host, websocket.WithClientHeader(header))
	svc.WsClient = wsClient

	topicCfg := c.Topic
	if topicCfg.MaxCandidates <= 0 {
		topicCfg = topic.DefaultConfig()
	}
	topicModel := immodels.MustConversationTopicModel(c.Mongo.Url, c.Mongo.Db)
	topicRepo := immodels.NewTopicRepository(topicModel)
	qwenCfg := qwenConfigFromTask(c)
	svc.TopicEngine = topic.NewEngine(topicCfg, topic.NewRedisStore(rdb, topicCfg.RedisTTL), topicRepo, qwenCfg)

	if len(c.TopicRoundTransfer.Brokers) > 0 && c.TopicRoundTransfer.Topic != "" {
		svc.TopicRoundClient = mqclient.NewTopicRoundClient(c.TopicRoundTransfer.Brokers, c.TopicRoundTransfer.Topic)
	} else if len(brokers) > 0 {
		svc.TopicRoundClient = mqclient.NewTopicRoundClient(brokers, "topicRoundComplete")
	}
	return svc
}

func (svc *ServiceContext) GetSystemToken() (string, error) {
	return svc.Redis.Get(constants.REDIS_SYSTEM_ROOT_TOKEN)
}

func qwenConfigFromTask(c config.Config) qwen.Config {
	cfg := qwen.DefaultConfig()
	if c.Qwen.ApiKey != "" {
		cfg.ApiKey = c.Qwen.ApiKey
	}
	if c.Qwen.Model != "" {
		cfg.Model = c.Qwen.Model
	}
	if c.Qwen.Endpoint != "" {
		cfg.Endpoint = c.Qwen.Endpoint
	}
	if c.Qwen.EmbeddingModel != "" {
		cfg.EmbeddingModel = c.Qwen.EmbeddingModel
	}
	return cfg
}
