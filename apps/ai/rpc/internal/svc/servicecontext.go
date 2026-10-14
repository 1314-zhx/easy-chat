package svc

import (
	"sync/atomic"

	"easy-chat/apps/ai/rpc/internal/config"
	"easy-chat/apps/im/immodels"
	"easy-chat/pkg/qwen"
	"easy-chat/pkg/rag"
	"easy-chat/pkg/tools"
)

type ServiceContext struct {
	Config             config.Config
	ChatLogModel       immodels.CustomChatLogModel
	MessageVectorModel immodels.MessageVectorModel
	UserMemoryModel    immodels.UserMemoryModel
	ToolRegistry       *tools.Registry
	RAGTracker         *rag.ConversationTracker
	runtimeConfig      atomic.Value
}

func NewServiceContext(c config.Config) *ServiceContext {
	immodels.EnsureIndexesAsync(c.Mongo.Url, c.Mongo.Db)
	s := &ServiceContext{
		Config:             c,
		ChatLogModel:       immodels.NewCustomChatLogModel(c.Mongo.Url, c.Mongo.Db, "chat_log"),
		MessageVectorModel: immodels.NewMessageVectorModel(c.Mongo.Url, c.Mongo.Db, "message_vectors"),
		UserMemoryModel:    immodels.NewUserMemoryModel(c.Mongo.Url, c.Mongo.Db, "user_memory"),
		ToolRegistry:       tools.NewRegistry(tools.Options{}),
		RAGTracker:         rag.NewConversationTracker(30),
	}
	s.runtimeConfig.Store(c)
	return s
}

func (s *ServiceContext) ApplyRuntimeConfig(c config.Config) {
	s.runtimeConfig.Store(c)
}

func (s *ServiceContext) RuntimeConfig() config.Config {
	if v := s.runtimeConfig.Load(); v != nil {
		if cfg, ok := v.(config.Config); ok {
			return cfg
		}
	}
	return s.Config
}

func (s *ServiceContext) QwenConfig() qwen.Config {
	q := s.RuntimeConfig().Qwen
	cfg := qwen.DefaultConfig()
	if q.ApiKey != "" {
		cfg.ApiKey = q.ApiKey
	}
	if q.Model != "" {
		cfg.Model = q.Model
	}
	if q.Endpoint != "" {
		cfg.Endpoint = q.Endpoint
	}
	if q.EmbeddingModel != "" {
		cfg.EmbeddingModel = q.EmbeddingModel
	}
	cfgCopy := cfg
	// 注入 qwen.Config 到 rag contextual 层
	rag.SetQwenConfig(cfgCopy)
	return cfg
}
