package config

import (
	"easy-chat/pkg/topic"

	"github.com/zeromicro/go-queue/kq"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	service.ServiceConf
	ListenOn         string
	MsgChatTransfer  kq.KqConf
	MsgReadTransfer  kq.KqConf
	TopicRoundTransfer kq.KqConf `json:",optional"`
	TraceConsumer    kq.KqConf `json:",optional"` // Trace 消费者配置
	DeadLetterTopic  string    `json:",optional"` // 死信队列 Topic

	MsgReadHandler struct {
		GroupMsgReadHandler          int
		GroupMsgReadRecordDelayTime  int64
		GroupMsgReadRecordDelayCount int
		GroupMsgReadAutoMergeThreshold int // 智能合并阈值：群人数超过此值时启用合并推送，0表示不启用
	}

	Redisx redis.RedisConf
	Mongo  struct {
		Url     string
		Db      string
		TraceDb string `json:",optional"` // Trace 存储的数据库，默认 jaeger
	}

	Qwen struct {
		ApiKey         string `json:",optional"`
		Model          string `json:",default=qwen-turbo"`
		Endpoint       string `json:",default=https://dashscope.aliyuncs.com"`
		EmbeddingModel string `json:",default=text-embedding-v3"`
	}

	Tools struct {
		GitHub struct {
			Enabled bool   `json:",optional"`
			Token   string `json:",optional"`
		}
	}

	SocialRpc zrpc.RpcClientConf
	AiRpc     zrpc.RpcClientConf `json:",optional"`

	Ws struct {
		Host string
	}

	Topic topic.Config `json:",optional"`
}
