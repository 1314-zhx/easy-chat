/**
 * @author: dn-jinmin/dn-jinmin
 * @doc:
 */

package handler

import (
	"context"

	"easy-chat/apps/task/mq/internal/handler/msgTransfer"
	"easy-chat/apps/task/mq/internal/handler/topicTransfer"
	"easy-chat/apps/task/mq/internal/svc"
	"easy-chat/pkg/tracing"

	"github.com/zeromicro/go-queue/kq"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/service"
)

// 监听对象
type Listen struct {
	svc          *svc.ServiceContext
	traceHandler *tracing.TraceHandler
}

func NewListen(svc *svc.ServiceContext) *Listen {
	return &Listen{svc: svc}
}

// 返回多个服务对象，及消费者，返回的是Service接口，有stopper和starter接口，为了给服务提供stop和start方法
func (l *Listen) Services() []service.Service {
	chat := msgTransfer.NewMsgChatTransfer(l.svc)
	services := []service.Service{
		// todo: 此处可以加载多个消费者
		kq.MustNewQueue(l.svc.Config.MsgReadTransfer, msgTransfer.NewMsgReadTransfer(l.svc)),
		// 聊天消息：持续拉取，批次落库成功后再提交位点
		msgTransfer.NewChatBatchConsumer(l.svc.Config.MsgChatTransfer, chat),
	}

	if l.svc.Config.TopicRoundTransfer.Topic != "" {
		services = append(services, kq.MustNewQueue(l.svc.Config.TopicRoundTransfer, topicTransfer.NewTopicRoundTransfer(l.svc)))
		logx.Info("topic round consumer 已注册")
	}

	// 注册 Trace 消费者
	if l.svc.Config.TraceConsumer.Name != "" {
		traceDb := l.svc.Config.Mongo.TraceDb
		if traceDb == "" {
			traceDb = "jaeger"
		}

		traceHandler, err := tracing.NewTraceHandler(
			context.Background(),
			l.svc.Config.Mongo.Url,
			traceDb,
		)
		if err != nil {
			logx.Errorf("创建trace handler失败: %v", err)
		} else {
			l.traceHandler = traceHandler
			services = append(services, kq.MustNewQueue(l.svc.Config.TraceConsumer, traceHandler))
			logx.Info("trace consumer 已注册")
		}
	}

	return services
}

func (l *Listen) Stop() {
	if l.traceHandler != nil {
		l.traceHandler.Stop()
	}
}
