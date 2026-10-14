package topicTransfer

import (
	"context"
	"encoding/json"

	"easy-chat/apps/task/mq/internal/svc"
	"easy-chat/apps/task/mq/mq"
	"easy-chat/pkg/topic"

	"github.com/zeromicro/go-queue/kq"
	"github.com/zeromicro/go-zero/core/logx"
)

type TopicRoundTransfer struct {
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewTopicRoundTransfer(svc *svc.ServiceContext) kq.ConsumeHandler {
	return &TopicRoundTransfer{
		svcCtx: svc,
		Logger: logx.WithContext(context.Background()),
	}
}

func (t *TopicRoundTransfer) Consume(key, value string) error {
	if t.svcCtx.TopicEngine == nil {
		return nil
	}
	var data mq.TopicRoundComplete
	if err := json.Unmarshal([]byte(value), &data); err != nil {
		return err
	}
	ev := topic.RoundEvent{
		ConversationId: data.ConversationId,
		UserId:         data.UserId,
		RoundId:        data.RoundId,
		UserMsgId:      data.UserMsgId,
		AiMsgId:        data.AiMsgId,
		UserContent:    data.UserContent,
		AiContent:      data.AiContent,
	}
	ctx := context.Background()
	result, rewrite, err := t.svcCtx.TopicEngine.OnRoundComplete(ctx, ev)
	if err != nil {
		t.Errorf("topic round error: conv=%s err=%v", data.ConversationId, err)
		return err
	}
	if result != nil && result.TopicId != "" {
		_ = t.svcCtx.ChatLogCustom.UpdateTopicTag(ctx, data.UserMsgId, result.TopicId, data.RoundId)
		_ = t.svcCtx.ChatLogCustom.UpdateTopicTag(ctx, data.AiMsgId, result.TopicId, data.RoundId)
	}
	if rewrite != nil {
		task := rewrite
		go func() {
			if err := t.svcCtx.TopicEngine.ApplySummaryRewrite(context.Background(), task); err != nil {
				t.Errorf("topic summary rewrite error: conv=%s topic=%s err=%v",
					task.ConversationId, task.TopicId, err)
			}
		}()
	}
	t.Infof("topic round done: conv=%s action=%s phase=%s topicId=%s",
		data.ConversationId, safeAction(result), safePhase(result), safeTopicId(result))
	return nil
}

func safeAction(r *topic.RoundResult) string {
	if r == nil {
		return ""
	}
	return r.Action
}

func safePhase(r *topic.RoundResult) string {
	if r == nil {
		return ""
	}
	return r.Phase
}

func safeTopicId(r *topic.RoundResult) string {
	if r == nil {
		return ""
	}
	return r.TopicId
}
