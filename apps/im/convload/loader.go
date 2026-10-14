package convload

import (
	"context"

	"easy-chat/apps/im/immodels"
	"easy-chat/pkg/redis"
)

func ChatLogToPoolItem(c *immodels.ChatLog) *redis.MsgPoolItem {
	if c == nil {
		return nil
	}
	return &redis.MsgPoolItem{
		Seq:     c.Seq,
		MsgId:   c.ID.Hex(),
		SendId:  c.SendId,
		Content: c.MsgContent,
		Type:    int32(c.MsgType),
		Time:    c.SendTime,
	}
}

// BuildMsgLoader 从 chat_log 按 seq 回源，供消息池 miss 时填充。
func BuildMsgLoader(model immodels.ChatLogModel) redis.MsgLoader {
	return func(conversationId string, anchorSeq int64, direction string, limit int) ([]*redis.MsgPoolItem, error) {
		if limit <= 0 {
			limit = redis.ConvMsgPoolMaxSize
		}
		var logs []*immodels.ChatLog
		var err error
		if direction == "up" {
			logs, err = model.ListBySeqLt(context.Background(), conversationId, anchorSeq, int64(limit))
		} else {
			logs, err = model.ListBySeqGt(context.Background(), conversationId, anchorSeq, int64(limit))
		}
		if err != nil {
			return nil, err
		}
		out := make([]*redis.MsgPoolItem, 0, len(logs))
		for _, c := range logs {
			out = append(out, ChatLogToPoolItem(c))
		}
		return out, nil
	}
}

type modelReadSeqStore struct {
	model immodels.UserConversationReadModel
}

// NewReadSeqStore 进房加载 / 退房写回 Mongo user_conversation_read
func NewReadSeqStore(model immodels.UserConversationReadModel) redis.ReadSeqStore {
	return &modelReadSeqStore{model: model}
}

func (s *modelReadSeqStore) Load(userId, conversationId string) (int64, error) {
	if s == nil || s.model == nil {
		return 0, nil
	}
	rec, err := s.model.FindByUserAndConversation(context.Background(), userId, conversationId)
	if err != nil {
		if err == immodels.ErrNotFound {
			return 0, nil
		}
		return 0, err
	}
	if rec == nil {
		return 0, nil
	}
	return rec.ReadSeq, nil
}

func (s *modelReadSeqStore) Save(userId, conversationId string, seq int64) error {
	if s == nil || s.model == nil {
		return nil
	}
	return s.model.UpsertByUserAndConversation(context.Background(), userId, conversationId, seq)
}
