package logic

import (
	"context"
	"easy-chat/apps/im/immodels"
	"easy-chat/apps/im/rpc/im"
	"easy-chat/apps/im/rpc/internal/svc"
	"easy-chat/pkg/constants"
	"easy-chat/pkg/redis"
	"easy-chat/pkg/xerr"
	"time"

	"github.com/pkg/errors"
	"github.com/zeromicro/go-zero/core/logx"
)

type PutConversationsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewPutConversationsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *PutConversationsLogic {
	return &PutConversationsLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// PutConversations
//
// 写入：
//   - 延迟双删 conv:list:{userId}（先删 → 写 Mongo → 延迟再删）
//   - MongoDB conversations (持久化)
//   - Redis conv:state:{convId}:{userId} (isShow/readSeq)
func (l *PutConversationsLogic) PutConversations(in *im.PutConversationsReq) (*im.PutConversationsResp, error) {
	if l.svcCtx.ConversationCache != nil {
		_ = l.svcCtx.ConversationCache.Del(in.UserId)
	}

	data, err := l.svcCtx.ConversationsModel.FindByUserId(l.ctx, in.UserId)
	if err != nil {
		return nil, errors.Wrapf(xerr.NewDBErr(), "ConversationsModel.FindByUserId err %v", err)
	}
	if data.ConversationList == nil {
		data.ConversationList = make(map[string]*immodels.Conversation)
	}

	for cid, conversation := range in.ConversationList {
		var oldTotal int
		if data.ConversationList[cid] != nil {
			oldTotal = data.ConversationList[cid].Total
		}

		// 1. MongoDB 兜底
		data.ConversationList[cid] = &immodels.Conversation{
			ConversationId: cid,
			ChatType:       constants.ChatType(conversation.ChatType),
			IsShow:         conversation.IsShow,
			Total:          int(conversation.Read) + oldTotal,
			Seq:            conversation.Seq,
		}

		// 2. Redis state 增量更新
		_ = l.svcCtx.ConvSession.State().SetFields(cid, in.UserId, map[string]string{
			redis.ConvStateFieldReadSeq: itoa64(int64(conversation.Read) + int64(oldTotal)),
			redis.ConvStateFieldIsShow:  boolStr(conversation.IsShow),
		})
	}

	if err := l.svcCtx.ConversationsModel.Update(l.ctx, data); err != nil {
		return nil, errors.Wrapf(xerr.NewDBErr(), "ConversationsModel.Update err %v", err)
	}

	// 延迟双删第二次：清掉并发读回源可能写回的脏数据
	if l.svcCtx.ConversationCache != nil {
		l.svcCtx.ConversationCache.ScheduleSecondDelete(in.UserId, 500*time.Millisecond)
	}

	return &im.PutConversationsResp{}, nil
}

func itoa64(n int64) string {
	// 轻量实现，避免 strconv 反复 import
	if n == 0 {
		return "0"
	}
	neg := false
	if n < 0 {
		neg = true
		n = -n
	}
	buf := [20]byte{}
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
