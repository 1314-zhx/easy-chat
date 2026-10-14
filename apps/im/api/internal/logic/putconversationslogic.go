// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package logic

import (
	"context"

	"easy-chat/apps/im/api/internal/svc"
	"easy-chat/apps/im/api/internal/types"
	"easy-chat/apps/im/rpc/imclient"
	"easy-chat/pkg/ctxdata"

	"github.com/zeromicro/go-zero/core/logx"
)

type PutConversationsLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewPutConversationsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *PutConversationsLogic {
	return &PutConversationsLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

// PutConversations
//
// 三层缓存由 RPC 层维护（conv:state 的 readSeq / isShow），
// API 层只做透传 + 调用方无需关心缓存失效。
func (l *PutConversationsLogic) PutConversations(req *types.PutConversationsReq) (resp *types.PutConversationsResp, err error) {
	userId := ctxdata.GetUId(l.ctx)

	conversationList := make(map[string]*imclient.Conversation)
	for id, conv := range req.ConversationList {
		conversationList[id] = &imclient.Conversation{
			ConversationId: conv.ConversationId,
			ChatType:       conv.ChatType,
			TargetId:       conv.TargetId,
			IsShow:         conv.IsShow,
			Read:           conv.Read,
			Total:          conv.Total,
		}
	}

	_, err = l.svcCtx.Im.PutConversations(l.ctx, &imclient.PutConversationsReq{
		Id:               userId,
		UserId:           userId,
		ConversationList: conversationList,
	})
	if err != nil {
		return nil, err
	}

	return &types.PutConversationsResp{}, nil
}
