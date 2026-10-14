package logic

import (
	"context"
	"easy-chat/apps/im/immodels"
	"easy-chat/apps/im/rpc/im"
	"easy-chat/apps/im/rpc/internal/svc"
	"easy-chat/pkg/constants"
	"easy-chat/pkg/redis"
	"easy-chat/pkg/xerr"

	"github.com/pkg/errors"
	"github.com/zeromicro/go-zero/core/logx"
)

type CreateGroupConversationLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewCreateGroupConversationLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CreateGroupConversationLogic {
	return &CreateGroupConversationLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// CreateGroupConversation 创建群会话
func (l *CreateGroupConversationLogic) CreateGroupConversation(in *im.CreateGroupConversationReq) (*im.CreateGroupConversationResp, error) {
	res := &im.CreateGroupConversationResp{}
	_, err := l.svcCtx.ConversationModel.FindOne(l.ctx, in.GroupId)
	if err == nil {
		return nil, nil
	}
	if !errors.Is(err, immodels.ErrNotFound) {
		return nil, errors.Wrapf(xerr.NewDBErr(), "ConversationModel.FindOne msgId err %v, req %v", err, in.GroupId)
	}

	// 1. MongoDB conversation 共享元信息
	err = l.svcCtx.ConversationModel.Insert(l.ctx, &immodels.Conversation{
		ConversationId: in.GroupId,
		ChatType:       constants.GroupChatType,
	})
	if err != nil {
		return nil, errors.Wrapf(xerr.NewDBErr(), "ConversationModel.Insert err %v", err)
	}

	// 2. Redis conv:meta:{groupId}：群类型
	// 群成员列表由社交服务管理，单独走 group:members 缓存或调用方传
	_ = l.svcCtx.ConvSession.Meta().Set(in.GroupId, &redis.ConvMeta{
		ChatType: int32(constants.GroupChatType),
	})

	// 3. 在创建者侧建立会话（其他成员由群加入流程触发 SetUpUserConversation）
	setup := NewSetUpUserConversationLogic(l.ctx, l.svcCtx)
	_, err = setup.SetUpUserConversation(&im.SetUpUserConversationReq{
		SendId:   in.CreateId,
		RecvId:   in.GroupId,
		ChatType: int32(constants.GroupChatType),
	})
	return res, err
}
