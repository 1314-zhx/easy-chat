// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package logic

import (
	"context"
	"easy-chat/apps/im/api/internal/svc"
	"easy-chat/apps/im/api/internal/types"
	"easy-chat/apps/im/rpc/imclient"
	"easy-chat/apps/social/rpc/socialclient"
	"easy-chat/apps/user/rpc/userclient"
	"easy-chat/pkg/constants"
	"easy-chat/pkg/ctxdata"
	msgredis "easy-chat/pkg/redis"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetConversationsLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetConversationsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetConversationsLogic {
	return &GetConversationsLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

// GetConversations
//
// 缓存策略：API 层不做会话缓存（避免双层缓存不一致）。
// 会话缓存由 RPC 层 + Redis 三层结构（conv:meta / conv:msg / conv:state）负责。
// API 层只做联系人元信息（contact:info）填充；miss 时 singleflight 回源。
func (l *GetConversationsLogic) GetConversations(req *types.GetConversationsReq) (resp *types.GetConversationsResp, err error) {
	userId := ctxdata.GetUId(l.ctx)

	// 1. RPC 拿会话清单（RPC 层已读 meta + state 三层缓存）
	conversations, err := l.svcCtx.Im.GetConversations(l.ctx, &imclient.GetConversationsReq{UserId: userId})
	if err != nil {
		return nil, err
	}

	resp = &types.GetConversationsResp{
		UserId:           userId,
		ConversationList: make(map[string]*types.Conversation),
	}

	// 2. 收集 TargetId 及 ChatType（回源时区分用户/群）
	var targetIds []string
	chatTypeByTarget := make(map[string]int32)
	for _, conv := range conversations.ConversationList {
		if conv.TargetId == "" {
			continue
		}
		targetIds = append(targetIds, conv.TargetId)
		chatTypeByTarget[conv.TargetId] = conv.ChatType
	}

	// 3. 联系人元信息：命中缓存直接返回；miss 时 per-id singleflight 回源并回填
	contactInfos := l.svcCtx.HotKeyCache.BatchGetContactInfoOrLoad(targetIds, func(id string) (*msgredis.ContactInfo, error) {
		return l.loadContactInfo(userId, id, chatTypeByTarget[id])
	})

	// 4. 组装会话列表
	for id, conv := range conversations.ConversationList {
		c := &types.Conversation{
			ConversationId: conv.ConversationId,
			ChatType:       conv.ChatType,
			TargetId:       conv.TargetId,
			IsShow:         conv.IsShow,
			Seq:            conv.Seq,
			Read:           conv.Read,
			Total:          conv.Total,
			Unread:         conv.ToRead,
		}

		if info, ok := contactInfos[conv.TargetId]; ok {
			c.TargetName = info.Name
			c.TargetIcon = info.Icon
		}

		if conv.Msg != nil {
			c.LastMsg = &types.LastMsg{
				SendId:  conv.Msg.SendId,
				Content: conv.Msg.MsgContent,
				Time:    conv.Msg.SendTime,
				Type:    conv.Msg.MsgType,
			}
		}

		resp.ConversationList[id] = c
	}

	return resp, nil
}

func (l *GetConversationsLogic) loadContactInfo(userId, targetId string, chatType int32) (*msgredis.ContactInfo, error) {
	switch constants.ChatType(chatType) {
	case constants.SingleChatType:
		u, err := l.svcCtx.User.GetUserInfo(l.ctx, &userclient.GetUserInfoReq{Id: targetId})
		if err != nil {
			l.Errorf("load contact user failed: id=%s err=%v", targetId, err)
			return nil, err
		}
		if u == nil || u.User == nil {
			return nil, nil
		}
		return &msgredis.ContactInfo{
			Id:   u.User.Id,
			Name: u.User.Nickname,
			Icon: u.User.Avatar,
			Type: 1,
		}, nil

	case constants.GroupChatType:
		groups, err := l.svcCtx.Social.GroupList(l.ctx, &socialclient.GroupListReq{UserId: userId})
		if err != nil {
			l.Errorf("load contact group failed: id=%s err=%v", targetId, err)
			return nil, err
		}
		for _, g := range groups.List {
			if g.Id == targetId {
				return &msgredis.ContactInfo{
					Id:   g.Id,
					Name: g.Name,
					Icon: g.Icon,
					Type: 2,
				}, nil
			}
		}
		return nil, nil

	default:
		// ChatType 未知时先试用户，再试群
		u, err := l.svcCtx.User.GetUserInfo(l.ctx, &userclient.GetUserInfoReq{Id: targetId})
		if err == nil && u != nil && u.User != nil {
			return &msgredis.ContactInfo{
				Id:   u.User.Id,
				Name: u.User.Nickname,
				Icon: u.User.Avatar,
				Type: 1,
			}, nil
		}
		groups, err := l.svcCtx.Social.GroupList(l.ctx, &socialclient.GroupListReq{UserId: userId})
		if err != nil {
			return nil, err
		}
		for _, g := range groups.List {
			if g.Id == targetId {
				return &msgredis.ContactInfo{
					Id:   g.Id,
					Name: g.Name,
					Icon: g.Icon,
					Type: 2,
				}, nil
			}
		}
		return nil, nil
	}
}
