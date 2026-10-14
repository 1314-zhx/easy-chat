package logic

import (
	"context"
	"easy-chat/apps/im/immodels"
	"easy-chat/apps/im/rpc/im"
	"easy-chat/apps/im/rpc/internal/svc"
	"easy-chat/pkg/constants"
	"easy-chat/pkg/redis"
	"easy-chat/pkg/wuid"
	"easy-chat/pkg/xerr"
	"time"

	"github.com/pkg/errors"
	"github.com/zeromicro/go-zero/core/logx"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type SetUpUserConversationLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewSetUpUserConversationLogic(ctx context.Context, svcCtx *svc.ServiceContext) *SetUpUserConversationLogic {
	return &SetUpUserConversationLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// SetUpUserConversation 建立会话 (群聊/私聊)
//
// 写入三层缓存：
//   - conv:meta:{convId}   共享会话元信息
//   - conv:state:{convId}:{userId}  用户状态（isShow/readSeq=0/unread=0）
//   - MongoDB `conversation`         共享会话元信息（持久化）
//   - MongoDB `conversations`        用户会话清单（持久化）
func (l *SetUpUserConversationLogic) SetUpUserConversation(in *im.SetUpUserConversationReq) (*im.SetUpUserConversationResp, error) {
	res := &im.SetUpUserConversationResp{}
	switch constants.ChatType(in.ChatType) {
	case constants.SingleChatType:
		conversationId := wuid.CombineId(in.SendId, in.RecvId)
		// 共享元信息：conversation 集合 + Redis meta
		exists, err := l.svcCtx.ConversationModel.FindOne(l.ctx, conversationId)
		if err != nil {
			if err == immodels.ErrNotFound {
				if err := l.svcCtx.ConversationModel.Insert(l.ctx, &immodels.Conversation{
					ConversationId: conversationId,
					ChatType:       constants.SingleChatType,
				}); err != nil {
					return nil, errors.Wrapf(xerr.NewDBErr(), "ConversationModel.Insert err %v", err)
				}
			} else {
				return nil, errors.Wrapf(xerr.NewDBErr(), "ConversationModel.FindOne err %v, req %v", err, conversationId)
			}
		}
		_ = exists

		// Redis meta：双方都设为单聊
		_ = l.svcCtx.ConvSession.Meta().Set(conversationId, &redis.ConvMeta{
			ChatType:    int32(constants.SingleChatType),
			MemberCount: 2,
			Members:     []string{in.SendId, in.RecvId},
		})

		// 双方各自一份 state + MongoDB `conversations`
		if err := l.setUpUserConversation(conversationId, in.SendId, in.RecvId, constants.SingleChatType, true); err != nil {
			return nil, err
		}
		if err := l.setUpUserConversation(conversationId, in.RecvId, in.SendId, constants.SingleChatType, false); err != nil {
			return nil, err
		}

	case constants.GroupChatType:
		// 群：convId == groupId
		if err := l.setUpUserConversation(in.RecvId, in.SendId, in.RecvId, constants.GroupChatType, true); err != nil {
			return nil, err
		}
		// 群 meta 已经在 CreateGroupConversation 里建过
	}

	return res, nil
}

func (l *SetUpUserConversationLogic) setUpUserConversation(conversationId, userId, recvId string,
	chatType constants.ChatType, isShow bool) error {
	// 1. MongoDB conversations 持久化（兜底）
	conversations, err := l.svcCtx.ConversationsModel.FindByUserId(l.ctx, userId)
	if err != nil {
		if err == immodels.ErrNotFound {
			conversations = &immodels.Conversations{
				ID:               primitive.NewObjectID(),
				UserId:           userId,
				ConversationList: make(map[string]*immodels.Conversation),
			}
		} else {
			return errors.Wrapf(xerr.NewDBErr(), "ConversationsModel.FindByUserId err %v, req %v", err, userId)
		}
	}
	if _, ok := conversations.ConversationList[conversationId]; !ok {
		// 延迟双删：先删清单缓存 → 写 Mongo → 延迟再删
		if l.svcCtx.ConversationCache != nil {
			_ = l.svcCtx.ConversationCache.Del(userId)
		}
		conversations.ConversationList[conversationId] = &immodels.Conversation{
			ConversationId: conversationId,
			ChatType:       chatType,
			IsShow:         isShow,
		}
		if err := l.svcCtx.ConversationsModel.Update(l.ctx, conversations); err != nil {
			return errors.Wrapf(xerr.NewDBErr(), "ConversationsModel.Update err %v", err)
		}
		if l.svcCtx.ConversationCache != nil {
			l.svcCtx.ConversationCache.ScheduleSecondDelete(userId, 500*time.Millisecond)
		}
	}

	// 2. Redis state
	return l.svcCtx.ConvSession.State().SetFields(conversationId, userId, map[string]string{
		redis.ConvStateFieldIsShow:   boolStr(isShow),
		redis.ConvStateFieldReadSeq:  "0",
		redis.ConvStateFieldUnread:   "0",
		redis.ConvStateFieldCacheSeq: "0",
	})
}

func boolStr(b bool) string {
	if b {
		return "1"
	}
	return "0"
}
