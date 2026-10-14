package logic

import (
	"context"
	"encoding/json"
	"easy-chat/apps/im/immodels"
	"easy-chat/apps/im/rpc/im"
	"easy-chat/apps/im/rpc/internal/svc"
	"easy-chat/pkg/redis"
	"easy-chat/pkg/xerr"

	"github.com/jinzhu/copier"
	"github.com/pkg/errors"
	"github.com/zeromicro/go-zero/core/logx"
)

type GetConversationsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetConversationsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetConversationsLogic {
	return &GetConversationsLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// GetConversations
//
// 流程：
//  1. 用户会话清单：旁路缓存 conv:list:{userId}，miss 回源 Mongo 并回填
//  2. 对每个会话从 Redis conv:meta / conv:hot 读元信息与热字段
//  3. 从 Redis conv:state 读用户水位线
//  4. 缺失 meta 从 MongoDB conversation 兜底
func (l *GetConversationsLogic) GetConversations(in *im.GetConversationsReq) (*im.GetConversationsResp, error) {
	// 1. 用户会话清单（旁路：先缓存，miss 再 Mongo 回填）
	data, err := l.loadUserConversations(in.UserId)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return &im.GetConversationsResp{}, nil
	}

	var res im.GetConversationsResp
	_ = copier.Copy(&res, &data)

	// 2. 收集 conversationIds
	convIds := make([]string, 0, len(data.ConversationList))
	for cid := range data.ConversationList {
		convIds = append(convIds, cid)
	}

	// 3. 批量读 Redis meta / hot
	metaMap := make(map[string]*redis.ConvMeta, len(convIds))
	hotMap := make(map[string]*redis.ConvHot, len(convIds))
	missIds := make([]string, 0)
	missSet := make(map[string]struct{})
	for _, cid := range convIds {
		m, _ := l.svcCtx.ConvSession.Meta().Get(cid)
		if m == nil {
			if _, ok := missSet[cid]; !ok {
				missSet[cid] = struct{}{}
				missIds = append(missIds, cid)
			}
		} else {
			metaMap[cid] = m
		}
		h, _ := l.svcCtx.ConvSession.Hot().Get(cid)
		if h != nil {
			hotMap[cid] = h
		} else if _, ok := missSet[cid]; !ok {
			missSet[cid] = struct{}{}
			missIds = append(missIds, cid)
		}
	}

	// 4. 兜底：meta/hot 缺失走 MongoDB conversation，并回填 Redis
	dbById := make(map[string]*immodels.Conversation)
	if len(missIds) > 0 {
		dbConversations, err := l.svcCtx.ConversationModel.ListByConversationIds(l.ctx, missIds)
		if err != nil {
			return nil, errors.Wrapf(xerr.NewDBErr(), "ConversationModel.ListByConversationIds err %v, req %v", err, missIds)
		}
		metaFill := make(map[string]*redis.ConvMeta, len(dbConversations))
		hotFill := make(map[string]*redis.ConvHot, len(dbConversations))
		for _, c := range dbConversations {
			dbById[c.ConversationId] = c
			if _, ok := metaMap[c.ConversationId]; !ok {
				metaFill[c.ConversationId] = &redis.ConvMeta{ChatType: int32(c.ChatType)}
			}
			if _, ok := hotMap[c.ConversationId]; !ok {
				h := hotFromMongoConversation(c)
				l.attachLastMsgReadFromSet(c.ConversationId, h)
				hotMap[c.ConversationId] = h
				hotFill[c.ConversationId] = h
			}
		}
		go func() {
			if len(metaFill) > 0 {
				_ = l.svcCtx.ConvSession.Meta().PipelineSet(context.Background(), metaFill)
			}
			for cid, h := range hotFill {
				_ = l.svcCtx.ConvSession.Hot().Set(cid, h)
			}
		}()
	}

	// 5. 逐会话合并：未读 = seq - readSeq；已读人数只来自 msg:read:last（集合不在则不返回 Read）
	for cid, conv := range res.ConversationList {
		_ = metaMap
		h := hotMap[cid]
		if h == nil {
			if dbc, ok := dbById[cid]; ok {
				h = hotFromMongoConversation(dbc)
				l.attachLastMsgReadFromSet(cid, h)
			}
		}
		if h != nil {
			conv.Seq = h.Seq
			conv.Total = int32(h.Total)
			if conv.Msg == nil {
				conv.Msg = &im.ChatLog{}
			}
			if h.LastMsgPreview != "" {
				conv.Msg.MsgContent = h.LastMsgPreview
			}
			if h.LastMsgTime > 0 {
				conv.Msg.SendTime = h.LastMsgTime
			}
			if n, ok := l.lastMsgReadCount(cid, h); ok {
				conv.Read = n
			}
		}

		readSeq := l.loadReadSeq(cid, in.UserId)
		seq := conv.Seq
		if seq < readSeq {
			conv.ToRead = 0
		} else {
			unread := seq - readSeq
			if unread > 1<<31-1 {
				conv.ToRead = int32(1<<31 - 1)
			} else {
				conv.ToRead = int32(unread)
			}
		}
		if state, _ := l.svcCtx.ConvSession.State().Get(cid, in.UserId); state != nil {
			conv.IsShow = state.IsShow
		} else {
			conv.IsShow = true
		}
	}

	return &res, nil
}

func (l *GetConversationsLogic) lastMsgReadCount(cid string, h *redis.ConvHot) (int32, bool) {
	n, ok := l.lastMsgReadFromSet(cid)
	if !ok {
		return 0, false
	}
	if l.svcCtx.ConvSession != nil && (h == nil || !h.HasLastMsgReadCount || h.LastMsgReadCount != n) {
		_ = l.svcCtx.ConvSession.Hot().UpdateReadCount(cid, n)
	}
	if n <= 0 {
		return 0, false
	}
	return int32(n), true
}

func (l *GetConversationsLogic) attachLastMsgReadFromSet(cid string, h *redis.ConvHot) {
	if h == nil {
		return
	}
	n, ok := l.lastMsgReadFromSet(cid)
	if !ok {
		return
	}
	h.LastMsgReadCount = n
	h.HasLastMsgReadCount = true
}

func (l *GetConversationsLogic) lastMsgReadFromSet(cid string) (int64, bool) {
	if l.svcCtx.ConvSession == nil || l.svcCtx.ConvSession.LastRead() == nil {
		return 0, false
	}
	n, exists, err := l.svcCtx.ConvSession.LastRead().CardIfExists(cid)
	if err != nil || !exists {
		return 0, false
	}
	return n, true
}

func hotFromMongoConversation(c *immodels.Conversation) *redis.ConvHot {
	h := &redis.ConvHot{}
	if c == nil {
		return h
	}
	h.Seq = c.Seq
	h.Total = int64(c.Total)
	if c.Msg != nil {
		if !c.Msg.ID.IsZero() {
			h.LastMsgId = c.Msg.ID.Hex()
		}
		h.LastMsgTime = c.Msg.SendTime
		content := c.Msg.MsgContent
		if len(content) > 64 {
			content = content[:64]
		}
		h.LastMsgPreview = content
	}
	return h
}

func (l *GetConversationsLogic) loadReadSeq(conversationId, userId string) int64 {
	if st, _ := l.svcCtx.ConvSession.State().Get(conversationId, userId); st != nil {
		return st.ReadSeq
	}
	if l.svcCtx.UserConversationReadModel == nil {
		return 0
	}
	rec, err := l.svcCtx.UserConversationReadModel.FindByUserAndConversation(l.ctx, userId, conversationId)
	if err != nil || rec == nil {
		return 0
	}
	return rec.ReadSeq
}

// loadUserConversations 旁路加载用户会话清单（不含热字段，热字段仍走 conv:hot/state）
func (l *GetConversationsLogic) loadUserConversations(userId string) (*immodels.Conversations, error) {
	if l.svcCtx.ConversationCache != nil {
		if raw, hit, err := l.svcCtx.ConversationCache.Get(userId); err == nil && hit {
			var cached immodels.Conversations
			if json.Unmarshal(raw, &cached) == nil && cached.UserId != "" {
				return &cached, nil
			}
		}
	}

	data, err := l.svcCtx.ConversationsModel.FindByUserId(l.ctx, userId)
	if err != nil {
		if err == immodels.ErrNotFound {
			return nil, nil
		}
		return nil, errors.Wrapf(xerr.NewDBErr(), "ConversationsModel.FindByUserId err %v, req %v", err, userId)
	}

	if l.svcCtx.ConversationCache != nil {
		_ = l.svcCtx.ConversationCache.Set(userId, data)
	}
	return data, nil
}
