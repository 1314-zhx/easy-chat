package msgTransfer

import (
	"context"
	"easy-chat/apps/im/ws/ws"
	"easy-chat/apps/social/rpc/socialclient"
	"easy-chat/apps/task/mq/internal/svc"
	"easy-chat/apps/task/mq/mq"
	"easy-chat/pkg/constants"
	"encoding/json"
	"sync"
	"time"

	"github.com/zeromicro/go-queue/kq"
)

var (
	GroupMsgReadRecordDelayTime  = time.Second
	GroupMsgReadRecordDelayCount = 10
)

const (
	GroupMsgReadHandlerAtTransfer = iota
	GroupMsgReadHandlerDelayTransfer
)

type MsgReadTransfer struct {
	*baseMsgTransfer
	mu        sync.Mutex
	groupMsgs map[string]*groupMsgRead
	push      chan *ws.Push
}

func NewMsgReadTransfer(svc *svc.ServiceContext) kq.ConsumeHandler {
	m := &MsgReadTransfer{
		baseMsgTransfer: NewBaseMsgTransfer(svc),
		groupMsgs:       make(map[string]*groupMsgRead, 1),
		push:            make(chan *ws.Push, 64),
	}
	if svc.Config.MsgReadHandler.GroupMsgReadHandler != GroupMsgReadHandlerAtTransfer {
		if svc.Config.MsgReadHandler.GroupMsgReadRecordDelayCount > 0 {
			GroupMsgReadRecordDelayCount = svc.Config.MsgReadHandler.GroupMsgReadRecordDelayCount
		}
		if svc.Config.MsgReadHandler.GroupMsgReadRecordDelayTime > 0 {
			GroupMsgReadRecordDelayTime = time.Duration(svc.Config.MsgReadHandler.GroupMsgReadRecordDelayTime) * time.Second
		}
	}
	go m.transfer()
	return m
}

func (m *MsgReadTransfer) Consume(key, value string) error {
	m.Infof("MsgReadTransfer %v %v", key, value)
	var data mq.MsgMarkRead
	if err := json.Unmarshal([]byte(value), &data); err != nil {
		return err
	}
	if err := m.UpdateChatLogRead(context.Background(), &data); err != nil {
		return err
	}
	push := &ws.Push{
		ConversationId: data.ConversationId,
		ChatType:       data.ChatType,
		SendId:         data.SendId,
		RecvId:         data.RecvId,
		ContentType:    constants.ContentMarkRead,
	}
	// 最后一条消息已读数：用户已读游标 >= 当前最新 seq 才累计
	if m.svcCtx.ConvSession != nil && data.MsgId != "" {
		if hot, err := m.svcCtx.ConvSession.Hot().Get(data.ConversationId); err == nil && hot != nil {
			if added, _, err := m.svcCtx.ConvSession.Hot().MarkLastMsgReadAtomic(
				data.ConversationId,
				data.LastReadSeq,
				data.SendId,
				m.svcCtx.ConvSession.LastRead(),
			); err != nil {
				m.Errorf("mark last message read error: %v", err)
			} else if !added && hot.LastMsgId == data.MsgId {
				m.Infof("duplicate last message read ignored, conversation=%s, user=%s", data.ConversationId, data.SendId)
			}
		}
	}
	if data.ChatType == constants.SingleChatType {
		m.push <- push
		return nil
	}
	pushMode := m.decidePushMode(data.RecvId)
	if pushMode == GroupMsgReadHandlerAtTransfer {
		m.push <- push
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	push.SendId = ""
	if g, ok := m.groupMsgs[push.ConversationId]; ok {
		if !g.mergePush(push) {
			delete(m.groupMsgs, push.ConversationId)
			m.groupMsgs[push.ConversationId] = newGroupMsgRead(push, m.push)
		}
	} else {
		m.groupMsgs[push.ConversationId] = newGroupMsgRead(push, m.push)
	}
	return nil
}

func (m *MsgReadTransfer) UpdateChatLogRead(ctx context.Context, data *mq.MsgMarkRead) error {
	if data.LastReadSeq <= 0 {
		return nil
	}
	// 1. v3 state: readSeq + unread=0
	if m.svcCtx.ConvSession != nil {
		if err := m.svcCtx.ConvSession.MarkRead(data.ConversationId, data.SendId, data.LastReadSeq); err != nil {
			m.Errorf("mark read error: %v", err)
		}
	}
	return nil
}

func (m *MsgReadTransfer) decidePushMode(groupId string) int {
	threshold := m.svcCtx.Config.MsgReadHandler.GroupMsgReadAutoMergeThreshold
	if threshold <= 0 {
		return m.svcCtx.Config.MsgReadHandler.GroupMsgReadHandler
	}
	groupUsers, err := m.svcCtx.Social.GroupUsers(context.Background(), &socialclient.GroupUsersReq{GroupId: groupId})
	if err != nil || len(groupUsers.List) == 0 {
		return GroupMsgReadHandlerDelayTransfer
	}
	if len(groupUsers.List) > int(threshold) {
		return GroupMsgReadHandlerDelayTransfer
	}
	return GroupMsgReadHandlerAtTransfer
}

func (m *MsgReadTransfer) transfer() {
	for push := range m.push {
		if push.RecvId != "" || len(push.RecvIds) > 0 {
			if err := m.Transfer(context.Background(), push); err != nil {
				m.Errorf("m transfer err %v push %v", err, push)
			}
		}
		if push.ChatType == constants.SingleChatType {
			continue
		}
		if m.svcCtx.Config.MsgReadHandler.GroupMsgReadHandler == GroupMsgReadHandlerAtTransfer {
			continue
		}
		m.mu.Lock()
		if _, ok := m.groupMsgs[push.ConversationId]; ok && m.groupMsgs[push.ConversationId].IsIdle() {
			m.groupMsgs[push.ConversationId].clear()
			delete(m.groupMsgs, push.ConversationId)
		}
		m.mu.Unlock()
	}
}
