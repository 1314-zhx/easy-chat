/**
 * @author: dn-jinmin/dn-jinmin
 * @doc:
 */

package push

import (
	"easy-chat/apps/im/ws/internal/svc"
	"easy-chat/apps/im/ws/websocket"
	"easy-chat/apps/im/ws/ws"
	"easy-chat/pkg/constants"
	"easy-chat/pkg/metrics"
	"time"

	"github.com/mitchellh/mapstructure"
)

// ws 的消息推送
func Push(svc *svc.ServiceContext) websocket.HandlerFunc {
	return func(srv *websocket.Server, conn *websocket.Conn, msg *websocket.Message) {
		pushStart := time.Now() // 埋点: 推送开始时间

		var data ws.Push
		if err := mapstructure.Decode(msg.Data, &data); err != nil {
			srv.Send(websocket.NewErrMessage(err))
			// 埋点: 推送失败 - 参数解析错误
			metrics.MessagePushTotal.WithLabelValues("unknown", "failed").Inc()
			metrics.ErrorTotal.WithLabelValues("validation").Inc()
			return
		}

		// 埋点: 消息大小
		metrics.MessageSize.WithLabelValues(data.ChatType.String(), "text").Observe(float64(len(data.Content)))

		switch data.ChatType {
		case constants.SingleChatType:
			if err := single(svc, srv, &data, data.RecvId); err != nil {
				metrics.MessagePushTotal.WithLabelValues(data.ChatType.String(), "failed").Inc()
			} else {
				metrics.MessagePushTotal.WithLabelValues(data.ChatType.String(), "success").Inc()
			}
		case constants.GroupChatType:
			if err := group(svc, srv, &data); err != nil {
				metrics.MessagePushTotal.WithLabelValues(data.ChatType.String(), "failed").Inc()
			} else {
				metrics.MessagePushTotal.WithLabelValues(data.ChatType.String(), "success").Inc()
			}
		}

		// 埋点: 推送延迟 (从收到push指令到处理完成)
		metrics.MessagePushLatency.WithLabelValues(data.ChatType.String(), "send").Observe(
			float64(time.Since(pushStart).Milliseconds()),
		)
	}
}

// 给 mq 或者客户端发消息
func single(svc *svc.ServiceContext, srv *websocket.Server, data *ws.Push, recvId string) error {
	rconn := srv.GetConn(recvId)
	if rconn == nil {
		return forwardPush(srv, data, recvId)
	}

	srv.Infof("push msg %v", data)

	active := rconn.IsActiveConv(data.ConversationId)
	if data.ContentType == constants.ContentMarkRead {
		if !active {
			return nil
		}
		return srv.Send(websocket.NewMessage(data.SendId, data), rconn)
	}
	if active {
		if svc != nil && svc.ConvSession != nil && data.Seq > 0 {
			_, _ = svc.ConvSession.State().AdvanceReadSeq(data.ConversationId, recvId, data.Seq)
		}
		return srv.Send(websocket.NewMessage(data.SendId, &ws.Chat{
			ConversationId: data.ConversationId,
			ChatType:       data.ChatType,
			SendTime:       data.SendTime,
			Msg: ws.Msg{
				MsgId:   data.MsgId,
				Seq:     data.Seq,
				MType:   data.MType,
				Content: data.Content,
			},
		}), rconn)
	}

	// 不在房：推列表增量（lastMsg + seq + unread），不推聊天气泡
	unread := int64(0)
	if svc != nil && svc.ConvSession != nil && data.Seq > 0 {
		readSeq, _ := svc.ConvSession.EnsureReadSeq(data.ConversationId, recvId)
		if data.Seq > readSeq {
			unread = data.Seq - readSeq
		}
	}
	return srv.Send(websocket.NewMessage(data.SendId, &ws.Push{
		ConversationId: data.ConversationId,
		ChatType:       data.ChatType,
		SendId:         data.SendId,
		RecvId:         data.RecvId,
		SendTime:       data.SendTime,
		MsgId:          data.MsgId,
		Seq:            data.Seq,
		Unread:         unread,
		MType:          data.MType,
		Content:        data.Content,
		ContentType:    constants.ContentConversationRefresh,
	}), rconn)
}

func forwardPush(srv *websocket.Server, data *ws.Push, recvId string) error {
	if srv == nil || data == nil || recvId == "" {
		return nil
	}
	msg := websocket.NewMessage(data.SendId, data)
	msg.Method = "push"
	return srv.ForwardToUser(recvId, msg)
}

func group(svc *svc.ServiceContext, srv *websocket.Server, data *ws.Push) error {
	for _, id := range data.RecvIds {
		uid := id
		srv.Schedule(func() {
			single(svc, srv, data, uid)
		})
	}
	return nil
}
