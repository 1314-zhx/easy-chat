package conversation

import (
	"easy-chat/apps/im/ws/internal/svc"
	"easy-chat/apps/im/ws/websocket"

	"github.com/mitchellh/mapstructure"
)

type conversationActive struct {
	ConversationId string `mapstructure:"conversationId"`
}

func Activate(svc *svc.ServiceContext) websocket.HandlerFunc {
	return func(srv *websocket.Server, conn *websocket.Conn, msg *websocket.Message) {
		var data conversationActive
		if err := mapstructure.Decode(msg.Data, &data); err != nil {
			srv.Send(websocket.NewErrMessage(err), conn)
			return
		}
		if data.ConversationId == "" {
			return
		}
		conn.AddActiveConv(data.ConversationId)
		if svc.ActiveConversationCache != nil {
			_ = svc.ActiveConversationCache.Add(conn.Uid, data.ConversationId)
		}
		if svc.ConvSession == nil {
			return
		}
		if err := svc.ConvSession.LoadCursorOnEnter(data.ConversationId, conn.Uid); err != nil {
			srv.Send(websocket.NewErrMessage(err), conn)
			return
		}
		items, fromCache, err := svc.ConvSession.RecentOnEnter(data.ConversationId, conn.Uid)
		if err != nil {
			srv.Send(websocket.NewErrMessage(err), conn)
			return
		}
		srv.Send(&websocket.Message{
			FrameType: websocket.FrameData,
			Method:    "conversation.snapshot",
			Data: map[string]interface{}{
				"conversationId": data.ConversationId,
				"fromCache":      fromCache,
				"list":           items,
			},
		}, conn)
	}
}

func Deactivate(svc *svc.ServiceContext) websocket.HandlerFunc {
	return func(srv *websocket.Server, conn *websocket.Conn, msg *websocket.Message) {
		var data conversationActive
		if err := mapstructure.Decode(msg.Data, &data); err != nil {
			srv.Send(websocket.NewErrMessage(err), conn)
			return
		}
		if data.ConversationId == "" {
			return
		}
		conn.RemoveActiveConv(data.ConversationId)
		if svc.ActiveConversationCache != nil {
			// 只移除该会话，保留用户其它打开中的会话
			_ = svc.ActiveConversationCache.Remove(conn.Uid, data.ConversationId)
		}
		if svc.ConvSession != nil {
			_ = svc.ConvSession.FlushCursor(data.ConversationId, conn.Uid)
		}
	}
}
