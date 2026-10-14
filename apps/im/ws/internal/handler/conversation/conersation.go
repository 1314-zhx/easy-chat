/**
 * @author: dn-jinmin/dn-jinmin
 * @doc:
 */

package conversation

import (
	"easy-chat/apps/im/ws/internal/svc"
	"easy-chat/apps/im/ws/websocket"
	"easy-chat/apps/im/ws/ws"
	"easy-chat/apps/task/mq/mq"
	"easy-chat/pkg/constants"
	"easy-chat/pkg/qwen"
	"easy-chat/pkg/wuid"
	"fmt"
	"time"

	"github.com/mitchellh/mapstructure"
)

func Chat(svc *svc.ServiceContext) websocket.HandlerFunc {
	return func(srv *websocket.Server, conn *websocket.Conn, msg *websocket.Message) {
		fmt.Printf(">>> [DEBUG] Chat handler called. Method: %s, Data: %+v\n", msg.Method, msg.Data)

		var data ws.Chat
		if err := mapstructure.Decode(msg.Data, &data); err != nil {
			fmt.Printf(">>> [ERROR] mapstructure.Decode failed: %v\n", err)
			srv.Send(websocket.NewErrMessage(err), conn)
			conn.NotifyKafkaResult(msg.Id, err)
			return
		}

		fmt.Printf(">>> [DEBUG] Decoded data: RecvId=%s, ChatType=%d, Msg=%+v\n",
			data.RecvId, data.ChatType, data.Msg)

		if data.ConversationId == "" {
			switch data.ChatType {
			case constants.SingleChatType:
				if qwen.IsAIMessage(data.RecvId) {
					data.ConversationId = fmt.Sprintf("ai_conv_%s_%d", conn.Uid, time.Now().UnixNano())
				} else {
					data.ConversationId = wuid.CombineId(conn.Uid, data.RecvId)
				}
			case constants.GroupChatType:
				data.ConversationId = data.RecvId
			}
		}

		fmt.Printf(">>> [DEBUG] Sending to Kafka. ConversationId=%s, SendId=%s, RecvId=%s\n",
			data.ConversationId, conn.Uid, data.RecvId)

		err := svc.MsgChatTransferClient.Push(&mq.MsgChatTransfer{
			ConversationId: data.ConversationId,
			ChatType:       data.ChatType,
			SendId:         conn.Uid,
			RecvId:         data.RecvId,
			SendTime:       time.Now().UnixNano(),
			MType:          data.Msg.MType,
			Content:        data.Msg.Content,
			ClientMsgId:    msg.Id, // 传递客户端消息 ID，用于 ACK 追踪
		})
		if err != nil {
			fmt.Printf(">>> [ERROR] Kafka Push failed: %v\n", err)
			srv.Send(websocket.NewErrMessage(err), conn)
			// Kafka 投递失败，客户端会超时重发
			return
		}

		fmt.Printf(">>> [SUCCESS] Message sent to Kafka successfully\n")

		// RigorAck: 存入 ShardedAckMap，等待 MongoDB 落库成功后发送 ACK
		if srv.IsRigorAck() {
			srv.StoreToShardedAckMap(msg.Id, msg)
		}
	}
}

func MarkRead(svc *svc.ServiceContext) websocket.HandlerFunc {
	return func(srv *websocket.Server, conn *websocket.Conn, msg *websocket.Message) {
		var data ws.MarkRead
		if err := mapstructure.Decode(msg.Data, &data); err != nil {
			srv.Send(websocket.NewErrMessage(err), conn)
			return
		}
		if data.ConversationId != "" && svc.ConvSession != nil {
			_ = svc.ConvSession.MarkRead(data.ConversationId, conn.Uid, data.LastReadSeq)
			if data.MsgId != "" && data.LastReadSeq > 0 {
				if hot, err := svc.ConvSession.Hot().Get(data.ConversationId); err == nil && hot != nil {
					_, _, _ = svc.ConvSession.Hot().MarkLastMsgReadAtomic(
						data.ConversationId, data.LastReadSeq, conn.Uid, svc.ConvSession.LastRead(),
					)
				}
			}
		}
		err := svc.MsgReadTransferClient.Push(&mq.MsgMarkRead{
			ChatType:       data.ChatType,
			ConversationId: data.ConversationId,
			SendId:         conn.Uid,
			RecvId:         data.RecvId,
			LastReadSeq:    data.LastReadSeq,
			MsgId:          data.MsgId,
		})
		if err != nil {
			srv.Send(websocket.NewErrMessage(err), conn)
			return
		}
	}
}
