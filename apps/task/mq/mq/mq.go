/**
 * @author: dn-jinmin/dn-jinmin
 * @doc:
 */

package mq

import "easy-chat/pkg/constants"

// MsgChatTransfer 聊天消息传输结构
// ClientMsgId: 客户端生成的消息 ID，用于三次 ACK 追踪
// 只有当 MongoDB 落库成功或 WebSocket 推送成功时，才返回 ACK 给客户端
type MsgChatTransfer struct {
	ConversationId string `json:"conversationId"`
	ChatType       constants.ChatType `json:"chatType"`
	SendId         string   `json:"sendId"`
	RecvId         string   `json:"recvId"`
	RecvIds        []string `json:"recvIds"`
	SendTime       int64    `json:"sendTime"`
	ClientMsgId    string   `json:"clientMsgId"` // 客户端消息 ID，用于三次 ACK 追踪

	MType  constants.MType `json:"mType"`
	Content string `json:"content"`
}
type MsgMarkRead struct {
	constants.ChatType `json:"chatType"`
	ConversationId     string `json:"conversationId"`
	SendId             string `json:"sendId"`
	RecvId             string `json:"recvId"`
	LastReadSeq        int64  `json:"lastReadSeq"`
	MsgId              string `json:"msgId"` // 已读的消息 ID，用于精确匹配最后一条消息
}

// TopicRoundComplete AI 一轮对话（用户问+AI答）完成事件。
type TopicRoundComplete struct {
	ConversationId string `json:"conversationId"`
	UserId         string `json:"userId"`
	RoundId        string `json:"roundId"`
	UserMsgId      string `json:"userMsgId"`
	AiMsgId        string `json:"aiMsgId"`
	UserContent    string `json:"userContent"`
	AiContent      string `json:"aiContent"`
}
