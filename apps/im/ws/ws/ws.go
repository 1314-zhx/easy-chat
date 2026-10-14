/**
 * @author: dn-jinmin/dn-jinmin
 * @doc:
 */

package ws

import "easy-chat/pkg/constants"

// 利用 mapstructure 将 map[string]interface{} 转化为所需要的结构，因为发送来的 data 是一个 interface{}
// mapstructure 的核心优势就在于它能处理这种“未知类型的接口”，并自动完成类型断言和类型转换
type (
	//  消息的内容和类型
	Msg struct {
		MsgId           string            `json:"msgId"`
		Seq             int64             `json:"seq,omitempty" mapstructure:"seq"`
		ReadRecords     map[string]string `mapstructure:"readRecords"`
		constants.MType `mapstructure:"mType"`
		Content         string `mapstructure:"content"`
	}
	// 用户聊天会话，发送方和接收方，发送时间和消息内容
	Chat struct {
		ConversationId     string `mapstructure:"conversationId"`
		constants.ChatType `mapstructure:"chatType"`
		SendId             string `mapstructure:"sendId"`
		RecvId             string `mapstructure:"recvId"`
		SendTime           int64  `mapstructure:"sendTime"`
		Msg                `mapstructure:"msg"`
	}

	Push struct {
		ConversationId     string `mapstructure:"conversationId"`
		constants.ChatType `mapstructure:"chatType"`
		SendId             string   `mapstructure:"sendId"`
		RecvId             string   `mapstructure:"recvId"`
		RecvIds            []string `mapstructure:"recvIds"`
		SendTime           int64    `mapstructure:"sendTime"`

		MsgId string `mapstructure:"msgId"`
		Seq   int64  `mapstructure:"seq" json:"seq,omitempty"`
		// Unread 会话列表未读（仅在线不在房的 refresh 推送使用）
		Unread int64 `mapstructure:"unread" json:"unread,omitempty"`

		ReadRecords map[string]string     `mapstructure:"readRecords"`
		ContentType constants.ContentType `mapstructure:"contentType"`

		constants.MType `mapstructure:"mType"`
		Content         string `mapstructure:"content"`
	}
	// 客户端给服务端发送的已读情况，当前会话读到了哪一个游标
	MarkRead struct {
		constants.ChatType `mapstructure:"chatType"`
		RecvId             string `mapstructure:"recvId"`
		ConversationId     string `mapstructure:"conversationId"` // 会话 Id
		LastReadSeq        int64  `mapstructure:"lastReadSeq"`
		MsgId              string `mapstructure:"msgId"`           // 已读的消息 ID
	}
)
