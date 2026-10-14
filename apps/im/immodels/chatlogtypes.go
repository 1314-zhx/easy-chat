package immodels

import (
	"easy-chat/pkg/constants"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

var DefaultChatLogLimit int64 = 100

type ChatLog struct {
	ID primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`

	ConversationId string             `bson:"conversationId"`
	SendId         string             `bson:"sendId"`
	RecvId         string             `bson:"recvId"`
	MsgFrom        int                `bson:"msgFrom"`
	ChatType       constants.ChatType `bson:"chatType"`
	MsgType        constants.MType    `bson:"msgType"`
	MsgContent     string             `bson:"msgContent"`
	SendTime       int64              `bson:"sendTime"`
	Seq            int64              `bson:"seq"` // 会话内递增序号，用于已读游标
	Status         int                `bson:"status"`
	ReadRecords    []byte             `bson:"readRecords,omitempty"` // 废弃，保留字段兼容旧数据
	TopicId        string             `bson:"topicId,omitempty"`
	RoundId        string             `bson:"roundId,omitempty"`
	ClientMsgId    string             `bson:"clientMsgId,omitempty"`

	// TODO: Fill your own fields
	UpdateAt time.Time `bson:"updateAt,omitempty" json:"updateAt,omitempty"`
	CreateAt time.Time `bson:"createAt,omitempty" json:"createAt,omitempty"`
}
