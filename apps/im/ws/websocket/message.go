/**
 * @author: dn-jinmin/dn-jinmin
 * @doc:
 */

package websocket

type FrameType uint8

const (
	FrameData  FrameType = 0x0
	FramePing  FrameType = 0x1
	FrameAck   FrameType = 0x2
	FrameNoAck FrameType = 0x3
	FrameErr   FrameType = 0x9
	FrameDbAck FrameType = 0xA // MongoDB 落库成功通知（用于 RigorAck 三次 ACK）
)

// FrameType 详细说明：
// - FrameData: 普通数据帧
// - FramePing: 心跳帧
// - FrameAck: ACK 确认帧（用于握手）
// - FrameNoAck: 不需要确认的帧
// - FrameErr: 错误帧
// - FrameDbAck: MongoDB 落库成功通知帧（用于 RigorAck 三次 ACK）

// AckSeq 协议说明：
// - 初始值 0：客户端发送消息
// - AckSeq=1：服务端第一次 ACK（收到消息）
// - AckSeq=2：服务端第三次 ACK（MongoDB 落库成功，仅 RigorAck）
type Message struct {
	FrameType `json:"frameType"`
	Id        string `json:"id"`
	AckSeq    int    `json:"ackSeq"`

	// ClientMsgId: 客户端原始消息 ID，用于追踪 MongoDB 落库结果（RigorAck）
	ClientMsgId string `json:"clientMsgId"`

	Method string      `json:"method"` // 标识要处理的方法
	FormId string      `json:"formId"` // 消息请求的来源，用于服务方给客户方发送
	Data   interface{} `json:"data"` // map[string]interface{}
}

func NewMessage(formId string, data interface{}) *Message {
	return &Message{
		FrameType: FrameData,
		FormId:    formId,
		Data:      data,
	}
}

func NewErrMessage(err error) *Message {
	return &Message{
		FrameType: FrameErr,
		Data:      err.Error(),
	}
}
