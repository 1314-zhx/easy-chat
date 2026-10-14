/**
 * @author: dn-jinmin/dn-jinmin
 * @doc:
 */

package websocket

import (
	"encoding/json"
	"net/url"

	"github.com/gorilla/websocket"
)

type Client interface {
	Close() error

	Send(v any) error
	Read(v any) error

	// SendDbAck 发送 MongoDB 落库成功通知（用于三次 ACK）
	SendDbAck(clientMsgId string, dbErr error) error
}

type client struct {
	*websocket.Conn
	host string

	opt dailOption
}

func NewClient(host string, opts ...DailOptions) *client {
	opt := newDailOptions(opts...)

	c := client{
		Conn: nil,
		host: host,
		opt:  opt,
	}

	conn, err := c.dail()
	if err != nil {
		panic(err)
	}

	c.Conn = conn
	return &c
}

// 获取一个连接对象
func (c *client) dail() (*websocket.Conn, error) {
	u := url.URL{Scheme: "ws", Host: c.host, Path: c.opt.pattern}
	conn, _, err := websocket.DefaultDialer.Dial(u.String(), c.opt.header)
	return conn, err
}

// SendWithClientMsgId 发送消息，并携带客户端消息 ID（用于三次 ACK 追踪）
// 用于 Kafka Consumer 发送 MongoDB 落库成功通知
func (c *client) SendWithClientMsgId(v any, clientMsgId string) error {
	// 先调用普通的 Send
	if err := c.Send(v); err != nil {
		return err
	}

	// 然后发送 MongoDB 落库成功通知（FrameDbAck）
	ackMsg := map[string]interface{}{
		"frameType":   FrameDbAck,
		"clientMsgId": clientMsgId,
		"err":         nil,
	}

	data, err := json.Marshal(ackMsg)
	if err != nil {
		return err
	}

	err = c.WriteMessage(websocket.TextMessage, data)
	if err == nil {
		return nil
	}

	// 如果失败，重连再发送
	conn, err := c.dail()
	if err != nil {
		return err
	}
	c.Conn = conn
	return c.WriteMessage(websocket.TextMessage, data)
}

// SendDbAck 发送 MongoDB 落库成功通知
// clientMsgId: 客户端消息 ID
// err: MongoDB 错误，如果有的话
func (c *client) SendDbAck(clientMsgId string, dbErr error) error {
	var errStr string
	if dbErr != nil {
		errStr = dbErr.Error()
	}

	ackMsg := map[string]interface{}{
		"frameType":   FrameDbAck,
		"clientMsgId": clientMsgId,
		"data":        errStr, // 如果没有错误，这里是空字符串
	}

	data, err := json.Marshal(ackMsg)
	if err != nil {
		return err
	}

	err = c.WriteMessage(websocket.TextMessage, data)
	if err == nil {
		return nil
	}

	// 如果失败，重连再发送
	conn, err := c.dail()
	if err != nil {
		return err
	}
	c.Conn = conn
	return c.WriteMessage(websocket.TextMessage, data)
}

func (c *client) Send(v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}

	err = c.WriteMessage(websocket.TextMessage, data)
	// 如果成功，直接返回
	if err == nil {
		return nil
	}

	// 如果失败，再增加一个重连发送
	conn, err := c.dail()
	if err != nil {
		return err
	}
	c.Conn = conn
	return c.WriteMessage(websocket.TextMessage, data)
}

// 传递的一般是某种数据结构的指针，将消息绑定到这个指针上
func (c *client) Read(v any) error {
	_, msg, err := c.Conn.ReadMessage()
	if err != nil {
		return err
	}

	return json.Unmarshal(msg, v)
}
