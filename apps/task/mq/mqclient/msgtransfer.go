/**
 * @author: dn-jinmin/dn-jinmin
 * @doc:
 */

package mqclient

import (
	"context"
	"easy-chat/apps/task/mq/mq"
	"encoding/json"
	"time"

	kafka "github.com/segmentio/kafka-go"
	"github.com/zeromicro/go-queue/kq"
)

type MsgChatTransferClient interface {
	Push(msg *mq.MsgChatTransfer) error
}

type keyedWriter struct {
	w *kafka.Writer
}

func newKeyedWriter(addrs []string, topic string) *keyedWriter {
	return &keyedWriter{
		w: &kafka.Writer{
			Addr:         kafka.TCP(addrs...),
			Topic:        topic,
			Balancer:     &kafka.Hash{},
			RequiredAcks: kafka.RequireOne,
			Async:        false,
		},
	}
}

func (p *keyedWriter) push(key, body string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return p.w.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: []byte(body),
	})
}

type msgChatTransferClient struct {
	keyed  *keyedWriter
	pusher *kq.Pusher
}

func NewMsgChatTransferClient(addr []string, topic string, opts ...kq.PushOption) MsgChatTransferClient {
	c := &msgChatTransferClient{}
	if len(addr) > 0 && topic != "" {
		c.keyed = newKeyedWriter(addr, topic)
	} else {
		c.pusher = kq.NewPusher(addr, topic, opts...)
	}
	return c
}

func (c *msgChatTransferClient) Push(msg *mq.MsgChatTransfer) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	key := msg.ConversationId
	if key == "" {
		key = msg.ClientMsgId
	}
	if c.keyed != nil {
		return c.keyed.push(key, string(body))
	}
	return c.pusher.Push(string(body))
}

type MsgReadTransferClient interface {
	Push(msg *mq.MsgMarkRead) error
}
type msgReadTransferClient struct {
	keyed  *keyedWriter
	pusher *kq.Pusher
}

func NewMsgReadTransferClient(addr []string, topic string, opts ...kq.PushOption) MsgReadTransferClient {
	c := &msgReadTransferClient{}
	if len(addr) > 0 && topic != "" {
		c.keyed = newKeyedWriter(addr, topic)
	} else {
		c.pusher = kq.NewPusher(addr, topic, opts...)
	}
	return c
}

func (c *msgReadTransferClient) Push(msg *mq.MsgMarkRead) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	key := msg.ConversationId
	if c.keyed != nil {
		return c.keyed.push(key, string(body))
	}
	return c.pusher.Push(string(body))
}
