package mqclient

import (
	"easy-chat/apps/task/mq/mq"
	"encoding/json"

	"github.com/zeromicro/go-queue/kq"
)

type TopicRoundClient interface {
	Push(msg *mq.TopicRoundComplete) error
}

type topicRoundClient struct {
	pusher *kq.Pusher
}

func NewTopicRoundClient(addr []string, topic string, opts ...kq.PushOption) TopicRoundClient {
	if len(addr) == 0 || topic == "" {
		return nil
	}
	return &topicRoundClient{pusher: kq.NewPusher(addr, topic, opts...)}
}

func (c *topicRoundClient) Push(msg *mq.TopicRoundComplete) error {
	if c == nil || c.pusher == nil {
		return nil
	}
	body, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return c.pusher.Push(string(body))
}
