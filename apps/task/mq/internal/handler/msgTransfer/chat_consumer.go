package msgTransfer

import (
	"context"
	"io"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/zeromicro/go-queue/kq"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/service"
)

// chatBatchConsumer 持续 Fetch，不在拉到消息时提交位点。
// 位点由批处理在落库成功后提交。
type chatBatchConsumer struct {
	reader *kafka.Reader
	h      *MsgChatTransfer
	cancel context.CancelFunc
}

func NewChatBatchConsumer(c kq.KqConf, h *MsgChatTransfer) service.Service {
	offset := kafka.LastOffset
	if c.Offset == "first" {
		offset = kafka.FirstOffset
	}
	cfg := kafka.ReaderConfig{
		Brokers:        c.Brokers,
		GroupID:        c.Group,
		Topic:          c.Topic,
		StartOffset:    offset,
		MinBytes:       c.MinBytes,
		MaxBytes:       c.MaxBytes,
		MaxWait:        time.Second,
		CommitInterval: 0, // 由我们在批次落库后同步 CommitMessages
	}
	if c.Username != "" && c.Password != "" {
		cfg.Dialer = &kafka.Dialer{
			SASLMechanism: plain.Mechanism{
				Username: c.Username,
				Password: c.Password,
			},
		}
	}
	reader := kafka.NewReader(cfg)
	commits := newOffsetCommitter(reader, c.Topic)
	h.commits = commits
	h.batchProcessor.onPersisted = func(msgs []kafka.Message) {
		commits.markDone(msgs...)
	}
	return &chatBatchConsumer{reader: reader, h: h}
}

func (c *chatBatchConsumer) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	logx.Infof("chat batch consumer started, topic=%s group=%s", c.reader.Config().Topic, c.reader.Config().GroupID)
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if err != context.Canceled && err != io.EOF {
				logx.Errorf("chat batch fetch: %v", err)
			}
			return
		}
		if err := c.h.accept(msg); err != nil {
			logx.Errorf("chat batch accept: %v", err)
		}
	}
}

func (c *chatBatchConsumer) Stop() {
	if c.cancel != nil {
		c.cancel()
	}
	if c.h != nil && c.h.batchProcessor != nil {
		c.h.batchProcessor.Stop()
	}
	if c.reader != nil {
		_ = c.reader.Close()
	}
}
