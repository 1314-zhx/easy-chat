package dlq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
	"easy-chat/apps/task/mq/pkg/retry"
)

// KafkaDLQProducer Kafka 死信队列生产者
type KafkaDLQProducer struct {
	brokers []string
	topic   string // 死信 topic 名称，如 "msgChatTransferDLT"
	producer sarama.SyncProducer
}

// NewKafkaDLQProducer 创建 Kafka 死信队列生产者
func NewKafkaDLQProducer(brokers []string, topic string) (*KafkaDLQProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll       // 等所有副本确认
	config.Producer.Retry.Max = 3                           // 重试 3 次
	config.Producer.Return.Successes = true                 // 返回成功
	config.Producer.Return.Errors = true                    // 返回错误
	config.Producer.Compression = sarama.CompressionSnappy // 压缩

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("create DLQ producer failed: %w", err)
	}

	return &KafkaDLQProducer{
		brokers:  brokers,
		topic:    topic,
		producer: producer,
	}, nil
}

// SendToDLQ 发送消息到死信队列
func (p *KafkaDLQProducer) SendToDLQ(ctx context.Context, topic, key, value, errMsg string) error {
	dlqMsg := retry.DeadLetterMessage{
		OriginalTopic: topic,
		OriginalKey:   key,
		OriginalValue: value,
		Error:         errMsg,
		RetryCount:    3,
		FailedAt:      time.Now(),
	}

	body, err := json.Marshal(dlqMsg)
	if err != nil {
		return fmt.Errorf("marshal DLQ message failed: %w", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: p.topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(body),
		Headers: []sarama.RecordHeader{
			{Key: []byte("original_topic"), Value: []byte(topic)},
			{Key: []byte("error"), Value: []byte(errMsg)},
			{Key: []byte("failed_at"), Value: []byte(time.Now().Format(time.RFC3339))},
		},
	}

	partition, offset, err := p.producer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("send to DLQ failed: %w", err)
	}

	log.Printf("[DLQ] Sent to partition=%d, offset=%d, topic=%s", partition, offset, p.topic)
	return nil
}

// Close 关闭生产者
func (p *KafkaDLQProducer) Close() error {
	return p.producer.Close()
}
