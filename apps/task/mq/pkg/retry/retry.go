package retry

import (
	"context"
	"fmt"
	"log"
	"time"
)

// DeadLetterMessage 死信消息结构
type DeadLetterMessage struct {
	OriginalTopic string    `json:"original_topic"`
	OriginalKey   string    `json:"original_key"`
	OriginalValue string    `json:"original_value"`
	Error         string    `json:"error"`
	RetryCount    int       `json:"retry_count"`
	FailedAt      time.Time `json:"failed_at"`
}

// DLQProducer 死信队列生产者接口
type DLQProducer interface {
	SendToDLQ(ctx context.Context, topic, key, value, errMsg string) error
}

// RetryConfig 重试配置
type RetryConfig struct {
	MaxRetries    int           // 最大重试次数
	InitialDelay  time.Duration // 初始延迟
	MaxDelay      time.Duration // 最大延迟
	BackoffMulti  float64       // 退避倍数
}

// DefaultRetryConfig 默认重试配置
var DefaultRetryConfig = RetryConfig{
	MaxRetries:   3,
	InitialDelay: 100 * time.Millisecond,
	MaxDelay:     5 * time.Second,
	BackoffMulti: 2.0,
}

// Retry 仅重试，不发 DLQ（由调用方决定失败后的降级/死信）
func Retry(ctx context.Context, fn func() error, cfg RetryConfig) error {
	var lastErr error
	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			delay := time.Duration(float64(cfg.InitialDelay) * pow(cfg.BackoffMulti, float64(attempt-1)))
			if delay > cfg.MaxDelay {
				delay = cfg.MaxDelay
			}
			log.Printf("[Retry] attempt %d/%d, waiting %v before retry...", attempt, cfg.MaxRetries, delay)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}
		lastErr = fn()
		if lastErr == nil {
			return nil
		}
		log.Printf("[Retry] attempt %d/%d failed: %v", attempt, cfg.MaxRetries, lastErr)
	}
	return fmt.Errorf("retry exhausted after %d attempts, last error: %w", cfg.MaxRetries+1, lastErr)
}

// RetryWithRetryAndDLQ 执行带重试和死信队列的函数
// fn: 要执行的函数，返回 error
// dlq: 死信队列生产者（可选）
// topic: 原始 topic
// key, value: 原始消息
func RetryWithRetryAndDLQ(
	ctx context.Context,
	fn func() error,
	dlq DLQProducer,
	topic, key, value string,
	cfg RetryConfig,
) error {
	lastErr := Retry(ctx, fn, cfg)
	if lastErr == nil {
		return nil
	}

	// 发送到死信队列
	if dlq != nil {
		go func() {
			if err := dlq.SendToDLQ(ctx, topic, key, value, lastErr.Error()); err != nil {
				log.Printf("[DLQ] Failed to send to dead letter queue: %v", err)
			} else {
				log.Printf("[DLQ] Message sent to dead letter queue: topic=%s, key=%s", topic, key)
			}
		}()
	}

	return lastErr
}

func pow(base, exp float64) float64 {
	result := 1.0
	for i := 0; i < int(exp); i++ {
		result *= base
	}
	return result
}
