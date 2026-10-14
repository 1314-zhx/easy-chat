/**
 * @Author: easy-chat
 * @Description: Prometheus 埋点示例 - 展示如何在业务代码中使用 metrics
 */

package metrics

import (
	"easy-chat/pkg/metrics"
	"context"
	"time"
)

// ExampleWebSocketConnectionMetrics WebSocket 连接指标埋点示例
//
// 埋点位置：websocket/connection.go - NewConn() 和 Close()
//
// 面试话术：
// "WebSocket 连接是高频事件，我们通过 Gauge 指标记录当前连接数，
// 通过 Counter 指标统计累计连接数和断开原因分布，便于分析用户在线行为和连接质量问题。"
func ExampleWebSocketConnectionMetrics() {
	// ========== 连接建立时 ==========
	// 指标说明：当前在线连接数 +1
	metrics.WebSocketConnections.Inc()
	metrics.WebSocketConnectionTotal.Inc()

	// ========== 连接断开时 ==========
	// 指标说明：按断开原因统计，便于分析连接质量
	// reason: normal-正常关闭, timeout-超时, error-错误, kicked-被踢出
	metrics.WebSocketDisconnectionTotal.WithLabelValues("normal").Inc()
	// metrics.WebSocketDisconnectionTotal.WithLabelValues("timeout").Inc()
	// metrics.WebSocketDisconnectionTotal.WithLabelValues("error").Inc()
	// metrics.WebSocketDisconnectionTotal.WithLabelValues("kicked").Inc()

	// ========== 连接断开时 ==========
	// 指标说明：当前在线连接数 -1
	metrics.WebSocketConnections.Dec()
}

// ExampleMessagePushMetrics 消息推送指标埋点示例
//
// 埋点位置：apps/im/ws/internal/handler/push/push.go
//
// 面试话术：
// "消息推送延迟是 IM 系统的核心性能指标，我们通过 Histogram 记录延迟分布，
// 可以计算 P50/P90/P99 延迟。Counter 记录成功/失败次数，便于计算成功率。
// Histogram 的 Buckets 设置依据：正常延迟在 10-100ms，设置细粒度分桶便于精确分析。"
func ExampleMessagePushMetrics(chatType string, startTime time.Time, success bool) {
	latency := time.Since(startTime).Milliseconds()

	// 指标说明：记录推送延迟分布
	// 维度 chat_type: single_chat / group_chat
	// 维度 operation: send / push
	metrics.MessagePushLatency.WithLabelValues(chatType, "push").Observe(float64(latency))

	// 指标说明：统计推送次数
	// 维度 status: success / failed
	status := "success"
	if !success {
		status = "failed"
	}
	metrics.MessagePushTotal.WithLabelValues(chatType, status).Inc()
}

// ExampleRedisMetrics Redis 缓存指标埋点示例
//
// 埋点位置：pkg/redis/*.go
//
// 面试话术：
// "未读数是 IM 系统的高频查询场景，我们采用 Redis 缓存 + 本地 BloomFilter 方案。
// 通过 CacheHit/CacheMiss 指标计算命中率，评估缓存效果。
// 通过 RedisOperationLatency 监控 Redis 操作延迟，发现慢查询。"
func ExampleRedisMetrics(ctx context.Context, userId string) {
	// 注意：这里只是示例，实际使用需要传入 redis 客户端
	start := time.Now()

	// 模拟业务操作（实际项目中替换为真实 Redis 操作）
	_ = userId

	// 记录延迟
	metrics.RedisOperationLatency.WithLabelValues("get", "unread").Observe(
		float64(time.Since(start).Milliseconds()),
	)

	// 指标说明：缓存命中
	metrics.UnreadCountCacheHit.Inc()
	// 或
	// metrics.UnreadCountCacheMiss.Inc()
}

// ExampleMongoMetrics MongoDB 操作指标埋点示例
//
// 埋点位置：数据库操作层
//
// 面试话术：
// "消息日志存储在 MongoDB，通过 MongoOperationLatency 监控查询延迟，
// 发现慢查询后可以针对性优化索引。MongoOperationTotal 统计操作次数和错误率。"
func ExampleMongoMetrics(collection string, startTime time.Time, err error) {
	latency := time.Since(startTime).Milliseconds()

	// 指标说明：记录 MongoDB 操作延迟
	metrics.MongoOperationLatency.WithLabelValues("find", collection).Observe(float64(latency))

	// 指标说明：统计操作次数和状态
	status := "success"
	if err != nil {
		status = "failed"
		// 记录错误类型
		metrics.ErrorTotal.WithLabelValues("mongo").Inc()
	}
	metrics.MongoOperationTotal.WithLabelValues("find", collection, status).Inc()
}

// ExampleKafkaMetrics Kafka 消息队列指标埋点示例
//
// 埋点位置：消息生产者
//
// 面试话术：
// "群聊消息和大群推送走 Kafka 异步处理，通过 KafkaProduceLatency 监控发送延迟，
// KafkaConsumeLag 监控消费者 lag 值，及时发现消息积压问题。"
func ExampleKafkaMetrics(topic string, startTime time.Time, err error) {
	latency := time.Since(startTime).Milliseconds()

	// 指标说明：记录 Kafka 发送延迟
	status := "success"
	if err != nil {
		status = "failed"
	}
	metrics.KafkaProduceLatency.WithLabelValues(topic, status).Observe(float64(latency))
	metrics.KafkaProduceTotal.WithLabelValues(topic, status).Inc()
}

// ExampleBusinessMetrics 业务指标埋点示例
//
// 面试话术：
// "除了技术指标，我们还监控业务指标：DAU、群聊规模分布等。
// 通过 GroupMemberCount 分布可以分析用户习惯，优化群聊功能设计。"
func ExampleBusinessMetrics() {
	// 指标说明：记录群成员数量分布
	metrics.GroupMemberCount.Observe(50) // 当前群的成员数量

	// 指标说明：更新会话数量
	metrics.ConversationCount.WithLabelValues("group_chat").Inc()
}
