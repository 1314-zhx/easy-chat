/**
 * @Author: easy-chat
 * @Description: Prometheus 监控指标定义
 */

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// ========== 消息推送相关指标 ==========

	// MessagePushLatency 消息推送延迟分布
	// 用途：计算 P50/P90/P99 延迟，评估推送性能
	MessagePushLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "easychat", // 指标前缀，建议用项目名
			Name:      "message_push_latency_milliseconds",
			Help:      "消息推送延迟分布，单位毫秒，用于计算 P50/P90/P99",
			// 延迟分桶：10ms、25ms、50ms、100ms、250ms、500ms、1s、2.5s
			Buckets: []float64{10, 25, 50, 100, 250, 500, 1000, 2500},
		},
		[]string{"chat_type", "operation"}, // chat_type: 单聊/群聊, operation: send/push
	)

	// MessagePushTotal 消息推送总量
	// 用途：统计消息发送量，计算 QPS
	MessagePushTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "easychat",
			Name:      "message_push_total",
			Help:      "消息推送总量，按状态和类型分组",
		},
		[]string{"chat_type", "status"}, // chat_type: single_chat/group_chat, status: success/failed
	)

	// MessageSize 消息大小分布
	// 用途：监控消息体积，防止大消息影响性能
	MessageSize = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "easychat",
			Name:      "message_size_bytes",
			Help:      "消息大小分布，单位字节",
			Buckets:   []float64{100, 500, 1000, 5000, 10000, 50000, 100000},
		},
		[]string{"chat_type", "content_type"}, // content_type: text/image/file
	)

	// ========== WebSocket 连接相关指标 ==========

	// WebSocketConnections 当前 WebSocket 连接数
	// 用途：监控在线用户数，评估服务器负载
	WebSocketConnections = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "easychat",
			Name:      "websocket_connections",
			Help:      "当前 WebSocket 连接数，反映在线用户数",
		},
	)

	// WebSocketConnectionTotal WebSocket 连接总数
	// 用途：统计累计连接数，用于分析用户行为
	WebSocketConnectionTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "easychat",
			Name:      "websocket_connection_total",
			Help:      "WebSocket 连接总数",
		},
	)

	// WebSocketDisconnectionTotal WebSocket 断开连接总数
	// 用途：分析连接断开原因
	WebSocketDisconnectionTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "easychat",
			Name:      "websocket_disconnection_total",
			Help:      "WebSocket 断开连接总数",
		},
		[]string{"reason"}, // reason: normal/timeout/error/kicked
	)

	// WebSocketMessageTotal WebSocket 消息收发总量
	// 用途：统计消息流量
	WebSocketMessageTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "easychat",
			Name:      "websocket_message_total",
			Help:      "WebSocket 消息收发总量",
		},
		[]string{"direction", "msg_type"}, // direction: sent/received, msg_type: text/image/system
	)

	// ========== Redis 缓存相关指标 ==========

	// UnreadCountCacheHit 未读数缓存命中数
	// 用途：计算缓存命中率，评估缓存效果
	UnreadCountCacheHit = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "easychat",
			Name:      "unread_count_cache_hit_total",
			Help:      "未读数缓存命中总数",
		},
	)

	// UnreadCountCacheMiss 未读数缓存未命中数
	// 用途：缓存未命中时需要查数据库，影响性能
	UnreadCountCacheMiss = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "easychat",
			Name:      "unread_count_cache_miss_total",
			Help:      "未读数缓存未命中总数",
		},
	)

	// UnreadCountValue 未读数当前值
	// 用途：监控用户未读数分布，优化产品体验
	UnreadCountValue = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "easychat",
			Name:      "unread_count_value",
			Help:      "用户当前未读数",
		},
		[]string{"chat_type"}, // 单聊/群聊
	)

	// ReadCursorCacheHit 已读游标缓存命中率
	// 用途：评估已读游标缓存效果
	ReadCursorCacheHit = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "easychat",
			Name:      "read_cursor_cache_hit_total",
			Help:      "已读游标缓存命中总数",
		},
	)

	ReadCursorCacheMiss = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "easychat",
			Name:      "read_cursor_cache_miss_total",
			Help:      "已读游标缓存未命中总数",
		},
	)

	// ========== Redis 操作相关指标 ==========

	// RedisOperationLatency Redis 操作延迟
	// 用途：监控 Redis 性能，发现慢查询
	RedisOperationLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "easychat",
			Name:      "redis_operation_latency_milliseconds",
			Help:      "Redis 操作延迟分布，单位毫秒",
			Buckets:   []float64{0.5, 1, 2, 5, 10, 25, 50, 100},
		},
		[]string{"operation", "key_type"}, // operation: get/set/setex/incr/del, key_type: unread/read/bloom
	)

	// RedisOperationTotal Redis 操作总数
	// 用途：统计 Redis 操作次数
	RedisOperationTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "easychat",
			Name:      "redis_operation_total",
			Help:      "Redis 操作总数，按操作类型和状态分组",
		},
		[]string{"operation", "status"}, // status: success/failed
	)

	// ========== MongoDB 操作相关指标 ==========

	// MongoOperationLatency MongoDB 操作延迟
	// 用途：监控数据库性能
	MongoOperationLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "easychat",
			Name:      "mongo_operation_latency_milliseconds",
			Help:      "MongoDB 操作延迟分布，单位毫秒",
			Buckets:   []float64{5, 10, 25, 50, 100, 250, 500, 1000},
		},
		[]string{"operation", "collection"}, // operation: find/insert/update/delete, collection: chatlog/conversation/user
	)

	// MongoOperationTotal MongoDB 操作总数
	// 用途：统计数据库操作次数
	MongoOperationTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "easychat",
			Name:      "mongo_operation_total",
			Help:      "MongoDB 操作总数，按操作类型和状态分组",
		},
		[]string{"operation", "collection", "status"},
	)

	// BatchInsertLatency 批量插入延迟
	// 用途：监控批量插入性能
	BatchInsertLatency = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "easychat",
			Name:      "batch_insert_latency_milliseconds",
			Help:      "批量插入延迟分布，单位毫秒",
			Buckets:   []float64{5, 10, 25, 50, 100, 250, 500, 1000, 2000},
		},
	)

	// BatchInsertSize 批量插入大小
	// 用途：监控批量插入的批次大小
	BatchInsertSize = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "easychat",
			Name:      "batch_insert_size",
			Help:      "批量插入的记录数分布",
			Buckets:   []float64{1, 5, 10, 25, 50, 100, 200, 500},
		},
	)

	// ========== 消息队列相关指标 ==========

	// KafkaProduceLatency Kafka 消息发送延迟
	// 用途：监控消息队列性能
	KafkaProduceLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "easychat",
			Name:      "kafka_produce_latency_milliseconds",
			Help:      "Kafka 消息发送延迟分布，单位毫秒",
			Buckets:   []float64{1, 5, 10, 25, 50, 100, 250, 500},
		},
		[]string{"topic", "status"}, // topic: msg_transfer/group_push
	)

	// KafkaProduceTotal Kafka 消息发送总量
	// 用途：统计消息入队量
	KafkaProduceTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "easychat",
			Name:      "kafka_produce_total",
			Help:      "Kafka 消息发送总量",
		},
		[]string{"topic", "status"},
	)

	// KafkaConsumeLag Kafka 消费 lag
	// 用途：监控消息积压情况
	KafkaConsumeLag = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "easychat",
			Name:      "kafka_consume_lag",
			Help:      "Kafka 消费者 lag 值，反映消息积压情况",
		},
		[]string{"topic", "partition"},
	)

	// ========== 业务相关指标 ==========

	// ActiveUsersDAU 日活用户数
	// 用途：统计每日活跃用户
	ActiveUsersDAU = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "easychat",
			Name:      "active_users_dau",
			Help:      "每日活跃用户数",
		},
	)

	// ConversationCount 会话数量
	// 用途：监控会话规模
	ConversationCount = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "easychat",
			Name:      "conversation_count",
			Help:      "会话数量",
		},
		[]string{"chat_type"}, // single_chat/group_chat
	)

	// GroupMemberCount 群成员数量分布
	// 用途：分析群聊规模
	GroupMemberCount = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "easychat",
			Name:      "group_member_count",
			Help:      "群成员数量分布",
			Buckets:   []float64{2, 5, 10, 20, 50, 100, 200, 500},
		},
	)

	// ========== 系统相关指标 ==========

	// HTTPRequestLatency HTTP 接口响应延迟
	// 用途：计算接口 P50/P90/P99，评估接口性能
	HTTPRequestLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "easychat",
			Name:      "http_request_latency_milliseconds",
			Help:      "HTTP 接口响应延迟分布，单位毫秒，用于计算 P50/P90/P99",
			Buckets:   []float64{5, 10, 25, 50, 100, 200, 500, 1000},
		},
		[]string{"path", "method", "status"},
	)

	// HTTPRequestTotal HTTP 接口请求总量
	// 用途：统计接口 QPS，计算错误率
	HTTPRequestTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "easychat",
			Name:      "http_request_total",
			Help:      "HTTP 接口请求总量",
		},
		[]string{"path", "method", "status"},
	)

	// ErrorTotal 错误总数
	// 用途：监控系统错误率
	ErrorTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "easychat",
			Name:      "error_total",
			Help:      "错误总数，按错误类型分组",
		},
		[]string{"error_type"}, // error_type: redis/mongo/kafka/websocket/validation
	)

	// PanicTotal Panic 总数
	// 用途：监控系统稳定性
	PanicTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "easychat",
			Name:      "panic_total",
			Help:      "Panic 总数",
		},
	)
)
