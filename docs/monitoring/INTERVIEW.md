# Easy-Chat 监控体系面试指南

> 本文档用于面试中回答"项目中有哪些监控指标"等问题

---

## 一、项目监控架构图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              监控架构                                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────┐      ┌─────────────┐      ┌─────────────┐                  │
│  │  应用层埋点  │ ──▶ │  Prometheus │ ──▶ │   Grafana   │                  │
│  │ (Go Client) │      │   (TSDB)    │      │  (Dashboard)│                  │
│  └─────────────┘      └─────────────┘      └─────────────┘                  │
│         │                    │                    │                         │
│         │                    ▼                    ▼                         │
│         │             ┌─────────────┐      ┌─────────────┐                  │
│         └─────────────▶│  AlertManager│ ──▶ │   钉钉/邮件  │                  │
│                       └─────────────┘      └─────────────┘                  │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 二、监控指标体系（核心）

### 2.1 指标分类

| 类别 | 指标名称 | 类型 | 用途 |
|:---|:---|:---|:---|
| **性能** | 消息推送延迟 | Histogram | 计算 P50/P90/P99，评估系统性能 |
| **性能** | Redis/MongoDB 延迟 | Histogram | 发现慢查询 |
| **性能** | Kafka 发送延迟 | Histogram | 评估消息队列性能 |
| **业务** | WebSocket 连接数 | Gauge | 监控在线用户数 |
| **业务** | 消息推送总量 | Counter | 统计 QPS |
| **业务** | 缓存命中率 | Counter | 评估缓存效果 |
| **业务** | Kafka Lag | Gauge | 监控消息积压 |
| **业务** | DAU/会话数 | Gauge | 业务指标 |
| **稳定** | 错误总数 | Counter | 监控系统质量 |
| **稳定** | Panic 总数 | Counter | 监控系统稳定性 |

---

## 三、核心指标详解

### 3.1 消息推送延迟 `message_push_latency_milliseconds`

**指标定义**：
```go
MessagePushLatency = promauto.NewHistogramVec(
    prometheus.HistogramOpts{
        Name:    "message_push_latency_milliseconds",
        Help:    "消息推送延迟分布，单位毫秒，用于计算 P50/P90/P99",
        Buckets: []float64{10, 25, 50, 100, 250, 500, 1000, 2500},
    },
    []string{"chat_type", "operation"},  // chat_type: single_chat/group_chat
)
```

**埋点位置**：`apps/im/ws/internal/handler/push/push.go`

```go
func single(srv *websocket.Server, data *ws.Push, recvId string) error {
    start := time.Now()

    rconn := srv.GetConn(recvId)
    // ... 业务逻辑 ...

    // 埋点：记录推送延迟
    metrics.MessagePushLatency.WithLabelValues("single_chat", "push").Observe(
        float64(time.Since(start).Milliseconds()),
    )
    metrics.MessagePushTotal.WithLabelValues("single_chat", "success").Inc()

    return nil
}
```

**PromQL 查询**：
```promql
# 单聊 P99 延迟
histogram_quantile(0.99,
  rate(easychat_message_push_latency_milliseconds_bucket{chat_type="single_chat"}[5m])
)

# 群聊 P99 延迟
histogram_quantile(0.99,
  rate(easychat_message_push_latency_milliseconds_bucket{chat_type="group_chat"}[5m])
)

# 平均延迟
rate(easychat_message_push_latency_milliseconds_sum[5m])
/
rate(easychat_message_push_latency_milliseconds_count[5m])
```

**为什么用 Histogram 而不是 Summary？**

| 对比 | Histogram ✅ | Summary ❌ |
|:---|:---|:---|
| 计算位置 | 服务端 | 客户端 |
| 灵活性 | 可计算任意分位数 | 分位数固定 |
| 聚合能力 | 支持跨服务聚合 | 不支持 |
| 回溯能力 | 可事后重新计算 | 不能回溯 |

---

### 3.2 WebSocket 连接数 `websocket_connections`

**指标定义**：
```go
WebSocketConnections = promauto.NewGauge(
    prometheus.GaugeOpts{
        Name: "websocket_connections",
        Help: "当前 WebSocket 连接数，反映在线用户数",
    },
)

WebSocketConnectionTotal = promauto.NewCounter(...)
WebSocketDisconnectionTotal = promauto.NewCounterVec(
    prometheus.CounterOpts{Name: "websocket_disconnection_total"},
    []string{"reason"},  // normal/timeout/error/kicked
)
```

**埋点位置**：`websocket/connection.go`

```go
func NewConn(s *Server, w http.ResponseWriter, r *http.Request) *Conn {
    // 连接建立
    metrics.WebSocketConnections.Inc()         // 当前连接数 +1
    metrics.WebSocketConnectionTotal.Inc()    // 累计连接数 +1
    return &Conn{...}
}

func (c *Conn) Close() {
    // 连接断开
    metrics.WebSocketConnections.Dec()                          // 当前连接数 -1
    metrics.WebSocketDisconnectionTotal.WithLabelValues("normal").Inc()
}
```

**数据用途**：
1. 实时监控在线用户数
2. 连接数 > 10000 触发扩容告警
3. 分析连接断开原因分布，优化稳定性

---

### 3.3 缓存命中率 `unread_count_cache_hit_total`

**指标定义**：
```go
UnreadCountCacheHit = promauto.NewCounter(...)
UnreadCountCacheMiss = promauto.NewCounter(...)
```

**埋点位置**：`pkg/redis/user_conversation_read_manager.go`

```go
func (m *Manager) GetUserUnreadCount(ctx context.Context, userId string) (int64, error) {
    key := fmt.Sprintf("unread:%s", userId)
    val, err := m.redis.Get(ctx, key).Int64()

    if err == nil {
        metrics.UnreadCountCacheHit.Inc()  // 缓存命中
        return val, nil
    }

    metrics.UnreadCountCacheMiss.Inc()     // 缓存未命中，查数据库
    // ...
}
```

**PromQL 查询**：
```promql
# 缓存命中率
rate(easychat_unread_count_cache_hit_total[5m])
/
(rate(easychat_unread_count_cache_hit_total[5m])
 + rate(easychat_unread_count_cache_miss_total[5m]))
```

---

### 3.4 数据库操作延迟 `redis/mongo_operation_latency_milliseconds`

**指标定义**：
```go
RedisOperationLatency = promauto.NewHistogramVec(
    prometheus.HistogramOpts{
        Name:    "redis_operation_latency_milliseconds",
        Buckets: []float64{0.5, 1, 2, 5, 10, 25, 50, 100},
    },
    []string{"operation", "key_type"},  // operation: get/set/setex
)

MongoOperationLatency = promauto.NewHistogramVec(...)
```

**埋点位置**：数据库操作层

```go
func GetUserUnreadCount(ctx context.Context, userId string) (int64, error) {
    start := time.Now()
    defer func() {
        metrics.RedisOperationLatency.WithLabelValues("get", "unread").Observe(
            float64(time.Since(start).Milliseconds()),
        )
    }()

    // Redis 操作
    val, err := redis.Get(ctx, "unread:"+userId).Int64()
    // ...
}
```

---

### 3.5 消息队列 Lag `kafka_consume_lag`

**指标定义**：
```go
KafkaConsumeLag = promauto.NewGaugeVec(
    prometheus.GaugeOpts{
        Name: "kafka_consume_lag",
        Help: "Kafka 消费者 lag 值，反映消息积压情况",
    },
    []string{"topic", "partition"},
)
```

**数据用途**：
1. 监控消息积压：lag > 10000 触发告警
2. 判断消费者是否正常
3. 评估扩容需求

---

## 四、告警规则

```yaml
# 延迟告警
- alert: SingleChatP99LatencyHigh
  expr: histogram_quantile(0.99, rate(easychat_message_push_latency_milliseconds_bucket{chat_type="single_chat"}[5m])) > 200
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "单聊消息推送 P99 延迟过高"
    description: "P99 延迟 {{ $value }}ms，超过阈值 200ms"

# 连接数告警
- alert: WebSocketConnectionsHigh
  expr: easychat_websocket_connections > 10000
  for: 5m
  labels:
    severity: warning

# 错误率告警
- alert: MessagePushErrorRateHigh
  expr: rate(easychat_message_push_total{status="failed"}[5m]) / rate(easychat_message_push_total[5m]) > 0.01
  for: 5m
  labels:
    severity: critical
```

---

## 五、面试话术

### Q1: 项目中有哪些监控指标？

**回答要点**：
1. 先说整体架构：Prometheus + Grafana
2. 分类说明：性能、业务、稳定三类指标
3. 举 2-3 个核心指标的例子

**参考答案**：
> 我们使用 Prometheus + Grafana 搭建了监控体系，主要监控以下指标：
>
> **性能指标**：
> - 消息推送延迟：通过 Histogram 记录单聊/群聊的推送延迟分布，计算 P50/P90/P99
> - Redis/MongoDB 操作延迟：监控数据库性能，发现慢查询
> - Kafka 消息发送延迟：评估消息队列性能
>
> **业务指标**：
> - WebSocket 连接数：通过 Gauge 记录当前在线用户数
> - 消息推送总量：通过 Counter 统计 QPS
> - 未读数缓存命中率：评估缓存效果，命中率 > 95%
> - Kafka Consumer Lag：监控消息积压
>
> **稳定性指标**：
> - 错误总数：按错误类型分类，监控系统质量
> - Panic 总数：监控系统稳定性
>
> 这些指标帮助我们实时掌握系统性能，快速定位问题。

---

### Q2: 为什么要用 Histogram 而不是 Summary？

**参考答案**：
> Histogram 和 Summary 都能计算延迟分位数，但 Histogram 更灵活：
>
> 1. **计算位置**：Histogram 在服务端计算，Summary 在客户端计算
> 2. **灵活性**：Histogram 可以事后计算任意分位数（P50/P90/P99/P999），Summary 只能计算预设的分位数
> 3. **聚合能力**：Histogram 支持跨服务聚合，Summary 不支持
>
> 举个例子，如果用 Summary 预设了 P99，后来发现 P95 更重要，Summary 就无法回溯计算了。但 Histogram 可以随时用 `histogram_quantile(0.95, ...)` 计算。
>
> 而且我们的指标需要按 `chat_type` 维度聚合，Histogram 的 PromQL 原生支持这个功能。

---

### Q3: 监控数据怎么用？

**参考答案**：
> 监控数据主要用于以下几个方面：
>
> 1. **日常巡检**：每天查看 Dashboard，检查核心指标是否正常
> 2. **性能优化**：通过延迟分布发现瓶颈
> 3. **容量规划**：根据在线用户数趋势预测流量峰值
> 4. **故障定位**：出现问题时快速定位问题环节
> 5. **版本验证**：每次发版后验证性能是否正常
>
> 举个例子，之前发现群聊 P99 延迟比较高，通过监控发现是群成员遍历时串行导致的，优化为并行推送后，延迟降低了 40%。

---

### Q4: Histogram 的 Buckets 怎么设置？

**参考答案**：
> Buckets 设置要根据业务延迟分布来定：
>
> ```go
> Buckets: []float64{10, 25, 50, 100, 250, 500, 1000, 2500}
> ```
>
> - **10ms**：快速路径，大部分请求在这个区间
> - **50ms**：正常延迟
> - **100ms**：可接受上限
> - **250ms**：需要优化
> - **500ms+**：严重问题
>
> 设置太粗会导致分位数不准确，设置太细会导致存储开销增加。一般设置 8-10 个桶比较合适。

---

### Q5: 告警怎么设计？

**参考答案**：
> 我们设计了分级告警机制：
>
> 1. **Critical（严重）**：服务不可用，如错误率 > 1%，立即处理
> 2. **Warning（警告）**：性能下降，如 P99 > 200ms，需要关注
> 3. **Info（提示）**：需要关注，但不需要立即处理
>
> 告警设置 `for: 5m`，是为了避免抖动导致误报。告警通过 AlertManager 发送到钉钉群。

---

## 六、快速启动

### 6.1 启动监控

```bash
cd deploy
docker-compose up -d

# 访问地址
# Prometheus: http://localhost:9090
# Grafana:   http://localhost:3000 (admin/admin123)
```

### 6.2 查看指标

```bash
# 启动服务后访问
curl http://localhost:12112/metrics | grep easychat
```

---

## 七、文件清单

```
easy-chat/
├── pkg/metrics/
│   ├── metrics.go       # 指标定义
│   ├── server.go        # /metrics 端点注册
│   └── example.go       # 埋点示例
├── deploy/
│   ├── docker-compose.yml    # 监控组件
│   ├── prometheus/
│   │   ├── prometheus.yml    # Prometheus 配置
│   │   └── alerts.yml        # 告警规则
│   └── grafana/
│       └── dashboards/
│           └── im-monitoring.json  # Dashboard JSON
└── docs/monitoring/
    └── README.md       # 详细文档
```
