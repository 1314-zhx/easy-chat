# Easy-Chat Prometheus 监控体系

## 一、监控体系建设背景

### 1.1 为什么需要监控？

IM 即时通讯系统对**实时性**和**可靠性**要求极高，需要监控来：

| 监控目标 | 具体指标 | 业务意义 |
|:---|:---|:---|
| **性能评估** | 消息推送延迟 P50/P90/P99 | 评估系统是否满足用户体验要求 |
| **稳定性保障** | 错误率、Panic 次数 | 及时发现故障，防止影响用户 |
| **容量规划** | 在线连接数、DAU | 预测流量峰值，指导扩容 |
| **问题定位** | Redis/MongoDB 延迟 | 快速定位慢查询，优化性能 |
| **缓存效果** | 缓存命中率 | 评估缓存策略有效性 |

### 1.2 监控技术选型

```
┌─────────────────────────────────────────────────────────────┐
│                        数据采集层                           │
├─────────────────────────────────────────────────────────────┤
│  Prometheus Client SDK                                     │
│  (Go 语言客户端，埋点采集业务指标)                            │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                        时序存储层                           │
├─────────────────────────────────────────────────────────────┤
│  Prometheus                                                │
│  (TSDB 时序数据库，支持 PromQL 查询)                         │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                        可视化层                             │
├─────────────────────────────────────────────────────────────┤
│  Grafana                                                   │
│  (图表展示，自定义 Dashboard)                                │
└─────────────────────────────────────────────────────────────┘
```

**为什么选择 Prometheus + Grafana？**

1. **Prometheus**: Pull 模型，无需在业务代码中写入，天然支持服务发现
2. **Histogram**: 原生支持延迟分桶统计，P99 计算只需一行 PromQL
3. **Grafana**: 开源可视化神器，社区 Dashboard 丰富
4. **生态完善**: 大量中间件自带 Exporter（Redis、MongoDB、Kafka 等）

---

## 二、监控指标设计

### 2.1 指标分类总览

```
┌────────────────────────────────────────────────────────────────┐
│                        监控指标体系                            │
├────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐        │
│  │   业务指标    │  │   性能指标    │  │   系统指标    │        │
│  ├──────────────┤  ├──────────────┤  ├──────────────┤        │
│  │ DAU          │  │ P50/P90/P99  │  │ CPU/内存     │        │
│  │ 消息量       │  │ QPS          │  │ 连接数       │        │
│  │ 会话数       │  │ 错误率       │  │ GC 频率      │        │
│  │ 群规模分布   │  │ 缓存命中率   │  │ 慢查询      │        │
│  └──────────────┘  └──────────────┘  └──────────────┘        │
└────────────────────────────────────────────────────────────────┘
```

### 2.2 核心指标详解

#### 指标一：消息推送延迟 `message_push_latency_milliseconds`

```go
// 定义位置：pkg/metrics/metrics.go
MessagePushLatency = promauto.NewHistogramVec(
    prometheus.HistogramOpts{
        Name:    "message_push_latency_milliseconds",
        Help:    "消息推送延迟分布，单位毫秒，用于计算 P50/P90/P99",
        Buckets: []float64{10, 25, 50, 100, 250, 500, 1000, 2500},
    },
    []string{"chat_type", "operation"},
)
```

| 维度 | 可选值 | 说明 |
|:---|:---|:---|
| chat_type | single_chat / group_chat | 单聊或群聊 |
| operation | send / push | 发送或推送 |

**埋点位置**：`apps/im/ws/internal/handler/push/push.go`

```go
func single(srv *websocket.Server, data *ws.Push, recvId string) error {
    start := time.Now()

    rconn := srv.GetConn(recvId)
    if rconn == nil {
        return nil
    }

    // ... 业务逻辑 ...

    // 埋点：记录推送延迟
    metrics.MessagePushLatency.WithLabelValues("single_chat", "push").Observe(
        float64(time.Since(start).Milliseconds()),
    )
    metrics.MessagePushTotal.WithLabelValues("single_chat", "success").Inc()

    return nil
}
```

**数据用途**：
1. 计算 P99 延迟，评估是否满足 <200ms 的目标
2. 对比单聊/群聊延迟差异，优化群聊推送逻辑
3. 设置告警：P99 > 200ms 触发告警

**PromQL 查询**：
```promql
# 单聊 P99 延迟
histogram_quantile(0.99,
  rate(easychat_message_push_latency_milliseconds_bucket{chat_type="single_chat"}[5m])
)

# 过去 5 分钟平均延迟
rate(easychat_message_push_latency_milliseconds_sum{chat_type="single_chat"}[5m])
/
rate(easychat_message_push_latency_milliseconds_count{chat_type="single_chat"}[5m])
```

---

#### 指标二：WebSocket 连接数 `websocket_connections`

```go
WebSocketConnections = promauto.NewGauge(
    prometheus.GaugeOpts{
        Name: "websocket_connections",
        Help: "当前 WebSocket 连接数，反映在线用户数",
    },
)
```

**埋点位置**：`websocket/connection.go`

```go
func NewConn(s *Server, w http.ResponseWriter, r *http.Request) *Conn {
    c, err := s.upgrader.Upgrade(w, r, nil)
    if err != nil {
        return nil
    }

    // 连接数 +1
    metrics.WebSocketConnections.Inc()
    metrics.WebSocketConnectionTotal.Inc()

    return &Conn{...}
}

func (c *Conn) Close() error {
    // 连接断开
    metrics.WebSocketConnections.Dec()
    metrics.WebSocketDisconnectionTotal.WithLabelValues("normal").Inc()

    return c.Conn.Close()
}
```

**数据用途**：
1. 实时监控在线用户数
2. 容量规划：连接数 > 10000 触发扩容告警
3. 分析用户在线规律，优化服务器部署

**PromQL 查询**：
```promql
# 当前在线用户数
easychat_websocket_connections

# 5 分钟内新增连接数
increase(easychat_websocket_connection_total[5m])

# 连接断开原因分布
sum by (reason) (increase(easychat_websocket_disconnection_total[1h]))
```

---

#### 指标三：未读数缓存命中率 `unread_count_cache_hit_total`

```go
UnreadCountCacheHit = promauto.NewCounter(...)
UnreadCountCacheMiss = promauto.NewCounter(...)
```

**埋点位置**：`pkg/redis/user_conversation_read_manager.go`

```go
func (m *Manager) GetUserUnreadCount(ctx context.Context, userId string) (int64, error) {
    start := time.Now()
    defer func() {
        // 记录 Redis 操作延迟
        metrics.RedisOperationLatency.WithLabelValues("get", "unread").Observe(
            float64(time.Since(start).Milliseconds()),
        )
    }()

    // 尝试从缓存获取
    key := fmt.Sprintf("unread:%s", userId)
    val, err := m.redis.Get(ctx, key).Int64()
    if err == nil {
        metrics.UnreadCountCacheHit.Inc()
        return val, nil
    }

    metrics.UnreadCountCacheMiss.Inc()
    // 缓存未命中，查数据库
    ...
}
```

**数据用途**：
1. 计算命中率：Hit / (Hit + Miss)
2. 命中率 < 80% 说明缓存策略需要优化
3. 结合 Redis 延迟，分析缓存性能

**PromQL 查询**：
```promql
# 缓存命中率
rate(easychat_unread_count_cache_hit_total[5m])
/
(rate(easychat_unread_count_cache_hit_total[5m]) + rate(easychat_unread_count_cache_miss_total[5m]))
```

---

#### 指标四：Redis/MongoDB 操作延迟

```go
RedisOperationLatency = promauto.NewHistogramVec(...)
MongoOperationLatency = promauto.NewHistogramVec(...)
```

**埋点位置**：数据库操作层

```go
func FindMessages(ctx context.Context, convId string) ([]*Message, error) {
    start := time.Now()
    defer func() {
        metrics.MongoOperationLatency.WithLabelValues("find", "chatlog").Observe(
            float64(time.Since(start).Milliseconds()),
        )
    }()

    cursor, err := m.collection.Find(ctx, bson.M{"conv_id": convId})
    // ...
}
```

**数据用途**：
1. 发现慢查询：P99 > 阈值触发告警
2. 优化索引：根据延迟分布调整索引策略
3. 容量规划：预测数据库负载

---

#### 指标五：消息队列 Lag `kafka_consume_lag`

```go
KafkaConsumeLag = promauto.NewGaugeVec(
    prometheus.GaugeOpts{
        Name: "kafka_consume_lag",
        Help: "Kafka 消费者 lag 值，反映消息积压情况",
    },
    []string{"topic", "partition"},
)
```

**埋点位置**：Kafka 消费者

```go
func (c *Consumer) Consume() {
    for {
        msg, err := c.client.Consume(...)
        if err != nil {
            continue
        }

        // 更新 lag 值
        lag := c.client.HighWatermark() - msg.Offset
        metrics.KafkaConsumeLag.WithLabelValues(c.topic, msg.Partition).Set(float64(lag))
    }
}
```

**数据用途**：
1. 监控消息积压：lag > 10000 触发告警
2. 判断消费者是否正常：lag 为 0 说明消费正常
3. 评估扩容需求：lag 持续增长需要增加消费者

---

## 三、指标设计原则

### 3.1 选择合适的指标类型

| 指标类型 | 使用场景 | 示例 |
|:---|:---|:---|
| **Counter** | 只增不减的累计值 | 请求总数、错误总数 |
| **Gauge** | 可增可减的瞬时值 | 当前连接数、CPU 使用率 |
| **Histogram** | 需要计算分位数的分布 | 请求延迟、消息大小 |
| **Summary** | 直接计算好分位数 | 客户端限定的延迟 |

**为什么用 Histogram 而不是 Summary？**

```
Histogram:
  - 服务端计算分位数，灵活
  - 可以事后重新计算任意分位数
  - PromQL 原生支持 histogram_quantile()

Summary:
  - 客户端计算分位数，不灵活
  - 无法事后重新计算
  - 无法跨服务聚合
```

### 3.2 合理设置 Buckets

```go
// 错误示例：Buckets 设置不合理
Buckets: []float64{1, 100, 1000}  // 分桶太粗，P99 不准确

// 正确示例：根据业务延迟分布设置
Buckets: []float64{10, 25, 50, 100, 250, 500, 1000, 2500}
// 10ms:    快速路径
// 50ms:    正常延迟
// 100ms:   可接受上限
// 250ms:   需要优化
// 500ms+:  严重问题
```

### 3.3 维度设计

```go
// 好的设计：维度有实际意义
MessagePushLatency.WithLabelValues("single_chat", "push")  // ✅
MessagePushLatency.WithLabelValues("group_chat", "push")   // ✅

// 避免：维度值过多、无意义
MessagePushLatency.WithLabelValues(userId)  // ❌ 用户数太多，Label 爆炸
MessagePushLatency.WithLabelValues("1")     // ❌ 无意义
```

---

## 四、Grafana Dashboard 设计

### 4.1 Dashboard 结构

```
┌─────────────────────────────────────────────────────────────────────┐
│                          Easy-Chat 监控面板                          │
├─────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐      │
│  │  消息推送 P99   │  │   在线用户数    │  │   缓存命中率    │      │
│  │    45ms ✅     │  │     1,234       │  │     95.5% ✅    │      │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘      │
│                                                                     │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │                      消息推送延迟分布                          │  │
│  │  100% │▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓│  │
│  │   50% │▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓│  │
│  │       └───────┴───────┴───────┴───────┴───────┴───────┴─────│  │
│  │              10ms    50ms    100ms   250ms   500ms   1s     │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                                                                     │
│  ┌──────────────────────┐  ┌──────────────────────────────────┐   │
│  │   Redis 延迟 P99      │  │        MongoDB 延迟 P99          │   │
│  │      5ms ✅          │  │         45ms ✅                  │   │
│  └──────────────────────┘  └──────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

### 4.2 核心 Panel 展示

| Panel | 图表类型 | 用途 |
|:---|:---|:---|
| 消息推送 P50/P90/P99 | Time Series | 展示延迟趋势 |
| 在线用户数 | Stat | 单值展示当前人数 |
| 消息 QPS | Time Series | 展示吞吐量趋势 |
| 缓存命中率 | Gauge | 展示当前命中率 |
| 错误分布 | Pie Chart | 展示错误类型分布 |
| Kafka Lag | Time Series | 展示消息积压趋势 |

---

## 五、告警规则设计

### 5.1 告警分级

| 级别 | 含义 | 处理方式 |
|:---|:---|:---|
| **Critical** | 服务不可用 | 立即处理，值班电话 |
| **Warning** | 性能下降 | 关注处理，工作时间处理 |
| **Info** | 需要关注 | 记录分析，不需要立即处理 |

### 5.2 核心告警规则

```yaml
# 延迟告警
- alert: SingleChatP99LatencyHigh
  expr: histogram_quantile(0.99, rate(easychat_message_push_latency_milliseconds_bucket{chat_type="single_chat"}[5m])) > 200
  for: 5m  # 持续 5 分钟才告警，避免抖动
  labels:
    severity: warning

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

### 5.3 告警抑制策略

```yaml
# 避免告警风暴：高优先级告警触发时，抑制低优先级告警
inhibit_rules:
  - source_match:
      severity: critical
    target_match:
      severity: warning
    equal: ['alertname']
```

---

## 六、面试话术汇总

### Q1: 你们项目中使用了哪些监控指标？

**回答**：
> 我们使用 Prometheus + Grafana 搭建了完整的监控体系，主要监控以下几类指标：
>
> 1. **消息推送延迟**：通过 Histogram 记录单聊/群聊的推送延迟分布，计算 P50/P90/P99
> 2. **WebSocket 连接数**：通过 Gauge 记录当前在线用户数，Counter 统计累计连接数
> 3. **缓存命中率**：通过 Counter 记录 Redis 缓存命中/未命中次数，计算命中率
> 4. **数据库操作延迟**：监控 Redis/MongoDB 操作延迟，发现慢查询
> 5. **消息队列积压**：监控 Kafka Consumer Lag，及时发现消息积压
>
> 这些指标帮助我们实时掌握系统性能，快速定位问题。

### Q2: 为什么要用 Histogram 而不是 Summary？

**回答**：
> Histogram 和 Summary 都能计算延迟分位数，但区别在于：
>
> 1. **计算位置不同**：Histogram 在服务端计算分位数，Summary 在客户端计算
> 2. **灵活性不同**：Histogram 可以事后重新计算任意分位数，Summary 不能
> 3. **聚合能力不同**：Histogram 支持跨服务聚合，Summary 不支持
>
> 比如我需要计算 P99，如果用 Summary，假设后来发现 P95 更重要，Summary 就无法回溯计算了。但 Histogram 可以随时用 `histogram_quantile(0.95, ...)` 计算。
>
> 而且我们的指标需要按 `chat_type` 维度聚合，Histogram 的 PromQL 原生支持这个功能。

### Q3: 你们的告警是怎么设计的？

**回答**：
> 我们设计了分级告警机制：
>
> 1. **P99 延迟告警**：单聊 P99 > 200ms 或群聊 P99 > 500ms，触发 Warning
> 2. **连接数告警**：在线用户数 > 10000，触发 Warning，提醒扩容
> 3. **错误率告警**：消息推送错误率 > 1%，触发 Critical，需要立即处理
> 4. **缓存命中率告警**：命中率 < 80%，触发 Warning，需要优化缓存策略
>
> 告警设置 `for: 5m`，是为了避免偶发的抖动导致误报。告警通过 AlertManager 发送到钉钉群。

### Q4: 监控数据怎么用？

**回答**：
> 监控数据主要用于以下几个方面：
>
> 1. **日常巡检**：每天早上查看 Dashboard，检查核心指标是否正常
> 2. **性能优化**：通过延迟分布发现性能瓶颈，比如 P99 延迟高说明有慢查询
> 3. **容量规划**：根据在线用户数趋势，预测流量峰值，提前扩容
> 4. **故障定位**：出现问题时，通过监控快速定位是哪个环节出问题
> 5. **版本验证**：每次发版后，监控延迟和错误率是否有异常
>
> 举个例子，之前发现群聊推送 P99 延迟比较高，通过监控发现是群成员遍历时串行导致的，优化为并行推送后，延迟降低了 40%。

---

## 七、快速启动

### 7.1 启动监控组件

```bash
cd deploy
docker-compose up -d

# 访问地址
# Prometheus: http://localhost:9090
# Grafana:   http://localhost:3000 (admin/admin123)
```

### 7.2 查看预置 Dashboard

1. 访问 http://localhost:3000
2. 登录后点击左侧 "+" -> "Import"
3. 导入 Grafana Dashboard JSON（见附录）

### 7.3 本地测试

```bash
# 启动 IM 服务
cd apps/im/ws
go run im.go -f etc/dev/im.yaml

# 访问 metrics
curl http://localhost:12112/metrics

# 查看关键指标
curl -s http://localhost:12112/metrics | grep easychat
```
