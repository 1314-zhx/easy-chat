# 消息处理失败重试机制

## 概述

当 MongoDB 入库失败时，系统会自动进行重试。重试 3 次后仍失败，消息会被发送到死信队列（Dead Letter Queue），供后续人工处理。

## 重试策略

| 参数 | 值 | 说明 |
|------|-----|------|
| 最大重试次数 | 3 | 共尝试 4 次（1 次原始 + 3 次重试） |
| 初始延迟 | 100ms | 第一次重试前等待 |
| 最大延迟 | 5s | 重试间隔上限 |
| 退避倍数 | 2.0 | 指数退避 |

## 重试时间线

```
第1次尝试     ────── X ──────
                            ↓ 失败
第2次尝试           ─── X ───
                              ↓ 失败
第3次尝试              ─ X ─
                                ↓ 失败
第4次尝试                 X
                                ↓ 失败
发送 DLQ ─────────────────────────> msgChatTransferDLT
```

延迟计算：
- 第1次重试：100ms
- 第2次重试：200ms
- 第3次重试：400ms

## 处理流程

```
Kafka 消息
    │
    ▼
┌─────────────────────┐
│  JSON 解析失败？     │
└─────────────────────┘
    │ 是
    ▼
直接返回错误（不重试，消息格式错误重试也无用）

    │ 否
    ▼
┌─────────────────────┐
│  MongoDB 入库        │
└─────────────────────┘
    │
    ├── 成功 ────────────────────────────────> 继续推送
    │
    └── 失败
        │
        ▼
    ┌─────────────────────┐
    │  等待 100ms         │
    └─────────────────────┘
        │
        ▼
    ┌─────────────────────┐
    │  重试第2次          │
    └─────────────────────┘
        │
        ├── 成功 ────────────────────────────────> 继续推送
        │
        └── 失败
            │
            ▼
        ┌─────────────────────┐
        │  等待 200ms         │
        └─────────────────────┘
            │
            ▼
        ┌─────────────────────┐
        │  重试第3次          │
        └─────────────────────┘
            │
            ├── 成功 ────────────────────────────────> 继续推送
            │
            └── 失败
                │
                ▼
            ┌─────────────────────┐
            │  等待 400ms         │
            └─────────────────────┘
                │
                ▼
            ┌─────────────────────┐
            │  重试第4次          │
            └─────────────────────┘
                │
                ├── 成功 ────────────────────────────────> 继续推送
                │
                └── 失败
                    │
                    ▼
            ┌─────────────────────┐
            │  发送到 DLQ         │
            │  msgChatTransferDLT │
            └─────────────────────┘
                    │
                    ▼
            返回 nil（告知 Kafka 已处理）
```

## 死信消息格式

发送到死信队列的消息包含原始消息信息和错误详情：

```json
{
  "original_topic": "msgChatTransfer",
  "original_key": "user123_convo456",
  "original_value": "{\"conversationId\":\"conv_001\",\"sendId\":\"user123\",\"recvId\":\"user456\",\"content\":\"Hello\"}",
  "error": "mongodb: connection refused",
  "retry_count": 3,
  "failed_at": "2026-08-14T12:00:00+08:00"
}
```

## 死信消息字段说明

| 字段 | 说明 |
|------|------|
| original_topic | 原始 Kafka topic |
| original_key | 原始消息 key |
| original_value | 原始消息内容（JSON 字符串） |
| error | 最后一次失败的错误信息 |
| retry_count | 重试次数 |
| failed_at | 最终失败时间 |

## Kafka 配置

### 创建死信 Topic

```bash
kafka-topics.sh --create \
  --topic msgChatTransferDLT \
  --partitions 3 \
  --replication-factor 1 \
  --bootstrap-server 127.0.0.1:9092
```

### Topic 列表

| Topic | 用途 | 消费者 |
|-------|------|--------|
| msgChatTransfer | 正常消息处理 | MsgChatTransfer |
| msgChatTransferDLT | 失败消息死信 | 人工/补偿服务 |

## 监控告警

建议对死信队列配置监控：

```bash
# 监控死信队列消息数量
kafka-consumer-groups.sh \
  --bootstrap-server 127.0.0.1:9092 \
  --group dead-letter-consumer \
  --describe \
  --topic msgChatTransferDLT
```

告警条件：
- 死信队列消息数 > 0
- 死信队列积压持续增长

## 死信消息处理

死信消息需要人工介入处理，常见方式：

1. **排查问题**：查看 error 字段确定失败原因
2. **修复数据**：根据 original_value 重新处理消息
3. **重放消息**：修复问题后，将 original_value 重新发送到原 topic

### 处理脚本示例

```python
import json
from kafka import KafkaConsumer, KafkaProducer

consumer = KafkaConsumer(
    'msgChatTransferDLT',
    bootstrap_servers=['127.0.0.1:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers=['127.0.0.1:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for msg in consumer:
    data = msg.value
    print(f"处理失败消息: {data['error']}")
    print(f"原始消息: {data['original_value']}")

    # 人工处理后，重新发送到原 topic
    # producer.send('msgChatTransfer', value=data['original_value'])
```

## 代码位置

| 文件 | 说明 |
|------|------|
| `apps/task/mq/pkg/retry/retry.go` | 重试 + DLQ 核心逻辑 |
| `apps/task/mq/pkg/dlq/producer.go` | Kafka DLQ 生产者 |
| `apps/task/mq/internal/handler/msgTransfer/msgChatTrasnfer.go` | 消息消费逻辑 |
| `apps/task/mq/etc/dev/task.yaml` | 配置文件 |

## 配置项

### task.yaml

```yaml
DeadLetterTopic: msgChatTransferDLT
```

### 重试配置（可自定义）

```go
var DefaultRetryConfig = retry.RetryConfig{
    MaxRetries:   3,              // 最大重试次数
    InitialDelay: 100 * time.Millisecond,  // 初始延迟
    MaxDelay:      5 * time.Second,        // 最大延迟
    BackoffMulti:  2.0,           // 退避倍数
}
```
