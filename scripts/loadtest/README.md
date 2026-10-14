# Easy-Chat 压测工具

## 快速开始

### 1. 启动监控组件

```bash
cd deploy
docker-compose up -d

# 确认服务运行
docker ps
```

### 2. 启动 IM 服务

```bash
cd apps/im/ws
go run im.go -f etc/dev/im.yaml
```

### 3. 启动 HTTP 压测

```bash
cd scripts/loadtest

# 初始化依赖
go mod tidy

# 运行压测（自动在 :9091 暴露 /metrics）
go run http_loadtest.go -url http://localhost:12112 -n 10000 -c 100
```

### 4. 查看 Prometheus 指标

```bash
# 查看压测生成的指标
curl http://localhost:9091/metrics | grep loadtest

# Prometheus 界面
# http://localhost:9090
# 查询 QPS:
#   rate(loadtest_http_requests_total{status="success"}[1m])

# 查询 P99 延迟:
#   histogram_quantile(0.99, rate(loadtest_http_request_duration_milliseconds_bucket[5m]))
```

---

## 工具说明

### HTTP 压测工具 (`http_loadtest.go`)

**参数说明：**

| 参数 | 默认值 | 说明 |
|:---|:---|:---|
| `-url` | http://localhost:12112 | 目标地址 |
| `-n` | 10000 | 总请求数 |
| `-c` | 100 | 并发数 |
| `-d` | 0 | 持续时间，如 `-d 30s` |

**生成的 Prometheus 指标：**

```
# HELP loadtest_http_requests_total Total HTTP requests
# TYPE loadtest_http_requests_total counter
loadtest_http_requests_total{method="POST",path="/api/message/send",status="success"} 5000
loadtest_http_requests_total{method="GET",path="/api/conversation/list",status="success"} 5000

# HELP loadtest_http_request_duration_milliseconds HTTP request duration in milliseconds
# TYPE loadtest_http_request_duration_milliseconds histogram
loadtest_http_request_duration_milliseconds_bucket{method="POST",path="/api/message/send",le="10"} 1000
loadtest_http_request_duration_milliseconds_bucket{method="POST",path="/api/message/send",le="50"} 4500
loadtest_http_request_duration_milliseconds_bucket{method="POST",path="/api/message/send",le="+Inf"} 5000
```

---

### WebSocket 压测工具 (`websocket_loadtest.go`)

**参数说明：**

| 参数 | 默认值 | 说明 |
|:---|:---|:---|
| `-addr` | localhost:12112 | 服务地址 |
| `-users` | 100 | 模拟用户数 |
| `-msgs` | 1000 | 每个用户发送消息数 |
| `-c` | 10 | 并发数 |

**使用方法：**

```bash
go run websocket_loadtest.go -addr localhost:12112 -users 50 -msgs 100
```

---

## 完整压测流程

### Step 1: 启动所有服务

```bash
# 终端 1: 启动 Prometheus + Grafana
cd deploy
docker-compose up -d

# 终端 2: 启动 IM 服务
cd apps/im/ws
go run im.go -f etc/dev/im.yaml

# 终端 3: 启动压测
cd scripts/loadtest
go run http_loadtest.go -url http://localhost:12112 -n 50000 -c 200
```

### Step 2: 在 Grafana 中查看 Dashboard

1. 访问 http://localhost:3000
2. 登录: `admin` / `admin123`
3. 导入 Dashboard: `deploy/grafana/dashboards/im-monitoring.json`

### Step 3: Prometheus 查询示例

```promql
# 服务端消息推送延迟 P99
histogram_quantile(0.99,
  rate(easychat_message_push_latency_milliseconds_bucket[5m])
)

# 压测客户端 QPS
rate(loadtest_http_requests_total{status="success"}[1m])

# 压测客户端延迟 P99
histogram_quantile(0.99,
  rate(loadtest_http_request_duration_milliseconds_bucket[5m])
)

# 成功率
rate(loadtest_http_requests_total{status="success"}[5m])
/
(rate(loadtest_http_requests_total{status="success"}[5m]) + rate(loadtest_http_requests_total{status="error"}[5m]))
```

---

## 模拟不同场景

### 低负载（正常使用）
```bash
go run http_loadtest.go -url http://localhost:12112 -n 1000 -c 10
```

### 正常负载
```bash
go run http_loadtest.go -url http://localhost:12112 -n 10000 -c 100
```

### 高负载（压测极限）
```bash
go run http_loadtest.go -url http://localhost:12112 -n 100000 -c 500
```

### 持续压测
```bash
# 持续压测 5 分钟
go run http_loadtest.go -url http://localhost:12112 -c 100 -d 5m
```

---

## Prometheus + Grafana 完整链路

```
┌─────────────────────────────────────────────────────────────────────────┐
│                            压测链路                                      │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   ┌─────────────┐      ┌─────────────┐      ┌─────────────┐           │
│   │  压测工具    │      │  IM 服务     │      │  Prometheus │           │
│   │ (负载生成)   │ ──▶  │  (被测服务)   │ ←─── │  (数据抓取)  │           │
│   └─────────────┘      └─────────────┘      └─────────────┘           │
│         │                    │                    │                    │
│         │   /metrics         │                    │                    │
│         └────────────────────┴────────────────────┘                    │
│                              │                                          │
│                              ▼                                          │
│                      ┌─────────────┐                                   │
│                      │   Grafana   │                                   │
│                      │  (可视化)   │                                   │
│                      └─────────────┘                                   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 数据流向

1. **压测工具** 生成负载 → 请求 IM 服务
2. **压测工具** 暴露 `/metrics` → Prometheus 抓取压测指标
3. **IM 服务** 暴露 `/metrics` → Prometheus 抓取业务指标
4. **Prometheus** 存储时序数据 → Grafana 查询展示

---

## 常见问题

### Q: 压测时 Prometheus 抓取不到数据？

检查：
1. Prometheus 是否正常运行: `docker ps`
2. 目标服务是否暴露 `/metrics`
3. prometheus.yml 是否配置正确

```bash
# 测试指标端点
curl http://localhost:12112/metrics | head
curl http://localhost:9091/metrics | head
```

### Q: 如何测试消息推送延迟？

需要在 IM 服务代码中添加埋点：

```go
// apps/im/ws/internal/handler/push/push.go
import "easy-chat/pkg/metrics"

func single(srv *websocket.Server, data *ws.Push, recvId string) error {
    start := time.Now()
    // ... 业务逻辑 ...
    metrics.MessagePushLatency.WithLabelValues("single_chat", "push").Observe(
        float64(time.Since(start).Milliseconds()),
    )
    return nil
}
```

### Q: 如何同时压测多个接口？

修改 `http_loadtest.go` 中的 `apis` 数组，添加更多接口。
