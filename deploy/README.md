# Jaeger + MongoDB 部署指南

## 架构

```
go-zero 应用 → OTLP Collector → Kafka → task-mq → MongoDB
                                                         ↓
                                               Jaeger Query ← Jaeger UI
```

## 部署步骤

### 1. 下载 Jaeger MongoDB Storage Plugin

**Linux / Mac:**
```bash
cd deploy
bash scripts/download-jaeger-plugin.sh
```

**Windows:**
手动下载：https://github.com/jaegertracing/jaeger-storage-mongodb/releases

解压到 `deploy/jaeger-plugin/` 目录

### 2. 启动基础服务

```bash
cd deploy
docker-compose up -d kafka mongo
```

### 3. 创建 Kafka Topic

```bash
docker exec easy-chat-kafka kafka-topics.sh --create \
  --topic jaeger-traces \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1
```

### 4. 启动 Jaeger (带 MongoDB 插件)

```bash
docker-compose up -d jaeger
```

### 5. 配置应用 Telemetry

在 `apps/im/api/etc/dev/im.yaml` 添加：

```yaml
Name: im
Host: 0.0.0.0
Port: 8888

Telemetry:
  Name: im-api
  Endpoint: http://localhost:4318
  Sampler: 1.0
  SamplerType: const

# ... 其他配置
```

### 6. 启动 task-mq

```bash
go run apps/task/mq/mq.go -f etc/dev/task.yaml
```

### 7. 启动应用

```bash
go run apps/im/api/im.go
```

## 验证

访问 http://localhost:16686 查看 Jaeger UI

## 常见问题

### Q: Jaeger 启动失败，提示找不到插件
A: 确保 `deploy/jaeger-plugin/` 目录下有 `jaeger-storage-grpc-plugin` 文件

### Q: 没有看到 trace 数据
A: 检查：
1. MongoDB 是否正常连接
2. Kafka topic 是否存在
3. task-mq 是否正常运行
4. 应用 Telemetry 是否配置正确
