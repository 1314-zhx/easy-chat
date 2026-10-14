# easy-chat

基于 [go-zero](https://github.com/zeromicro/go-zero) 的即时通讯（IM）学习/实践项目：用户与社交走 HTTP/gRPC，聊天走 WebSocket + Kafka 异步落库，支持多实例 WS 转发、已读回执、AI 会话和可观测性。

## 架构

```
客户端 ──HTTP──► user-api / social-api / im-api
                │
                └── zrpc + etcd 发现 + P2C ──► user-rpc / social-rpc / im-rpc

客户端 ──WS────► im.ws  ──Kafka──► task-mq ──Mongo 落库
                   │                  │
                   │                  └── 再推回 im.ws（本机连接或 Relayer gRPC）
                   └── Redis：在线状态、用户所在 WS 实例
```

| 服务 | 作用 |
|---|---|
| `apps/user` | 注册、登录、JWT、用户资料（MySQL） |
| `apps/social` | 好友、群（MySQL） |
| `apps/im/api` | 会话、历史消息 HTTP 查询 |
| `apps/im/rpc` | 会话与聊天记录 RPC |
| `apps/im/ws` | 长连接、收发、跨实例 Relayer |
| `apps/task/mq` | 消费 Kafka，批量写 Mongo，推送与 ACK |
| `apps/ai/rpc` | AI 对话 / RAG |

配置热更新走 etcd（`pkg/etcdwatch`，key 如 `/config/im.ws`）。入口网关可选用 compose 里的 APISIX（默认未配路由）。

## 技术栈

- Go、go-zero、gRPC、etcd
- MySQL（用户/社交）、MongoDB（chat_log 等）、Redis
- Kafka（`msgChatTransfer` / `msgReadTransfer` 等）
- WebSocket（gorilla）、Prometheus / 可选 Jaeger

消息幂等依赖 Mongo 稀疏唯一索引 `clientMsgId`；WS 侧分片 map 用于 ACK 握手，不是 Kafka 去重。

## 快速开始

环境：Go 1.25+、Docker。先起依赖：

```bash
docker compose up -d
```

会拉起 etcd、Redis、MySQL、Mongo、Kafka、APISIX 等。初始化库表见 `deploy/sql/`。

Windows 可一键起业务进程：

```bat
start.cmd
```

或分别 `go run .`：

- user-rpc / user-api
- social-rpc / social-api
- im-rpc / im.ws / im-api
- ai-rpc、task-mq

开发配置在各服务 `etc/dev/*.yaml`（端口、etcd key、数据源）。本地示例：user-api `8888`，im.ws `10090`。简单页面在 `web-easy-im/`。

## 仓库说明

- `pkg/` 公共库（JWT、限流无关的缓存、指标、生命周期）
- `deploy/` Dockerfile、Makefile、监控与 SQL
- `scripts/` 压测、e2e、pprof
- `docs/` ACK、热更新、监控等说明

`components/*/data` 等本地数据卷已在 `.gitignore` 中排除。yaml 里含开发用密钥，生产请替换。
