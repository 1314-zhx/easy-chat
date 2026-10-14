# ACK 机制 & 消息推送链路实现文档

## 一、ACK 机制概述

项目支持三种 ACK 模式：

| 模式 | 握手次数 | 说明 |
|------|---------|------|
| **NoAck** | 无 | 无确认，最快但不可靠 |
| **OnlyAck** | 1次 | 服务端收到消息后立即返回 ACK |
| **RigorAck** | 3次 | 等待 MongoDB 落库成功后返回 ACK，客户端确认后完成 |

---

## 二、三种模式时序图

### NoAck

```
客户端 ── 消息 ──► 服务端 ── Kafka ──► 完成（无 ACK）
```

### OnlyAck（1次 ACK）

```
客户端 ── 消息 (AckSeq=0) ──► 服务端
                               │
                        ──► Kafka
                               │
客户端 ◄── ACK (AckSeq=1) ←────
                               │
客户端 ── 确认 ──────────────► 服务端（可选）
```

### RigorAck（三次握手）

```
客户端                服务端              Kafka/Consumer       MongoDB
  │                     │                     │                  │
  │── 消息 (AckSeq=0) ──►│                     │                  │
  │                     │── Push ───────────►│                  │
  │                     │                     │── 消费 ────────►│
  │                     │                     │                  │
  │                     │                     │◄─ 落库成功 ────│
  │                     │                     │                  │
  │                     │◄─ FrameDbAck ───────│                  │
  │                     │                     │                  │
  │◄── ACK (AckSeq=1) ──│                     │                  │
  │                     │                     │                  │
  │── ACK (AckSeq=2) ──►│                     │                  │
  │                     │                     │                  │
  │  ✅ 三次握手完成     │                     │                  │
```

---

## 三、OnlyAck 一次 ACK 完整链路

### 流程

```
1. 客户端发送消息 (AckSeq=0)
         ↓
2. 服务端收到，加入 readMessage 队列
         ↓
3. 服务端发送 ACK (AckSeq=1)
         ↓
4. 服务端处理消息（Kafka Push）
         ↓
5. 客户端收到 ACK，消息发送成功
         ↓
6. 客户端发送确认（AckSeq=2，可选）
```

### 代码位置

| 功能 | 文件 | 方法 |
|------|------|------|
| 接收消息 | `server.go` | `handlerConn` |
| 发送 ACK | `server.go` | `processAckQueue` |
| Kafka Push | `conversation.go` | `Chat` |

---

## 四、RigorAck 三次握手完整链路

### 流程

```
1. 客户端发送消息 (AckSeq=0)
         ↓
2. 服务端收到，存入 ShardedAckMap
         ↓
3. 服务端 Push Kafka
         ↓
4. Consumer 消费，MongoDB 落库
         ↓
5. Consumer 发送 FrameDbAck
         ↓
6. 服务端发送 ACK (AckSeq=1)
         ↓
7. 客户端收到 ACK，发送第三次 ACK (AckSeq=2)
         ↓
8. 服务端从 ShardedAckMap 删除，完成三次握手
```

### 核心代码

```go
// Chat handler - RigorAck 存入 ShardedAckMap
func Chat(svc *svc.ServiceContext) websocket.HandlerFunc {
    return func(srv *websocket.Server, conn *websocket.Conn, msg *websocket.Message) {
        // Kafka Push
        err := svc.MsgChatTransferClient.Push(&mq.MsgChatTransfer{...})
        if err != nil {
            srv.Send(websocket.NewErrMessage(err), conn)
            return
        }

        // RigorAck: 存入 ShardedAckMap
        if srv.IsRigorAck() {
            srv.StoreToShardedAckMap(msg.Id, msg)
        }
    }
}

// handleDbAck - MongoDB 落库成功后发送 ACK
func (s *Server) handleDbAck(conn *Conn, message *Message) {
    // 从 ShardedAckMap 获取消息
    shard := s.getShardIndex(msgId)
    s.shardedAckMu[shard].Lock()
    shardedMsg := s.shardedAckMap[shard][msgId]
    s.shardedAckMu[shard].Unlock()

    // 发送 ACK (AckSeq=1)
    ackMsg := &Message{
        FrameType:  FrameAck,
        Id:         shardedMsg.Id,
        AckSeq:     1,
        ClientMsgId: message.ClientMsgId,
    }
    s.sendWithRetry(ackMsg, conn)
}

// handleClientAck - 客户端第三次 ACK，从 ShardedAckMap 删除
func (s *Server) handleClientAck(conn *Conn, message *Message) {
    if message.AckSeq < 2 {
        return
    }

    shard := s.getShardIndex(message.Id)
    s.shardedAckMu[shard].Lock()
    delete(s.shardedAckMap[shard], message.Id)
    s.shardedAckMu[shard].Unlock()
}
```

---

## 五、ShardedAckMap 数据结构

用于 RigorAck 三次握手，存储待确认的消息：

```go
type Server struct {
    // 32 个分片，减少锁竞争
    shardedAckMu  [32]sync.Mutex
    shardedAckMap [32]map[string]*Message
}

func (s *Server) getShardIndex(key string) int {
    return int(key[len(key)-1]) % 32
}
```

---

## 六、完整消息链路流程

### 第一步：WS 连接建立

```
ServerWs
    │
    ├─► NewConn → 封装 websocket.Conn、启动心跳检测
    │
    ├─► 鉴权
    │
    └─► s.addConn → conn↔user双向映射
              └─► go handlerConn(conn)
```

### 第二步：handlerConn 初始化

```
handlerConn
    │
    ├─► conn.Uid = GetUsers(conn)[0]
    │
    ├─► go handlerWrite(conn)
    │
    └─► for 循环监听 ReadMessage()
```

### 第三步：接收客户端消息

```
handlerConn
    │
    ├─► json.Unmarshal
    │
    ├─► FrameAck + RigorAck → handleClientAck() // 第三次 ACK
    │
    └─► isAck(&message) → conn.appendMsgMq() → readMessage队列
```

### 第四步：handlerWrite 处理业务

```
handlerWrite
    │
    └─► routes[Method](s, conn, message)
              │
              └─► Chat 方法
```

### 第五步：Chat 方法

```
Chat
    │
    ├─► Kafka Push
    │
    └─► RigorAck: StoreToShardedAckMap(msg.Id, msg)
```

### 第六步：Kafka Consumer

```
Consume
    │
    ├─► MongoDB 落库
    │
    └─► sendDbAck(clientMsgId) → FrameDbAck
```

### 第七步：handleDbAck

```
handleDbAck
    │
    ├─► 从 ShardedAckMap 获取消息
    │
    └─► 发送 ACK (AckSeq=1)
```

### 第八步：客户端收到 ACK

```
客户端收到 AckSeq=1
    │
    └─► 发送 ACK (AckSeq=2)
```

### 第九步：handleClientAck

```
handleClientAck
    │
    └─► 从 ShardedAckMap 删除消息
              │
              └─► 三次握手完成
```

### 第十步：Transfer 推送消息

```
Transfer
    └─► WsClient.Send(msg)
```

### 第十一步：push.Push

```
push.Push
    ├─► srv.GetConn(recvId)
    └─► WorkerPool.Send()
```

---

## 七、异常流程

### 7.1 RigorAck 异常场景

**MongoDB 落库失败：**
```
1. Kafka Push 成功
2. Consumer 落库失败
3. 进入重试逻辑
4. 重试达上限，进入死信队列
5. Consumer 发送 FrameDbAck（带错误信息）
6. 服务端收到，不发送 ACK
7. 客户端超时重发（从 ShardedAckMap 重试）
```

**客户端超时重发：**
```
1. 客户端发送消息 (id: "msg-123")
2. 服务端存入 ShardedAckMap
3. MongoDB 落库成功，发送 ACK (AckSeq=1)
4. 客户端未收到 ACK
5. 客户端超时重发 (id: "msg-123")
6. 服务端收到，ShardedAckMap 中已有
7. 直接重新发送 ACK (AckSeq=1)
```

---

## 八、FrameType 定义

```go
const (
    FrameData  FrameType = 0x0  // 普通数据帧
    FramePing  FrameType = 0x1  // 心跳帧
    FrameAck   FrameType = 0x2  // ACK 确认帧
    FrameNoAck FrameType = 0x3  // 不需要确认的帧
    FrameErr   FrameType = 0x9  // 错误帧
    FrameDbAck FrameType = 0xA  // MongoDB 落库成功通知
)
```

---

## 九、配置方式

```go
// 启动 WebSocket Server 时指定 ACK 模式
ws.NewServer(
    ws.WithAck(ws.OnlyAck),  // 可选: NoAck, OnlyAck, RigorAck
    // ...
)
```

---

## 十、性能指标

| 指标 | 数值 |
|------|------|
| OnlyAck 延迟 | < 20ms |
| RigorAck 延迟 | ~50-100ms |
| WorkerPool 并发数 | 100 个 goroutine |
| 任务队列容量 | 10000 |
| ShardedAckMap 分片数 | 32 |
