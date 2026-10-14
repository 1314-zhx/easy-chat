/**
 * WebSocket E2E 延迟测试 - 调试版
 */

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

var (
	addr  = flag.String("addr", "localhost:10090", "服务地址")
	users = flag.Int("users", 2, "发送/接收用户对数")
	msgs  = flag.Int("msgs", 10, "每对用户消息数")
)

var (
	success int64
	failed  int64
	pending = make(map[string]time.Time)
	pMu     sync.RWMutex
	recvLog []string
	mu      sync.Mutex
)

func main() {
	flag.Parse()

	fmt.Printf("===========================================\n")
	fmt.Printf("   WebSocket E2E 延迟测试\n")
	fmt.Printf("===========================================\n")

	sendId := fmt.Sprintf("e2e_send")
	recvId := fmt.Sprintf("e2e_recv")
	
	sendURL := fmt.Sprintf("ws://%s/ws?userId=%s", *addr, sendId)
	recvURL := fmt.Sprintf("ws://%s/ws?userId=%s", *addr, recvId)

	// 连接发送方
	sendConn, _, err := websocket.DefaultDialer.Dial(sendURL, nil)
	if err != nil {
		fmt.Printf("发送方连接失败: %v\n", err)
		return
	}
	defer sendConn.Close()

	// 连接接收方
	recvConn, _, err := websocket.DefaultDialer.Dial(recvURL, nil)
	if err != nil {
		fmt.Printf("接收方连接失败: %v\n", err)
		return
	}
	defer recvConn.Close()

	// 接收方等待消息
	go func() {
		for i := 0; i < *msgs; i++ {
			_, data, err := recvConn.ReadMessage()
			if err != nil {
				fmt.Printf("接收方读取错误: %v\n", err)
				return
			}

			mu.Lock()
			recvLog = append(recvLog, string(data))
			mu.Unlock()

			var msg map[string]interface{}
			if err := json.Unmarshal(data, &msg); err != nil {
				fmt.Printf("JSON 解析错误: %v\n", err)
				continue
			}

			fmt.Printf("[RECV] frameType=%v, id=%v\n", msg["frameType"], msg["id"])

			frameType, _ := msg["frameType"].(float64)
			if int(frameType) == 0 || int(frameType) == 2 { // FrameData or FrameAck
				msgId, _ := msg["id"].(string)

				pMu.RLock()
				sendTime, exists := pending[msgId]
				pMu.RUnlock()

				if exists {
					latency := time.Since(sendTime).Seconds() * 1000
					fmt.Printf("[HIT] msgId=%s, latency=%.2fms\n", msgId, latency)
					atomic.AddInt64(&success, 1)
					pMu.Lock()
					delete(pending, msgId)
					pMu.Unlock()
				}
			}
		}
	}()

	// 发送消息
	for i := 0; i < *msgs; i++ {
		msgId := fmt.Sprintf("e2e_%s_%d", sendId, i)
		sendTime := time.Now()

		pMu.Lock()
		pending[msgId] = sendTime
		pMu.Unlock()

		msg := map[string]interface{}{
			"id":     msgId,
			"ackSeq": 0,
			"method": "conversation.chat",
			"data": map[string]interface{}{
				"chatType":       1,
				"sendId":         sendId,
				"recvId":         recvId,
				"conversationId": fmt.Sprintf("conv_%s_%s", sendId, recvId),
				"msgId":          msgId,
				"content":        "test",
				"mType":          1,
			},
		}

		data, _ := json.Marshal(msg)
		if err := sendConn.WriteMessage(websocket.TextMessage, data); err != nil {
			fmt.Printf("发送失败: %v\n", err)
			atomic.AddInt64(&failed, 1)
		} else {
			fmt.Printf("[SEND] msgId=%s\n", msgId)
		}

		time.Sleep(100 * time.Millisecond)
	}

	time.Sleep(2 * time.Second)
	fmt.Printf("\n===========================================\n")
	fmt.Printf("成功: %d, 失败: %d\n", atomic.LoadInt64(&success), atomic.LoadInt64(&failed))
}
