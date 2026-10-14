/**
 * WebSocket 完整 E2E 延迟测试
 * 测：从发送方发出 → 接收方收到
 */

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

var (
	addr  = flag.String("addr", "localhost:10090", "服务地址")
	users = flag.Int("users", 10, "发送/接收用户对数")
	msgs  = flag.Int("msgs", 100, "每对用户消息数")
)

var (
	success int64
	failed  int64
	e2eLatencies []float64
	mu        sync.Mutex

	pending = make(map[string]time.Time)
	pMu     sync.RWMutex
)

func main() {
	flag.Parse()

	fmt.Printf("===========================================\n")
	fmt.Printf("   WebSocket E2E 延迟测试 (发送→接收)\n")
	fmt.Printf("===========================================\n")
	fmt.Printf("用户对数: %d, 每对消息数: %d\n", *users, *msgs)
	fmt.Printf("===========================================\n\n")

	start := time.Now()
	var wg sync.WaitGroup

	// 创建多对用户
	for u := 0; u < *users; u++ {
		sendId := fmt.Sprintf("e2e_send_%d", u)
		recvId := fmt.Sprintf("e2e_recv_%d", u)
		wg.Add(1)
		go func(sendId, recvId string) {
			defer wg.Done()
			runUserPair(sendId, recvId)
		}(sendId, recvId)
	}

	wg.Wait()
	duration := time.Since(start)

	fmt.Printf("\n===========================================\n")
	fmt.Printf("         E2E 延迟结果\n")
	fmt.Printf("===========================================\n")
	fmt.Printf("总耗时: %v\n", duration)
	fmt.Printf("成功: %d, 失败: %d\n", atomic.LoadInt64(&success), atomic.LoadInt64(&failed))

	s := atomic.LoadInt64(&success)
	if s > 0 {
		fmt.Printf("QPS: %.2f\n", float64(s)/duration.Seconds())
	}

	mu.Lock()
	if len(e2eLatencies) > 0 {
		calcPercentiles(e2eLatencies)
	}
	mu.Unlock()
}

func runUserPair(sendId, recvId string) {
	sendURL := fmt.Sprintf("ws://%s/ws?userId=%s", *addr, sendId)
	recvURL := fmt.Sprintf("ws://%s/ws?userId=%s", *addr, recvId)

	// 连接发送方
	sendConn, _, err := websocket.DefaultDialer.Dial(sendURL, nil)
	if err != nil {
		atomic.AddInt64(&failed, int64(*msgs))
		return
	}
	defer sendConn.Close()

	// 连接接收方
	recvConn, _, err := websocket.DefaultDialer.Dial(recvURL, nil)
	if err != nil {
		atomic.AddInt64(&failed, int64(*msgs))
		return
	}
	defer recvConn.Close()

	// 接收方等待消息
	recvDone := make(chan struct{})
	go func() {
		defer close(recvDone)
		for {
			_, data, err := recvConn.ReadMessage()
			if err != nil {
				return
			}

			var msg map[string]interface{}
			if err := json.Unmarshal(data, &msg); err != nil {
				continue
			}

			// frameType=2 是 ACK, frameType=1 是普通消息
			frameType, _ := msg["frameType"].(float64)
			if int(frameType) == 1 { // 收到聊天消息
				msgId, _ := msg["id"].(string)

				pMu.RLock()
				sendTime, exists := pending[msgId]
				pMu.RUnlock()

				if exists {
					latency := time.Since(sendTime).Seconds() * 1000
					mu.Lock()
					e2eLatencies = append(e2eLatencies, latency)
					mu.Unlock()
					atomic.AddInt64(&success, 1)

					pMu.Lock()
					delete(pending, msgId)
					pMu.Unlock()
				}
			}
		}
	}()

	// 发送方发消息
	for seq := 0; seq < *msgs; seq++ {
		msgId := fmt.Sprintf("e2e_%s_%d", sendId, seq)
		sendTime := time.Now()

		pMu.Lock()
		pending[msgId] = sendTime
		pMu.Unlock()

		msg := map[string]interface{}{
			"id":     msgId,
			"ackSeq": 0,
			"method": "conversation.chat",
			"data": map[string]interface{}{
				"chatType":       2, // 2=SingleChatType
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
			pMu.Lock()
			delete(pending, msgId)
			pMu.Unlock()
			atomic.AddInt64(&failed, 1)
		}

		time.Sleep(10 * time.Millisecond) // 控制发送节奏
	}

	// 等待所有消息到达
	time.Sleep(3 * time.Second)
}

func calcPercentiles(latencies []float64) {
	sort.Float64s(latencies)
	n := len(latencies)

	fmt.Printf("\n--- E2E 延迟 (ms) ---\n")
	fmt.Printf("P50:   %.2f ms\n", latencies[n*50/100])
	fmt.Printf("P90:   %.2f ms\n", latencies[n*90/100])
	fmt.Printf("P95:   %.2f ms\n", latencies[n*95/100])
	fmt.Printf("P99:   %.2f ms ★\n", latencies[n*99/100])
	fmt.Printf("Max:   %.2f ms\n", latencies[n-1])

	sum := 0.0
	for _, v := range latencies {
		sum += v
	}
	fmt.Printf("Avg:   %.2f ms\n", sum/float64(n))
}
