/**
 * WebSocket ACK 延迟压测 - 异步版
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
	users = flag.Int("users", 10, "并发用户数")
	msgs  = flag.Int("msgs", 1000, "每用户消息数")
)

var (
	success   int64
	failed    int64
	latencies []float64
	mu        sync.Mutex

	// 待匹配的发送记录
	pending = make(map[string]time.Time)
	pMu     sync.RWMutex
)

func main() {
	flag.Parse()

	fmt.Printf("===========================================\n")
	fmt.Printf("   WebSocket ACK 延迟压测\n")
	fmt.Printf("===========================================\n")
	fmt.Printf("用户数: %d, 每用户消息数: %d\n", *users, *msgs)
	fmt.Printf("===========================================\n\n")

	start := time.Now()
	var wg sync.WaitGroup

	for u := 0; u < *users; u++ {
		wg.Add(1)
		go func(uid int) {
			defer wg.Done()
			runUser(uid)
		}(u)
	}

	wg.Wait()
	duration := time.Since(start)

	fmt.Printf("\n===========================================\n")
	fmt.Printf("         ACK 延迟结果\n")
	fmt.Printf("===========================================\n")
	fmt.Printf("总耗时: %v\n", duration)
	fmt.Printf("成功: %d, 失败: %d\n", atomic.LoadInt64(&success), atomic.LoadInt64(&failed))

	s := atomic.LoadInt64(&success)
	if s > 0 {
		fmt.Printf("QPS: %.2f\n", float64(s)/duration.Seconds())
	}

	mu.Lock()
	if len(latencies) > 0 {
		calcPercentiles(latencies)
	}
	mu.Unlock()
}

func runUser(uid int) {
	sendId := fmt.Sprintf("user_%d", uid)
	wsURL := fmt.Sprintf("ws://%s/ws?userId=%s", *addr, sendId)

	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		atomic.AddInt64(&failed, int64(*msgs))
		return
	}
	defer conn.Close()

	// 读取协程 - 异步处理 ACK
	go func() {
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}

			var msg map[string]interface{}
			if err := json.Unmarshal(data, &msg); err != nil {
				continue
			}

			if frameType, ok := msg["frameType"].(float64); ok && int(frameType) == 2 {
				msgId, _ := msg["id"].(string)

				pMu.RLock()
				sendTime, exists := pending[msgId]
				pMu.RUnlock()

				if exists {
					latency := time.Since(sendTime).Seconds() * 1000
					mu.Lock()
					latencies = append(latencies, latency)
					mu.Unlock()
					atomic.AddInt64(&success, 1)

					pMu.Lock()
					delete(pending, msgId)
					pMu.Unlock()
				}
			}
		}
	}()

	for seq := 0; seq < *msgs; seq++ {
		msgId := fmt.Sprintf("%s_%d_%d", sendId, uid, seq)
		sendTime := time.Now()

		// 记录发送时间
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
				"recvId":         fmt.Sprintf("user_%d", (uid+1)%*users),
				"conversationId": fmt.Sprintf("conv_%s", sendId),
				"msgId":          msgId,
				"content":        "test",
				"mType":          1,
			},
		}

		data, _ := json.Marshal(msg)
		if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
			pMu.Lock()
			delete(pending, msgId)
			pMu.Unlock()
			atomic.AddInt64(&failed, 1)
		}

		// 小的发送间隔防止过载
		if seq%10 == 0 {
			time.Sleep(100 * time.Microsecond)
		}
	}

	// 等待 ACK 全部返回
	time.Sleep(2 * time.Second)
}

func calcPercentiles(latencies []float64) {
	sort.Float64s(latencies)
	n := len(latencies)

	fmt.Printf("\n--- ACK 延迟 (ms) ---\n")
	fmt.Printf("P50:   %.2f ms\n", latencies[n*50/100])
	fmt.Printf("P90:   %.2f ms\n", latencies[n*90/100])
	fmt.Printf("P95:   %.2f ms\n", latencies[n*95/100])
	fmt.Printf("P99:   %.2f ms ★\n", latencies[n*99/100])
	fmt.Printf("P99.9: %.2f ms\n", latencies[n*999/1000])
	fmt.Printf("Max:   %.2f ms\n", latencies[n-1])

	sum := 0.0
	for _, v := range latencies {
		sum += v
	}
	fmt.Printf("Avg:   %.2f ms\n", sum/float64(n))
}
