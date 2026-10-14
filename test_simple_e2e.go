/**
 * 简单 E2E 测试 - 直接测推送
 * 接收方发送 conversation.activate 激活会话，
 * 接收任何带 conversationId 的 push 消息计算延迟
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
	msgs  = flag.Int("msgs", 50, "消息数")
	users = flag.Int("users", 1, "收发对数")
)

var (
	success   int64
	recvCount int64
	pending   = make(map[string]time.Time)
	pMu       sync.RWMutex

	e2eLatencies []float64
	latMu        sync.Mutex
)

func main() {
	flag.Parse()

	fmt.Printf("===========================================\n")
	fmt.Printf("   简单 E2E 延迟测试\n")
	fmt.Printf("===========================================\n")
	fmt.Printf("用户对数: %d, 每对消息数: %d\n", *users, *msgs)

	var wg sync.WaitGroup

	start := time.Now()

	for u := 0; u < *users; u++ {
		sendId := fmt.Sprintf("send_user_%d", u)
		recvId := fmt.Sprintf("recv_user_%d", u)
		conversationId := fmt.Sprintf("conv_%d_%d", u, time.Now().UnixNano())
		wg.Add(1)
		go func(sendId, recvId, conv string) {
			defer wg.Done()
			runPair(sendId, recvId, conv)
		}(sendId, recvId, conversationId)
	}
	wg.Wait()

	duration := time.Since(start)
	fmt.Printf("\n总耗时: %v\n", duration)

	s := atomic.LoadInt64(&success)
	r := atomic.LoadInt64(&recvCount)
	fmt.Printf("发送: %d, 接收: %d, 成功匹配: %d\n", *users*(*msgs), r, s)

	latMu.Lock()
	if len(e2eLatencies) > 0 {
		calcPercentiles(e2eLatencies)
	}
	latMu.Unlock()
}

func runPair(sendId, recvId, conv string) {
	sendURL := fmt.Sprintf("ws://%s/ws?userId=%s", *addr, sendId)
	recvURL := fmt.Sprintf("ws://%s/ws?userId=%s", *addr, recvId)

	sendConn, _, err := websocket.DefaultDialer.Dial(sendURL, nil)
	if err != nil {
		fmt.Printf("[%s] 发送方连接失败: %v\n", sendId, err)
		return
	}
	defer sendConn.Close()
	fmt.Printf("[%s] 发送方已连接\n", sendId)

	recvConn, _, err := websocket.DefaultDialer.Dial(recvURL, nil)
	if err != nil {
		fmt.Printf("[%s] 接收方连接失败: %v\n", recvId, err)
		return
	}
	defer recvConn.Close()
	fmt.Printf("[%s] 接收方已连接\n", recvId)

	// 接收方激活 conversation
	activateMsg := map[string]interface{}{
		"id":     fmt.Sprintf("act_%s", recvId),
		"method": "conversation.activate",
		"data": map[string]interface{}{
			"conversationId": conv,
		},
	}
	aData, _ := json.Marshal(activateMsg)
	recvConn.WriteMessage(websocket.TextMessage, aData)

	// 接收方goroutine
	go func() {
		for {
			_, data, err := recvConn.ReadMessage()
			if err != nil {
				return
			}

			var msg map[string]interface{}
			if err := json.Unmarshal(data, &msg); err != nil {
				continue
			}

			atomic.AddInt64(&recvCount, 1)

			method, _ := msg["method"].(string)
			frameType, _ := msg["frameType"].(float64)
			id, _ := msg["id"].(string)

			// 处理 ACK 响应
			if int(frameType) == 2 || method == "ack" {
				continue
			}

			// 通过 id 匹配发送时间
			pMu.RLock()
			sendTime, exists := pending[id]
			pMu.RUnlock()

			if exists {
				latency := time.Since(sendTime).Seconds() * 1000
				latMu.Lock()
				e2eLatencies = append(e2eLatencies, latency)
				latMu.Unlock()
				atomic.AddInt64(&success, 1)
				pMu.Lock()
				delete(pending, id)
				pMu.Unlock()

				if atomic.LoadInt64(&success) <= 5 || atomic.LoadInt64(&success)%10 == 0 {
					fmt.Printf("[%s→%s] latency=%.2fms id=%s\n", sendId, recvId, latency, id)
				}
			}
		}
	}()

	// 等待连接稳定
	time.Sleep(300 * time.Millisecond)

	// 发送消息
	for i := 0; i < *msgs; i++ {
		msgId := fmt.Sprintf("msg_%s_%d", sendId, i)
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
				"conversationId": conv,
				"msgId":          msgId,
				"content":        "test content",
				"mType":          1,
			},
		}

		data, _ := json.Marshal(msg)
		if err := sendConn.WriteMessage(websocket.TextMessage, data); err != nil {
			fmt.Printf("[%s] 发送失败: %v\n", sendId, err)
			pMu.Lock()
			delete(pending, msgId)
			pMu.Unlock()
		}

		time.Sleep(20 * time.Millisecond)
	}

	// 等待接收
	time.Sleep(3 * time.Second)
}

func calcPercentiles(latencies []float64) {
	sort.Float64s(latencies)
	n := len(latencies)
	fmt.Printf("\n--- E2E 延迟 (ms) ---\n")
	fmt.Printf("样本数: %d\n", n)
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