/**
 * @Author: easy-chat
 * @Description: WebSocket 压测工具 - 模拟大量消息推送
 */

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// 配置参数
var (
	addr      = flag.String("addr", "localhost:12112", "WebSocket 服务地址")
	users     = flag.Int("users", 100, "模拟用户数")
	msgs      = flag.Int("msgs", 1000, "每个用户发送消息数")
	concurrency = flag.Int("c", 10, "并发数")
)

var (
	sendSuccess int64
	sendFailed  int64
	sendLatencies []float64
	latencyMu    sync.Mutex
)

func main() {
	flag.Parse()

	fmt.Printf("=== Easy-Chat WebSocket 压测工具 ===\n")
	fmt.Printf("地址: ws://%s\n", *addr)
	fmt.Printf("用户数: %d\n", *users)
	fmt.Printf("每用户消息数: %d\n", *msgs)
	fmt.Printf("并发数: %d\n", *concurrency)
	fmt.Printf("===================================\n\n")

	start := time.Now()

	// 并发创建连接
	var wg sync.WaitGroup
	batchSize := *users / *concurrency

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(batch int) {
			defer wg.Done()
			start := batch * batchSize
			end := start + batchSize
			if batch == *concurrency-1 {
				end = *users
			}
			for u := start; u < end; u++ {
				runUser(u)
			}
		}(i)
	}

	wg.Wait()

	duration := time.Since(start)

	fmt.Printf("\n=== 压测结果 ===\n")
	fmt.Printf("总耗时: %v\n", duration)
	fmt.Printf("发送成功: %d\n", atomic.LoadInt64(&sendSuccess))
	fmt.Printf("发送失败: %d\n", atomic.LoadInt64(&sendFailed))
	fmt.Printf("总 QPS: %.2f\n", float64(atomic.LoadInt64(&sendSuccess))/duration.Seconds())

	// 统计延迟
	latencyMu.Lock()
	defer latencyMu.Unlock()
	if len(sendLatencies) > 0 {
		calcPercentiles()
	}
}

// 单个用户模拟
func runUser(userId int) {
	u := url.URL{Scheme: "ws", Host: *addr, Path: "/ws"}
	header := make(map[string][]string)
	header["token"] = []string{fmt.Sprintf("test_token_%d", userId)}

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), header)
	if err != nil {
		atomic.AddInt64(&sendFailed, int64(*msgs))
		return
	}
	defer conn.Close()

	// 发送消息
	for i := 0; i < *msgs; i++ {
		sendMsg(conn, userId, i)
		time.Sleep(10 * time.Millisecond) // 控制发送频率
	}
}

func sendMsg(conn *websocket.Conn, userId, seq int) {
	start := time.Now()

	msg := map[string]interface{}{
		"type": "push",
		"data": map[string]interface{}{
			"chat_type":       "single_chat",
			"send_id":         fmt.Sprintf("user_%d", userId),
			"recv_id":         fmt.Sprintf("user_%d", (userId+1)%*users),
			"conversation_id": fmt.Sprintf("conv_%d_%d", userId, (userId+1)%*users),
			"msg_id":          fmt.Sprintf("msg_%d_%d", userId, seq),
			"content":         fmt.Sprintf("Hello from user %d, msg %d", userId, seq),
			"m_type":          1,
		},
	}

	data, _ := json.Marshal(msg)
	err := conn.WriteMessage(websocket.TextMessage, data)
	if err != nil {
		atomic.AddInt64(&sendFailed, 1)
		return
	}

	latency := time.Since(start).Seconds() * 1000 // ms
	atomic.AddInt64(&sendSuccess, 1)

	latencyMu.Lock()
	sendLatencies = append(sendLatencies, latency)
	latencyMu.Unlock()
}

func calcPercentiles() {
	// 简单排序计算分位数
	n := len(sendLatencies)

	p50 := sendLatencies[n*50/100]
	p90 := sendLatencies[n*90/100]
	p99 := sendLatencies[n*99/100]

	fmt.Printf("延迟 P50: %.2f ms\n", p50)
	fmt.Printf("延迟 P90: %.2f ms\n", p90)
	fmt.Printf("延迟 P99: %.2f ms\n", p99)
}
