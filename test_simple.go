/**
 * 简单 ACK 测试
 */

package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/gorilla/websocket"
)

var addr = "localhost:10090"

func main() {
	fmt.Println("连接服务器...")
	wsURL := fmt.Sprintf("ws://%s/ws?userId=test_user", addr)

	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		fmt.Printf("连接失败: %v\n", err)
		return
	}
	defer conn.Close()

	fmt.Println("发送消息...")
	msgId := "test_msg_1"
	msg := map[string]interface{}{
		"id":     msgId,
		"ackSeq": 0,
		"method": "conversation.chat",
		"data": map[string]interface{}{
			"chatType":       1,
			"sendId":         "test_user",
			"recvId":         "user_1",
			"conversationId": "conv_test_user_user_1",
			"msgId":          msgId,
			"content":        "Hello",
			"mType":          1,
		},
	}

	data, _ := json.Marshal(msg)
	sendTime := time.Now()
	err = conn.WriteMessage(websocket.TextMessage, data)
	if err != nil {
		fmt.Printf("发送失败: %v\n", err)
		return
	}

	fmt.Println("等待服务器响应...")
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			fmt.Printf("读取失败: %v\n", err)
			return
		}

		latency := time.Since(sendTime)
		fmt.Printf("收到响应 (延迟: %v): %s\n", latency, string(data))

		var msg map[string]interface{}
		json.Unmarshal(data, &msg)
		if frameType, ok := msg["frameType"].(float64); ok {
			fmt.Printf("FrameType: %.0f\n", frameType)
		}
	}
}
