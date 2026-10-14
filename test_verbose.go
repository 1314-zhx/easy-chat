/**
 * 详细日志测试
 */

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"time"

	"github.com/gorilla/websocket"
)

var addr = flag.String("addr", "localhost:10090", "服务地址")

func main() {
	flag.Parse()

	sendId := "send_user"
	recvId := "recv_user"

	// 先连接接收方
	fmt.Println("连接接收方:", recvId)
	recvConn, _, err := websocket.DefaultDialer.Dial(
		fmt.Sprintf("ws://%s/ws?userId=%s", *addr, recvId), nil)
	if err != nil {
		fmt.Printf("接收方连接失败: %v\n", err)
		return
	}
	fmt.Println("接收方连接成功")

	// 接收方读消息（详细日志）
	go func() {
		for i := 0; i < 100; i++ {
			_, data, err := recvConn.ReadMessage()
			if err != nil {
				fmt.Printf("接收方读取错误: %v\n", err)
				return
			}
			
			var msg map[string]interface{}
			if err := json.Unmarshal(data, &msg); err != nil {
				fmt.Printf("JSON解析错误: %v\n", err)
				continue
			}
			
			b, _ := json.Marshal(msg)
			fmt.Printf("接收方收到: %s\n", string(b))
		}
	}()

	// 等待一下
	time.Sleep(2 * time.Second)

	// 再连接发送方
	fmt.Println("\n连接发送方:", sendId)
	sendConn, _, err := websocket.DefaultDialer.Dial(
		fmt.Sprintf("ws://%s/ws?userId=%s", *addr, sendId), nil)
	if err != nil {
		fmt.Printf("发送方连接失败: %v\n", err)
		return
	}
	fmt.Println("发送方连接成功")

	// 发送一条消息
	msg := map[string]interface{}{
		"id":     "test_msg_1",
		"ackSeq": 0,
		"method": "conversation.chat",
		"data": map[string]interface{}{
			"chatType":       1,
			"sendId":         sendId,
			"recvId":         recvId,
			"conversationId": "conv_test",
			"msgId":          "test_msg_1",
			"content":        "Hello!",
			"mType":          1,
		},
	}
	data, _ := json.Marshal(msg)
	fmt.Printf("\n发送消息: %s\n", string(data))
	
	if err := sendConn.WriteMessage(websocket.TextMessage, data); err != nil {
		fmt.Printf("发送失败: %v\n", err)
		return
	}
	fmt.Println("消息已发送")

	// 等待结果
	fmt.Println("\n等待10秒...")
	time.Sleep(10 * time.Second)

	fmt.Println("\n测试结束")
	sendConn.Close()
	recvConn.Close()
}
