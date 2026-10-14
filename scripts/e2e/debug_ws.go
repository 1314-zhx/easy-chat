package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/gorilla/websocket"
)

func main() {
	// 连接发送方
	sendId := "test_send_001"
	recvId := "test_recv_001"
	
	sendURL := fmt.Sprintf("ws://localhost:10090/ws?userId=%s", sendId)
	recvURL := fmt.Sprintf("ws://localhost:10090/ws?userId=%s", recvId)

	fmt.Printf("连接发送方: %s\n", sendURL)
	sendConn, _, err := websocket.DefaultDialer.Dial(sendURL, nil)
	if err != nil {
		fmt.Printf("发送方连接失败: %v\n", err)
		return
	}
	defer sendConn.Close()
	fmt.Println("发送方连接成功")

	fmt.Printf("连接接收方: %s\n", recvURL)
	recvConn, _, err := websocket.DefaultDialer.Dial(recvURL, nil)
	if err != nil {
		fmt.Printf("接收方连接失败: %v\n", err)
		return
	}
	defer recvConn.Close()
	fmt.Println("接收方连接成功")

	// 接收方等待消息
	go func() {
		for {
			_, data, err := recvConn.ReadMessage()
			if err != nil {
				fmt.Printf("接收方读消息失败: %v\n", err)
				return
			}
			var msg map[string]interface{}
			json.Unmarshal(data, &msg)
			fmt.Printf("接收方收到: %+v\n", msg)
		}
	}()

	// 发送消息
	time.Sleep(1 * time.Second)
	fmt.Println("\n发送消息...")
	
	msg := map[string]interface{}{
		"id":     "test_msg_001",
		"ackSeq":  0,
		"method": "conversation.chat",
		"data": map[string]interface{}{
			"chatType":       2, // 2=SingleChatType
			"sendId":         sendId,
			"recvId":         recvId,
			"conversationId": fmt.Sprintf("conv_%s_%s", sendId, recvId),
			"msgId":          "test_msg_001",
			"content":        "hello",
			"mType":          1,
		},
	}
	
	data, _ := json.Marshal(msg)
	if err := sendConn.WriteMessage(websocket.TextMessage, data); err != nil {
		fmt.Printf("发送失败: %v\n", err)
		return
	}
	fmt.Println("消息已发送")
	
	// 等待响应
	time.Sleep(5 * time.Second)
	fmt.Println("测试完成")
}
