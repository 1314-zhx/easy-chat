/**
 * 极简测试 - 只连接不发消息
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

	// 测试1: 单连接
	fmt.Println("测试1: 单连接保活")
	conn, _, err := websocket.DefaultDialer.Dial(fmt.Sprintf("ws://%s/ws?userId=test_single", *addr), nil)
	if err != nil {
		fmt.Printf("连接失败: %v\n", err)
		return
	}
	defer conn.Close()
	fmt.Println("连接成功，保持30秒...")

	// 读消息
	go func() {
		for i := 0; i < 100; i++ {
			_, data, err := conn.ReadMessage()
			if err != nil {
				fmt.Printf("读取错误: %v\n", err)
				return
			}
			var msg map[string]interface{}
			json.Unmarshal(data, &msg)
			fmt.Printf("收到: %v\n", msg)
		}
	}()

	time.Sleep(30 * time.Second)
	fmt.Println("测试1完成")
}
