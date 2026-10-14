package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

func goroutineProfile() (int, string, error) {
	resp, err := http.Get("http://127.0.0.1:6060/debug/pprof/goroutine?debug=1")
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, "", err
	}
	text := string(b)
	total := 0
	fmt.Sscanf(text, "goroutine profile: total %d", &total)
	return total, text, nil
}

func countStacks(text string, needles []string) map[string]int {
	out := make(map[string]int, len(needles))
	for _, n := range needles {
		out[n] = strings.Count(text, n)
	}
	return out
}

func main() {
	users := 50
	msgs := 5
	needles := []string{
		"easy-chat/apps/im/ws/websocket.(*Conn).keepalive",
		"easy-chat/apps/im/ws/websocket.(*ConnQueue).run",
		"easy-chat/apps/im/ws/websocket.(*Server).handlerConn",
		"easy-chat/apps/im/ws/websocket.(*Server).handlerWrite",
		"easy-chat/apps/im/ws/websocket.(*Server).readAck",
		"easy-chat/apps/im/ws/websocket.(*WorkerPool).worker",
	}

	base, baseText, err := goroutineProfile()
	if err != nil {
		fmt.Println("pprof failed:", err)
		os.Exit(1)
	}
	fmt.Printf("baseline goroutines: %d stacks=%v\n", base, countStacks(baseText, needles))

	var mu sync.Mutex
	conns := make([]*websocket.Conn, 0, users)
	var wg sync.WaitGroup
	for u := 0; u < users; u++ {
		wg.Add(1)
		go func(uid int) {
			defer wg.Done()
			url := fmt.Sprintf("ws://127.0.0.1:10090/ws?userId=pprof_%d", uid)
			c, _, err := websocket.DefaultDialer.Dial(url, nil)
			if err != nil {
				fmt.Println("dial fail", uid, err)
				return
			}
			go func() {
				for {
					if _, _, err := c.ReadMessage(); err != nil {
						return
					}
				}
			}()
			for seq := 0; seq < msgs; seq++ {
				id := fmt.Sprintf("pprof_%d_%d", uid, seq)
				body, _ := json.Marshal(map[string]interface{}{
					"id":     id,
					"ackSeq": 0,
					"method": "conversation.chat",
					"data": map[string]interface{}{
						"chatType":       1,
						"sendId":         fmt.Sprintf("pprof_%d", uid),
						"recvId":         fmt.Sprintf("pprof_%d", (uid+1)%users),
						"conversationId": fmt.Sprintf("conv_pprof_%d", uid),
						"content":        "pprof",
						"mType":          1,
					},
				})
				_ = c.WriteMessage(websocket.TextMessage, body)
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
		}(u)
	}
	wg.Wait()
	time.Sleep(800 * time.Millisecond)

	peak, peakText, err := goroutineProfile()
	if err != nil {
		fmt.Println("pprof peak failed:", err)
		os.Exit(1)
	}
	fmt.Printf("peak goroutines:     %d stacks=%v  delta=%d\n", peak, countStacks(peakText, needles), peak-base)

	for _, c := range conns {
		_ = c.Close()
	}
	time.Sleep(3 * time.Second)

	after, afterText, err := goroutineProfile()
	if err != nil {
		fmt.Println("pprof after failed:", err)
		os.Exit(1)
	}
	fmt.Printf("after close+3s:      %d stacks=%v  vs_baseline=%d\n", after, countStacks(afterText, needles), after-base)

	_ = os.WriteFile("scripts/pprof/goroutine_after.txt", []byte(afterText), 0644)
	if after-base > users { // more than 1 leftover per user is suspicious
		fmt.Println("WARN: goroutines did not fall back near baseline")
		os.Exit(2)
	}
	fmt.Println("OK: connection goroutines mostly released")
}
