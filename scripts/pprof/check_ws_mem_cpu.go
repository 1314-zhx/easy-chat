package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

func get(url string) ([]byte, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func goroutineTotal() (int, error) {
	b, err := get("http://127.0.0.1:6060/debug/pprof/goroutine?debug=1")
	if err != nil {
		return 0, err
	}
	n := 0
	fmt.Sscanf(string(b), "goroutine profile: total %d", &n)
	return n, nil
}

func memstats() (heapAlloc, heapInuse, stackInuse uint64, err error) {
	b, err := get("http://127.0.0.1:6060/debug/pprof/heap?debug=1")
	if err != nil {
		return 0, 0, 0, err
	}
	text := string(b)
	re := regexp.MustCompile(`(?m)^# (HeapAlloc|HeapInuse|StackInuse) = (\d+)`)
	for _, m := range re.FindAllStringSubmatch(text, -1) {
		v, _ := strconv.ParseUint(m[2], 10, 64)
		switch m[1] {
		case "HeapAlloc":
			heapAlloc = v
		case "HeapInuse":
			heapInuse = v
		case "StackInuse":
			stackInuse = v
		}
	}
	return
}

func kb(n uint64) string { return fmt.Sprintf("%.1f KB", float64(n)/1024) }
func mb(n uint64) string { return fmt.Sprintf("%.2f MB", float64(n)/1024/1024) }

func printSnap(tag string, g int, alloc, inuse, stack uint64) {
	fmt.Printf("%s  goroutine=%d  HeapAlloc=%s  HeapInuse=%s  StackInuse=%s\n",
		tag, g, mb(alloc), mb(inuse), kb(stack))
}

func main() {
	users := 50
	var stop atomic.Bool

	g0, _ := goroutineTotal()
	a0, i0, s0, err := memstats()
	if err != nil {
		fmt.Println("heap failed:", err)
		os.Exit(1)
	}
	printSnap("baseline ", g0, a0, i0, s0)

	var mu sync.Mutex
	conns := make([]*websocket.Conn, 0, users)
	var wg sync.WaitGroup
	for u := 0; u < users; u++ {
		wg.Add(1)
		go func(uid int) {
			defer wg.Done()
			c, _, err := websocket.DefaultDialer.Dial(
				fmt.Sprintf("ws://127.0.0.1:10090/ws?userId=mem_%d_%d", uid, time.Now().UnixNano()), nil)
			if err != nil {
				fmt.Println("dial", err)
				return
			}
			go func() {
				for {
					if _, _, e := c.ReadMessage(); e != nil {
						return
					}
				}
			}()
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
			seq := 0
			for !stop.Load() {
				id := fmt.Sprintf("mem_%d_%d", uid, seq)
				seq++
				body, _ := json.Marshal(map[string]interface{}{
					"id": id, "ackSeq": 0, "method": "conversation.chat",
					"data": map[string]interface{}{
						"chatType": 1, "sendId": fmt.Sprintf("mem_%d", uid),
						"recvId": fmt.Sprintf("mem_%d", (uid+1)%users),
						"conversationId": fmt.Sprintf("conv_mem_%d", uid),
						"content":        "pprof-mem-cpu", "mType": 1,
					},
				})
				if err := c.WriteMessage(websocket.TextMessage, body); err != nil {
					return
				}
				time.Sleep(20 * time.Millisecond)
			}
		}(u)
	}
	time.Sleep(1500 * time.Millisecond)

	g1, _ := goroutineTotal()
	a1, i1, s1, _ := memstats()
	printSnap("peak+send", g1, a1, i1, s1)
	fmt.Printf("delta G=%d  HeapAlloc=%s  HeapInuse=%s  StackInuse=%s\n",
		g1-g0, mb(a1-a0), mb(i1-i0), kb(s1-s0))

	fmt.Println("cpu profile 10s ...")
	cpu, err := get("http://127.0.0.1:6060/debug/pprof/profile?seconds=10")
	if err != nil {
		fmt.Println("cpu profile failed:", err)
	} else {
		_ = os.WriteFile("scripts/pprof/cpu_10s.pb.gz", cpu, 0644)
		cmd := exec.Command("go", "tool", "pprof", "-top", "-nodecount=15", "scripts/pprof/cpu_10s.pb.gz")
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		_ = cmd.Run()
	}

	stop.Store(true)
	wg.Wait()
	mu.Lock()
	for _, c := range conns {
		_ = c.Close()
	}
	mu.Unlock()
	time.Sleep(3 * time.Second)

	g2, _ := goroutineTotal()
	a2, i2, s2, _ := memstats()
	printSnap("after    ", g2, a2, i2, s2)
}
