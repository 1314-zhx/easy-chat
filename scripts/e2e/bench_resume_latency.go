package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"sort"
	"time"

	"easy-chat/apps/im/immodels"
	"easy-chat/apps/task/mq/mq"
	"easy-chat/apps/task/mq/mqclient"
	"easy-chat/pkg/constants"

	"github.com/gorilla/websocket"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

var (
	mongoURL  = flag.String("mongo", "mongodb://root:easy-chat@127.0.0.1:27017", "Mongo URL")
	mongoDB   = flag.String("db", "easy-chat", "Mongo DB")
	wsAddr    = flag.String("ws", "localhost:10090", "WS 地址")
	kafka     = flag.String("kafka", "127.0.0.1:9092", "Kafka broker")
	n         = flag.Int("n", 200, "每种路径样本数")
	skipMongo = flag.Bool("skip-mongo", false, "跳过 Mongo 插入")
	skipKafka = flag.Bool("skip-kafka", false, "跳过 Kafka produce")
	skipWS    = flag.Bool("skip-ws", false, "跳过 WS ACK")
)

func main() {
	flag.Parse()
	fmt.Println("========== 简历两项延迟实测 ==========")
	fmt.Printf("样本: %d  (warmup 另计)\n\n", *n)

	if !*skipMongo {
		runMongoInsert()
	}
	if !*skipKafka {
		runKafkaProduce()
	}
	if !*skipWS {
		runWSAck()
	}
}

func runMongoInsert() {
	fmt.Println("--- A. 同步 Mongo Insert（解耦前：发送方等落库）---")
	model := immodels.MustChatLogModel(*mongoURL, *mongoDB)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		if err := model.Insert(ctx, newChatLog("bench_mongo", i)); err != nil {
			fmt.Printf("Mongo Insert 失败: %v\n\n", err)
			return
		}
	}

	lat := make([]float64, 0, *n)
	for i := 0; i < *n; i++ {
		start := time.Now()
		err := model.Insert(ctx, newChatLog("bench_mongo", i+10))
		ms := float64(time.Since(start).Microseconds()) / 1000.0
		if err != nil {
			fmt.Printf("Mongo Insert 失败: %v\n\n", err)
			return
		}
		lat = append(lat, ms)
	}
	printStats("Mongo Insert", lat)
}

func newChatLog(conv string, i int) *immodels.ChatLog {
	return &immodels.ChatLog{
		ID:             primitive.NewObjectID(),
		ConversationId: conv,
		SendId:         "bench_send",
		RecvId:         "bench_recv",
		ChatType:       constants.SingleChatType,
		MsgType:        constants.TextMType,
		MsgContent:     "bench",
		SendTime:       time.Now().UnixNano(),
		Seq:            int64(i),
		CreateAt:       time.Now(),
		UpdateAt:       time.Now(),
	}
}

func runKafkaProduce() {
	fmt.Println("--- B. Kafka Produce（解耦后：发送方只等投递）---")
	client := mqclient.NewMsgChatTransferClient([]string{*kafka}, "msgChatTransfer")

	msg := &mq.MsgChatTransfer{
		ConversationId: "bench_kafka",
		ChatType:       constants.SingleChatType,
		SendId:         "bench_send",
		RecvId:         "bench_recv",
		SendTime:       time.Now().UnixNano(),
		MType:          constants.TextMType,
		Content:        "bench",
		ClientMsgId:    "warmup",
	}
	if err := client.Push(msg); err != nil {
		fmt.Printf("Kafka 不可用，跳过: %v\n\n", err)
		return
	}

	lat := make([]float64, 0, *n)
	for i := 0; i < *n; i++ {
		msg.ClientMsgId = fmt.Sprintf("k_%d", i)
		msg.SendTime = time.Now().UnixNano()
		start := time.Now()
		err := client.Push(msg)
		ms := float64(time.Since(start).Microseconds()) / 1000.0
		if err != nil {
			fmt.Printf("Kafka Push 失败: %v\n\n", err)
			return
		}
		lat = append(lat, ms)
	}
	printStats("Kafka Produce", lat)
}

func runWSAck() {
	fmt.Println("--- C. WS 发送方 ACK（串行：发一条等一条，客户端等待）---")
	sendURL := fmt.Sprintf("ws://%s/ws?userId=bench_ack_send", *wsAddr)
	conn, _, err := websocket.DefaultDialer.Dial(sendURL, nil)
	if err != nil {
		fmt.Printf("WS 不可用，跳过: %v\n\n", err)
		return
	}
	defer conn.Close()

	ackCh := make(chan string, 64)
	go func() {
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var msg map[string]interface{}
			if json.Unmarshal(data, &msg) != nil {
				continue
			}
			ft, _ := msg["frameType"].(float64)
			id, _ := msg["id"].(string)
			if int(ft) == 2 && id != "" {
				ackCh <- id
			}
		}
	}()

	waitOne := func(id string) (float64, bool) {
		body := map[string]interface{}{
			"id":     id,
			"ackSeq": 0,
			"method": "conversation.chat",
			"data": map[string]interface{}{
				"chatType":       2,
				"recvId":         "bench_ack_recv",
				"conversationId": "conv_bench_ack",
				"content":        "bench",
				"mType":          0,
			},
		}
		data, _ := json.Marshal(body)
		start := time.Now()
		if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
			return 0, false
		}
		timer := time.NewTimer(3 * time.Second)
		defer timer.Stop()
		for {
			select {
			case got := <-ackCh:
				if got != id {
					continue
				}
				return float64(time.Since(start).Microseconds()) / 1000.0, true
			case <-timer.C:
				return 0, false
			}
		}
	}

	for i := 0; i < 5; i++ {
		_, _ = waitOne(fmt.Sprintf("warm_%d", i))
	}

	lat := make([]float64, 0, *n)
	miss := 0
	for i := 0; i < *n; i++ {
		ms, ok := waitOne(fmt.Sprintf("ack_%d", i))
		if !ok {
			miss++
			continue
		}
		lat = append(lat, ms)
	}
	printStats(fmt.Sprintf("WS ACK 串行 (成功 %d, 超时 %d)", len(lat), miss), lat)
}

func printStats(title string, lat []float64) {
	if len(lat) == 0 {
		fmt.Printf("%s: 无样本\n\n", title)
		return
	}
	sort.Float64s(lat)
	n := len(lat)
	sum := 0.0
	for _, v := range lat {
		sum += v
	}
	p := func(q int) float64 {
		i := n * q / 100
		if i >= n {
			i = n - 1
		}
		return lat[i]
	}
	fmt.Printf("%s  n=%d\n", title, n)
	fmt.Printf("  P50=%.2f  P90=%.2f  P95=%.2f  P99=%.2f  Max=%.2f  Avg=%.2f  (ms)\n\n",
		p(50), p(90), p(95), p(99), lat[n-1], sum/float64(n))
}
