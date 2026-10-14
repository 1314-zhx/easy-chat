package websocket

import (
	"fmt"
	"sync"
	"time"

	"easy-chat/pkg/metrics"

	"github.com/gorilla/websocket"
	"github.com/zeromicro/go-zero/core/logx"
)

const (
	defaultConnQueueBuf     = 256
	defaultConnQueueTimeout = 5 * time.Second
)

// ConnQueue 是每个 Conn 对应的串行写入队列（Actor 模型）。
//
// 核心保证：同一 conn 永远只有 1 个 goroutine 在写 WriteMessage，
// 无论多少个 Task 要发给它，全部串行通过 ch。
//
// 解决两个问题：
//  1. gorilla/websocket 的 WriteMessage 不是并发安全的，多个 worker 同时写同一 conn 会 panic。
//  2. 慢客户端阻塞自己的队列而不占用 worker pool 配额。
type ConnQueue struct {
	conn    *Conn
	ch      chan []byte
	done    chan struct{}
	closeOnce sync.Once
	closed  chan struct{} // closed 后所有 Submit 立即返回 error
	bufSize int
	logger  logx.Logger
}

// NewConnQueue 创建并启动一个 ConnQueue。
// 启动后会立刻运行一个 goroutine 串行消费 ch 中的数据并写入 conn。
func NewConnQueue(conn *Conn, bufSize int) *ConnQueue {
	if bufSize <= 0 {
		bufSize = defaultConnQueueBuf
	}
	cq := &ConnQueue{
		conn:    conn,
		ch:      make(chan []byte, bufSize),
		done:    make(chan struct{}),
		closed:  make(chan struct{}),
		bufSize: bufSize,
		logger:  logx.WithContext(nil),
	}
	go cq.run()
	return cq
}

// run 是 ConnQueue 的核心 goroutine，串行消费 ch 中的数据。
// 同一 conn 只有这一个 goroutine 在调用 WriteMessage。
func (cq *ConnQueue) run() {
	defer close(cq.done)

	for {
		select {
		case data, ok := <-cq.ch:
			if !ok {
				return
			}
			if err := cq.conn.WriteMessage(websocket.TextMessage, data); err != nil {
				cq.logger.Errorf("connqueue write error: %v", err)
			}
			metrics.WebSocketMessageTotal.WithLabelValues("sent", "text").Inc()
		case <-cq.closed:
			// 队列已关闭，排空剩余消息后退出
			cq.drain()
			return
		}
	}
}

// drain 排空 channel 中剩余的消息，尽量不丢消息
func (cq *ConnQueue) drain() {
	for {
		select {
		case data, ok := <-cq.ch:
			if !ok {
				return
			}
			if err := cq.conn.WriteMessage(websocket.TextMessage, data); err != nil {
				cq.logger.Errorf("connqueue drain write error: %v", err)
			}
		default:
			return
		}
	}
}

// Submit 向队列提交一条已序列化的消息。
// 如果 buffer 满则判定为慢客户端，超时后踢下线。
// 队列已关闭时立即返回 error，不会 panic。
func (cq *ConnQueue) Submit(data []byte) error {
	// 快速检查：队列是否已关闭
	select {
	case <-cq.closed:
		return fmt.Errorf("connqueue closed")
	default:
	}

	select {
	case cq.ch <- data:
		return nil
	case <-cq.closed:
		return fmt.Errorf("connqueue closed")
	case <-time.After(defaultConnQueueTimeout):
		cq.logger.Errorf("connqueue buffer full, closing slow conn: uid=%s", cq.conn.Uid)
		metrics.ErrorTotal.WithLabelValues("conn_queue_buffer_full").Inc()
		cq.conn.Close()
		return fmt.Errorf("conn slow: buffer full")
	}
}

// Close 关闭队列，释放底层 goroutine。
// 可安全多次调用（sync.Once 保证）。
func (cq *ConnQueue) Close() {
	cq.closeOnce.Do(func() {
		close(cq.closed)
	})
	<-cq.done
}
