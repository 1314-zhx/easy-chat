/**
 * @author: dn-jinmin/dn-jinmin
 * @doc:
 */

package websocket

import (
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type Conn struct {
	idleMu sync.Mutex

	Uid string

	activeConvMu sync.RWMutex
	activeConvs  map[string]struct{} // 本连接打开中的会话集合

	*websocket.Conn
	s *Server

	idle              time.Time
	maxConnectionIdle time.Duration // 最大空闲时间

	messageMu      sync.Mutex
	readAckCond    *sync.Cond // 与 messageMu 绑定：入队 Signal，关闭 Broadcast
	readMessage    []*Message
	readMessageSeq map[string]*Message // 存储消息id 和消息

	message chan *Message

	done      chan struct{} // 关闭连接的通道
	closeOnce sync.Once

	queue *ConnQueue // 每 conn 一个串行写入队列（Actor 模型）

	locMu        sync.Mutex
	lastLocTouch time.Time
}

func NewConn(s *Server, w http.ResponseWriter, r *http.Request) *Conn {
	c, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.Errorf("upgrade err %v", err)
		return nil
	}

	conn := &Conn{
		Conn:              c,
		s:                 s,
		idle:              time.Now(),
		maxConnectionIdle: s.opt.maxConnectionIdle,
		readMessage:       make([]*Message, 0, 2),
		readMessageSeq:    make(map[string]*Message, 2),
		message:           make(chan *Message, 1),
		done:              make(chan struct{}),
	}
	conn.readAckCond = sync.NewCond(&conn.messageMu)
	conn.queue = NewConnQueue(conn, 0)
	// 心跳检测
	go conn.keepalive()
	return conn
}

// GetKafkaResultChan 获取或创建 Kafka 结果通知通道
func (c *Conn) GetKafkaResultChan(msgId string) chan error {
	return nil
}

// NotifyKafkaResult 通知 Kafka 结果（发送到 kafkaResultCh）
func (c *Conn) NotifyKafkaResult(msgId string, err error) {
}

// DelKafkaResult 删除 Kafka 结果通道
func (c *Conn) DelKafkaResult(msgId string) {
}

// 将消息暂存在 mq 中，等待ack确认，这里的 mq 是 conn 自身维护的一共切片
func (c *Conn) appendMsgMq(msg *Message) {
	c.messageMu.Lock()
	defer c.messageMu.Unlock()
	if c.readMessageSeq == nil {
		return
	}

	// 读队列中
	if m, ok := c.readMessageSeq[msg.Id]; ok {
		// 已经有消息的记录，该消息已经有ack的确认
		if len(c.readMessage) == 0 {
			return
		}

		// msg.AckSeq > m.AckSeq，消息的序号一定要大于系统中存储的序号，否则返回
		// 因为ack 的确认就是给序号加1，如果当前序号小于或等于上一次的系统序号就认为是没有成功进行ack确认的消息
		if m.AckSeq >= msg.AckSeq {
			return
		}
		cp := new(Message)
		*cp = *msg
		c.readMessageSeq[cp.Id] = cp
		return
	}
	// 还没有进行ack的确认, 避免客户端重复发送多余的ack消息
	if msg.FrameType == FrameAck {
		return
	}
	cp := new(Message)
	*cp = *msg
	c.readMessage = append(c.readMessage, cp)
	c.readMessageSeq[cp.Id] = cp
	c.readAckCond.Signal()
}

func (c *Conn) isDone() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

func (c *Conn) ReadMessage() (messageType int, p []byte, err error) {
	messageType, p, err = c.Conn.ReadMessage()

	c.idleMu.Lock()
	defer c.idleMu.Unlock()
	c.idle = time.Time{} // 重置空闲时间为0值，暂停空闲超时计数，连接现在正在处理消息，处于活跃状态，请勿因超时而关闭此连接
	if c.s != nil {
		c.s.touchUserLocation(c)
	}
	return
}

const writeDeadline = 5 * time.Second

func (c *Conn) WriteMessage(messageType int, data []byte) error {
	c.idleMu.Lock()
	defer c.idleMu.Unlock()
	_ = c.Conn.SetWriteDeadline(time.Now().Add(writeDeadline))
	err := c.Conn.WriteMessage(messageType, data)
	c.idle = time.Now()
	return err
}

// Close 安全关闭连接，可多次调用。
// sync.Once 保证 close(done) 和 queue.Close() 只执行一次。
func (c *Conn) Close() error {
	c.closeOnce.Do(func() {
		close(c.done)
		c.messageMu.Lock()
		c.readAckCond.Broadcast()
		c.readMessage = nil
		c.readMessageSeq = nil
		c.messageMu.Unlock()
		if c.queue != nil {
			c.queue.Close()
		}
	})
	return c.Conn.Close()
}

// dispatch 把一帧交给 handlerWrite。连接已关闭时立即返回 false，避免往无人接收的 channel 上堵死。
func (c *Conn) dispatch(msg *Message) bool {
	if c == nil || msg == nil {
		return false
	}
	cp := new(Message)
	*cp = *msg
	select {
	case <-c.done:
		return false
	case c.message <- cp:
		return true
	}
}

func (c *Conn) AddActiveConv(conversationId string) {
	if conversationId == "" {
		return
	}
	c.activeConvMu.Lock()
	defer c.activeConvMu.Unlock()
	if c.activeConvs == nil {
		c.activeConvs = make(map[string]struct{})
	}
	c.activeConvs[conversationId] = struct{}{}
}

func (c *Conn) RemoveActiveConv(conversationId string) {
	c.activeConvMu.Lock()
	defer c.activeConvMu.Unlock()
	if c.activeConvs == nil {
		return
	}
	delete(c.activeConvs, conversationId)
}

func (c *Conn) IsActiveConv(conversationId string) bool {
	c.activeConvMu.RLock()
	defer c.activeConvMu.RUnlock()
	if c.activeConvs == nil {
		return false
	}
	_, ok := c.activeConvs[conversationId]
	return ok
}

func (c *Conn) GetActiveConvs() []string {
	c.activeConvMu.RLock()
	defer c.activeConvMu.RUnlock()
	if len(c.activeConvs) == 0 {
		return nil
	}
	out := make([]string, 0, len(c.activeConvs))
	for id := range c.activeConvs {
		out = append(out, id)
	}
	return out
}

// SetActiveConv 兼容旧调用：加入活跃集合
func (c *Conn) SetActiveConv(conversationId string) {
	c.AddActiveConv(conversationId)
}

// GetActiveConv 兼容旧调用：返回集合中任意一个（无序）
func (c *Conn) GetActiveConv() string {
	c.activeConvMu.RLock()
	defer c.activeConvMu.RUnlock()
	for id := range c.activeConvs {
		return id
	}
	return ""
}

func (c *Conn) ClearActiveConv() {
	c.activeConvMu.Lock()
	defer c.activeConvMu.Unlock()
	c.activeConvs = nil
}

// 复制 grpc 的方法 核心部分
func (c *Conn) keepalive() {
	// 启动一个计时器，初始设置为 maxConnectionIdle（最大空闲时间）
	idleTimer := time.NewTimer(c.maxConnectionIdle)
	defer func() {
		idleTimer.Stop()
	}()

	for {
		select {
		// 定时器到期了，需要检查连接状态
		case <-idleTimer.C:
			c.idleMu.Lock()
			idle := c.idle
			// 判断连接是否活跃，如果当前连接的上次活跃时间是time.Time{}零值，表示正在活跃中,true
			// 如果是一个具体的时间戳，表示已经空闲下来了，这个时间戳是上一次写操作的时间,false
			if idle.IsZero() { // The connection is non-idle.
				c.idleMu.Unlock()
				// 重置为完整的周期，防止下面的逻辑误杀活跃连接
				idleTimer.Reset(c.maxConnectionIdle)
				continue
			}
			// 这里处理的是空闲连接
			val := c.maxConnectionIdle - time.Since(idle)
			c.idleMu.Unlock()
			// 已经超过最大空闲时间，关闭
			if val <= 0 {
				// The connection has been idle for a duration of keepalive.MaxConnectionIdle or more.
				// Gracefully close the connection.
				c.s.Close(c)
				return
			}
			// 重置下一次检查时间，不是完整周期
			idleTimer.Reset(val)
			// 手动关闭
		case <-c.done:
			return
		}
	}
}
