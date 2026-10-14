/**
 * @author: dn-jinmin/dn-jinmin
 * @doc:
 */

package websocket

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"time"

	"net/http"
	"sync"

	"easy-chat/pkg/metrics"

	"github.com/gorilla/websocket"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/threading"
)

type AckType int

const (
	NoAck AckType = iota
	OnlyAck
	RigorAck
)

func (t AckType) ToString() string {
	switch t {
	case OnlyAck:
		return "OnlyAck"
	case RigorAck:
		return "RigorAck"
	}

	return "NoAck"
}

type Server struct {
	sync.RWMutex // 涉及map 操作要加锁

	*threading.TaskRunner

	opt            *serverOption
	authentication Authentication

	routes map[string]HandlerFunc
	addr   string
	patten string

	connToUser map[*Conn]string
	userToConn map[string]*Conn

	// 工作池配置
	poolSize    int
	queueSize   int
	poolThreshold int

	// 工作池（延迟初始化）
	workerPool *WorkerPool

	// ShardedAckMap：存储待确认的消息，支持 NoAck 幂等、OnlyAck/RigorAck 三次握手
	// Key: msgId, Value: *Message
	shardedAckMu  [32]sync.Mutex
	shardedAckMap [32]map[string]*Message

	// 分布式支持：用户位置管理器（Redis）
	userLocationMgr   *UserLocationManager
	localInstanceAddr string
	forwardRemote     func(uid string, msg *Message) error
	kickRemote        func(instanceAddr, uid, reason string)

	upgrader websocket.Upgrader
	logx.Logger
}

// 构建websocket服务对象
func NewServer(addr string, opts ...ServerOptions) *Server {
	opt := newServerOptions(opts...) // 动态设置配置

	s := &Server{
		routes:   make(map[string]HandlerFunc),
		addr:     addr,
		patten:   opt.patten,
		opt:      &opt,
		upgrader: websocket.Upgrader{},

		authentication: opt.Authentication,

		connToUser: make(map[*Conn]string),
		userToConn: make(map[string]*Conn),

		// 工作池配置（默认值）
		poolSize:       defaultPushPoolMax,
		queueSize:      defaultPushQueueSize,
		poolThreshold:  defaultPushIdleMs,

		// 分布式支持
		userLocationMgr:   opt.userLocationMgr,
		localInstanceAddr: opt.localInstanceAddr,

		Logger:     logx.WithContext(context.Background()),
		TaskRunner: threading.NewTaskRunner(opt.concurrency),
	}

	// 初始化分片 Map
	for i := range s.shardedAckMap {
		s.shardedAckMap[i] = make(map[string]*Message)
	}

	// 初始化工作池
	s.workerPool = newWorkerPool(defaultPushPoolCore, s.poolSize, s.queueSize, s.poolThreshold)

	return s
}

// 用于接收并处理请求
func (s *Server) ServerWs(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if r := recover(); r != nil {
			s.Errorf("server handler ws recover err %v", r)
			metrics.PanicTotal.Inc()
		}
	}()
	// 封装 upgrade 将 http 升级为 ws，开启心跳检测，为当前用户New一个conn
	conn := NewConn(s, w, r)
	if conn == nil {
		return
	}
	//conn, err := s.upgrader.Upgrade(w, r, nil)
	//if err != nil {
	//	s.Errorf("upgrade err %v", err)
	//	return
	//}
	// 鉴权，默认鉴权
	if !s.authentication.Auth(w, r) {
		//conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprint("不具备访问权限")))
		s.Send(&Message{FrameType: FrameData, Data: fmt.Sprint("不具备访问权限")}, conn)
		conn.Close()
		// 埋点: 鉴权失败断开
		metrics.WebSocketDisconnectionTotal.WithLabelValues("auth_failed").Inc()
		return
	}

	// 记录当前用户的连接，如果以前登录过了，就把以前的conn 踢掉，换成新的conn，如果第一次登录，就记录进  map
	s.addConn(conn, r)

	// 埋点: WebSocket 连接成功
	metrics.WebSocketConnections.Inc()
	metrics.WebSocketConnectionTotal.Inc()

	// 处理连接
	go s.handlerConn(conn)
}

// 根据连接对象执行任务处理
func (s *Server) handlerConn(conn *Conn) {
	// 获取当前 conn 的 uid
	uids := s.GetUsers(conn)
	// 因为只传了一个 conn 所以返回的切片第一个就是该用户
	conn.Uid = uids[0]

	// 异步处理任务
	go s.handlerWrite(conn)
	if s.isAck(nil) {
		go s.readAck(conn)
	}

	// for 循环阻塞
	for {
		// 第一部分，处理客户端的消息
		// 服务端获取客户端的请求消息
		_, msg, err := conn.ReadMessage()
		if err != nil {
			s.Errorf("websocket conn read message err %v", err)
			// 埋点: 连接断开 - 读取错误
			s.closeWithReason(conn, "read_error")
			return
		}

		// 埋点: WebSocket 接收消息
		metrics.WebSocketMessageTotal.WithLabelValues("received", "text").Inc()

		// 解析消息
		var message Message
		if err = json.Unmarshal(msg, &message); err != nil {
			s.Errorf("json unmarshal err %v, msg %v", err, string(msg))
			// 埋点: 连接断开 - 解析错误
			s.closeWithReason(conn, "parse_error")
			return
		}

		// 处理客户端的 ACK（RigorAck 第三次 ACK）
		if message.FrameType == FrameAck && s.opt.ack == RigorAck {
			s.handleClientAck(conn, &message)
			continue
		}

		// 第二部分，对读取后的消息进行 ack 确认
		// 依据消息进行处理
		if s.isAck(&message) {
			s.Infof("conn message read ack msg %v", message)
			conn.appendMsgMq(&message)
		} else if !conn.dispatch(&message) {
			s.closeWithReason(conn, "closed")
			return
		}
	}
}

// getShardIndex 使用 FNV-1a hash 计算分片索引，保证均匀分布。
// msgId 是 MongoDB ObjectID hex 字符串（24 字符），用最后一个字节取模只命中 16 个 shard，
// FNV hash 可以让 32 个 shard 均匀使用。
func (s *Server) getShardIndex(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) % 32
}

// readAck 消费 readMessage 队列：OnlyAck 先回 ACK 再交给业务，发送方等待不含落库。
func (s *Server) readAck(conn *Conn) {
	for {
		conn.messageMu.Lock()
		for len(conn.readMessage) == 0 {
			if conn.isDone() {
				conn.messageMu.Unlock()
				return
			}
			conn.readAckCond.Wait()
		}
		if conn.isDone() {
			conn.messageMu.Unlock()
			return
		}

		message := conn.readMessage[0]
		conn.readMessage = conn.readMessage[1:]
		if s.opt.ack == OnlyAck {
			ackMsg := &Message{
				FrameType: FrameAck,
				Id:        message.Id,
				AckSeq:    message.AckSeq + 1,
			}
			conn.messageMu.Unlock()
			_ = s.Send(ackMsg, conn)
		} else {
			conn.messageMu.Unlock()
		}
		if !conn.dispatch(message) {
			return
		}
	}
}

func (s *Server) isAck(message *Message) bool {
	if message == nil {
		return s.opt.ack != NoAck
	}
	return s.opt.ack != NoAck && message.FrameType != FrameNoAck
}

// IsRigorAck 判断是否启用 RigorAck 模式
func (s *Server) IsRigorAck() bool {
	return s.opt.ack == RigorAck
}

// StoreToShardedAckMap 存储消息到 ShardedAckMap（RigorAck 三次握手）
func (s *Server) StoreToShardedAckMap(msgId string, msg *Message) {
	shard := s.getShardIndex(msgId)
	s.shardedAckMu[shard].Lock()
	s.shardedAckMap[shard][msgId] = msg
	s.shardedAckMu[shard].Unlock()
	s.Infof("Message stored in ShardedAckMap, msgId: %s", msgId)
}

// handleClientAck 处理客户端的 ACK（RigorAck 三次握手）
// 客户端收到服务端发送的 ACK (AckSeq=1) 后，发送第三次 ACK (AckSeq=2)
// 服务端收到后从 ShardedAckMap 删除消息，完成三次握手
func (s *Server) handleClientAck(conn *Conn, message *Message) {
	if s.opt.ack != RigorAck {
		return
	}

	// 检查是否是三次握手的第三次 ACK (AckSeq >= 2)
	if message.AckSeq < 2 {
		return
	}

	// 从 ShardedAckMap 中删除消息
	shard := s.getShardIndex(message.Id)
	s.shardedAckMu[shard].Lock()
	delete(s.shardedAckMap[shard], message.Id)
	s.shardedAckMu[shard].Unlock()

	s.Infof("Three-way handshake completed, msgId: %s, AckSeq: %d", message.Id, message.AckSeq)
}

// handleDbAck 处理 MongoDB 落库成功通知（用于 RigorAck 三次 ACK）
// MongoDB 落库成功后，发送 ACK (AckSeq=1) 给客户端
func (s *Server) handleDbAck(conn *Conn, message *Message) {
	if s.opt.ack != RigorAck {
		return
	}

	msgId := message.ClientMsgId

	// 从 ShardedAckMap 中获取消息
	shard := s.getShardIndex(msgId)
	s.shardedAckMu[shard].Lock()
	shardedMsg, ok := s.shardedAckMap[shard][msgId]
	if !ok {
		s.shardedAckMu[shard].Unlock()
		s.Infof("MongoDB ack received but message not found in ShardedAckMap, msgId: %s", msgId)
		return
	}
	s.shardedAckMu[shard].Unlock()

	// 发送 ACK（AckSeq: 0 → 1）
	ackMsg := &Message{
		FrameType:  FrameAck,
		Id:         shardedMsg.Id,
		AckSeq:     1, // 落库成功后发送 AckSeq=1
		ClientMsgId: message.ClientMsgId,
	}

	if err := s.sendWithRetry(ackMsg, conn); err != nil {
		s.Errorf("Failed to send ACK (MongoDB ack), msgId: %s, err: %v", msgId, err)
		return
	}

	s.Infof("ACK (MongoDB ack) sent, msgId: %s, AckSeq: %d", msgId, ackMsg.AckSeq)
}

// sendWithRetry 发送消息并重试
func (s *Server) sendWithRetry(msg *Message, conn *Conn) error {
	const (
		maxRetries    = 3
		retryInterval = 100 * time.Millisecond
	)

	var err error
	for i := 0; i < maxRetries; i++ {
		if err = s.Send(msg, conn); err == nil {
			return nil
		}
		s.Errorf("ACK send failed, retry %d/%d, err: %v", i+1, maxRetries, err)
		time.Sleep(retryInterval * time.Duration(i+1))
	}
	return fmt.Errorf("ACK send failed after %d retries: %w", maxRetries, err)
}

// 任务的处理
func (s *Server) handlerWrite(conn *Conn) {
	for {
		select {
		case <-conn.done:
			// 连接关闭
			return
		case message := <-conn.message:
			fmt.Printf(">>> [DEBUG] handlerWrite received message. Method: %s, FrameType: %d\n",
				message.Method, message.FrameType)

			switch message.FrameType {
			case FramePing:
				s.Send(&Message{FrameType: FramePing}, conn)
			case FrameData:
				if handler, ok := s.routes[message.Method]; ok {
					fmt.Printf(">>> [DEBUG] Routing to handler: %s\n", message.Method)
					// 调用业务 handler
					handler(s, conn, message)
				} else {
					fmt.Printf(">>> [ERROR] No handler found for method: %s\n", message.Method)
					s.Send(&Message{FrameType: FrameData, Data: fmt.Sprintf("不存在执行的方法 %v 请检查", message.Method)}, conn)
				}
			}
		}
	}
}

func (s *Server) addConn(conn *Conn, req *http.Request) {
	uid := s.authentication.UserId(req)
	conn.Uid = uid

	s.RWMutex.Lock()
	old := s.userToConn[uid]
	s.connToUser[conn] = uid
	s.userToConn[uid] = conn
	locMgr := s.userLocationMgr
	localAddr := s.localInstanceAddr
	s.RWMutex.Unlock()

	if old != nil && old != conn {
		s.Close(old)
	}

	var oldAddr string
	if locMgr != nil {
		oldAddr, _ = locMgr.GetUserInstance(uid)
	}
	if s.kickRemote != nil && oldAddr != "" && oldAddr != localAddr {
		go s.kickRemote(oldAddr, uid, "replaced")
	}

	if locMgr != nil && localAddr != "" {
		if err := locMgr.SetUserInstance(uid, localAddr); err != nil {
			s.Errorf("Failed to set user location: uid=%s, addr=%s, err=%v", uid, localAddr, err)
		}
	}
}

// EnableDistribution 挂上 Redis 位置（addConn / Close / 续期）
func (s *Server) EnableDistribution(mgr *UserLocationManager, instanceAddr string) {
	s.userLocationMgr = mgr
	s.localInstanceAddr = instanceAddr
}

// SetKickRemote 跨机顶号（须在 SetUserInstance 之前用旧地址调用）
func (s *Server) SetKickRemote(fn func(instanceAddr, uid, reason string)) {
	s.kickRemote = fn
}

// SetForwardRemote 本机 GetConn 为空时转发到用户所在实例
func (s *Server) SetForwardRemote(fn func(uid string, msg *Message) error) {
	s.forwardRemote = fn
}

// ForwardToUser 跨实例推送已封装好的下行帧
func (s *Server) ForwardToUser(uid string, msg *Message) error {
	if s.forwardRemote == nil {
		return nil
	}
	return s.forwardRemote(uid, msg)
}

func (s *Server) touchUserLocation(conn *Conn) {
	if s.userLocationMgr == nil || s.localInstanceAddr == "" || conn == nil || conn.Uid == "" {
		return
	}
	now := time.Now()
	conn.locMu.Lock()
	if now.Sub(conn.lastLocTouch) < 5*time.Minute {
		conn.locMu.Unlock()
		return
	}
	conn.lastLocTouch = now
	conn.locMu.Unlock()
	_ = s.userLocationMgr.KeepAlive(conn.Uid, s.localInstanceAddr)
}

// 根据 uid 获取 conn
func (s *Server) GetConn(uid string) *Conn {
	// 读取也要加锁，用读锁
	// 读写锁 (sync.RWMutex)：图书馆规定：
	//读操作 (RLock)：只要没人正在写书（修改内容），大家可以同时进去看书。多少人看都行，互不干扰。
	//写操作 (Lock)：一旦有人要写书（修改内容），图书馆清场。此时不能有人看书，也不能有人写书，必须等这个人写完出来，其他人才能再进。
	s.RWMutex.RLock()
	defer s.RWMutex.RUnlock()

	return s.userToConn[uid]
}

func (s *Server) GetConns(uids ...string) []*Conn {
	if len(uids) == 0 {
		return nil
	}

	s.RWMutex.RLock()
	defer s.RWMutex.RUnlock()

	res := make([]*Conn, 0, len(uids))
	for _, uid := range uids {
		if c := s.userToConn[uid]; c != nil {
			res = append(res, c)
		}
	}
	return res
}

// 根据 conn 批量获取 uid
func (s *Server) GetUsers(conns ...*Conn) []string {

	s.RWMutex.RLock()
	defer s.RWMutex.RUnlock()

	var res []string
	if len(conns) == 0 {
		// 获取全部
		res = make([]string, 0, len(s.connToUser))
		for _, uid := range s.connToUser {
			res = append(res, uid)
		}
	} else {
		// 获取部分
		res = make([]string, 0, len(conns))
		for _, conn := range conns {
			res = append(res, s.connToUser[conn])
		}
	}

	return res
}

func (s *Server) Close(conn *Conn) {
	if conn == nil {
		return
	}

	s.RWMutex.Lock()
	uid := s.connToUser[conn]
	if uid == "" {
		s.RWMutex.Unlock()
		return
	}
	activeConvs := conn.GetActiveConvs()
	delete(s.connToUser, conn)
	if s.userToConn[uid] == conn {
		delete(s.userToConn, uid)
	}
	onDisconnect := s.opt.onDisconnect
	locMgr := s.userLocationMgr
	s.RWMutex.Unlock()

	conn.ClearActiveConv()

	if onDisconnect != nil {
		go onDisconnect(uid, activeConvs)
	}

	if locMgr != nil {
		s.RWMutex.RLock()
		cur := s.userToConn[uid]
		s.RWMutex.RUnlock()
		if cur == nil {
			if err := locMgr.DelUserInstance(uid); err != nil {
				s.Errorf("Failed to delete user location: uid=%s, err=%v", uid, err)
			}
		}
	}

	metrics.WebSocketConnections.Dec()
	metrics.WebSocketDisconnectionTotal.WithLabelValues("normal").Inc()

	conn.Close()
}

// closeWithReason 关闭连接并记录原因
func (s *Server) closeWithReason(conn *Conn, reason string) {
	// 埋点: 连接断开原因
	metrics.WebSocketDisconnectionTotal.WithLabelValues(reason).Inc()
	metrics.WebSocketConnections.Dec()
	s.Close(conn)
}

func (s *Server) SendByUserId(msg interface{}, sendIds ...string) error {
	if len(sendIds) == 0 {
		return nil
	}
	// 调用 Send 发送，根据传递的 uids ，通过 GetConns 获取 conns
	return s.Send(msg, s.GetConns(sendIds...)...)
}

func (s *Server) Send(msg interface{}, conns ...*Conn) error {
	// 如果连接对象为空，直接返回
	if len(conns) == 0 {
		return nil
	}

	// 提交到工作池异步执行，等待 100ms 超时
	return s.workerPool.Submit(&Task{
		Msg:   msg,
		Conns: conns,
	}, time.Second)
}

func (s *Server) GetActiveConv(uid string) string {
	c := s.GetConn(uid)
	if c == nil {
		return ""
	}
	return c.GetActiveConv()
}

func (s *Server) SetActiveConv(uid, conversationId string) {
	c := s.GetConn(uid)
	if c == nil {
		return
	}
	c.SetActiveConv(conversationId)
}

func (s *Server) ClearActiveConv(uid string) {
	c := s.GetConn(uid)
	if c == nil {
		return
	}
	c.ClearActiveConv()
}

func (s *Server) AddRoutes(rs []Route) {
	for _, r := range rs {
		s.routes[r.Method] = r.Handler
	}
}

func (s *Server) HasRoute(method string) bool {
	if s == nil || method == "" {
		return false
	}
	_, ok := s.routes[method]
	return ok
}

// Dispatch 走本机已注册 handler（跨实例推送须再走 push，才能判断在房/列表）
func (s *Server) Dispatch(conn *Conn, message *Message) {
	if s == nil || conn == nil || message == nil {
		return
	}
	handler, ok := s.routes[message.Method]
	if !ok {
		return
	}
	handler(s, conn, message)
}

func (s *Server) Start() {
	http.HandleFunc(s.patten, s.ServerWs)
	s.Info(http.ListenAndServe(s.addr, nil))
}

func (s *Server) Stop() {
	fmt.Println("停止服务, worker pool running:", s.workerPool.RunningWorkers())
	s.RWMutex.Lock()
	conns := make([]*Conn, 0, len(s.connToUser))
	for c := range s.connToUser {
		conns = append(conns, c)
	}
	s.RWMutex.Unlock()
	for _, c := range conns {
		s.Close(c)
	}
	s.workerPool.Stop()
}
