package distributed

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

	"easy-chat/apps/im/ws/websocket"
	"easy-chat/apps/im/ws/wsrelay"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// WsRelayer 分布式 WebSocket 消息转发器
// 负责跨实例消息转发
type WsRelayer struct {
	// 本地 Server
	localServer *websocket.Server

	// 用户位置管理器
	userLocationMgr *websocket.UserLocationManager

	// 本地实例地址
	localInstanceAddr string

	// gRPC 客户端（用于跨实例通信）
	// key: instanceAddr, value: *grpc.ClientConn
	grpcConns map[string]*grpc.ClientConn
	grpcMu   sync.RWMutex

	// gRPC 服务器（用于接收远程调用）
	grpcServer *grpc.Server
	listener   net.Listener
}

// NewWsRelayer 创建分布式 WebSocket 转发器
func NewWsRelayer(localServer *websocket.Server, userLocationMgr *websocket.UserLocationManager, localInstanceAddr string) *WsRelayer {
	return &WsRelayer{
		localServer:       localServer,
		userLocationMgr:   userLocationMgr,
		localInstanceAddr: localInstanceAddr,
		grpcConns:        make(map[string]*grpc.ClientConn),
	}
}

// wsRelayImpl gRPC 服务端实现
type wsRelayImpl struct {
	wsrelay.UnimplementedWsRelayServer
	server *websocket.Server
}

// StartGrpcServer 启动 gRPC 服务器（接收远程调用）
func (r *WsRelayer) StartGrpcServer(addr string) error {
	r.grpcServer = grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:             30 * time.Second,
			Timeout:          10 * time.Second,
			MaxConnectionIdle: 15 * time.Minute,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	)

	// 注册 gRPC 处理方法
	wsrelay.RegisterWsRelayServer(r.grpcServer, &wsRelayImpl{server: r.localServer})

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	r.listener = lis

	logx.Infof("WsRelayer gRPC server started on %s", addr)

	// 启动 gRPC 服务协程
	go func() {
		if err := r.grpcServer.Serve(lis); err != nil {
			logx.Errorf("gRPC serve error: %v", err)
		}
	}()

	return nil
}

// StopGrpcServer 停止 gRPC 服务器
func (r *WsRelayer) StopGrpcServer() {
	if r.grpcServer != nil {
		r.grpcServer.GracefulStop()
	}
}

// SendToUser gRPC 服务端实现：发送消息给指定用户
func (impl *wsRelayImpl) SendToUser(ctx context.Context, req *wsrelay.SendToUserReq) (*wsrelay.SendToUserResp, error) {
	logx.Infof("WsRelay.SendToUser: userId=%s, sendId=%s, method=%s", req.UserId, req.SendId, req.Method)

	resp := &wsrelay.SendToUserResp{}

	// 1. 获取目标用户的 conn
	conn := impl.server.GetConn(req.UserId)
	if conn == nil {
		logx.Infof("WsRelay.SendToUser: user not found, userId=%s", req.UserId)
		resp.Success = false
		resp.Message = "user not found"
		return resp, nil
	}

	// 2. 构建消息
	msg := &websocket.Message{
		FormId:      req.SendId,
		FrameType:   websocket.FrameType(req.FrameType),
		Method:      req.Method,
		Id:          req.MsgId,
		ClientMsgId: req.ClientMsgId,
	}

	// 3. 解析消息内容
	if req.Data != "" {
		var data interface{}
		if err := json.Unmarshal([]byte(req.Data), &data); err != nil {
			logx.Errorf("WsRelay.SendToUser: unmarshal data failed, err=%v", err)
		}
		msg.Data = data
	}

	// 4. 本机 push 再走 handler，才能按在房/不在房下发
	if req.Method == "" {
		msg.Method = "push"
	}
	if msg.Method != "" && impl.server.HasRoute(msg.Method) {
		impl.server.Dispatch(conn, msg)
	} else if err := impl.server.Send(msg, conn); err != nil {
		logx.Errorf("WsRelay.SendToUser: send failed, userId=%s, err=%v", req.UserId, err)
		resp.Success = false
		resp.Message = err.Error()
		return resp, nil
	}

	logx.Infof("WsRelay.SendToUser: success, userId=%s", req.UserId)
	resp.Success = true
	resp.Message = "success"
	return resp, nil
}

// NotifyOffline gRPC 服务端实现：通知用户下线
func (impl *wsRelayImpl) NotifyOffline(ctx context.Context, req *wsrelay.NotifyOfflineReq) (*wsrelay.NotifyOfflineResp, error) {
	logx.Infof("WsRelay.NotifyOffline: userId=%s, reason=%s", req.UserId, req.Reason)

	resp := &wsrelay.NotifyOfflineResp{}

	conn := impl.server.GetConn(req.UserId)
	if conn == nil {
		resp.Success = false
		resp.Message = "user not found"
		return resp, nil
	}

	impl.server.Close(conn)
	resp.Success = true
	resp.Message = "success"
	return resp, nil
}

// getOrCreateClient 获取或创建 gRPC 客户端
func (r *WsRelayer) getOrCreateClient(instanceAddr string) (wsrelay.WsRelayClient, error) {
	// 先尝试读缓存
	r.grpcMu.RLock()
	conn, ok := r.grpcConns[instanceAddr]
	r.grpcMu.RUnlock()
	if ok && conn != nil {
		return wsrelay.NewWsRelayClient(conn), nil
	}

	// 创建新连接
	r.grpcMu.Lock()
	defer r.grpcMu.Unlock()

	// 双重检查
	if conn, ok = r.grpcConns[instanceAddr]; ok && conn != nil {
		return wsrelay.NewWsRelayClient(conn), nil
	}

	// 禁用 TLS，使用不安全凭证
	conn, err := grpc.NewClient(
		instanceAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second,
			Timeout:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to dial grpc: %w", err)
	}

	r.grpcConns[instanceAddr] = conn
	return wsrelay.NewWsRelayClient(conn), nil
}

// SendResult 发送结果
type SendResult struct {
	UserId  string
	Success bool
	Error   error
}

// SendToUsers 发送消息给用户（自动路由：本地走 Map，远程走 gRPC）
func (r *WsRelayer) SendToUsers(sendId string, userIds []string, msg *websocket.Message) []SendResult {
	results := make([]SendResult, 0, len(userIds))

	// 1. 按实例分组用户
	localUsers := make([]string, 0)
	remoteUsers := make(map[string][]string) // instanceAddr -> userIds

	for _, userId := range userIds {
		instanceAddr, err := r.userLocationMgr.GetUserInstance(userId)
		if err != nil {
			logx.Errorf("GetUserInstance failed: userId=%s, err=%v", userId, err)
			results = append(results, SendResult{
				UserId:  userId,
				Success: false,
				Error:   err,
			})
			continue
		}

		// 用户不在线
		if instanceAddr == "" {
			results = append(results, SendResult{
				UserId:  userId,
				Success: false,
				Error:   fmt.Errorf("user offline"),
			})
			continue
		}

		// 判断是否本地
		if instanceAddr == r.localInstanceAddr {
			localUsers = append(localUsers, userId)
		} else {
			remoteUsers[instanceAddr] = append(remoteUsers[instanceAddr], userId)
		}
	}

	// 2. 本地用户：直接从 Map 获取 conn 发送
	if len(localUsers) > 0 {
		conns := r.localServer.GetConns(localUsers...)
		if err := r.localServer.Send(msg, conns...); err != nil {
			logx.Errorf("Local Send failed: err=%v", err)
			for _, userId := range localUsers {
				results = append(results, SendResult{
					UserId:  userId,
					Success: false,
					Error:   err,
				})
			}
		} else {
			for _, userId := range localUsers {
				results = append(results, SendResult{
					UserId:  userId,
					Success: true,
				})
			}
		}
	}

	// 3. 远程用户：按人走 gRPC（协议一次一个 userId）
	for instanceAddr, userIdList := range remoteUsers {
		client, err := r.getOrCreateClient(instanceAddr)
		if err != nil {
			logx.Errorf("getOrCreateClient failed: addr=%s, err=%v", instanceAddr, err)
			r.dropConn(instanceAddr)
			for _, userId := range userIdList {
				results = append(results, SendResult{
					UserId:  userId,
					Success: false,
					Error:   err,
				})
			}
			continue
		}

		data, _ := json.Marshal(msg.Data)
		for _, userId := range userIdList {
			req := &wsrelay.SendToUserReq{
				UserId:      userId,
				SendId:      sendId,
				FrameType:   int32(msg.FrameType),
				Method:      msg.Method,
				MsgId:       msg.Id,
				ClientMsgId: msg.ClientMsgId,
				Data:        string(data),
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			resp, err := client.SendToUser(ctx, req)
			cancel()
			if err != nil || resp == nil || !resp.Success {
				errMsg := err
				if resp != nil && resp.Message != "" {
					errMsg = fmt.Errorf(resp.Message)
				}
				if errMsg == nil {
					errMsg = fmt.Errorf("send failed")
				}
				logx.Errorf("gRPC SendToUser failed: userId=%s, err=%v", userId, errMsg)
				if err != nil {
					r.dropConn(instanceAddr)
				}
				results = append(results, SendResult{
					UserId:  userId,
					Success: false,
					Error:   errMsg,
				})
				continue
			}
			results = append(results, SendResult{
				UserId:  userId,
				Success: true,
			})
		}
	}

	return results
}

// SendToUser 发送消息给单个用户
func (r *WsRelayer) SendToUser(sendId, userId string, msg *websocket.Message) SendResult {
	results := r.SendToUsers(sendId, []string{userId}, msg)
	if len(results) > 0 {
		return results[0]
	}
	return SendResult{
		UserId:  userId,
		Success: false,
		Error:   fmt.Errorf("no result"),
	}
}

// NotifyUserOffline 通知用户下线（跨实例踢人）
func (r *WsRelayer) NotifyUserOffline(userId, reason string) error {
	instanceAddr, err := r.userLocationMgr.GetUserInstance(userId)
	if err != nil {
		return err
	}

	// 用户不在线
	if instanceAddr == "" {
		return fmt.Errorf("user offline")
	}

	// 本地用户
	if instanceAddr == r.localInstanceAddr {
		conn := r.localServer.GetConn(userId)
		if conn != nil {
			r.localServer.Close(conn)
		}
		return nil
	}

	// 远程用户
	client, err := r.getOrCreateClient(instanceAddr)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &wsrelay.NotifyOfflineReq{UserId: userId, Reason: reason}
	resp, err := client.NotifyOffline(ctx, req)
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf(resp.Message)
	}
	return nil
}

// KickOn 向指定实例踢人（必须用 SetUserInstance 之前读到的旧地址）
func (r *WsRelayer) KickOn(instanceAddr, userId, reason string) error {
	if instanceAddr == "" || userId == "" {
		return nil
	}
	if instanceAddr == r.localInstanceAddr {
		conn := r.localServer.GetConn(userId)
		if conn != nil {
			r.localServer.Close(conn)
		}
		return nil
	}
	client, err := r.getOrCreateClient(instanceAddr)
	if err != nil {
		r.dropConn(instanceAddr)
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := client.NotifyOffline(ctx, &wsrelay.NotifyOfflineReq{UserId: userId, Reason: reason})
	if err != nil {
		r.dropConn(instanceAddr)
		return err
	}
	if resp != nil && !resp.Success {
		return fmt.Errorf(resp.Message)
	}
	return nil
}

func (r *WsRelayer) dropConn(instanceAddr string) {
	r.grpcMu.Lock()
	defer r.grpcMu.Unlock()
	if conn, ok := r.grpcConns[instanceAddr]; ok && conn != nil {
		_ = conn.Close()
		delete(r.grpcConns, instanceAddr)
	}
}

// UserOnline 用户上线
func (r *WsRelayer) UserOnline(userId string) error {
	return r.userLocationMgr.SetUserInstance(userId, r.localInstanceAddr)
}

// UserOffline 用户下线
func (r *WsRelayer) UserOffline(userId string) error {
	return r.userLocationMgr.DelUserInstance(userId)
}

// GetUserLocation 获取用户位置
func (r *WsRelayer) GetUserLocation(userId string) (string, error) {
	return r.userLocationMgr.GetUserInstance(userId)
}

// IsLocalUser 判断用户是否在本地实例
func (r *WsRelayer) IsLocalUser(userId string) (bool, error) {
	instanceAddr, err := r.userLocationMgr.GetUserInstance(userId)
	if err != nil {
		return false, err
	}
	return instanceAddr == r.localInstanceAddr, nil
}

// Close 关闭所有连接
func (r *WsRelayer) Close() {
	r.StopGrpcServer()

	r.grpcMu.Lock()
	defer r.grpcMu.Unlock()
	for _, conn := range r.grpcConns {
		conn.Close()
	}
}

// 确保实现了 wsrelay.WsRelayServer 接口
var _ wsrelay.WsRelayServer = (*wsRelayImpl)(nil)
