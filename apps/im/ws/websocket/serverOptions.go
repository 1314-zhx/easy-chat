/**
 * @author: dn-jinmin/dn-jinmin
 * @doc:
 */

package websocket

import "time"

type ServerOptions func(opt *serverOption)

// 对服务处理的操作
type serverOption struct {
	Authentication

	ack          AckType
	ackTimeout   time.Duration
	sendErrCount int

	patten string

	maxConnectionIdle time.Duration

	concurrency int // 指定并发的量级

	// 分布式支持
	userLocationMgr   *UserLocationManager
	localInstanceAddr string

	onDisconnect DisconnectHandler
}

// DisconnectHandler 连接关闭前回调
type DisconnectHandler func(uid string, conversationIds []string)

// 设置服务操作
func newServerOptions(opts ...ServerOptions) serverOption {
	// 默认配置
	o := serverOption{
		Authentication:    new(authentication),
		maxConnectionIdle: defaultMaxConnectionIdle,
		ackTimeout:        defaultAckTimeout,
		patten:            "/ws",
		concurrency:       defaultConcurrency,
	}
	// 如果有其它动态配置，for 循环添加
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

func WithServerAuthentication(auth Authentication) ServerOptions {
	return func(opt *serverOption) {
		opt.Authentication = auth
	}
}

func WithServerPatten(patten string) ServerOptions {
	return func(opt *serverOption) {
		opt.patten = patten
	}
}

func WithServerAck(ack AckType) ServerOptions {
	return func(opt *serverOption) {
		opt.ack = ack
	}
}

func WithServerMaxConnectionIdle(maxConnectionIdle time.Duration) ServerOptions {
	return func(opt *serverOption) {
		if maxConnectionIdle > 0 {
			opt.maxConnectionIdle = maxConnectionIdle
		}
	}
}

// WithUserLocationManager 设置用户位置管理器（分布式支持）
func WithUserLocationManager(mgr *UserLocationManager) ServerOptions {
	return func(opt *serverOption) {
		opt.userLocationMgr = mgr
	}
}

// WithLocalInstanceAddr 设置本地实例地址（分布式支持）
func WithLocalInstanceAddr(addr string) ServerOptions {
	return func(opt *serverOption) {
		opt.localInstanceAddr = addr
	}
}

// WithServerOnDisconnect 断线时刷已读游标等
func WithServerOnDisconnect(h DisconnectHandler) ServerOptions {
	return func(opt *serverOption) {
		opt.onDisconnect = h
	}
}
