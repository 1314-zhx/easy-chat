/**
 * @author: dn-jinmin/dn-jinmin
 * @doc:
 */

package main

import (
	"context"
	"easy-chat/apps/im/ws/internal/config"
	"easy-chat/apps/im/ws/internal/distributed"
	"easy-chat/apps/im/ws/internal/handler"
	"easy-chat/apps/im/ws/internal/svc"
	"easy-chat/apps/im/ws/websocket"
	"easy-chat/pkg/etcdwatch"
	"easy-chat/pkg/lifecycle"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os/signal"
	"syscall"
	"time"

	"github.com/zeromicro/go-zero/core/conf"
)

var configFile = flag.String("f", "etc/dev/im.yaml", "the config file")

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)

	ctx := svc.NewServiceContext(c)
	hot := lifecycle.NewController()
	runtimeCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 启动 pprof 服务 (默认端口 6060)
	go func() {
		pprofAddr := ":6060"
		fmt.Printf("pprof server listening on %s\n", pprofAddr)
		fmt.Println("  - Memory:   http://localhost:6060/debug/pprof/heap")
		fmt.Println("  - CPU:      http://localhost:6060/debug/pprof/profile?seconds=30")
		fmt.Println("  - Goroutine: http://localhost:6060/debug/pprof/goroutine?debug=1")
		if err := http.ListenAndServe(pprofAddr, nil); err != nil {
			fmt.Printf("pprof server error: %v\n", err)
		}
	}()

	go func() {
		raw, err := etcdwatch.Encode(c)
		if err != nil {
			fmt.Printf("etcd config: encode failed, use local file: %v\n", err)
			return
		}
		etcdwatch.SyncAndWatch(runtimeCtx, c.Etcd.Hosts, "/config/im.ws", raw,
			func(value []byte) {
				var nc config.Config
				if err := etcdwatch.Decode(value, &nc); err != nil {
					fmt.Printf("etcd config: decode put failed: %v\n", err)
					return
				}
				ctx.ApplyRuntimeConfig(nc)
				fmt.Println("im.ws hot config updated from etcd")
			},
			func() {
				ctx.ApplyRuntimeConfig(c)
				fmt.Println("im.ws config deleted in etcd, fallback to local file")
			},
		)
	}()

	var srv *websocket.Server
	opts := []websocket.ServerOptions{
		websocket.WithServerAck(websocket.OnlyAck),
		websocket.WithServerOnDisconnect(func(uid string, convs []string) {
			if ctx.ConvSession != nil {
				ctx.ConvSession.FlushCursors(uid, convs)
			}
			if ctx.ActiveConversationCache == nil || srv == nil {
				return
			}
			if srv.GetConn(uid) != nil {
				return
			}
			for _, cid := range convs {
				_ = ctx.ActiveConversationCache.Remove(uid, cid)
			}
		}),
	}
	if c.MaxConnectionIdleSeconds > 0 {
		opts = append(opts, websocket.WithServerMaxConnectionIdle(time.Duration(c.MaxConnectionIdleSeconds)*time.Second))
	}

	srv = websocket.NewServer(c.ListenOn, opts...)

	if c.InstanceAddr != "" && ctx.Redis != nil {
		locMgr := websocket.NewUserLocationManager(ctx.Redis, 24*time.Hour)
		srv.EnableDistribution(locMgr, c.InstanceAddr)
		relayer := distributed.NewWsRelayer(srv, locMgr, c.InstanceAddr)
		if c.RelayListenOn != "" {
			if err := relayer.StartGrpcServer(c.RelayListenOn); err != nil {
				fmt.Printf("WsRelayer listen failed: %v\n", err)
			}
		}
		srv.SetKickRemote(func(addr, uid, reason string) {
			_ = relayer.KickOn(addr, uid, reason)
		})
		srv.SetForwardRemote(func(uid string, msg *websocket.Message) error {
			res := relayer.SendToUser("", uid, msg)
			return res.Error
		})
		defer relayer.Close()
	}

	defer func() {
		hot.BeginDrain()
		srv.Stop()
	}()

	handler.RegisterHandlers(srv, ctx)

	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-sigCtx.Done()
		hot.BeginDrain()
		time.Sleep(2 * time.Second)
		srv.Stop()
		hot.Close()
	}()

	fmt.Println("start websocket server at ", c.ListenOn, " ..... ")
	srv.Start()

}
