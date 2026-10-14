package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"easy-chat/apps/ai/rpc/ai"
	"easy-chat/apps/ai/rpc/internal/config"
	"easy-chat/apps/ai/rpc/internal/server"
	"easy-chat/apps/ai/rpc/internal/svc"
	"easy-chat/pkg/etcdwatch"
	"easy-chat/pkg/lifecycle"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var configFile = flag.String("f", "etc/dev/ai.yaml", "the config file")

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)
	ctx := svc.NewServiceContext(c)

	hot := lifecycle.NewController()
	runtimeCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		raw, err := etcdwatch.Encode(c)
		if err != nil {
			fmt.Printf("etcd config: encode failed, use local file: %v\n", err)
			return
		}
		etcdwatch.SyncAndWatch(runtimeCtx, c.Etcd.Hosts, "/config/ai.rpc", raw,
			func(value []byte) {
				var nc config.Config
				if err := etcdwatch.Decode(value, &nc); err != nil {
					fmt.Printf("etcd config: decode put failed: %v\n", err)
					return
				}
				ctx.ApplyRuntimeConfig(nc)
				fmt.Println("ai.rpc hot config updated from etcd")
			},
			func() {
				ctx.ApplyRuntimeConfig(c)
				fmt.Println("ai.rpc config deleted in etcd, fallback to local file")
			},
		)
	}()

	s := zrpc.MustNewServer(c.RpcServerConf, func(grpcServer *grpc.Server) {
		ai.RegisterAiAgentServer(grpcServer, server.NewAiAgentServer(ctx))

		if c.Mode == service.DevMode || c.Mode == service.TestMode {
			reflection.Register(grpcServer)
		}
	})
	defer func() {
		hot.BeginDrain()
		s.Stop()
	}()

	fmt.Printf("Starting rpc server at %s...\n", c.ListenOn)
	s.Start()
	_ = hot
	_ = time.Second
}
