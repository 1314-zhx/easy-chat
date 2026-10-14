/**
 * @author: dn-jinmin/dn-jinmin
 * @doc:
 */

package main

import (
	"context"
	"easy-chat/apps/task/mq/internal/config"
	"easy-chat/apps/task/mq/internal/handler"
	"easy-chat/apps/task/mq/internal/svc"
	"easy-chat/pkg/lifecycle"
	"flag"
	"fmt"
	"os/signal"
	"syscall"
	"time"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/service"
)

var configFile = flag.String("f", "etc/dev/task.yaml", "the config file")

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)
	if err := c.SetUp(); err != nil {
		panic(err)
	}
	ctx := svc.NewServiceContext(c)
	listen := handler.NewListen(ctx)
	serviceGroup := service.NewServiceGroup()
	for _, s := range listen.Services() {
		serviceGroup.Add(s)
	}

	ctrl := lifecycle.NewController()
	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		<-sigCtx.Done()
		ctrl.BeginDrain()
		time.Sleep(2 * time.Second)
		ctrl.Close()
	}()

	fmt.Println("Starting mqueue at ...")
	serviceGroup.Start()
	_ = ctrl
}
