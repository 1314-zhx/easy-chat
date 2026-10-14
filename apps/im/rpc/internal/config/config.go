package config

import (
	"github.com/zeromicro/go-zero/zrpc"
	"github.com/zeromicro/go-zero/core/stores/redis"
)

type Config struct {
	zrpc.RpcServerConf

	Mongo struct {
		Url string
		Db  string
	}

	Redisx redis.RedisConf
}
