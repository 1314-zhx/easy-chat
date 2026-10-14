/**
 * @author: dn-jinmin/dn-jinmin
 * @doc:
 */

package config

import "github.com/zeromicro/go-zero/core/stores/redis"

type Config struct {
	ListenOn string

	Redis redis.RedisConf

	Etcd struct {
		Hosts []string `json:",optional"`
	}

	JwtAuth struct {
		AccessSecret string
	}

	Mongo struct {
		Url string
		Db  string
	}

	MsgChatTransfer struct {
		Topic string
		Addrs []string
	}

	MsgReadTransfer struct {
		Topic string
		Addrs []string
	}

	// RelayListenOn 跨实例 gRPC 监听；空则不启 Relayer 服务端
	RelayListenOn string `json:",optional"`
	// InstanceAddr 写入 Redis 的本机 Relayer 地址，供其它实例 Dial
	InstanceAddr string `json:",optional"`
	// MaxConnectionIdleSeconds 用户 WS 空闲掐连接，0 表示不启用（保持原 MaxInt64）
	MaxConnectionIdleSeconds int64 `json:",optional"`
}
