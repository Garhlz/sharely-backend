package config

import (
	"github.com/zeromicro/go-zero/core/stores/redis" // 引入 redis 定义
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	zrpc.RpcServerConf

	Metrics struct {
		Host string
		Port int
		Path string
	}

	// 兼容 YAML 的 BizRedis 字段；仅复用 go-zero 配置类型，业务使用 go-redis。
	BizRedis redis.RedisConf

	// 对应 YAML 中的 RabbitMQ 部分
	RabbitMQ struct {
		DataSource string
	}

	Mongo struct {
		Uri      string
		Database string
	}
}
