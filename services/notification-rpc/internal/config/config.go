package config

import "github.com/zeromicro/go-zero/zrpc"

type Config struct {
	zrpc.RpcServerConf

	Metrics struct {
		Host string
		Port int
		Path string
	}

	Mongo struct {
		Uri      string
		Database string
	}
	RabbitMQ struct {
		DataSource string `json:",optional"`
	} `json:",optional"`
	Mysql struct {
		DataSource string `json:",optional"`
	} `json:",optional"`
}
