package config

import (
	"context"
	"crypto/tls"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/stores/redis"
)

// NewRedisClient 兼容现有 YAML 配置，仅复用配置结构，业务命令直接使用 go-redis。
func NewRedisClient(conf redis.RedisConf) goredis.UniversalClient {
	if err := conf.Validate(); err != nil {
		panic(err)
	}
	opts := &goredis.UniversalOptions{
		Addrs:                 []string{strings.TrimSpace(conf.Host)},
		Username:              conf.User,
		Password:              conf.Pass,
		IsClusterMode:         conf.Type == "cluster",
		ContextTimeoutEnabled: true,
	}
	if opts.IsClusterMode {
		opts.Addrs = strings.Split(conf.Host, ",")
		for i := range opts.Addrs {
			opts.Addrs[i] = strings.TrimSpace(opts.Addrs[i])
		}
	}
	if conf.Tls {
		opts.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	client := goredis.NewUniversalClient(opts)
	if !conf.NonBlock {
		timeout := conf.PingTimeout
		if timeout <= 0 {
			timeout = time.Second
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		if err := client.Ping(ctx).Err(); err != nil {
			_ = client.Close()
			panic(err)
		}
	}
	return client
}
