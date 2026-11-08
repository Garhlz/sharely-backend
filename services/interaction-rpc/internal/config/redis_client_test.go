package config

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/zeromicro/go-zero/core/stores/redis"
)

func TestRedisClientPreservesConnectionOptions(t *testing.T) {
	conf := redis.RedisConf{Host: "127.0.0.1:6379", Type: "node", User: "reader", Pass: "secret", Tls: true, NonBlock: true}
	client := NewRedisClient(conf)
	defer client.Close()
	node, ok := client.(*goredis.Client)
	require.True(t, ok)
	require.Equal(t, conf.User, node.Options().Username)
	require.Equal(t, conf.Pass, node.Options().Password)
	require.NotNil(t, node.Options().TLSConfig)
	conf.Type = "cluster"
	cluster := NewRedisClient(conf)
	defer cluster.Close()
	_, ok = cluster.(*goredis.ClusterClient)
	require.True(t, ok)
}

func TestRedisClientStartupPingAndAuthentication(t *testing.T) {
	server := miniredis.RunT(t)
	server.RequireAuth("secret")
	conf := redis.RedisConf{Host: server.Addr(), Type: "node", Pass: "secret", PingTimeout: time.Second}
	client := NewRedisClient(conf)
	defer client.Close()
	require.NoError(t, client.Ping(context.Background()).Err())
	conf.Pass = "wrong"
	require.Panics(t, func() { NewRedisClient(conf) })
}
