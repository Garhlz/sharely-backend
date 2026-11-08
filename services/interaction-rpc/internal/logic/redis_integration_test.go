package logic

import (
	"context"
	"os"
	"testing"
	"time"

	"interaction-rpc/internal/config"
	"interaction-rpc/internal/svc"

	"github.com/stretchr/testify/require"
	"github.com/zeromicro/go-zero/core/stores/redis"
)

func TestUnifiedRedisClientWithRedisBloom(t *testing.T) {
	addr := os.Getenv("SHARELY_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("requires isolated Redis Stack")
	}
	client := config.NewRedisClient(redis.RedisConf{Host: addr, Type: "node", PingTimeout: time.Second})
	defer client.Close()
	s := &svc.ServiceContext{Redis: client}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	prefix := "sharely:test:" + time.Now().Format("150405.000000000")
	set, bloom := prefix+":set", prefix+":bloom"
	defer client.Del(context.Background(), set, bloom)
	_, err := client.Eval(ctx, fillSetScript, []string{set}, 60, "8", "9").Result()
	require.NoError(t, err)
	count, err := countSetWithoutDummy(ctx, s, set)
	require.NoError(t, err)
	require.EqualValues(t, 2, count)
	ttl, err := client.TTL(ctx, set).Result()
	require.NoError(t, err)
	require.Greater(t, ttl, time.Duration(0))
	require.LessOrEqual(t, ttl, time.Minute)
	AddBloom(ctx, s, bloom, "target")
	ready, exists := CheckBloom(ctx, s, bloom, "target")
	require.True(t, ready)
	require.True(t, exists)
	ready, exists = CheckBloom(ctx, s, bloom, "missing")
	require.True(t, ready)
	require.False(t, exists)
}
