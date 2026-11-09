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

func TestUnifiedRedisCacheWithRealRedis(t *testing.T) {
	addr := os.Getenv("SHARELY_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("requires isolated Redis")
	}
	client := config.NewRedisClient(redis.RedisConf{Host: addr, Type: "node", PingTimeout: time.Second})
	defer client.Close()
	s := &svc.ServiceContext{Redis: client}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	prefix := "sharely:test:" + time.Now().Format("150405.000000000")
	set, hash := prefix+":set", prefix+":hash"
	defer client.Del(context.Background(), set, hash)
	_, err := client.Eval(ctx, fillSetScript, []string{set}, 60, "8", "9").Result()
	require.NoError(t, err)
	count, err := countSetWithoutDummy(ctx, s, set)
	require.NoError(t, err)
	require.EqualValues(t, 2, count)
	ttl, err := client.TTL(ctx, set).Result()
	require.NoError(t, err)
	require.Greater(t, ttl, time.Duration(0))
	require.LessOrEqual(t, ttl, time.Minute)
	_, err = client.Eval(ctx, fillHashScript, []string{hash}, 60, "8", "4.5").Result()
	require.NoError(t, err)
	score, err := client.HGet(ctx, hash, "8").Result()
	require.NoError(t, err)
	require.Equal(t, "4.5", score)
	ttl, err = client.TTL(ctx, hash).Result()
	require.NoError(t, err)
	require.Greater(t, ttl, time.Duration(0))
	require.LessOrEqual(t, ttl, time.Minute)
}
