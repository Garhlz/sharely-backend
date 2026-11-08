package logic

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRedisCacheFillAndLockSemantics(t *testing.T) {
	s, server := newInteractionTestContext(t)
	ctx := context.Background()
	_, err := s.Redis.Eval(ctx, fillSetScript, []string{"test:set"}, 60, "8", "9").Result()
	require.NoError(t, err)
	count, err := countSetWithoutDummy(ctx, s, "test:set")
	require.NoError(t, err)
	require.EqualValues(t, 2, count)
	require.Equal(t, time.Minute, server.TTL("test:set"))
	_, err = s.Redis.Eval(ctx, fillSetScript, []string{"test:set"}, 60, DummyUserId).Result()
	require.NoError(t, err)
	count, err = countSetWithoutDummy(ctx, s, "test:set")
	require.NoError(t, err)
	require.Zero(t, count)
	_, err = s.Redis.Eval(ctx, fillHashScript, []string{"test:hash"}, 60, "8", "4.5").Result()
	require.NoError(t, err)
	score, err := s.Redis.HGet(ctx, "test:hash", "8").Result()
	require.NoError(t, err)
	require.Equal(t, "4.5", score)
	require.Equal(t, time.Minute, server.TTL("test:hash"))
	locked, err := s.Redis.SetNX(ctx, "test:lock", "1", 5*time.Second).Result()
	require.NoError(t, err)
	require.True(t, locked)
	locked, err = s.Redis.SetNX(ctx, "test:lock", "1", 5*time.Second).Result()
	require.NoError(t, err)
	require.False(t, locked)
	server.FastForward(5 * time.Second)
	locked, err = s.Redis.SetNX(ctx, "test:lock", "1", 5*time.Second).Result()
	require.NoError(t, err)
	require.True(t, locked)
}
