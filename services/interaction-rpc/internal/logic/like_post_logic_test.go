package logic

import (
	"context"
	"errors"
	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"interaction-rpc/interaction"
	"interaction-rpc/internal/store"
	"interaction-rpc/internal/svc"
	"testing"
)

func newInteractionTestContext(t *testing.T) (*svc.ServiceContext, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	raw := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = raw.Close() })
	return &svc.ServiceContext{Redis: redis.MustNewRedis(redis.RedisConf{Host: server.Addr(), Type: "node"}), RawRedis: raw}, server
}

type fakeWriter struct {
	calls   int
	command store.Command
	err     error
	changed bool
}

func (s *fakeWriter) Apply(_ context.Context, c store.Command) (bool, error) {
	s.calls++
	s.command = c
	return s.changed, s.err
}

func TestLikePostPersistenceBeforeCache(t *testing.T) {
	const id = "507f1f77bcf86cd799439011"
	for _, tc := range []struct {
		name    string
		dbErr   error
		changed bool
	}{
		{"new", nil, true}, {"duplicate", nil, false}, {"database failure", errors.New("mongo unavailable"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, cache := newInteractionTestContext(t)
			key := KeyPostLikeSet + id
			cache.SAdd(key, "8")
			writer := &fakeWriter{err: tc.dbErr, changed: tc.changed}
			s.Interactions = writer
			resp, err := NewLikePostLogic(context.Background(), s).LikePost(&interaction.InteractionRequest{UserId: 9, TargetId: id})
			require.Equal(t, 1, writer.calls)
			if tc.dbErr != nil {
				require.Error(t, err)
				require.Nil(t, resp)
				require.True(t, cache.Exists(key))
			} else {
				require.NoError(t, err)
				require.NotNil(t, resp)
				require.False(t, cache.Exists(key))
			}
			require.Equal(t, store.Command{UserID: 9, TargetID: id, Kind: "LIKE"}, writer.command)
		})
	}
}
func TestInteractionRejectsInvalidInputBeforeStore(t *testing.T) {
	writer := &fakeWriter{}
	s := &svc.ServiceContext{Interactions: writer}
	_, err := NewLikePostLogic(context.Background(), s).LikePost(&interaction.InteractionRequest{UserId: 1, TargetId: "invalid"})
	require.Error(t, err)
	require.Zero(t, writer.calls)
}
func TestCommittedInteractionSurvivesRedisFailure(t *testing.T) {
	s, cache := newInteractionTestContext(t)
	cache.Close()
	writer := &fakeWriter{changed: true}
	s.Interactions = writer
	_, err := NewLikePostLogic(context.Background(), s).LikePost(&interaction.InteractionRequest{UserId: 9, TargetId: "507f1f77bcf86cd799439011"})
	require.NoError(t, err)
	require.Equal(t, 1, writer.calls)
}
func TestAllMutationRoutesUsePersistentStore(t *testing.T) {
	s, _ := newInteractionTestContext(t)
	w := &fakeWriter{changed: true}
	s.Interactions = w
	ctx := context.Background()
	req := &interaction.InteractionRequest{UserId: 9, TargetId: "507f1f77bcf86cd799439011"}
	_, err := NewUnlikePostLogic(ctx, s).UnlikePost(req)
	require.NoError(t, err)
	require.True(t, w.command.Remove)
	_, err = NewCollectPostLogic(ctx, s).CollectPost(req)
	require.NoError(t, err)
	require.Equal(t, "COLLECT", w.command.Kind)
	_, err = NewUncollectPostLogic(ctx, s).UncollectPost(req)
	require.NoError(t, err)
	require.True(t, w.command.Remove)
	_, err = NewLikeCommentLogic(ctx, s).LikeComment(req)
	require.NoError(t, err)
	require.Equal(t, "COMMENT_LIKE", w.command.Kind)
	_, err = NewUnlikeCommentLogic(ctx, s).UnlikeComment(req)
	require.NoError(t, err)
	require.True(t, w.command.Remove)
	_, err = NewRatePostLogic(ctx, s).RatePost(&interaction.RateRequest{UserId: 9, TargetId: req.TargetId, Score: 4.5})
	require.NoError(t, err)
	require.Equal(t, 4.5, w.command.Score)
}
