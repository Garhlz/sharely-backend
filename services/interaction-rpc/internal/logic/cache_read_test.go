package logic

import (
	"context"
	"testing"
	"time"

	"interaction-rpc/interaction"
	"interaction-rpc/internal/svc"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
)

func TestSetCacheLoadsFactsWithoutBloom(t *testing.T) {
	for _, tc := range []struct {
		name   string
		spec   setCacheSpec
		ensure func(context.Context, *svc.ServiceContext, string) error
	}{
		{"like", postLikeCacheSpec, EnsurePostLikeCache},
		{"collect", postCollectCacheSpec, EnsurePostCollectCache},
		{"comment_like", commentLikeCacheSpec, EnsureCommentLikeCache},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
			for _, empty := range []bool{false, true} {
				name := "populated"
				if empty {
					name = "empty"
				}
				mt.Run(name, func(mt *mtest.T) {
					s, cache := newInteractionTestContext(mt.T)
					s.Mongo = mt.DB
					var docs []bson.D
					if !empty {
						docs = append(docs, bson.D{{Key: "userId", Value: int64(9)}})
					}
					mt.AddMockResponses(mtest.CreateCursorResponse(0, mt.DB.Name()+"."+tc.spec.mongoCollection, mtest.FirstBatch, docs...))
					require.NoError(mt, tc.ensure(context.Background(), s, "target"))
					key := tc.spec.redisKeyPrefix + "target"
					count, err := countSetWithoutDummy(context.Background(), s, key)
					require.NoError(mt, err)
					if empty {
						require.Zero(mt, count)
						member, err := s.Redis.SIsMember(context.Background(), key, DummyUserId).Result()
						require.NoError(mt, err)
						require.True(mt, member)
					} else {
						require.EqualValues(mt, 1, count)
						member, err := s.Redis.SIsMember(context.Background(), key, "9").Result()
						require.NoError(mt, err)
						require.True(mt, member)
					}
					require.Equal(mt, time.Minute, cache.TTL(key))
					commands := len(mt.GetAllStartedEvents())
					require.NoError(mt, tc.ensure(context.Background(), s, "target"))
					require.Len(mt, mt.GetAllStartedEvents(), commands, "cache hit must not read Mongo again")
				})
			}
		})
	}
}

func TestRateCacheLoadsFactsWithoutBloom(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	for _, empty := range []bool{false, true} {
		name := "populated"
		if empty {
			name = "empty"
		}
		mt.Run(name, func(mt *mtest.T) {
			s, cache := newInteractionTestContext(mt.T)
			s.Mongo = mt.DB
			var docs []bson.D
			if !empty {
				docs = append(docs, bson.D{{Key: "userId", Value: int64(9)}, {Key: "score", Value: 4.5}})
			}
			mt.AddMockResponses(mtest.CreateCursorResponse(0, mt.DB.Name()+".post_ratings", mtest.FirstBatch, docs...))
			require.NoError(mt, EnsurePostRateCache(context.Background(), s, "target"))
			key := KeyPostRateHash + "target"
			if empty {
				require.Equal(mt, "0", cache.HGet(key, DummyUserId))
			} else {
				require.Equal(mt, "4.5", cache.HGet(key, "9"))
			}
			require.Equal(mt, time.Minute, cache.TTL(key))
			commands := len(mt.GetAllStartedEvents())
			require.NoError(mt, EnsurePostRateCache(context.Background(), s, "target"))
			require.Len(mt, mt.GetAllStartedEvents(), commands)
		})
	}
}

func TestBatchPostStatsReadsExistingCaches(t *testing.T) {
	s, cache := newInteractionTestContext(t)
	cache.SAdd(KeyPostLikeSet+"target", "9")
	cache.SAdd(KeyPostCollectSet+"target", DummyUserId)
	cache.HSet(KeyPostRateHash+"target", "9", "4.5")
	resp, err := NewBatchPostStatsLogic(context.Background(), s).BatchPostStats(&interaction.BatchPostStatsRequest{UserId: 9, PostIds: []string{"target"}})
	require.NoError(t, err)
	stats := resp.Stats["target"]
	require.EqualValues(t, 1, stats.LikeCount)
	require.True(t, stats.IsLiked)
	require.Zero(t, stats.CollectCount)
	require.False(t, stats.IsCollected)
	require.Equal(t, 4.5, stats.MyScore)
}
