package logic

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCacheSpecificationsMatchStorageContract(t *testing.T) {
	assert.Equal(t, "post:like:", postLikeCacheSpec.redisKeyPrefix)
	assert.Equal(t, "post_likes", postLikeCacheSpec.mongoCollection)
	assert.Equal(t, "postId", postLikeCacheSpec.targetField)
	assert.Equal(t, "post:collect:", postCollectCacheSpec.redisKeyPrefix)
	assert.Equal(t, "post_collects", postCollectCacheSpec.mongoCollection)
	assert.Equal(t, "comment:like:", commentLikeCacheSpec.redisKeyPrefix)
	assert.Equal(t, "comment_likes", commentLikeCacheSpec.mongoCollection)
	assert.Equal(t, "commentId", commentLikeCacheSpec.targetField)
	assert.Equal(t, "post:rate:", postRateCacheSpec.redisKeyPrefix)
	assert.Equal(t, "post_ratings", postRateCacheSpec.mongoCollection)
	assert.Equal(t, "score", postRateCacheSpec.valueField)
}

func TestCacheConstants(t *testing.T) {
	assert.Equal(t, "-1", DummyUserId)
	assert.Equal(t, 60, cacheTTLSeconds)
	assert.Equal(t, 5, cacheWarmLockSeconds)
}

func TestSetCacheMetricKind(t *testing.T) {
	assert.Equal(t, "like", setCacheMetricKind(postLikeCacheSpec))
	assert.Equal(t, "collect", setCacheMetricKind(postCollectCacheSpec))
	assert.Equal(t, "comment_like", setCacheMetricKind(commentLikeCacheSpec))
	assert.Equal(t, "custom", setCacheMetricKind(setCacheSpec{redisKeyPrefix: ":custom:"}))
}
