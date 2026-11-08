package logic

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestRequestIDFromContext(t *testing.T) {
	tests := []struct {
		name string
		ctx  context.Context
		want string
	}{
		{name: "request id wins", ctx: metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-request-id", "req-1", "x-trace-id", "trace-1")), want: "req-1"},
		{name: "trace id fallback", ctx: metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-trace-id", "trace-1")), want: "trace-1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, requestIDFromContext(tt.ctx))
		})
	}
	assert.NotEmpty(t, requestIDFromContext(nil))
}

func TestInteractionEventJSONContract(t *testing.T) {
	tests := []struct {
		name  string
		event InteractionEvent
		want  string
	}{
		{"like add", InteractionEvent{UserId: 9, TargetId: "post-1", Type: "LIKE", Action: "ADD"}, `{"userId":9,"targetId":"post-1","type":"LIKE","action":"ADD","value":null}`},
		{"like remove", InteractionEvent{UserId: 9, TargetId: "post-1", Type: "LIKE", Action: "REMOVE"}, `{"userId":9,"targetId":"post-1","type":"LIKE","action":"REMOVE","value":null}`},
		{"collect", InteractionEvent{UserId: 9, TargetId: "post-1", Type: "COLLECT", Action: "ADD"}, `{"userId":9,"targetId":"post-1","type":"COLLECT","action":"ADD","value":null}`},
		{"comment like", InteractionEvent{UserId: 9, TargetId: "comment-1", Type: "COMMENT_LIKE", Action: "ADD"}, `{"userId":9,"targetId":"comment-1","type":"COMMENT_LIKE","action":"ADD","value":null}`},
		{"rate", InteractionEvent{UserId: 9, TargetId: "post-1", Type: "RATE", Action: "ADD", Value: 4.5}, `{"userId":9,"targetId":"post-1","type":"RATE","action":"ADD","value":4.5}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, err := json.Marshal(tt.event)
			require.NoError(t, err)
			assert.JSONEq(t, tt.want, string(body))
		})
	}
}

func TestAMQPHeaderCarrier(t *testing.T) {
	headers := amqp091.Table{"text": "value", "bytes": []byte("raw"), "number": int32(1)}
	carrier := amqpHeaderCarrier(headers)
	assert.Equal(t, "value", carrier.Get("text"))
	assert.Equal(t, "raw", carrier.Get("bytes"))
	assert.Empty(t, carrier.Get("number"))
	carrier.Set("traceparent", "trace-value")
	assert.Equal(t, "trace-value", headers["traceparent"])
	assert.ElementsMatch(t, []string{"text", "bytes", "number", "traceparent"}, carrier.Keys())
}

func TestCacheSpecificationsMatchStorageContract(t *testing.T) {
	assert.Equal(t, "post:like:", postLikeCacheSpec.redisKeyPrefix)
	assert.Equal(t, "post_likes", postLikeCacheSpec.mongoCollection)
	assert.Equal(t, "postId", postLikeCacheSpec.targetField)
	assert.Equal(t, KeyBloomPostLike, postLikeCacheSpec.bloomKey)
	assert.Equal(t, "post:collect:", postCollectCacheSpec.redisKeyPrefix)
	assert.Equal(t, "post_collects", postCollectCacheSpec.mongoCollection)
	assert.Equal(t, "comment:like:", commentLikeCacheSpec.redisKeyPrefix)
	assert.Equal(t, "comment_likes", commentLikeCacheSpec.mongoCollection)
	assert.Equal(t, "commentId", commentLikeCacheSpec.targetField)
	assert.Equal(t, "post:rate:", postRateCacheSpec.redisKeyPrefix)
	assert.Equal(t, "post_ratings", postRateCacheSpec.mongoCollection)
	assert.Equal(t, "score", postRateCacheSpec.valueField)
}

func TestInteractionConstantsMatchCrossLanguageContract(t *testing.T) {
	assert.Equal(t, "platform.topic.exchange", ExchangeName)
	assert.Equal(t, "interaction.create", RoutingKeyCreate)
	assert.Equal(t, "interaction.delete", RoutingKeyDelete)
	assert.Equal(t, "-1", DummyUserId)
	assert.Equal(t, 60, cacheTTLSeconds)
	assert.Equal(t, 5, cacheWarmLockSeconds)
}

func TestRequestIDFallbackIsNonEmpty(t *testing.T) {
	assert.NotEmpty(t, requestIDFromContext(context.Background()))
}

func TestSetCacheMetricKind(t *testing.T) {
	assert.Equal(t, "like", setCacheMetricKind(postLikeCacheSpec))
	assert.Equal(t, "collect", setCacheMetricKind(postCollectCacheSpec))
	assert.Equal(t, "comment_like", setCacheMetricKind(commentLikeCacheSpec))
	assert.Equal(t, "custom", setCacheMetricKind(setCacheSpec{redisKeyPrefix: ":custom:"}))
}

func TestInteractionEventIfChanged(t *testing.T) {
	t.Run("duplicate add does not publish", func(t *testing.T) {
		event, publish := interactionEventIfChanged(0, 9, "post-1", "LIKE", "ADD", nil)
		assert.False(t, publish)
		assert.Nil(t, event)
	})

	t.Run("missing member remove does not publish", func(t *testing.T) {
		event, publish := interactionEventIfChanged(0, 9, "post-1", "LIKE", "REMOVE", nil)
		assert.False(t, publish)
		assert.Nil(t, event)
	})

	t.Run("changed state publishes exact contract", func(t *testing.T) {
		event, publish := interactionEventIfChanged(1, 9, "post-1", "COLLECT", "ADD", nil)
		require.True(t, publish)
		assert.Equal(t, &InteractionEvent{
			UserId:   9,
			TargetId: "post-1",
			Type:     "COLLECT",
			Action:   "ADD",
		}, event)
	})

	t.Run("rating preserves numeric value", func(t *testing.T) {
		event, publish := interactionEventIfChanged(1, 9, "post-1", "RATE", "ADD", 4.5)
		require.True(t, publish)
		assert.Equal(t, 4.5, event.Value)
	})
}
