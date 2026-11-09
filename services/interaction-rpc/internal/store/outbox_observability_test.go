package store

import (
	"context"
	"testing"
	"time"

	"interaction-rpc/internal/metrics"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
	"go.opentelemetry.io/otel"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestOutboxSnapshot(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("backlog then empty", func(mt *mtest.T) {
		s := &MongoStore{DB: mt.DB}
		mt.AddMockResponses(mtest.CreateCursorResponse(0, mt.DB.Name()+".interaction_outbox", mtest.FirstBatch, bson.D{{Key: "count", Value: int64(2)}, {Key: "oldest", Value: time.Now().Add(-3 * time.Minute)}}))
		require.NoError(t, s.snapshotOutbox(context.Background()))
		require.Equal(t, float64(2), testutil.ToFloat64(metrics.OutboxPending))
		require.GreaterOrEqual(t, testutil.ToFloat64(metrics.OutboxOldest), float64(179))
		require.Equal(t, float64(1), testutil.ToFloat64(metrics.OutboxCollectionUp))
		mt.AddMockResponses(mtest.CreateCursorResponse(0, mt.DB.Name()+".interaction_outbox", mtest.FirstBatch))
		require.NoError(t, s.snapshotOutbox(context.Background()))
		require.Zero(t, testutil.ToFloat64(metrics.OutboxPending))
		require.Zero(t, testutil.ToFloat64(metrics.OutboxOldest))
	})
	mt.Run("failure remains visibly failed", func(mt *mtest.T) {
		mt.AddMockResponses(mtest.CreateCommandErrorResponse(mtest.CommandError{Code: 13, Message: "forbidden"}))
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		s := &MongoStore{DB: mt.DB}
		// 已取消的采集也必须标记失败，不把旧值伪装为新的零积压。
		s.observeOutbox(ctx)
		require.Zero(t, testutil.ToFloat64(metrics.OutboxCollectionUp))
	})
}

func TestOutboxPublishErrorRetainsTraceParent(t *testing.T) {
	oldProvider, oldProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	defer func() {
		otel.SetTracerProvider(oldProvider)
		otel.SetTextMapPropagator(oldProp)
		_ = provider.Shutdown(context.Background())
	}()
	ctx, parent := provider.Tracer("test").Start(context.Background(), "request")
	headers := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, headers)
	parent.End()
	s := &MongoStore{}
	// JSON 编码失败发生在网络调用前，不需要 Broker。
	err := s.publishEntry(context.Background(), nil, nil, nil, Outbox{Headers: headers, Payload: bson.M{"bad": make(chan int)}})
	require.Error(t, err)
	spans := recorder.Ended()
	require.Len(t, spans, 2)
	require.Equal(t, parent.SpanContext().TraceID(), spans[1].SpanContext().TraceID())
	require.Equal(t, parent.SpanContext().SpanID(), spans[1].Parent().SpanID())
	require.Equal(t, otelcodes.Error, spans[1].Status().Code)
}
