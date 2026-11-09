package consumer

import (
	"context"
	"errors"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestProcessAttemptTraceAndFailure(t *testing.T) {
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
	ctx, parent := provider.Tracer("test").Start(context.Background(), "publish")
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	parent.End()
	d := amqp.Delivery{Headers: amqp.Table{"traceparent": carrier["traceparent"]}, MessageId: "event1"}
	for _, failure := range []bool{false, true} {
		err := processAttempt(context.Background(), d, func(ctx context.Context) error {
			require.Equal(t, parent.SpanContext().TraceID(), trace.SpanContextFromContext(ctx).TraceID())
			_, ok := ctx.Deadline()
			require.True(t, ok)
			if failure {
				return errors.New("temporary")
			}
			return nil
		})
		require.Equal(t, failure, err != nil)
	}
	spans := recorder.Ended()
	require.Len(t, spans, 3)
	require.Equal(t, parent.SpanContext().SpanID(), spans[1].Parent().SpanID())
	require.Equal(t, otelcodes.Error, spans[2].Status().Code)
}
