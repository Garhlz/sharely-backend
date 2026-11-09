package handler

import (
	"context"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestSyncFailureSpan(t *testing.T) {
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
	h := &SyncHandler{}
	h.Handle(amqp.Delivery{Headers: amqp.Table{"traceparent": "00-11111111111111111111111111111111-2222222222222222-01"}})
	spans := recorder.Ended()
	require.Len(t, spans, 1)
	require.Equal(t, "11111111111111111111111111111111", spans[0].SpanContext().TraceID().String())
	require.Equal(t, codes.Error, spans[0].Status().Code)
}
