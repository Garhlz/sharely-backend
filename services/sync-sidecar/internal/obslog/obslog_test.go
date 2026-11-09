package obslog

import (
	"bytes"
	"encoding/json"
	"log"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

func TestStructuredDeliveryLog(t *testing.T) {
	previous, prop := logger, otel.GetTextMapPropagator()
	defer func() { logger = previous; otel.SetTextMapPropagator(prop) }()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	var buf bytes.Buffer
	logger = log.New(&buf, "", 0)
	d := amqp.Delivery{MessageId: "event1", RoutingKey: "post.update", Headers: amqp.Table{"X-Request-Id": "req1", "X-Trace-Id": "not-an-otel-id", "traceparent": "00-11111111111111111111111111111111-2222222222222222-01"}}
	DeliveryErrorf(d, "failure %s", "reason")
	var entry map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &entry))
	require.Equal(t, "error", entry["level"])
	require.Equal(t, "req1", entry["requestId"])
	require.Equal(t, "11111111111111111111111111111111", entry["traceId"])
	require.Equal(t, "event1", entry["messageId"])
	require.Equal(t, "failure reason", entry["message"])
}
