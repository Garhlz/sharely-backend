package obslog

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"sync-sidecar/internal/telemetry"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/trace"
)

var logger = log.New(os.Stdout, "", 0)

func Infof(format string, args ...any)                           { emit("info", nil, format, args...) }
func Errorf(format string, args ...any)                          { emit("error", nil, format, args...) }
func DeliveryInfof(d amqp.Delivery, format string, args ...any)  { emit("info", &d, format, args...) }
func DeliveryErrorf(d amqp.Delivery, format string, args ...any) { emit("error", &d, format, args...) }

func emit(level string, d *amqp.Delivery, format string, args ...any) {
	fields := map[string]any{"service": "sync-sidecar", "level": level, "timestamp": time.Now().UTC().Format(time.RFC3339Nano), "message": fmt.Sprintf(format, args...)}
	if d != nil {
		fields["requestId"] = headerValue(d.Headers, "X-Request-Id")
		fields["routingKey"] = d.RoutingKey
		fields["messageId"] = d.MessageId
		ctx := telemetry.ExtractDeliveryContext(context.Background(), *d)
		if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
			fields["traceId"] = sc.TraceID().String()
		}
	}
	body, err := json.Marshal(fields)
	if err == nil {
		logger.Print(string(body))
	}
}

func headerValue(headers amqp.Table, key string) string {
	if value, ok := headers[key]; ok && value != nil {
		return fmt.Sprint(value)
	}
	return ""
}
