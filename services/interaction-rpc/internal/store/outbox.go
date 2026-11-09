package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"interaction-rpc/internal/metrics"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/zeromicro/go-zero/core/logx"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// RunOutbox 每个进程串行投递，Confirm 后删除；崩溃窗口允许重复，不允许静默丢失。
func (s *MongoStore) RunOutbox(ctx context.Context, url string) {
	observerCtx, stopObserver := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); s.observeOutbox(observerCtx) }()
	defer func() { stopObserver(); <-done }()
	metrics.OutboxPublisherUp.Set(0)
	for ctx.Err() == nil {
		if err := s.deliver(ctx, url); err != nil && ctx.Err() == nil {
			logx.Errorf("interaction outbox: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}

func (s *MongoStore) deliver(ctx context.Context, url string) error {
	conn, err := amqp.Dial(url)
	if err != nil {
		return err
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()
	// 新队列在发布前声明；旧 ES 队列由 Sidecar/Java 声明，mandatory 防止未绑定时丢失。
	if _, err := ch.QueueDeclare(NotificationQueue, true, false, false, false, amqp.Table{"x-dead-letter-exchange": "interaction.notification.dlx"}); err != nil {
		return err
	}
	if err := ch.Confirm(false); err != nil {
		return err
	}
	metrics.OutboxPublisherUp.Set(1)
	defer metrics.OutboxPublisherUp.Set(0)
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	returns := ch.NotifyReturn(make(chan amqp.Return, 1))
	closed := ch.NotifyClose(make(chan *amqp.Error, 1))
	for ctx.Err() == nil {
		select {
		case <-closed:
			return fmt.Errorf("outbox channel closed")
		default:
		}
		var entry Outbox
		err := s.DB.Collection("interaction_outbox").FindOne(ctx, bson.M{}, options.FindOne().SetSort(bson.D{{Key: "createdAt", Value: 1}, {Key: "_id", Value: 1}})).Decode(&entry)
		if err != nil {
			if err != mongo.ErrNoDocuments {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-closed:
				return fmt.Errorf("outbox channel closed")
			case <-time.After(time.Second):
				continue
			}
		}
		if err := s.publishEntry(ctx, ch, confirms, returns, entry); err != nil {
			return err
		}
	}
	return ctx.Err()
}

// 每次重投创建独立 Producer Span；父上下文来自持久化 Outbox，而非后台线程。
func (s *MongoStore) publishEntry(ctx context.Context, ch *amqp.Channel, confirms <-chan amqp.Confirmation, returns <-chan amqp.Return, entry Outbox) (err error) {
	start := time.Now()
	parent := otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(entry.Headers))
	publishCtx, span := otel.Tracer("interaction-rpc/outbox").Start(parent, "outbox.publish", trace.WithSpanKind(trace.SpanKindProducer))
	publishCtx = logx.ContextWithFields(publishCtx, logx.Field("requestId", entry.Headers["X-Request-Id"]))
	if sc := span.SpanContext(); sc.IsValid() {
		publishCtx = logx.ContextWithFields(publishCtx, logx.Field("traceId", sc.TraceID().String()))
	}
	span.SetAttributes(attribute.String("messaging.message.id", entry.ID.Hex()), attribute.String("messaging.destination.name", entry.Queue))
	result := "error"
	defer func() {
		metrics.ObserveOutboxPublish(result, time.Since(start))
		if err != nil {
			span.RecordError(err)
			span.SetStatus(otelcodes.Error, "outbox publish failed")
			logx.WithContext(publishCtx).Errorf("outbox publish failed messageId=%s queue=%s err=%v", entry.ID.Hex(), entry.Queue, err)
		}
		span.End()
	}()
	body, err := json.Marshal(entry.Payload)
	if err != nil {
		return err
	}
	publishCtx, cancel := context.WithTimeout(publishCtx, 10*time.Second)
	defer cancel()
	headers := amqp.Table{"__TypeId__": entry.TypeID}
	for key, value := range entry.Headers {
		headers[key] = value
	}
	otel.GetTextMapPropagator().Inject(publishCtx, headerCarrier(headers))
	err = ch.PublishWithContext(publishCtx, "", entry.Queue, true, false, amqp.Publishing{ContentType: "application/json", DeliveryMode: amqp.Persistent, MessageId: entry.ID.Hex(), Body: body, Headers: headers})
	if err == nil {
		select {
		case confirm, ok := <-confirms:
			if !ok || !confirm.Ack {
				err = fmt.Errorf("publish not confirmed")
			}
		case <-publishCtx.Done():
			err = publishCtx.Err()
		}
		// Return 在相应 Confirm 之前发送；两个独立通知通道均需检查。
		select {
		case returned := <-returns:
			err = fmt.Errorf("unroutable: %s", returned.ReplyText)
		default:
		}
	}
	if err != nil {
		return err
	}
	if _, err := s.DB.Collection("interaction_outbox").DeleteOne(ctx, bson.M{"_id": entry.ID}); err != nil {
		return err
	}
	result = "success"
	return nil
}

type headerCarrier amqp.Table

func (c headerCarrier) Get(key string) string { value, _ := c[key].(string); return value }
func (c headerCarrier) Set(key, value string) { c[key] = value }
func (c headerCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for key := range c {
		keys = append(keys, key)
	}
	return keys
}
