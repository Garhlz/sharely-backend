package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"notification-rpc/internal/metrics"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const Queue = "interaction.notification.v1"

type Event struct {
	EventID       string    `json:"eventId"`
	SchemaVersion int       `json:"schemaVersion"`
	UserID        int64     `json:"userId"`
	TargetID      string    `json:"targetId"`
	Type          string    `json:"type"`
	Action        string    `json:"action"`
	OccurredAt    time.Time `json:"occurredAt"`
	Notify        bool      `json:"notify"`
	Version       int64     `json:"version"`
}

func NotificationType(e Event) (string, error) {
	if e.EventID == "" || e.SchemaVersion != 1 || e.UserID <= 0 || e.OccurredAt.IsZero() || e.Version <= 0 {
		return "", errors.New("invalid interaction event")
	}
	if _, err := primitive.ObjectIDFromHex(e.TargetID); err != nil {
		return "", err
	}
	if e.Action != "ADD" && e.Action != "REMOVE" {
		return "", errors.New("invalid action")
	}
	switch e.Type {
	case "LIKE":
		return "LIKE_POST", nil
	case "COLLECT":
		return "COLLECT_POST", nil
	case "RATE":
		return "RATE_POST", nil
	case "COMMENT_LIKE":
		return "LIKE_COMMENT", nil
	default:
		return "", errors.New("unsupported interaction type")
	}
}

func Run(ctx context.Context, db *mongo.Database, url, dsn string) {
	if dsn == "" {
		logx.Error("interaction notifications require Mysql.DataSource")
		return
	}
	users := sqlx.NewMysql(dsn)
	for ctx.Err() == nil {
		if err := consume(ctx, db, users, url); err != nil && ctx.Err() == nil {
			logx.Errorf("interaction notification consumer: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}

func consume(ctx context.Context, db *mongo.Database, users UserReader, url string) error {
	return consumeQueue(ctx, db, users, url, Queue, "interaction.notification.dlx", "interaction.notification.dead")
}

func consumeQueue(ctx context.Context, db *mongo.Database, users UserReader, url, queue, dlx, dead string) error {
	metrics.ConsumerUp.Set(0)
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
	if err = ch.ExchangeDeclare(dlx, "fanout", true, false, false, false, nil); err != nil {
		return err
	}
	if _, err = ch.QueueDeclare(dead, true, false, false, false, nil); err != nil {
		return err
	}
	if err = ch.QueueBind(dead, "", dlx, false, nil); err != nil {
		return err
	}
	if _, err = ch.QueueDeclare(queue, true, false, false, false, amqp.Table{"x-dead-letter-exchange": dlx}); err != nil {
		return err
	}
	if err = ch.Qos(1, 0, false); err != nil {
		return err
	}
	deliveries, err := ch.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	metrics.ConsumerUp.Set(1)
	defer metrics.ConsumerUp.Set(0)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d, ok := <-deliveries:
			if !ok {
				return errors.New("notification channel closed")
			}
			var e Event
			if err := json.Unmarshal(d.Body, &e); err != nil {
				logx.Errorf("invalid notification JSON: %v", err)
				if err := d.Nack(false, false); err != nil {
					metrics.ObserveConsume("ack_error")
					return err
				}
				metrics.ObserveConsume("dead_letter")
				continue
			}
			if _, err := NotificationType(e); err != nil {
				logx.Errorf("invalid notification event: %v", err)
				if err := d.Nack(false, false); err != nil {
					metrics.ObserveConsume("ack_error")
					return err
				}
				metrics.ObserveConsume("dead_letter")
				continue
			}
			for {
				err = processAttempt(ctx, d, func(attempt context.Context) error { return Handle(attempt, db, users, e) })
				if err == nil {
					if err = d.Ack(false); err != nil {
						metrics.ObserveConsume("ack_error")
						return err
					}
					metrics.ObserveConsume("acked")
					break
				}
				logx.Errorf("retry notification event=%s: %v", e.EventID, err)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(2 * time.Second):
				}
			}
		}
	}
}

type UserReader interface {
	QueryRowCtx(context.Context, any, string, ...any) error
}

// Handle 在同一事务内写事件收据与通知，重投不会恢复已读状态。
func Handle(ctx context.Context, db *mongo.Database, users UserReader, e Event) error {
	typeName, err := NotificationType(e)
	if err != nil {
		return err
	}
	if !e.Notify || e.Action != "ADD" {
		return nil
	}
	id, _ := primitive.ObjectIDFromHex(e.TargetID)
	collection := "posts"
	if e.Type == "COMMENT_LIKE" {
		collection = "comments"
	}
	var target struct {
		UserID    int64  `bson:"userId"`
		Title     string `bson:"title"`
		Content   string `bson:"content"`
		PostID    string `bson:"postId"`
		IsDeleted int    `bson:"isDeleted"`
	}
	err = db.Collection(collection).FindOne(ctx, bson.M{"_id": id}).Decode(&target)
	if err == mongo.ErrNoDocuments {
		return nil
	}
	if err != nil {
		return err
	}
	if target.IsDeleted != 0 {
		return nil
	}
	if target.UserID == e.UserID {
		return nil
	}
	var sender struct {
		Nickname string `db:"nickname"`
		Avatar   string `db:"avatar"`
	}
	err = users.QueryRowCtx(ctx, &sender, "SELECT nickname, avatar FROM users WHERE id = ? AND is_deleted = 0", e.UserID)
	if err == sqlx.ErrNotFound {
		return nil
	}
	if err != nil {
		return err
	}
	targetID := e.TargetID
	preview := target.Title
	if preview == "" {
		preview = target.Content
	}
	if e.Type == "COMMENT_LIKE" {
		targetID = target.PostID
	}
	runes := []rune(preview)
	if len(runes) > 50 {
		preview = string(runes[:50])
	}
	session, err := db.Client().StartSession()
	if err != nil {
		return err
	}
	defer session.EndSession(ctx)
	_, err = session.WithTransaction(ctx, func(sc mongo.SessionContext) (any, error) {
		var receipt bson.M
		err := db.Collection("notification_event_receipts").FindOne(sc, bson.M{"_id": e.EventID}).Decode(&receipt)
		if err == nil {
			return nil, nil
		}
		if err != mongo.ErrNoDocuments {
			return nil, err
		}
		key := bson.M{"receiverId": target.UserID, "senderId": e.UserID, "type": typeName, "targetId": targetID}
		var old struct {
			LastEventAt time.Time `bson:"lastEventAt"`
			LastVersion int64     `bson:"lastInteractionVersion"`
			LastSource  string    `bson:"lastInteractionSource"`
		}
		err = db.Collection("notifications").FindOne(sc, key).Decode(&old)
		if err != nil && err != mongo.ErrNoDocuments {
			return nil, err
		}
		newer := !old.LastEventAt.After(e.OccurredAt)
		if old.LastSource == e.TargetID {
			newer = e.Version > old.LastVersion
		}
		if newer {
			set := bson.M{"senderNickname": sender.Nickname, "senderAvatar": sender.Avatar, "targetPreview": preview, "isRead": false, "createdAt": e.OccurredAt, "lastEventAt": e.OccurredAt, "lastInteractionSource": e.TargetID, "lastInteractionVersion": e.Version}
			if _, err = db.Collection("notifications").UpdateOne(sc, key, bson.M{"$set": set, "$setOnInsert": key}, options.Update().SetUpsert(true)); err != nil {
				return nil, err
			}
		}
		_, err = db.Collection("notification_event_receipts").InsertOne(sc, bson.M{"_id": e.EventID, "processedAt": time.Now()})
		return nil, err
	})
	if err != nil {
		return fmt.Errorf("notification transaction: %w", err)
	}
	return nil
}

// 一条消息可有多次处理尝试；success 表示处理成功，acked 才表示确认调用成功。
func processAttempt(ctx context.Context, d amqp.Delivery, handle func(context.Context) error) (err error) {
	carrier := propagation.MapCarrier{}
	for key, value := range d.Headers {
		if str, ok := value.(string); ok {
			carrier[key] = str
		}
	}
	parent := otel.GetTextMapPropagator().Extract(ctx, carrier)
	attempt, cancel := context.WithTimeout(parent, 15*time.Second)
	defer cancel()
	attempt, span := otel.Tracer("notification-rpc/consumer").Start(attempt, "notification.process", trace.WithSpanKind(trace.SpanKindConsumer))
	attempt = logx.ContextWithFields(attempt, logx.Field("requestId", carrier["X-Request-Id"]))
	if sc := span.SpanContext(); sc.IsValid() {
		attempt = logx.ContextWithFields(attempt, logx.Field("traceId", sc.TraceID().String()))
	}
	span.SetAttributes(attribute.String("messaging.message.id", d.MessageId), attribute.String("messaging.destination.name", Queue))
	start := time.Now()
	defer func() {
		result := "success"
		if err != nil {
			result = "retry"
			span.RecordError(err)
			span.SetStatus(otelcodes.Error, "notification handling failed")
			logx.WithContext(attempt).Errorf("notification processing failed messageId=%s err=%v", d.MessageId, err)
		}
		metrics.ObserveConsumeAttempt(result, time.Since(start))
		span.End()
	}()
	return handle(attempt)
}
