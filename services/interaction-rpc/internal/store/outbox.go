package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/zeromicro/go-zero/core/logx"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// RunOutbox 每个进程串行投递，Confirm 后删除；崩溃窗口允许重复，不允许静默丢失。
func (s *MongoStore) RunOutbox(ctx context.Context, url string) {
	for ctx.Err() == nil {
		if err := s.deliver(ctx, url); err != nil {
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
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	returns := ch.NotifyReturn(make(chan amqp.Return, 1))
	for ctx.Err() == nil {
		var entry Outbox
		err := s.DB.Collection("interaction_outbox").FindOne(ctx, bson.M{}, options.FindOne().SetSort(bson.D{{Key: "createdAt", Value: 1}, {Key: "_id", Value: 1}})).Decode(&entry)
		if err != nil {
			if err != mongo.ErrNoDocuments {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
				continue
			}
		}
		body, err := json.Marshal(entry.Payload)
		if err != nil {
			return err
		}
		publishCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		headers := amqp.Table{"__TypeId__": entry.TypeID}
		for key, value := range entry.Headers {
			headers[key] = value
		}
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
		cancel()
		if err != nil {
			return err
		}
		if _, err := s.DB.Collection("interaction_outbox").DeleteOne(ctx, bson.M{"_id": entry.ID}); err != nil {
			return err
		}
	}
	return ctx.Err()
}
