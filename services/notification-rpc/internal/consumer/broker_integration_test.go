package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type retryUsers struct{ calls atomic.Int32 }

func (u *retryUsers) QueryRowCtx(ctx context.Context, v any, q string, args ...any) error {
	if u.calls.Add(1) == 1 {
		return errors.New("temporary profile lookup failure")
	}
	return (testUsers{}).QueryRowCtx(ctx, v, q, args...)
}

func TestBrokerNotificationRetryAndDeadLetter(t *testing.T) {
	uri, url := os.Getenv("SHARELY_TEST_MONGO_URI"), os.Getenv("SHARELY_TEST_RABBITMQ_URL")
	if uri == "" || url == "" {
		t.Skip("requires isolated Mongo replica set and RabbitMQ")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	require.NoError(t, err)
	defer client.Disconnect(context.Background())
	db := client.Database("sharely_notification_test_" + primitive.NewObjectID().Hex())
	defer db.Drop(context.Background())
	id := primitive.NewObjectID()
	_, err = db.Collection("posts").InsertOne(ctx, bson.M{"_id": id, "userId": 2})
	require.NoError(t, err)
	queue := Queue + "." + id.Hex()
	dlx, dead := queue+".dlx", queue+".dead"
	users := &retryUsers{}
	workerCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- consumeQueue(workerCtx, db, users, url, queue, dlx, dead) }()
	t.Cleanup(func() {
		stop()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("consumer did not stop")
		}
	})
	conn, err := amqp.Dial(url)
	require.NoError(t, err)
	defer conn.Close()
	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()
	_, err = ch.QueueDeclare(queue, true, false, false, false, amqp.Table{"x-dead-letter-exchange": dlx})
	require.NoError(t, err)
	// 等待消费者声明并订阅队列。
	require.Eventually(t, func() bool { q, err := ch.QueueInspect(queue); return err == nil && q.Consumers > 0 }, 5*time.Second, 50*time.Millisecond)
	e := Event{EventID: primitive.NewObjectID().Hex(), SchemaVersion: 1, UserID: 1, TargetID: id.Hex(), Type: "LIKE", Action: "ADD", Notify: true, OccurredAt: time.Now(), Version: 1}
	body, err := json.Marshal(e)
	require.NoError(t, err)
	for i := 0; i < 2; i++ {
		require.NoError(t, ch.PublishWithContext(ctx, "", queue, false, false, amqp.Publishing{ContentType: "application/json", Body: body}))
	}
	require.Eventually(t, func() bool {
		n, err := db.Collection("notification_event_receipts").CountDocuments(ctx, bson.M{"_id": e.EventID})
		return err == nil && n == 1
	}, 10*time.Second, 50*time.Millisecond)
	require.GreaterOrEqual(t, users.calls.Load(), int32(2))
	require.Eventually(t, func() bool {
		q, err := ch.QueueInspect(queue)
		return err == nil && q.Messages == 0 && users.calls.Load() >= 3
	}, 5*time.Second, 50*time.Millisecond)
	n, err := db.Collection("notifications").CountDocuments(ctx, bson.M{})
	require.NoError(t, err)
	require.EqualValues(t, 1, n)
	deadBefore, err := ch.QueueInspect(dead)
	require.NoError(t, err)
	require.NoError(t, ch.PublishWithContext(ctx, "", queue, false, false, amqp.Publishing{Body: []byte("invalid-json")}))
	require.Eventually(t, func() bool {
		q, err := ch.QueueInspect(dead)
		return err == nil && q.Messages > deadBefore.Messages
	}, 5*time.Second, 50*time.Millisecond)
}
