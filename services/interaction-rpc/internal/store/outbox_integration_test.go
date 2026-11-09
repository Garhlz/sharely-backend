package store

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"interaction-rpc/internal/metrics"

	"github.com/prometheus/client_golang/prometheus/testutil"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc/metadata"
)

// 此测试要求独立 RabbitMQ：队列名与生产一致，不允许指向共享开发 broker。
func TestOutboxBrokerRecovery(t *testing.T) {
	uri, url := os.Getenv("SHARELY_TEST_MONGO_URI"), os.Getenv("SHARELY_TEST_RABBITMQ_URL")
	if uri == "" || url == "" {
		t.Skip("requires isolated Mongo replica set and RabbitMQ")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	oldProp := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	defer otel.SetTextMapPropagator(oldProp)
	const traceparent = "00-11111111111111111111111111111111-2222222222222222-01"
	ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier{"traceparent": traceparent})
	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("x-request-id", "obs-request"))
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	require.NoError(t, err)
	defer client.Disconnect(context.Background())
	db := client.Database("sharely_outbox_test_" + primitive.NewObjectID().Hex())
	defer db.Drop(context.Background())
	s := &MongoStore{DB: db}
	require.NoError(t, s.Init(ctx))
	id := primitive.NewObjectID()
	_, err = db.Collection("posts").InsertOne(ctx, bson.M{"_id": id, "userId": 2})
	require.NoError(t, err)
	_, err = s.Apply(ctx, Command{UserID: 1, TargetID: id.Hex(), Kind: "LIKE"})
	require.NoError(t, err)
	// 无法连接 Broker 时，持久化任务保留，互动不回滚。
	require.Error(t, s.deliver(ctx, "amqp://127.0.0.1:1/"))
	n, err := db.Collection("interaction_outbox").CountDocuments(ctx, bson.M{})
	require.NoError(t, err)
	require.EqualValues(t, 2, n)
	// 即使 Broker 不可达，独立采集仍能看到已持久化积压。
	offlineCtx, stopOffline := context.WithCancel(ctx)
	offlineDone := make(chan struct{})
	go func() { defer close(offlineDone); s.RunOutbox(offlineCtx, "amqp://127.0.0.1:1/") }()
	t.Cleanup(func() { stopOffline(); <-offlineDone })
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(metrics.OutboxCollectionUp) == 1 && testutil.ToFloat64(metrics.OutboxPending) == 2
	}, 5*time.Second, 25*time.Millisecond)
	stopOffline()
	<-offlineDone
	conn, err := amqp.Dial(url)
	require.NoError(t, err)
	defer conn.Close()
	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()
	notifyQueue := NotificationQueue + "." + id.Hex()
	searchQueue := SearchQueue + "." + id.Hex()
	for original, isolated := range map[string]string{NotificationQueue: notifyQueue, SearchQueue: searchQueue} {
		_, err = ch.QueueDeclare(isolated, true, false, false, false, nil)
		require.NoError(t, err)
		_, err = db.Collection("interaction_outbox").UpdateMany(ctx, bson.M{"queue": original}, bson.M{"$set": bson.M{"queue": isolated}})
		require.NoError(t, err)
		defer ch.QueueDelete(isolated, false, false, false)
	}
	workerCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); s.RunOutbox(workerCtx, url) }()
	t.Cleanup(func() {
		stop()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("outbox worker did not stop")
		}
	})
	require.Eventually(t, func() bool {
		n, err := db.Collection("interaction_outbox").CountDocuments(ctx, bson.M{})
		return err == nil && n == 0
	}, 10*time.Second, 50*time.Millisecond)
	d, ok, err := ch.Get(notifyQueue, false)
	require.NoError(t, err)
	require.True(t, ok)
	var event Event
	require.NoError(t, json.Unmarshal(d.Body, &event))
	require.Equal(t, "LIKE", event.Type)
	require.Equal(t, id.Hex(), event.TargetID)
	require.NotEmpty(t, event.EventID)
	require.Equal(t, "obs-request", d.Headers["X-Request-Id"])
	require.Regexp(t, "^00-11111111111111111111111111111111-[0-9a-f]{16}-01$", d.Headers["traceparent"])
	require.EqualValues(t, 1, event.Version)
	require.NoError(t, d.Ack(false))
	d, ok, err = ch.Get(searchQueue, false)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "PostUpdateEvent", d.Headers["__TypeId__"])
	require.Regexp(t, "^00-11111111111111111111111111111111-[0-9a-f]{16}-01$", d.Headers["traceparent"])
	require.NoError(t, d.Ack(false))
	stop()
	<-done
	require.NoError(t, s.snapshotOutbox(ctx))
	require.Zero(t, testutil.ToFloat64(metrics.OutboxPending))
	// mandatory 退回的任务不能因 Broker 的 ACK 被误删。
	missing := Outbox{ID: primitive.NewObjectID(), Queue: "missing." + primitive.NewObjectID().Hex(), Payload: bson.M{"test": true}, CreatedAt: time.Now()}
	_, err = db.Collection("interaction_outbox").InsertOne(ctx, missing)
	require.NoError(t, err)
	require.ErrorContains(t, s.deliver(ctx, url), "unroutable")
	n, err = db.Collection("interaction_outbox").CountDocuments(ctx, bson.M{"_id": missing.ID})
	require.NoError(t, err)
	require.EqualValues(t, 1, n)
}
