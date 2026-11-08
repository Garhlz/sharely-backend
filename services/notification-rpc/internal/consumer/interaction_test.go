package consumer

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestNotificationContract(t *testing.T) {
	var e Event
	require.NoError(t, json.Unmarshal([]byte(`{"eventId":"event1","schemaVersion":1,"userId":9,"targetId":"507f1f77bcf86cd799439011","type":"LIKE","action":"ADD","occurredAt":"2026-11-08T12:00:00Z","notify":true,"version":1}`), &e))
	typeName, err := NotificationType(e)
	require.NoError(t, err)
	require.Equal(t, "LIKE_POST", typeName)
	e.SchemaVersion = 2
	_, err = NotificationType(e)
	require.Error(t, err)
}

type testUsers struct{}

func (testUsers) QueryRowCtx(_ context.Context, v any, _ string, _ ...any) error {
	return json.Unmarshal([]byte(`{"Nickname":"sender","Avatar":"avatar"}`), v)
}

func TestMongoNotificationReplay(t *testing.T) {
	uri := os.Getenv("SHARELY_TEST_MONGO_URI")
	if uri == "" {
		t.Skip("set SHARELY_TEST_MONGO_URI to an isolated replica set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	require.NoError(t, err)
	defer client.Disconnect(context.Background())
	db := client.Database("sharely_notification_test_" + primitive.NewObjectID().Hex())
	defer db.Drop(context.Background())
	id := primitive.NewObjectID()
	_, err = db.Collection("posts").InsertOne(ctx, bson.M{"_id": id, "userId": 2, "title": "post"})
	require.NoError(t, err)
	e := Event{EventID: "new", SchemaVersion: 1, UserID: 1, TargetID: id.Hex(), Type: "LIKE", Action: "ADD", Notify: true, OccurredAt: time.Now().UTC(), Version: 3}
	require.NoError(t, Handle(ctx, db, testUsers{}, e))
	_, err = db.Collection("notifications").UpdateMany(ctx, bson.M{}, bson.M{"$set": bson.M{"isRead": true}})
	require.NoError(t, err)
	require.NoError(t, Handle(ctx, db, testUsers{}, e))
	e.EventID = "old"
	e.Version = 1
	e.OccurredAt = e.OccurredAt.Add(time.Hour)
	require.NoError(t, Handle(ctx, db, testUsers{}, e))
	var notification struct {
		Read bool `bson:"isRead"`
	}
	require.NoError(t, db.Collection("notifications").FindOne(ctx, bson.M{}).Decode(&notification))
	require.True(t, notification.Read)
	n, err := db.Collection("notifications").CountDocuments(ctx, bson.M{})
	require.NoError(t, err)
	require.EqualValues(t, 1, n)
}
