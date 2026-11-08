package store

import (
	"context"
	"math"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestValidate(t *testing.T) {
	base := Command{UserID: 1, TargetID: primitive.NewObjectID().Hex(), Kind: "RATE", Score: 4.5}
	require.NoError(t, Validate(base))
	for _, score := range []float64{0, 5.5, 4.1, math.NaN(), math.Inf(1)} {
		c := base
		c.Score = score
		require.Error(t, Validate(c))
	}
	c := base
	c.Kind = "UNKNOWN"
	require.Error(t, Validate(c))
	c = base
	c.UserID = 0
	require.Error(t, Validate(c))
	c = base
	c.TargetID = "bad"
	require.Error(t, Validate(c))
}

// 仅使用随机命名的隔离库；需显式提供测试副本集，普通单测不连接真实中间件。
func TestMongoTransactions(t *testing.T) {
	uri := os.Getenv("SHARELY_TEST_MONGO_URI")
	if uri == "" {
		t.Skip("set SHARELY_TEST_MONGO_URI to an isolated replica set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	require.NoError(t, err)
	defer client.Disconnect(context.Background())
	db := client.Database("sharely_interaction_test_" + primitive.NewObjectID().Hex())
	defer db.Drop(context.Background())
	s := &MongoStore{DB: db}
	require.NoError(t, s.Init(ctx))
	id := primitive.NewObjectID()
	_, err = db.Collection("posts").InsertOne(ctx, bson.M{"_id": id, "userId": 99, "isDeleted": 0})
	require.NoError(t, err)
	c := Command{UserID: 1, TargetID: id.Hex(), Kind: "LIKE"}
	changed, err := s.Apply(ctx, c)
	require.NoError(t, err)
	require.True(t, changed)
	changed, err = s.Apply(ctx, c)
	require.NoError(t, err)
	require.False(t, changed)
	n, err := db.Collection("interaction_outbox").CountDocuments(ctx, bson.M{})
	require.NoError(t, err)
	require.EqualValues(t, 2, n)
	c.Remove = true
	changed, err = s.Apply(ctx, c)
	require.NoError(t, err)
	require.True(t, changed)
	changed, err = s.Apply(ctx, c)
	require.NoError(t, err)
	require.False(t, changed)
	var post struct {
		Likes       int64   `bson:"likeCount"`
		RatingCount int64   `bson:"ratingCount"`
		Average     float64 `bson:"ratingAverage"`
	}
	require.NoError(t, db.Collection("posts").FindOne(ctx, bson.M{"_id": id}).Decode(&post))
	require.Zero(t, post.Likes)
	for _, score := range []float64{4, 5} {
		_, err = s.Apply(ctx, Command{UserID: 1, TargetID: id.Hex(), Kind: "RATE", Score: score})
		require.NoError(t, err)
	}
	_, err = s.Apply(ctx, Command{UserID: 2, TargetID: id.Hex(), Kind: "RATE", Score: 3})
	require.NoError(t, err)
	require.NoError(t, db.Collection("posts").FindOne(ctx, bson.M{"_id": id}).Decode(&post))
	require.EqualValues(t, 2, post.RatingCount)
	require.Equal(t, 4.0, post.Average)
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := int64(10); i < 18; i++ {
		wg.Add(1)
		go func(user int64) {
			defer wg.Done()
			_, err := s.Apply(ctx, Command{UserID: user, TargetID: id.Hex(), Kind: "LIKE"})
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.NoError(t, db.Collection("posts").FindOne(ctx, bson.M{"_id": id}).Decode(&post))
	require.EqualValues(t, 8, post.Likes)
	// Mongo 数值比较兼容 int32/int64，已删除目标不能继续产生互动。
	deletedID := primitive.NewObjectID()
	_, err = db.Collection("posts").InsertOne(ctx, bson.M{"_id": deletedID, "isDeleted": int64(1)})
	require.NoError(t, err)
	_, err = s.Apply(ctx, Command{UserID: 1, TargetID: deletedID.Hex(), Kind: "LIKE"})
	require.ErrorIs(t, err, mongo.ErrNoDocuments)
	// 强制 Outbox 插入失败，确认明细和统计一起回滚。
	require.NoError(t, db.RunCommand(ctx, bson.D{{Key: "collMod", Value: "interaction_outbox"}, {Key: "validator", Value: bson.M{"forbidden": bson.M{"$exists": true}}}, {Key: "validationLevel", Value: "strict"}}).Err())
	_, err = s.Apply(ctx, Command{UserID: 100, TargetID: id.Hex(), Kind: "COLLECT"})
	require.Error(t, err)
	n, err = db.Collection("post_collects").CountDocuments(ctx, bson.M{"userId": 100})
	require.NoError(t, err)
	require.Zero(t, n)
}
