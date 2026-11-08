package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
)

func TestTransactionCommitsOutboxWithFacts(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("new like", func(mt *mtest.T) {
		id := primitive.NewObjectID()
		mt.AddMockResponses(
			mtest.CreateCursorResponse(0, mt.DB.Name()+".posts", mtest.FirstBatch, bson.D{{Key: "_id", Value: id}, {Key: "interactionVersion", Value: int64(4)}}),
			mtest.CreateCursorResponse(0, mt.DB.Name()+".post_likes", mtest.FirstBatch),
			mtest.CreateSuccessResponse(bson.E{Key: "n", Value: 1}),
			mtest.CreateCursorResponse(0, mt.DB.Name()+".post_likes", mtest.FirstBatch, bson.D{{Key: "n", Value: int64(1)}}),
			mtest.CreateSuccessResponse(bson.E{Key: "n", Value: 1}, bson.E{Key: "nModified", Value: 1}),
			mtest.CreateSuccessResponse(bson.E{Key: "n", Value: 2}),
			mtest.CreateSuccessResponse(),
		)
		changed, err := (&MongoStore{DB: mt.DB}).Apply(context.Background(), Command{UserID: 1, TargetID: id.Hex(), Kind: "LIKE"})
		require.NoError(mt, err)
		require.True(mt, changed)
		events := mt.GetAllStartedEvents()
		require.Equal(mt, "commitTransaction", events[len(events)-1].CommandName)
		var inserts int
		for _, e := range events {
			if e.CommandName == "insert" {
				inserts++
				require.True(mt, e.Command.Lookup("autocommit").Type != 0)
			}
		}
		require.Equal(mt, 2, inserts)
	})
	mt.Run("outbox rejected aborts transaction", func(mt *mtest.T) {
		id := primitive.NewObjectID()
		mt.AddMockResponses(
			mtest.CreateCursorResponse(0, mt.DB.Name()+".posts", mtest.FirstBatch, bson.D{{Key: "_id", Value: id}}),
			mtest.CreateCursorResponse(0, mt.DB.Name()+".post_likes", mtest.FirstBatch),
			mtest.CreateSuccessResponse(bson.E{Key: "n", Value: 1}),
			mtest.CreateCursorResponse(0, mt.DB.Name()+".post_likes", mtest.FirstBatch, bson.D{{Key: "n", Value: int64(1)}}),
			mtest.CreateSuccessResponse(bson.E{Key: "n", Value: 1}),
			mtest.CreateWriteErrorsResponse(mtest.WriteError{Index: 0, Code: 121, Message: "validation failed"}),
			mtest.CreateSuccessResponse(),
		)
		_, err := (&MongoStore{DB: mt.DB}).Apply(context.Background(), Command{UserID: 1, TargetID: id.Hex(), Kind: "LIKE"})
		require.Error(mt, err)
		events := mt.GetAllStartedEvents()
		require.Equal(mt, "abortTransaction", events[len(events)-1].CommandName)
	})
}
