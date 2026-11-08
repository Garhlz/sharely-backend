package store

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

const NotificationQueue = "interaction.notification.v1"
const SearchQueue = "platform.es.sync.queue"

type Command struct {
	UserID   int64
	TargetID string
	Kind     string
	Remove   bool
	Score    float64
}

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

type Outbox struct {
	ID        primitive.ObjectID `bson:"_id"`
	Queue     string             `bson:"queue"`
	TypeID    string             `bson:"typeId"`
	Payload   bson.M             `bson:"payload"`
	CreatedAt time.Time          `bson:"createdAt"`
	Headers   map[string]string  `bson:"headers,omitempty"`
}

type Writer interface {
	Apply(context.Context, Command) (bool, error)
}
type MongoStore struct{ DB *mongo.Database }

func Collection(kind string) (string, string, error) {
	switch kind {
	case "LIKE":
		return "post_likes", "postId", nil
	case "COLLECT":
		return "post_collects", "postId", nil
	case "RATE":
		return "post_ratings", "postId", nil
	case "COMMENT_LIKE":
		return "comment_likes", "commentId", nil
	default:
		return "", "", errors.New("unknown interaction type")
	}
}

func Validate(c Command) error {
	if c.UserID <= 0 {
		return errors.New("userId must be positive")
	}
	if _, err := primitive.ObjectIDFromHex(c.TargetID); err != nil {
		return errors.New("invalid targetId")
	}
	if _, _, err := Collection(c.Kind); err != nil {
		return err
	}
	if c.Kind == "RATE" && (c.Remove || math.IsNaN(c.Score) || math.IsInf(c.Score, 0) || c.Score < .5 || c.Score > 5 || math.Mod(c.Score*2, 1) != 0) {
		return errors.New("score must be a half-point value between 0.5 and 5")
	}
	return nil
}

// Init 在接收写请求前确认事务能力，并复用 Java 的索引名称与字段顺序。
func (s *MongoStore) Init(ctx context.Context) error {
	var hello struct {
		SetName string `bson:"setName"`
	}
	if err := s.DB.RunCommand(ctx, bson.M{"hello": 1}).Decode(&hello); err != nil {
		return err
	}
	if hello.SetName == "" {
		return errors.New("interaction writes require a MongoDB replica set; see docs/interaction_migration.md")
	}
	for _, kind := range []string{"LIKE", "COLLECT", "RATE", "COMMENT_LIKE"} {
		coll, field, _ := Collection(kind)
		name := "idx_user_post_unique"
		if kind == "RATE" {
			name = "idx_user_post_rating_unique"
		}
		if kind == "COMMENT_LIKE" {
			name = "idx_user_comment_unique"
		}
		if _, err := s.DB.Collection(coll).Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "userId", Value: 1}, {Key: field, Value: 1}}, Options: options.Index().SetName(name).SetUnique(true)}); err != nil {
			return err
		}
	}
	_, err := s.DB.Collection("interaction_outbox").Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "createdAt", Value: 1}}})
	return err
}

// Apply 将互动事实、统计投影和待投递事件一起提交；同一目标的统计文档写入使并发事务串行化。
func (s *MongoStore) Apply(ctx context.Context, c Command) (bool, error) {
	if err := Validate(c); err != nil {
		return false, err
	}
	canonicalID, _ := primitive.ObjectIDFromHex(c.TargetID)
	c.TargetID = canonicalID.Hex()
	session, err := s.DB.Client().StartSession()
	if err != nil {
		return false, err
	}
	defer session.EndSession(ctx)
	result, err := session.WithTransaction(ctx, func(sc mongo.SessionContext) (any, error) {
		collName, targetField, _ := Collection(c.Kind)
		coll := s.DB.Collection(collName)
		parent := s.DB.Collection("posts")
		if c.Kind == "COMMENT_LIKE" {
			parent = s.DB.Collection("comments")
		}
		id, _ := primitive.ObjectIDFromHex(c.TargetID)
		var target bson.M
		if err := parent.FindOne(sc, bson.M{"_id": id, "isDeleted": bson.M{"$ne": 1}}).Decode(&target); err != nil {
			return false, err
		}
		filter := bson.M{"userId": c.UserID, targetField: c.TargetID}
		var old struct {
			Score float64 `bson:"score"`
		}
		oldErr := coll.FindOne(sc, filter).Decode(&old)
		if oldErr != nil && oldErr != mongo.ErrNoDocuments {
			return false, oldErr
		}
		exists := oldErr == nil
		if (c.Remove && !exists) || (!c.Remove && exists && (c.Kind != "RATE" || old.Score == c.Score)) {
			return false, nil
		}
		now := time.Now().UTC()
		if c.Remove {
			if _, err := coll.DeleteOne(sc, filter); err != nil {
				return false, err
			}
		} else if exists {
			if _, err := coll.UpdateOne(sc, filter, bson.M{"$set": bson.M{"score": c.Score, "updatedAt": now}}); err != nil {
				return false, err
			}
		} else {
			doc := bson.M{"userId": c.UserID, targetField: c.TargetID, "createdAt": now}
			if c.Kind == "RATE" {
				doc["score"] = c.Score
			}
			if _, err := coll.InsertOne(sc, doc); err != nil {
				return false, err
			}
		}
		count, err := coll.CountDocuments(sc, bson.M{targetField: c.TargetID})
		if err != nil {
			return false, err
		}
		field := "likeCount"
		if c.Kind == "COLLECT" {
			field = "collectCount"
		}
		stats := bson.M{field: count}
		if c.Kind == "RATE" {
			cursor, err := coll.Aggregate(sc, mongo.Pipeline{{{Key: "$match", Value: bson.M{targetField: c.TargetID}}}, {{Key: "$group", Value: bson.M{"_id": nil, "average": bson.M{"$avg": "$score"}}}}})
			if err != nil {
				return false, err
			}
			defer cursor.Close(sc)
			var averages []struct {
				Average float64 `bson:"average"`
			}
			if err := cursor.All(sc, &averages); err != nil {
				return false, err
			}
			avg := 0.0
			if len(averages) > 0 {
				avg = averages[0].Average
			}
			stats = bson.M{"ratingCount": count, "ratingAverage": avg}
		}
		if _, err := parent.UpdateOne(sc, bson.M{"_id": id}, bson.M{"$set": stats, "$inc": bson.M{"interactionVersion": 1}}); err != nil {
			return false, err
		}
		action := "ADD"
		if c.Remove {
			action = "REMOVE"
		}
		version := int64(1)
		switch old := target["interactionVersion"].(type) {
		case int64:
			version += old
		case int32:
			version += int64(old)
		}
		event := Event{EventID: primitive.NewObjectID().Hex(), SchemaVersion: 1, UserID: c.UserID, TargetID: c.TargetID, Type: c.Kind, Action: action, OccurredAt: now, Notify: !c.Remove && !exists, Version: version}
		// 使用 JSON 字段名保存跨服务载荷。
		notification := bson.M{"eventId": event.EventID, "schemaVersion": 1, "userId": c.UserID, "targetId": c.TargetID, "type": c.Kind, "action": action, "occurredAt": now.Format(time.RFC3339Nano), "notify": event.Notify, "version": version}
		postID := c.TargetID
		if c.Kind == "COMMENT_LIKE" {
			postID, _ = target["postId"].(string)
		}
		headers := propagation.MapCarrier{}
		otel.GetTextMapPropagator().Inject(ctx, headers)
		docs := []any{Outbox{ID: primitive.NewObjectID(), Queue: NotificationQueue, Payload: notification, CreatedAt: now, Headers: headers}}
		if postID != "" {
			docs = append(docs, Outbox{ID: primitive.NewObjectID(), Queue: SearchQueue, TypeID: "PostUpdateEvent", Payload: bson.M{"postId": postID}, CreatedAt: now, Headers: headers})
		}
		if _, err := s.DB.Collection("interaction_outbox").InsertMany(sc, docs); err != nil {
			return false, err
		}
		return true, nil
	}, options.Transaction().SetReadConcern(readconcern.Snapshot()).SetWriteConcern(writeconcern.Majority()))
	if err != nil {
		return false, fmt.Errorf("commit interaction: %w", err)
	}
	return result.(bool), nil
}
