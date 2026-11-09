package store

import (
	"context"
	"time"

	"interaction-rpc/internal/metrics"

	"github.com/zeromicro/go-zero/core/logx"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// 独立于 MQ 连接采集，避免 Broker 不可用时积压指标也停止更新。
func (s *MongoStore) observeOutbox(ctx context.Context) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		snapshotCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := s.snapshotOutbox(snapshotCtx)
		cancel()
		if err != nil {
			metrics.OutboxCollectionUp.Set(0)
			if ctx.Err() == nil {
				logx.WithContext(ctx).Errorf("outbox snapshot failed: %v", err)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *MongoStore) snapshotOutbox(ctx context.Context) error {
	collection := s.DB.Collection("interaction_outbox")
	// 一次聚合得到同一快照的计数与最早时间；先按现有索引排序。
	cursor, err := collection.Aggregate(ctx, mongo.Pipeline{
		{{Key: "$sort", Value: bson.D{{Key: "createdAt", Value: 1}}}},
		{{Key: "$group", Value: bson.M{"_id": nil, "count": bson.M{"$sum": 1}, "oldest": bson.M{"$first": "$createdAt"}}}},
	}, options.Aggregate().SetMaxTime(4*time.Second))
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)
	var rows []struct {
		Count  int64     `bson:"count"`
		Oldest time.Time `bson:"oldest"`
	}
	if err := cursor.All(ctx, &rows); err != nil {
		return err
	}
	count, age := int64(0), 0.0
	if len(rows) > 0 {
		count = rows[0].Count
		age = max(0, time.Since(rows[0].Oldest).Seconds())
	}
	metrics.OutboxPending.Set(float64(count))
	metrics.OutboxOldest.Set(age)
	metrics.OutboxSnapshotTime.Set(float64(time.Now().Unix()))
	metrics.OutboxCollectionUp.Set(1)
	return nil
}
