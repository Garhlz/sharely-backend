package svc

import (
	"context"
	"log"

	"notification-rpc/internal/config"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type ServiceContext struct {
	Config config.Config
	// Mongo 指向通知所在的业务库。
	// 当前 notification-rpc 还没有引入缓存层，读写都直接命中这个集合。
	Mongo *mongo.Database
}

func NewServiceContext(c config.Config) *ServiceContext {
	var mongoDb *mongo.Database
	if c.Mongo.Uri != "" {
		// 通知服务当前的数据主存储就是 MongoDB。
		// 如果连接失败，直接 panic，让容器尽快失败重启，而不是带着半残状态运行。
		client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(c.Mongo.Uri))
		if err != nil {
			panic("failed to connect MongoDB: " + err.Error())
		}
		dbName := c.Mongo.Database
		if dbName == "" {
			dbName = "rednote"
		}
		mongoDb = client.Database(dbName)
		if c.RabbitMQ.DataSource != "" {
			if c.Mysql.DataSource == "" {
				panic("Mysql.DataSource is required for interaction notifications")
			}
			var hello struct {
				SetName string `bson:"setName"`
			}
			if err := mongoDb.RunCommand(context.Background(), bson.M{"hello": 1}).Decode(&hello); err != nil {
				panic(err)
			}
			if hello.SetName == "" {
				panic("interaction notifications require a MongoDB replica set")
			}
		}
		ensureNotificationIndexes(mongoDb, c.RabbitMQ.DataSource != "")
	}

	return &ServiceContext{
		Config: c,
		Mongo:  mongoDb,
	}
}

func ensureNotificationIndexes(db *mongo.Database, requireUnique bool) {
	ctx := context.Background()
	_, err := db.Collection("notifications").Indexes().CreateMany(ctx, []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "receiverId", Value: 1}, {Key: "isRead", Value: 1}, {Key: "createdAt", Value: -1}},
			Options: options.Index().SetName("idx_notifications_receiver_read_created"),
		},
		{
			Keys:    bson.D{{Key: "receiverId", Value: 1}, {Key: "createdAt", Value: -1}},
			Options: options.Index().SetName("idx_notifications_receiver_created"),
		},
	})
	if err != nil {
		panic("failed to create notification indexes: " + err.Error())
	}

	_, err = db.Collection("notifications").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "receiverId", Value: 1}, {Key: "senderId", Value: 1}, {Key: "type", Value: 1}, {Key: "targetId", Value: 1}},
		// 只有“状态型通知”才做唯一约束：
		// 点赞、收藏、评分、关注这些通知会走 upsert；
		// 评论、回复、系统通知需要保留完整历史，不能被这个唯一索引卡住。
		Options: options.Index().
			SetName("uk_notifications_state_key").
			SetUnique(true).
			SetPartialFilterExpression(bson.M{
				"type": bson.M{
					"$in": []string{
						"LIKE_POST",
						"COLLECT_POST",
						"RATE_POST",
						"LIKE_COMMENT",
						"FOLLOW",
					},
				},
			}),
	})
	if err != nil {
		// 老环境里如果已经存在重复的状态型通知数据，唯一索引会创建失败。
		if requireUnique {
			panic("interaction notifications require uk_notifications_state_key: " + err.Error())
		}
		// 这里记录告警但不阻断服务启动，让读写接口和清洗接口仍然可用。
		log.Printf("warning: failed to create notification unique index uk_notifications_state_key: %v", err)
	}
}
