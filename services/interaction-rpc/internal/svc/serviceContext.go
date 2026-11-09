package svc

import (
	"context"
	"interaction-rpc/internal/config"
	"interaction-rpc/internal/store"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx" // 用于打印日志
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// 装配 Redis 读缓存与 Mongo 事务写入；MQ 连接由后台 Outbox 发布器管理。
type ServiceContext struct {
	Config config.Config

	// 1. 定义 Redis 客户端变量
	Redis goredis.UniversalClient

	Mongo        *mongo.Database
	Interactions store.Writer
}

func NewServiceContext(c config.Config) *ServiceContext {
	redisClient := config.NewRedisClient(c.BizRedis)

	var mongoDb *mongo.Database
	if c.Mongo.Uri != "" {
		client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(c.Mongo.Uri))
		if err != nil {
			panic("Failed to connect MongoDB: " + err.Error())
		}
		dbName := c.Mongo.Database
		if dbName == "" {
			dbName = "rednote"
		}
		mongoDb = client.Database(dbName)
	}

	if mongoDb == nil {
		panic("Mongo is required for interaction writes")
	}
	writer := &store.MongoStore{DB: mongoDb}
	initCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := writer.Init(initCtx); err != nil {
		panic(err)
	}
	logx.Info("BizRedis and Mongo initialized; MQ is delivered by durable outbox")

	return &ServiceContext{
		Config:       c,
		Redis:        redisClient,
		Mongo:        mongoDb,
		Interactions: writer,
	}
}
