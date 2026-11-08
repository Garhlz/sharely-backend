package svc

import (
	"context"
	"interaction-rpc/internal/config"
	"interaction-rpc/internal/store"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx" // 用于打印日志
	"github.com/zeromicro/go-zero/core/stores/redis"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// 全称 Service Context。这里面装着所有和第三方组件（Redis、Mongo、MQ）建好的连接
type ServiceContext struct {
	Config config.Config

	// 1. 定义 Redis 客户端变量
	Redis    *redis.Redis
	RawRedis *goredis.Client

	Mongo        *mongo.Database
	Interactions store.Writer
}

func NewServiceContext(c config.Config) *ServiceContext {
	// A. 初始化 BizRedis
	// MustNewRedis 意味着如果连不上(比如配置错了)，程序直接崩溃报错，防止带病运行
	redisClient := redis.MustNewRedis(c.BizRedis)
	addr := strings.TrimSpace(c.BizRedis.Host)
	if addr == "" {
		panic("BizRedis.Host is required")
	}
	rawRedis := goredis.NewClient(&goredis.Options{
		Addr:     addr,
		Password: c.BizRedis.Pass,
		DB:       0,
	})

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
		RawRedis:     rawRedis,
		Mongo:        mongoDb,
		Interactions: writer,
	}
}
