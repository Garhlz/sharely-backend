package service

import (
	"context"
	"log"

	"sync-sidecar/internal/config"

	elasticsearch "github.com/elastic/go-elasticsearch/v8"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Infra struct {
	Mongo         *mongo.Client
	ES            *elasticsearch.Client
	RMQ           *amqp.Connection
	Cfg           *config.AppConfig
	consumerCtx   context.Context
	stopConsumers context.CancelFunc
}

func InitInfra(cfg *config.AppConfig) *Infra {
	// 1. Mongo
	mOpts := options.Client().ApplyURI(cfg.MongoURI).SetMaxPoolSize(50)
	mongoClient, err := mongo.Connect(context.Background(), mOpts)
	if err != nil {
		log.Fatal("Mongo Connect Error:", err)
	}

	// 2. ES
	esClient, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{cfg.ESAddress},
	})
	if err != nil {
		log.Fatal("ES Connect Error:", err)
	}

	// 3. RabbitMQ
	rmqConn, err := amqp.Dial(cfg.RabbitMQURL)
	if err != nil {
		log.Fatal("RabbitMQ Connect Error:", err)
	}

	consumerCtx, stopConsumers := context.WithCancel(context.Background())
	return &Infra{
		Mongo:         mongoClient,
		ES:            esClient,
		RMQ:           rmqConn,
		Cfg:           cfg,
		consumerCtx:   consumerCtx,
		stopConsumers: stopConsumers,
	}
}

func (i *Infra) Close() {
	if i.stopConsumers != nil {
		i.stopConsumers()
	}
	if i.Mongo != nil {
		i.Mongo.Disconnect(context.Background())
	}
	if i.RMQ != nil {
		i.RMQ.Close()
	}
}
