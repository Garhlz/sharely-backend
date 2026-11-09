package config

import (
	"fmt"
	"os"
	"time"
)

type AppConfig struct {
	RabbitMQURL  string
	MongoURI     string
	ESAddress    string
	IndexName    string
	AuditEnable  bool
	WorkerCount  int // 每个队列的并发数
	ReindexBatch int
	TimeLocation *time.Location
	AdminHost    string
	AdminPort    string
	AdminToken   string
	MetricsAddr  string

	// 队列名称配置 (与 Java RabbitConfig 保持一致)
	QueueLog    string
	QueueEsSync string
	QueueUser   string

	ExchangeMain string
	ExchangeDLX  string

	QueueBindings map[string][]string
}

func LoadConfig() *AppConfig {
	loc, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		loc = time.FixedZone("CST", 8*3600)
	}

	return &AppConfig{
		RabbitMQURL:  getEnv("RABBITMQ_URL", "amqp://admin:admin@rabbitmq:5672/"),
		MongoURI:     getEnv("MONGO_URI", "mongodb://mongo:27017"),
		ESAddress:    getEnv("ES_ADDRESS", "http://elasticsearch:9200"),
		IndexName:    "posts",
		AuditEnable:  getEnv("POST_AUDIT_ENABLE", "false") == "true",
		WorkerCount:  20,
		ReindexBatch: getEnvAsInt("REINDEX_BATCH_SIZE", 200),
		TimeLocation: loc,
		AdminHost:    getEnv("ADMIN_HOST", "0.0.0.0"),
		AdminPort:    getEnv("ADMIN_PORT", "8088"),
		AdminToken:   getEnv("ADMIN_TOKEN", "szu123"),
		MetricsAddr:  getEnv("METRICS_ADDR", ":40097"),

		// 队列名硬编码以匹配 Java 配置
		QueueLog:    "platform.log.queue",
		QueueEsSync: "platform.es.sync.queue",
		QueueUser:   "platform.user.queue",

		ExchangeMain: "platform.topic.exchange",
		ExchangeDLX:  "platform.dlx.exchange",

		QueueBindings: map[string][]string{
			"platform.log.queue":     {"log.#"},
			"platform.user.queue":    {"user.#"},
			"platform.es.sync.queue": {"post.#", "user.update", "post.audit.pass"},
		},
	}
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func getEnvAsInt(key string, fallback int) int {
	if value, ok := os.LookupEnv(key); ok {
		var parsed int
		if _, err := fmt.Sscanf(value, "%d", &parsed); err == nil && parsed > 0 {
			return parsed
		}
	}
	return fallback
}
