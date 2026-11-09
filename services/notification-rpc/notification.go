package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"notification-rpc/internal/config"
	"notification-rpc/internal/consumer"
	appmetrics "notification-rpc/internal/metrics"
	"notification-rpc/internal/server"
	"notification-rpc/internal/svc"
	"notification-rpc/internal/trace"
	"notification-rpc/notification"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var configFile = flag.String("f", "etc/notification.yaml", "the config file")

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c, conf.UseEnv())
	shutdownTelemetry, err := trace.InitProvider(context.Background(), "notification-rpc")
	if err != nil {
		panic(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTelemetry(ctx)
	}()
	logx.AddGlobalFields(logx.Field("service", "notification-rpc"))

	// ServiceContext 负责初始化 notification-rpc 运行时依赖。
	// 当前这个服务的核心依赖比较简单，主要就是 MongoDB。
	ctx := svc.NewServiceContext(c)
	consumerCtx, stopConsumer := context.WithCancel(context.Background())
	defer stopConsumer()
	if c.RabbitMQ.DataSource != "" {
		appmetrics.ConsumerEnabled.Set(1)
		go consumer.Run(consumerCtx, ctx.Mongo, c.RabbitMQ.DataSource, c.Mysql.DataSource)
	}

	// 按现有 Go 服务的约定，notification-rpc 单独起一个 metrics HTTP 服务，
	// 避免和 zRPC 主端口耦合，方便 Prometheus 直接抓取。
	appmetrics.StartServer(c.Metrics.Host, c.Metrics.Port, c.Metrics.Path)

	s := zrpc.MustNewServer(c.RpcServerConf, func(grpcServer *grpc.Server) {
		notification.RegisterNotificationServiceServer(grpcServer, server.NewNotificationServiceServer(ctx))

		if c.Mode == service.DevMode || c.Mode == service.TestMode {
			reflection.Register(grpcServer)
		}
	})
	defer s.Stop()
	s.AddUnaryInterceptors(trace.UnaryServerInterceptor("notification-rpc"))

	fmt.Printf("Starting rpc server at %s...\n", c.ListenOn)
	s.Start()
}
