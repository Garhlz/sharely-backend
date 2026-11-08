package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"user-rpc/internal/config"
	appmetrics "user-rpc/internal/metrics"
	"user-rpc/internal/server"
	"user-rpc/internal/svc"
	"user-rpc/internal/trace"
	"user-rpc/user"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var configFile = flag.String("f", "etc/user.yaml", "the config file")

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c, conf.UseEnv())
	shutdownTelemetry, err := trace.InitProvider(context.Background(), "user-rpc")
	if err != nil {
		panic(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTelemetry(ctx)
	}()
	logx.AddGlobalFields(logx.Field("service", "user-rpc"))
	ctx := svc.NewServiceContext(c)
	defer ctx.Redis.Close()
	appmetrics.StartServer(c.Metrics.Host, c.Metrics.Port, c.Metrics.Path)

	s := zrpc.MustNewServer(c.RpcServerConf, func(grpcServer *grpc.Server) {
		user.RegisterUserServiceServer(grpcServer, server.NewUserServiceServer(ctx))

		if c.Mode == service.DevMode || c.Mode == service.TestMode {
			reflection.Register(grpcServer)
		}
	})
	defer s.Stop()
	s.AddUnaryInterceptors(trace.UnaryServerInterceptor("user-rpc"))

	fmt.Printf("Starting rpc server at %s...\n", c.ListenOn)
	s.Start()
}
