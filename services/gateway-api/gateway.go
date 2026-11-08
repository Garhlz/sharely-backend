// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"gateway-api/internal/config"
	"gateway-api/internal/handler"
	appmetrics "gateway-api/internal/metrics"
	"gateway-api/internal/middleware"
	"gateway-api/internal/response"
	"gateway-api/internal/svc"
	"gateway-api/internal/telemetry"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/rest/httpx"
)

var configFile = flag.String("f", "etc/gateway.yaml", "the config file")

func main() {
	flag.Parse()

	var c config.Config
	// 启动时读取网关配置，并允许环境变量覆盖。
	// 这样在 Docker 内外切换 Java 地址、Redis 地址、RPC 地址时，不必反复改源码。
	conf.MustLoad(*configFile, &c, conf.UseEnv())

	shutdownTelemetry, err := telemetry.InitProvider(context.Background(), "gateway-api")
	if err != nil {
		panic(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTelemetry(ctx)
	}()

	// 创建 go-zero 的 HTTP Server。
	// gateway-api 本身就是整个系统对外暴露的统一入口。
	server := rest.MustNewServer(c.RestConf)
	defer server.Stop()
	logx.AddGlobalFields(logx.Field("service", "gateway-api"))

	// 统一成功响应包装。
	// 业务 logic 返回的 data 会在这里被包成 {code, message, data} 风格的统一响应。
	httpx.SetOkHandler(func(_ context.Context, data any) any {
		return response.Ok(data)
	})
	// 统一错误响应包装。
	// 这样各个 logic / middleware 不需要自己关心最终的 JSON 错误结构。
	httpx.SetErrorHandlerCtx(response.ErrorHandler)

	// ServiceContext 是网关的依赖注入中心。
	// 里面统一初始化：
	// 1. user/search/interaction 三个 RPC client
	// 2. Redis
	// 3. Java 代理用的 HTTP Client
	ctx := svc.NewServiceContext(c)
	defer ctx.Redis.Close()
	// metrics server 独立监听一个专门端口，只对 Prometheus 暴露 /metrics。
	// 这样不会和业务接口、鉴权逻辑混在一起。
	appmetrics.StartServer(c.Metrics.Host, c.Metrics.Port, c.Metrics.Path)

	// 所有请求先经过鉴权中间件：
	// 1. 判断是否跳过鉴权
	// 2. 解析 JWT
	// 3. 校验 token version
	// 4. 把 userId / role / nickname 等信息放进 context
	// metrics middleware 放在比较外层，这样能看到整个网关处理链路的最终耗时。
	server.Use(middleware.NewTracingMiddleware("gateway-api").Handle)
	server.Use(middleware.NewRequestIdMiddleware().Handle)
	server.Use(middleware.NewMetricsMiddleware())
	server.Use(middleware.NewAuthMiddleware(c, ctx.Redis))

	// 注册所有路由。
	// 路由层只决定“这个请求由谁处理”：
	// 1. 纯代理到 Java
	// 2. 直接调用 Go RPC
	// 3. 由网关自己做聚合编排
	handler.RegisterHandlers(server, ctx)

	fmt.Printf("Starting server at %s:%d...\n", c.Host, c.Port)
	server.Start()
}
