package svc

import (
	"context"
	"net"
	"net/http"
	"os"
	"time"

	"gateway-api/internal/config"
	"gateway-api/internal/pkg/ctxutil"
	"gateway-api/internal/telemetry"

	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/zrpc"
	"go.opentelemetry.io/otel"
	oteltrace "go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type ServiceContext struct {
	Config          config.Config
	UserRpc         zrpc.Client
	SearchRpc       zrpc.Client
	InteractionRpc  zrpc.Client
	NotificationRpc zrpc.Client
	CommentRpc      zrpc.Client
	Redis           redis.UniversalClient
	JavaHttpClient  *http.Client
	// JavaApiBaseUrl 是 Java 主业务服务的入口地址。
	// 当前项目里，很多帖子/评论/通知/后台接口仍然由 Java 负责，网关需要反向代理到这里。
	JavaApiBaseUrl string
}

func NewServiceContext(c config.Config) *ServiceContext {
	// 三个 RPC client 对应网关后面的 Go 微服务。
	// gateway-api 自己不保存业务状态，它主要负责把请求分发到这些后端能力上。
	clientOption := zrpc.WithUnaryClientInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		tracer := otel.Tracer("gateway-api")
		ctx, span := tracer.Start(ctx, method, oteltrace.WithSpanKind(oteltrace.SpanKindClient))
		defer span.End()

		requestID := ctxutil.RequestID(ctx)
		if requestID == "" {
			requestID = ctxutil.TraceID(ctx)
		}
		if requestID == "" {
			requestID = telemetry.TraceID(ctx)
		}
		md, _ := metadata.FromOutgoingContext(ctx)
		md = md.Copy()
		if requestID != "" {
			md.Set("x-request-id", requestID)
			md.Set("x-trace-id", requestID)
		}
		otel.GetTextMapPropagator().Inject(ctx, metadataCarrier(md))
		ctx = metadata.NewOutgoingContext(ctx, md)
		return invoker(ctx, method, req, reply, cc, opts...)
	})

	userClient := zrpc.MustNewClient(c.UserRpc, clientOption)
	searchClient := zrpc.MustNewClient(c.SearchRpc, clientOption)
	interactionClient := zrpc.MustNewClient(c.InteractionRpc, clientOption)
	notificationClient := zrpc.MustNewClient(c.NotificationRpc, clientOption)
	commentClient := zrpc.MustNewClient(c.CommentRpc, clientOption)

	// Java 地址支持三层来源：
	// 1. 配置文件
	// 2. 环境变量
	// 3. 默认回退值
	// 这样能兼容“Java 跑容器内”和“Java 跑宿主机”两种开发模式。
	javaBaseURL := c.JavaApi.BaseUrl
	if javaBaseURL == "" {
		javaBaseURL = os.Getenv("JAVA_API_BASE_URL")
	}
	if javaBaseURL == "" {
		javaBaseURL = "http://host.docker.internal:8080"
	}

	timeout := time.Duration(c.JavaApi.TimeoutMs) * time.Millisecond
	if timeout == 0 {
		timeout = 8 * time.Second
	}
	// Java 代理单独维护一个 HTTP Client，统一控制超时、连接池和代理设置。
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ResponseHeaderTimeout: timeout,
		IdleConnTimeout:       90 * time.Second,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   10,
	}

	return &ServiceContext{
		Config:          c,
		UserRpc:         userClient,
		SearchRpc:       searchClient,
		InteractionRpc:  interactionClient,
		NotificationRpc: notificationClient,
		CommentRpc:      commentClient,
		Redis:           config.NewRedisClient(c.Redis),
		JavaHttpClient:  &http.Client{Timeout: timeout, Transport: transport},
		JavaApiBaseUrl:  javaBaseURL,
	}
}

type metadataCarrier metadata.MD

func (c metadataCarrier) Get(key string) string {
	values := metadata.MD(c).Get(key)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func (c metadataCarrier) Set(key, value string) {
	metadata.MD(c).Set(key, value)
}

func (c metadataCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range metadata.MD(c) {
		keys = append(keys, k)
	}
	return keys
}
