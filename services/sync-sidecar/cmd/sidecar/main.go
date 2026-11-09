package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"sync-sidecar/internal/config"
	"sync-sidecar/internal/handler"
	"sync-sidecar/internal/obslog"
	"sync-sidecar/internal/service"
	"sync-sidecar/internal/telemetry"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	// 1. 加载配置
	cfg := config.LoadConfig()
	obslog.Infof("sidecar starting")
	shutdownTelemetry, err := telemetry.InitProvider(context.Background(), "sync-sidecar")
	if err != nil {
		panic(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTelemetry(ctx)
	}()

	// 2. 初始化基础设施 (Infra)
	infra := service.InitInfra(cfg)
	defer infra.Close()

	// 3. 初始化 Handlers
	logHandler := &handler.LogHandler{Infra: infra}
	syncHandler := &handler.SyncHandler{Infra: infra}
	userHandler := &handler.UserHandler{Infra: infra}

	// 3.1 启动管理接口。
	// 目前主要提供手动触发 Mongo -> ES 全量回填，用于历史数据补索引。
	adminMux := http.NewServeMux()
	adminMux.HandleFunc("/admin/reindex/posts", syncHandler.HandleReindexPosts)
	adminServer := &http.Server{
		Addr:    cfg.AdminHost + ":" + cfg.AdminPort,
		Handler: telemetry.HTTPMiddleware("sync-sidecar", adminMux),
	}
	go func() {
		obslog.Infof("admin http listening addr=%s", adminServer.Addr)
		if err := adminServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			obslog.Errorf("admin server error err=%v", err)
			panic(err)
		}
	}()

	// 4. 启动多队列消费者
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	metricsServer := &http.Server{Addr: cfg.MetricsAddr, Handler: metricsMux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			panic(err)
		}
	}()
	var wg sync.WaitGroup

	// Group 1: 日志
	infra.StartConsumerGroup(&wg, cfg.QueueLog, logHandler.Handle)

	// Group 2: 搜索历史

	// Group 3: ES 同步 (只负责 ES)
	infra.StartConsumerGroup(&wg, cfg.QueueEsSync, syncHandler.Handle)

	// Group 4: 用户数据同步 (只负责 Mongo) 【新增】
	// 监听 platform.user.queue
	infra.StartConsumerGroup(&wg, cfg.QueueUser, userHandler.Handle)

	// 5. 优雅停机
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	obslog.Infof("sidecar shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := adminServer.Shutdown(shutdownCtx); err != nil {
		obslog.Errorf("admin server shutdown error err=%v", err)
	}
	_ = metricsServer.Shutdown(shutdownCtx)
	// 注意：rabbitmq connection 关闭会导致 channel 关闭，从而让 worker loop 退出
	infra.Close()
	wg.Wait()
	obslog.Infof("sidecar stopped gracefully")
}
