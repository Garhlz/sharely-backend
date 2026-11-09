package metrics

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/zeromicro/go-zero/core/logx"
)

// 缓存回填与空值占位指标；持久化事件发布由 outbox.go 记录。
var (
	// 缓存预热次数，result 用来区分命中 Redis、回源 Mongo、写 Redis 出错等不同结果。
	cacheWarmTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "interaction_cache_warm_total",
			Help: "Total number of cache warmup attempts in interaction-rpc.",
		},
		[]string{"kind", "result"},
	)
	cacheWarmDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "interaction_cache_warm_duration_seconds",
			Help:    "Duration of cache warmup in interaction-rpc.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"kind"},
	)
	dummyOpsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "interaction_dummy_ops_total",
			Help: "Total number of dummy placeholder operations in interaction-rpc.",
		},
		[]string{"kind", "action"},
	)
)

func init() {
	prometheus.MustRegister(
		cacheWarmTotal,
		cacheWarmDuration,
		dummyOpsTotal,
	)
}

func StartServer(host string, port int, path string) {
	if port <= 0 {
		return
	}
	if strings.TrimSpace(path) == "" {
		path = "/metrics"
	}
	addr := fmt.Sprintf("%s:%d", host, port)
	go func() {
		// 和 gateway 一样，interaction-rpc 也单独暴露 metrics 服务，
		// 避免和 zRPC 主服务端口耦合。
		mux := http.NewServeMux()
		mux.Handle(path, promhttp.Handler())
		logx.Infof("starting interaction metrics server at %s%s", addr, path)
		if err := http.ListenAndServe(addr, mux); err != nil {
			logx.Errorf("interaction metrics server stopped: %v", err)
		}
	}()
}

func ObserveCacheWarm(kind, result string, duration time.Duration) {
	// 一次预热既记次数，也记耗时。
	cacheWarmTotal.WithLabelValues(kind, result).Inc()
	cacheWarmDuration.WithLabelValues(kind).Observe(duration.Seconds())
}

func IncDummyOp(kind, action string) {
	dummyOpsTotal.WithLabelValues(kind, action).Inc()
}
