package middleware

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"gateway-api/internal/pkg/ctxutil"
	"gateway-api/internal/telemetry"

	"github.com/zeromicro/go-zero/core/logx"
)

const (
	requestIDHeader = "X-Request-Id"
	traceIDHeader   = "X-Trace-Id"
)

// RequestIdMiddleware 统一生成/透传请求标识。
// 旧 X-Trace-Id 响应头保留兼容；日志 traceId 使用真实 OTel 标识，不与 requestId 混用。
type RequestIdMiddleware struct{}

func NewRequestIdMiddleware() *RequestIdMiddleware {
	return &RequestIdMiddleware{}
}

func (m *RequestIdMiddleware) Handle(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		requestID := accessRequestID(r)
		w.Header().Set(requestIDHeader, requestID)
		w.Header().Set(traceIDHeader, requestID)

		ctx := context.WithValue(r.Context(), "requestId", requestID)
		ctx = context.WithValue(ctx, "traceId", requestID)
		ctx = logx.ContextWithFields(ctx,
			logx.Field("requestId", requestID),
			logx.Field("traceId", telemetry.TraceID(ctx)),
		)

		next(w, r.WithContext(ctx))
	}
}

func accessRequestID(r *http.Request) string {
	if requestID := r.Header.Get(requestIDHeader); requestID != "" {
		return requestID
	}
	if traceID := r.Header.Get(traceIDHeader); traceID != "" {
		return traceID
	}
	if traceID := telemetry.TraceID(r.Context()); traceID != "" {
		return traceID
	}
	if requestID := ctxutil.RequestID(r.Context()); requestID != "" {
		return requestID
	}

	return strconv.FormatInt(time.Now().UnixNano(), 36)
}
