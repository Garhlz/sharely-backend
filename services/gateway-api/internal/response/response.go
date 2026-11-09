package response

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"gateway-api/internal/metrics"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Envelope struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data"`
}

type AppError struct {
	Code     int
	Message  string
	HttpCode int
}

func (e *AppError) Error() string {
	return e.Message
}

func Ok(data any) Envelope {
	return Envelope{
		Code:    200,
		Message: "操作成功",
		Data:    data,
	}
}

func NewError(code int, message string, httpCode int) *AppError {
	return &AppError{
		Code:     code,
		Message:  message,
		HttpCode: httpCode,
	}
}

func Unauthorized() *AppError {
	return NewError(40101, "请先登录", http.StatusUnauthorized)
}

func TokenExpired() *AppError {
	return NewError(40102, "登录已过期，请重新登录", http.StatusUnauthorized)
}

func ErrorHandler(_ context.Context, err error) (httpCode int, body any) {
	defer func() {
		if err != nil {
			if envelope, ok := body.(Envelope); ok {
				metrics.ObserveResponseError(envelope.Code)
			}
		}
	}()
	if err == nil {
		return http.StatusOK, Ok(nil)
	}

	var appErr *AppError
	if errors.As(err, &appErr) {
		return appErr.HttpCode, Envelope{Code: appErr.Code, Message: appErr.Message, Data: nil}
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		httpCode, code, msg := mapGrpcError(status.FromContextError(err))
		return httpCode, Envelope{Code: code, Message: msg, Data: nil}
	}

	if grpcStatus, ok := status.FromError(err); ok {
		httpCode, code, msg := mapGrpcError(grpcStatus)
		return httpCode, Envelope{Code: code, Message: msg, Data: nil}
	}

	message := err.Error()
	if message == "" {
		message = "服务器开小差了"
	}
	// httpx.Parse 的参数解析错误也走此入口；保留 400，不将非结构化错误一律视为内部故障。
	return http.StatusBadRequest, Envelope{Code: 40001, Message: message, Data: nil}
}

func mapGrpcError(st *status.Status) (int, int, string) {
	msg := st.Message()
	low := strings.ToLower(msg)

	switch st.Code() {
	case codes.InvalidArgument:
		return http.StatusBadRequest, 40001, pickMessage(msg, "请求参数错误")
	case codes.NotFound:
		if strings.Contains(low, "user") || strings.Contains(low, "email") {
			return http.StatusNotFound, 40401, pickMessage(msg, "该账号未注册")
		}
		return http.StatusNotFound, 40402, pickMessage(msg, "资源不存在")
	case codes.AlreadyExists:
		return http.StatusConflict, 40901, pickMessage(msg, "该邮箱已被其他账号绑定")
	case codes.FailedPrecondition:
		return http.StatusConflict, 40902, pickMessage(msg, "已设置过密码，请调用修改密码接口")
	case codes.PermissionDenied:
		if strings.Contains(low, "permission") || strings.Contains(low, "forbidden") {
			return http.StatusForbidden, 40302, pickMessage(msg, "无权限访问")
		}
		return http.StatusForbidden, 40301, pickMessage(msg, "账号已被禁用")
	case codes.Unauthenticated:
		return http.StatusUnauthorized, 40102, pickMessage(msg, "登录已过期，请重新登录")
	case codes.ResourceExhausted:
		return http.StatusTooManyRequests, 42901, pickMessage(msg, "操作太频繁，请稍后再试")
	case codes.Unavailable:
		return http.StatusBadGateway, 50005, pickMessage(msg, "服务器错误")
	case codes.DeadlineExceeded:
		return http.StatusGatewayTimeout, 50004, pickMessage(msg, "下游服务超时")
	case codes.Canceled:
		return http.StatusRequestTimeout, 40001, pickMessage(msg, "请求已取消")
	case codes.Internal:
		if strings.Contains(low, "mail") || strings.Contains(low, "email") {
			return http.StatusInternalServerError, 50002, pickMessage(msg, "邮件服务异常")
		}
		return http.StatusInternalServerError, 50001, pickMessage(msg, "服务器开小差了")
	default:
		return http.StatusInternalServerError, 50001, pickMessage(msg, "服务器开小差了")
	}
}

func pickMessage(actual, fallback string) string {
	if strings.TrimSpace(actual) == "" {
		return fallback
	}
	return actual
}
