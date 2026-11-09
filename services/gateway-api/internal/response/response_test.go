package response

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestOk(t *testing.T) {
	got := Ok(map[string]string{"id": "1"})
	assert.Equal(t, 200, got.Code)
	assert.Equal(t, "操作成功", got.Message)
	assert.Equal(t, map[string]string{"id": "1"}, got.Data)
}

func TestErrorHandler_AppError(t *testing.T) {
	httpCode, body := ErrorHandler(nil, NewError(40901, "邮箱冲突", http.StatusConflict))
	assert.Equal(t, http.StatusConflict, httpCode)
	envelope, ok := body.(Envelope)
	require.True(t, ok)
	assert.Equal(t, 40901, envelope.Code)
	assert.Nil(t, envelope.Data)
}

func TestErrorHandler_GrpcMappings(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		httpCode int
		business int
	}{
		{"invalid argument", status.Error(codes.InvalidArgument, "bad input"), 400, 40001},
		{"user not found", status.Error(codes.NotFound, "user not found"), 404, 40401},
		{"resource not found", status.Error(codes.NotFound, "post not found"), 404, 40402},
		{"already exists", status.Error(codes.AlreadyExists, "exists"), 409, 40901},
		{"permission denied", status.Error(codes.PermissionDenied, "permission denied"), 403, 40302},
		{"account disabled", status.Error(codes.PermissionDenied, "account disabled"), 403, 40301},
		{"unauthenticated", status.Error(codes.Unauthenticated, "expired"), 401, 40102},
		{"rate limited", status.Error(codes.ResourceExhausted, "slow down"), 429, 42901},
		{"mail failure", status.Error(codes.Internal, "mail send failed"), 500, 50002},
		{"unavailable", status.Error(codes.Unavailable, "rpc unavailable"), 502, 50005},
		{"deadline", status.Error(codes.DeadlineExceeded, "timeout"), 504, 50004},
		{"canceled", status.Error(codes.Canceled, "canceled"), 408, 40001},
		{"unknown grpc", status.Error(codes.Unknown, "failure"), 500, 50001},
		{"context deadline", context.DeadlineExceeded, 504, 50004},
		{"context canceled", context.Canceled, 408, 40001},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			httpCode, body := ErrorHandler(nil, tt.err)
			assert.Equal(t, tt.httpCode, httpCode)
			envelope := body.(Envelope)
			assert.Equal(t, tt.business, envelope.Code)
		})
	}
}

func TestErrorHandler_UnknownError(t *testing.T) {
	httpCode, body := ErrorHandler(nil, errors.New("boom"))
	assert.Equal(t, http.StatusBadRequest, httpCode)
	assert.Equal(t, 40001, body.(Envelope).Code)
	assert.Equal(t, "boom", body.(Envelope).Message)
}
