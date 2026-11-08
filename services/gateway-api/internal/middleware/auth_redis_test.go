package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/golang-jwt/jwt/v5"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestAuthMiddlewareRedisTokenVersion(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	cfg := defaultAuthConfig()
	cfg.Jwt.CheckTokenVersion = true
	token := buildToken(t, jwt.MapClaims{"userId": 42, "type": "access", "tokenVersion": 3, "exp": time.Now().Add(time.Hour).Unix()})
	for _, tc := range []struct {
		name, path, version string
		status              int
		user                int64
	}{
		{"matching version", "/api/user/profile", "3", 200, 42},
		{"revoked version", "/api/user/profile", "4", 401, 0},
		{"missing version", "/api/user/profile", "", 401, 0},
		{"public anonymous fallback", "/api/post/list", "", 200, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server.Del("auth:token:version:42")
			if tc.version != "" {
				require.NoError(t, client.Set(context.Background(), "auth:token:version:42", tc.version, 0).Err())
			}
			var user int64
			h := NewAuthMiddleware(cfg, client)(func(w http.ResponseWriter, r *http.Request) {
				user, _ = r.Context().Value("userId").(int64)
				w.WriteHeader(200)
			})
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			req.Header.Set("Authorization", "Bearer "+token)
			rec := httptest.NewRecorder()
			h(rec, req)
			require.Equal(t, tc.status, rec.Code)
			require.Equal(t, tc.user, user)
		})
	}
}
