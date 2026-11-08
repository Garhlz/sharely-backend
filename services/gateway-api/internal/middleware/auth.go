package middleware

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"gateway-api/internal/config"
	"gateway-api/internal/response"

	"github.com/golang-jwt/jwt/v5"
	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/rest/httpx"
)

type authClaims struct {
	UserId       int64  `json:"userId"`
	Role         string `json:"role,omitempty"`
	Nickname     string `json:"nickname,omitempty"`
	Type         string `json:"type"`
	TokenVersion int64  `json:"tokenVersion"`
	jwt.RegisteredClaims
}

const tokenVersionKeyPrefix = "auth:token:version:"

func NewAuthMiddleware(cfg config.Config, redisClient redis.UniversalClient) rest.Middleware {
	secret := []byte(cfg.Jwt.Secret)
	patterns := cfg.Jwt.IgnorePatterns
	// 如果开启了 token version 校验，网关会去 Redis 读取当前用户的 tokenVersion，
	// 从而让 JWT 具备“可即时失效”的能力。
	checkTokenVersion := cfg.Jwt.CheckTokenVersion && redisClient != nil
	publicPaths := map[string]struct{}{
		"/api/post/list":           {},
		"/api/post/search":         {},
		"/api/post/search/suggest": {},
		"/api/tag/hot":             {},
		"/api/comment/list":        {},
		"/api/comment/sub-list":    {},
	}

	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			// 第一类路径：公开路径或配置里声明跳过鉴权的路径。
			// 这类路径允许匿名访问，但如果请求里带了 access token，网关仍会尽量解析它，
			// 这样列表页/搜索页也能拿到“当前用户是否点赞/收藏”这类弱登录态信息。
			if r.URL.Path != "/api/user/profile" && (shouldSkipAuth(r.URL.Path, patterns) || isPublicPath(r.URL.Path, publicPaths)) {
				if r.URL.Path == "/api/auth/logout" {
					// 登出接口比较特殊：即便本身允许跳过严格鉴权，也要把 access/refresh token 透传下去，
					// 让 user-rpc 有机会做 token 拉黑和 refresh key 删除。
					ctx := r.Context()
					if token := accessTokenFromRequest(r); token != "" {
						ctx = context.WithValue(ctx, "accessToken", token)
					}
					if rt := refreshTokenFromRequest(r); rt != "" {
						ctx = context.WithValue(ctx, "refreshToken", rt)
					}
					next.ServeHTTP(w, r.WithContext(ctx))
					return
				}
				ctx := r.Context()
				token := accessTokenFromRequest(r)
				if token != "" {
					// 公开接口上的“弱鉴权”：
					// token 合法就把用户上下文注入进去，后续 logic 就可以做个性化聚合；
					// token 不合法则直接忽略，继续按匿名访问处理。
					claims := &authClaims{}
					_, err := jwt.ParseWithClaims(token, claims, func(t *jwt.Token) (any, error) {
						return secret, nil
					}, jwt.WithLeeway(5*time.Second))
					if err == nil && claims.UserId > 0 && claims.Type == "access" {
						if !checkTokenVersion || tokenVersionOk(r.Context(), redisClient, claims) {
							ctx = context.WithValue(ctx, "userId", claims.UserId)
							ctx = context.WithValue(ctx, "role", claims.Role)
							ctx = context.WithValue(ctx, "nickname", claims.Nickname)
							ctx = context.WithValue(ctx, "accessToken", token)
						}
					}
				}
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}

			// 第二类路径：必须登录后才能访问。
			token := accessTokenFromRequest(r)
			if token == "" {
				httpx.ErrorCtx(r.Context(), w, response.Unauthorized())
				return
			}

			claims := &authClaims{}
			_, err := jwt.ParseWithClaims(token, claims, func(t *jwt.Token) (any, error) {
				return secret, nil
			}, jwt.WithLeeway(5*time.Second))
			if err != nil || claims.UserId == 0 || claims.Type != "access" {
				httpx.ErrorCtx(r.Context(), w, response.TokenExpired())
				return
			}
			if checkTokenVersion {
				// 这里是“JWT + Redis tokenVersion”组合方案的关键：
				// 即便 token 签名正确，只要 Redis 里的最新版本号已经变化，旧 token 也会立即失效。
				key := fmt.Sprintf("%s%d", tokenVersionKeyPrefix, claims.UserId)
				value, err := redisClient.Get(r.Context(), key).Result()
				if err != nil {
					httpx.ErrorCtx(r.Context(), w, response.TokenExpired())
					return
				}
				if value != fmt.Sprintf("%d", claims.TokenVersion) {
					httpx.ErrorCtx(r.Context(), w, response.TokenExpired())
					return
				}
			}

			ctx := context.WithValue(r.Context(), "userId", claims.UserId)
			ctx = context.WithValue(ctx, "role", claims.Role)
			ctx = context.WithValue(ctx, "nickname", claims.Nickname)
			// accessToken / refreshToken 也放进 context，便于后续 Java 代理或 user-rpc 调用时透传。
			ctx = context.WithValue(ctx, "accessToken", token)
			if rt := refreshTokenFromRequest(r); rt != "" {
				ctx = context.WithValue(ctx, "refreshToken", rt)
			}

			next.ServeHTTP(w, r.WithContext(ctx))
		}
	}
}

// extractBearer 把 "Bearer xxx" 形式的 Authorization 头还原成纯 token。
func extractBearer(value string) string {
	if value == "" {
		return ""
	}
	if strings.HasPrefix(value, "Bearer ") {
		return strings.TrimSpace(strings.TrimPrefix(value, "Bearer "))
	}
	return ""
}

// accessTokenFromRequest 先取 Authorization 头，再兜底读取 accessToken Cookie。
// 这样前端如果采用 cookie 存 token，公开路径上的弱鉴权和强鉴权路径都能拿到一致登录态。
func accessTokenFromRequest(r *http.Request) string {
	if r == nil {
		return ""
	}
	if token := extractBearer(r.Header.Get("Authorization")); token != "" {
		return token
	}
	if cookie, err := r.Cookie("accessToken"); err == nil {
		return cookie.Value
	}
	return ""
}

// refreshTokenFromRequest 先取显式 header，再兜底读取 refreshToken Cookie。
func refreshTokenFromRequest(r *http.Request) string {
	if r == nil {
		return ""
	}
	if token := extractBearer(r.Header.Get("X-Refresh-Token")); token != "" {
		return token
	}
	if cookie, err := r.Cookie("refreshToken"); err == nil {
		return cookie.Value
	}
	return ""
}

// tokenVersionOk 仅用于公开路径上的弱校验。
// 如果这里返回 false，网关不会报错，而是把请求当成匿名访问。
func tokenVersionOk(ctx context.Context, redisClient redis.UniversalClient, claims *authClaims) bool {
	if redisClient == nil || claims == nil {
		return false
	}
	key := fmt.Sprintf("%s%d", tokenVersionKeyPrefix, claims.UserId)
	value, err := redisClient.Get(ctx, key).Result()
	if err != nil {
		return false
	}
	return value == fmt.Sprintf("%d", claims.TokenVersion)
}

// shouldSkipAuth / isPublicPath / matchPattern 共同决定“这条路径是否需要强制鉴权”。
func shouldSkipAuth(path string, patterns []string) bool {
	for _, pattern := range patterns {
		if matchPattern(path, pattern) {
			return true
		}
	}
	return false
}

func isPublicPath(path string, publicPaths map[string]struct{}) bool {
	if _, ok := publicPaths[path]; ok {
		return true
	}
	return false
}

func matchPattern(path, pattern string) bool {
	if pattern == "" {
		return false
	}
	if pattern == path {
		return true
	}
	if !strings.Contains(pattern, "*") {
		return false
	}
	parts := strings.Split(pattern, "*")
	if len(parts) == 2 {
		return strings.HasPrefix(path, parts[0]) && strings.HasSuffix(path, parts[1])
	}
	prefix := parts[0]
	return strings.HasPrefix(path, prefix)
}
