package logic

import (
	"context"
	"time"

	appmetrics "user-rpc/internal/metrics"
	"user-rpc/internal/model"
	"user-rpc/internal/svc"
	"user-rpc/user"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type RefreshTokenLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewRefreshTokenLogic(ctx context.Context, svcCtx *svc.ServiceContext) *RefreshTokenLogic {
	return &RefreshTokenLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *RefreshTokenLogic) RefreshToken(in *user.RefreshTokenRequest) (*user.AuthResponse, error) {
	start := time.Now()
	metricResult := ""
	defer func() {
		if metricResult != "" {
			appmetrics.ObserveAuth("refresh", metricResult, time.Since(start))
		}
	}()
	if in.GetRefreshToken() == "" {
		metricResult = "invalid_argument"
		return nil, status.Error(codes.InvalidArgument, "refresh token required")
	}

	// 签名合法还不够：续期入口只接受带 JTI 的 Refresh Token。
	claims, err := parseToken(l.svcCtx.Config, in.GetRefreshToken())
	if err != nil {
		metricResult = "invalid_token"
		return nil, status.Error(codes.Unauthenticated, "invalid refresh token")
	}
	if claims.Type != "refresh" {
		metricResult = "invalid_type"
		return nil, status.Error(codes.InvalidArgument, "invalid token type")
	}
	if claims.ID == "" {
		metricResult = "missing_jti"
		return nil, status.Error(codes.Unauthenticated, "invalid refresh token")
	}

	// 黑名单支持在 Token 自然过期前主动撤销。
	blocked, err := l.svcCtx.Redis.ExistsCtx(l.ctx, blockedTokenKey(in.GetRefreshToken()))
	if err != nil {
		metricResult = "blocked_check_error"
		return nil, status.Error(codes.Internal, "refresh token check failed")
	}
	if blocked {
		metricResult = "blocked"
		return nil, status.Error(codes.Unauthenticated, "refresh token revoked")
	}

	// 数据库中的 tokenVersion 是账号级版本；登出或改密后旧版本 Token 全部失效。
	u, err := l.svcCtx.Users.FindOne(l.ctx, claims.UserId)
	if err == model.ErrNotFound {
		metricResult = "user_not_found"
		return nil, status.Error(codes.NotFound, "user not found")
	}
	if err != nil {
		metricResult = "query_error"
		return nil, status.Error(codes.Internal, "query user failed")
	}
	if u.Status != 1 {
		metricResult = "account_disabled"
		return nil, status.Error(codes.PermissionDenied, "account disabled")
	}
	if u.TokenVersion != claims.TokenVersion {
		metricResult = "token_revoked"
		return nil, status.Error(codes.Unauthenticated, "token revoked")
	}

	// JTI 对应 Redis 中的一次登录会话，不存在说明该 Refresh Token 已被消费或撤销。
	ok, err := l.svcCtx.Redis.ExistsCtx(l.ctx, refreshTokenKey(claims.UserId, claims.ID))
	if err != nil {
		metricResult = "refresh_check_error"
		return nil, status.Error(codes.Internal, "refresh token check failed")
	}
	if !ok {
		metricResult = "refresh_revoked"
		return nil, status.Error(codes.Unauthenticated, "refresh token revoked")
	}

	// 采用 Refresh Token Rotation：签发新双 Token 后删除旧 JTI。
	tokens, refreshJti, err := buildTokenPair(l.svcCtx.Config, u)
	if err != nil {
		metricResult = "token_error"
		return nil, status.Error(codes.Internal, "create token failed")
	}
	if err := l.svcCtx.Redis.Setex(
		refreshTokenKey(u.Id, refreshJti),
		"1",
		int(l.svcCtx.Config.Jwt.RefreshExpireSeconds),
	); err != nil {
		metricResult = "store_refresh_error"
		return nil, status.Error(codes.Internal, "store refresh token failed")
	}
	if err := setTokenVersion(l.ctx, l.svcCtx, u.Id, u.TokenVersion); err != nil {
		metricResult = "store_version_error"
		return nil, status.Error(codes.Internal, "store token version failed")
	}
	_, _ = l.svcCtx.Redis.DelCtx(l.ctx, refreshTokenKey(claims.UserId, claims.ID))
	metricResult = "success"

	return &user.AuthResponse{
		Tokens: tokens,
		User:   buildUserSummary(u),
	}, nil
}
