package logic

import (
	"context"
	"strings"
	"time"

	appmetrics "user-rpc/internal/metrics"
	"user-rpc/internal/model"
	"user-rpc/internal/svc"
	"user-rpc/user"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type LoginLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewLoginLogic(ctx context.Context, svcCtx *svc.ServiceContext) *LoginLogic {
	return &LoginLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *LoginLogic) Login(in *user.LoginRequest) (*user.AuthResponse, error) {
	start := time.Now()
	metricResult := ""
	defer func() {
		if metricResult != "" {
			appmetrics.ObserveAuth("login", metricResult, time.Since(start))
		}
	}()
	// 先做无副作用的输入校验，避免无效请求访问数据库。
	email := strings.TrimSpace(in.GetEmail())
	password := in.GetPassword()
	if email == "" || password == "" {
		metricResult = "invalid_argument"
		return nil, status.Error(codes.InvalidArgument, "email and password are required")
	}

	// MySQL 保存账号事实数据：依次校验用户是否存在、账号状态和密码哈希。
	u, err := l.svcCtx.Users.FindOneByEmail(l.ctx, email)
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
	if !checkPassword(u.Password, password) {
		metricResult = "invalid_password"
		return nil, status.Error(codes.Unauthenticated, "invalid password")
	}

	// Access Token 用于访问接口；Refresh Token 携带唯一 JTI，用于会话续期和单会话撤销。
	tokens, refreshJti, err := buildTokenPair(l.svcCtx.Config, u)
	if err != nil {
		metricResult = "token_error"
		return nil, status.Error(codes.Internal, "create token failed")
	}
	// Redis 同时记录 Refresh 会话和 tokenVersion，使无状态 JWT 具备可撤销能力。
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
	metricResult = "success"

	return &user.AuthResponse{
		Tokens: tokens,
		User:   buildUserSummary(u),
	}, nil
}
