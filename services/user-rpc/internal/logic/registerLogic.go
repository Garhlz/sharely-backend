package logic

import (
	"context"
	"database/sql"
	"strings"
	"time"

	appmetrics "user-rpc/internal/metrics"
	"user-rpc/internal/model"
	"user-rpc/internal/svc"
	"user-rpc/user"

	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type RegisterLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewRegisterLogic(ctx context.Context, svcCtx *svc.ServiceContext) *RegisterLogic {
	return &RegisterLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *RegisterLogic) Register(in *user.RegisterRequest) (*user.AuthResponse, error) {
	start := time.Now()
	metricResult := ""
	defer func() {
		if metricResult != "" {
			appmetrics.ObserveAuth("register", metricResult, time.Since(start))
		}
	}()
	email := strings.TrimSpace(in.GetEmail())
	code := strings.TrimSpace(in.GetCode())
	password := in.GetPassword()
	nickname := strings.TrimSpace(in.GetNickname())
	if email == "" || code == "" || password == "" {
		metricResult = "invalid_argument"
		return nil, status.Error(codes.InvalidArgument, "email, code and password are required")
	}

	cached, err := l.svcCtx.Redis.GetCtx(l.ctx, emailCodeKey(emailSceneRegister, email))
	if err != nil && err != redis.Nil {
		metricResult = "verify_code_error"
		return nil, status.Error(codes.Internal, "verify code error")
	}
	if cached == "" || cached != code {
		metricResult = "invalid_verify_code"
		return nil, status.Error(codes.InvalidArgument, "invalid verify code")
	}

	exist, err := l.svcCtx.Users.FindOneByEmail(l.ctx, email)
	if err == nil && exist != nil {
		metricResult = "email_exists"
		return nil, status.Error(codes.AlreadyExists, "email already exists")
	}
	if err != nil && err != model.ErrNotFound {
		metricResult = "query_error"
		return nil, status.Error(codes.Internal, "query user failed")
	}

	if nickname == "" {
		nickname = "用户"
	}

	hashed, err := hashPassword(password)
	if err != nil {
		metricResult = "hash_error"
		return nil, status.Error(codes.Internal, "hash password failed")
	}

	now := time.Now()
	userData := &model.Users{
		Email:             email,
		Password:          hashed,
		Nickname:          nickname,
		Avatar:            l.svcCtx.Config.User.DefaultAvatar,
		Gender:            0,
		Birthday:          sql.NullTime{Valid: false},
		Region:            sql.NullString{Valid: false},
		Bio:               "",
		Role:              "USER",
		Status:            1,
		IsDeleted:         0,
		TokenVersion:      0,
		PasswordChangedAt: now,
		UpdatedAt:         now,
		CreatedAt:         now,
	}

	id, err := l.svcCtx.Users.InsertAndReturnID(l.ctx, userData)
	if err != nil {
		metricResult = "create_user_error"
		return nil, status.Error(codes.Internal, "create user failed")
	}
	userData.Id = id

	tokens, refreshJti, err := buildTokenPair(l.svcCtx.Config, userData)
	if err != nil {
		metricResult = "token_error"
		return nil, status.Error(codes.Internal, "create token failed")
	}
	if err := l.svcCtx.Redis.Setex(
		refreshTokenKey(userData.Id, refreshJti),
		"1",
		int(l.svcCtx.Config.Jwt.RefreshExpireSeconds),
	); err != nil {
		metricResult = "store_refresh_error"
		return nil, status.Error(codes.Internal, "store refresh token failed")
	}
	if err := setTokenVersion(l.ctx, l.svcCtx, userData.Id, userData.TokenVersion); err != nil {
		metricResult = "store_version_error"
		return nil, status.Error(codes.Internal, "store token version failed")
	}

	_, _ = l.svcCtx.Redis.Del(emailCodeKey(emailSceneRegister, email))
	metricResult = "success"

	return &user.AuthResponse{
		Tokens: tokens,
		User:   buildUserSummary(userData),
	}, nil
}
