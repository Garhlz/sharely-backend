package logic

import (
	"context"
	"strings"
	"time"

	"user-rpc/internal/model"
	"user-rpc/internal/svc"
	"user-rpc/user"

	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ResetPasswordLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewResetPasswordLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ResetPasswordLogic {
	return &ResetPasswordLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ResetPasswordLogic) ResetPassword(in *user.ResetPasswordRequest) (*user.Empty, error) {
	email := strings.TrimSpace(in.GetEmail())
	code := strings.TrimSpace(in.GetCode())
	newPassword := in.GetNewPassword()
	if email == "" || code == "" || newPassword == "" {
		return nil, status.Error(codes.InvalidArgument, "invalid params")
	}
	if len(newPassword) < 6 {
		return nil, status.Error(codes.InvalidArgument, "password too short")
	}

	cached, err := l.svcCtx.Redis.Get(l.ctx, emailCodeKey(emailSceneResetPassword, email)).Result()
	if err != nil && err != redis.Nil {
		return nil, status.Error(codes.Internal, "verify code error")
	}
	if cached == "" || cached != code {
		return nil, status.Error(codes.InvalidArgument, "invalid verify code")
	}

	u, err := l.svcCtx.Users.FindOneByEmail(l.ctx, email)
	if err == model.ErrNotFound {
		return nil, status.Error(codes.NotFound, "user not found")
	}
	if err != nil {
		return nil, status.Error(codes.Internal, "query user failed")
	}

	hashed, err := hashPassword(newPassword)
	if err != nil {
		return nil, status.Error(codes.Internal, "hash password failed")
	}

	u.Password = hashed
	u.TokenVersion++
	u.PasswordChangedAt = time.Now()
	u.UpdatedAt = time.Now()
	if err := l.svcCtx.Users.Update(l.ctx, u); err != nil {
		return nil, status.Error(codes.Internal, "update user failed")
	}
	if err := setTokenVersion(l.ctx, l.svcCtx, u.Id, u.TokenVersion); err != nil {
		return nil, status.Error(codes.Internal, "store token version failed")
	}

	_, _ = l.svcCtx.Redis.Del(l.ctx, emailCodeKey(emailSceneResetPassword, email)).Result()
	return &user.Empty{}, nil
}
