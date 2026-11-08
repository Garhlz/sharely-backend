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

type BindEmailLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewBindEmailLogic(ctx context.Context, svcCtx *svc.ServiceContext) *BindEmailLogic {
	return &BindEmailLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *BindEmailLogic) BindEmail(in *user.BindEmailRequest) (*user.Empty, error) {
	email := strings.TrimSpace(in.GetEmail())
	code := strings.TrimSpace(in.GetCode())
	if in.GetUserId() <= 0 || email == "" || code == "" {
		return nil, status.Error(codes.InvalidArgument, "invalid params")
	}

	cached, err := l.svcCtx.Redis.Get(l.ctx, emailCodeKey(emailSceneBindEmail, email)).Result()
	if err != nil && err != redis.Nil {
		return nil, status.Error(codes.Internal, "verify code error")
	}
	if cached == "" || cached != code {
		return nil, status.Error(codes.InvalidArgument, "invalid verify code")
	}

	exist, err := l.svcCtx.Users.FindOneByEmail(l.ctx, email)
	if err != nil && err != model.ErrNotFound {
		return nil, status.Error(codes.Internal, "query user failed")
	}
	if exist != nil && exist.Id != in.GetUserId() {
		return nil, status.Error(codes.AlreadyExists, "email already exists")
	}

	u, err := l.svcCtx.Users.FindOne(l.ctx, in.GetUserId())
	if err == model.ErrNotFound {
		return nil, status.Error(codes.NotFound, "user not found")
	}
	if err != nil {
		return nil, status.Error(codes.Internal, "query user failed")
	}

	u.Email = email
	u.UpdatedAt = time.Now()
	if err := l.svcCtx.Users.Update(l.ctx, u); err != nil {
		return nil, status.Error(codes.Internal, "update user failed")
	}

	_, _ = l.svcCtx.Redis.Del(l.ctx, emailCodeKey(emailSceneBindEmail, email)).Result()
	return &user.Empty{}, nil
}
