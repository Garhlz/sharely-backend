package logic

import (
	"context"
	"strings"

	"user-rpc/internal/svc"
	"user-rpc/user"

	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
)

type VerifyEmailCodeLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewVerifyEmailCodeLogic(ctx context.Context, svcCtx *svc.ServiceContext) *VerifyEmailCodeLogic {
	return &VerifyEmailCodeLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *VerifyEmailCodeLogic) VerifyEmailCode(in *user.VerifyEmailCodeRequest) (*user.VerifyEmailCodeResponse, error) {
	email := strings.TrimSpace(in.GetEmail())
	code := strings.TrimSpace(in.GetCode())
	scene, ok := normalizeEmailScene(in.GetScene())
	if email == "" || code == "" || !ok {
		return &user.VerifyEmailCodeResponse{Valid: false}, nil
	}

	val, err := l.svcCtx.Redis.Get(l.ctx, emailCodeKey(scene, email)).Result()
	if err != nil {
		if err == redis.Nil {
			return &user.VerifyEmailCodeResponse{Valid: false}, nil
		}
		return &user.VerifyEmailCodeResponse{Valid: false}, nil
	}

	return &user.VerifyEmailCodeResponse{Valid: val == code}, nil
}
