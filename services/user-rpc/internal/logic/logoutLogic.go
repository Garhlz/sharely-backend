package logic

import (
	"context"
	"time"

	"user-rpc/internal/model"
	"user-rpc/internal/svc"
	"user-rpc/user"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type LogoutLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewLogoutLogic(ctx context.Context, svcCtx *svc.ServiceContext) *LogoutLogic {
	return &LogoutLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *LogoutLogic) Logout(in *user.LogoutRequest) (*user.Empty, error) {
	now := time.Now().Unix()
	// 黑名单 TTL 取 Token 的剩余寿命，过期后无需继续保留撤销记录。
	blockToken := func(token string) {
		if token == "" {
			return
		}
		claims, err := parseToken(l.svcCtx.Config, token)
		if err != nil || claims.ExpiresAt == nil {
			return
		}
		ttl := claims.ExpiresAt.Unix() - now
		if ttl <= 0 {
			return
		}
		_ = l.svcCtx.Redis.Setex(blockedTokenKey(token), "1", int(ttl))
	}

	blockToken(in.GetAccessToken())
	blockToken(in.GetRefreshToken())

	if in.GetRefreshToken() != "" {
		// 删除当前 Refresh JTI，阻止该会话继续轮换 Token。
		claims, err := parseToken(l.svcCtx.Config, in.GetRefreshToken())
		if err == nil && claims.Type == "refresh" && claims.ID != "" {
			_, _ = l.svcCtx.Redis.DelCtx(l.ctx, refreshTokenKey(claims.UserId, claims.ID))
		}
	}

	if in.GetUserId() > 0 {
		// 递增账号级版本，使该用户此前签发的所有 Token 一并失效。
		u, err := l.svcCtx.Users.FindOne(l.ctx, in.GetUserId())
		if err != nil && err != model.ErrNotFound {
			return nil, status.Error(codes.Internal, "query user failed")
		}
		if u != nil {
			u.TokenVersion++
			u.UpdatedAt = time.Now()
			if err := l.svcCtx.Users.Update(l.ctx, u); err != nil {
				return nil, status.Error(codes.Internal, "update user failed")
			}
			if err := setTokenVersion(l.ctx, l.svcCtx, u.Id, u.TokenVersion); err != nil {
				return nil, status.Error(codes.Internal, "store token version failed")
			}
		}
	}

	return &user.Empty{}, nil
}
