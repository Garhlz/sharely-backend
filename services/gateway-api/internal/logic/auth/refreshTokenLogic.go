// 由 goctl 生成的代码骨架，可按业务需要修改。
// goctl 1.9.2

package auth

import (
	"context"

	"gateway-api/internal/svc"
	"gateway-api/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
	"user-rpc/userservice"
)

type RefreshTokenLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewRefreshTokenLogic(ctx context.Context, svcCtx *svc.ServiceContext) *RefreshTokenLogic {
	return &RefreshTokenLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *RefreshTokenLogic) RefreshToken(req *types.AuthRefreshReq) (resp *types.AuthResponse, err error) {
	// Refresh Token 的合法性、会话状态和轮换逻辑均由 user-rpc 负责。
	client := userservice.NewUserService(l.svcCtx.UserRpc)
	result, err := client.RefreshToken(l.ctx, &userservice.RefreshTokenRequest{
		RefreshToken: req.RefreshToken,
	})
	if err != nil {
		return nil, err
	}
	return mapAuthResponse(result), nil
}
