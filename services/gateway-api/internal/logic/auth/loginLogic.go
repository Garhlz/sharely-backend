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

type LoginLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewLoginLogic(ctx context.Context, svcCtx *svc.ServiceContext) *LoginLogic {
	return &LoginLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *LoginLogic) Login(req *types.AuthLoginReq) (resp *types.AuthResponse, err error) {
	// 网关只负责协议转换；账号校验和 Token 签发统一由 user-rpc 完成。
	client := userservice.NewUserService(l.svcCtx.UserRpc)
	result, err := client.Login(l.ctx, &userservice.LoginRequest{
		Email:    req.Email,
		Password: req.Password,
	})
	if err != nil {
		return nil, err
	}
	return mapAuthResponse(result), nil
}
