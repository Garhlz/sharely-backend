// 由 goctl 生成的代码骨架，可按业务需要修改。
// goctl 1.9.2

package auth

import (
	"context"

	"gateway-api/internal/svc"
	"gateway-api/internal/types"
	"gateway-api/internal/pkg/ctxutil"

	"github.com/zeromicro/go-zero/core/logx"
	"user-rpc/userservice"
)

type LogoutLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewLogoutLogic(ctx context.Context, svcCtx *svc.ServiceContext) *LogoutLogic {
	return &LogoutLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *LogoutLogic) Logout() (resp *types.Empty, err error) {
	client := userservice.NewUserService(l.svcCtx.UserRpc)
	// 鉴权中间件已将身份和双 Token 写入 Context，此处统一取出并交给 user-rpc 撤销。
	_, err = client.Logout(l.ctx, &userservice.LogoutRequest{
		UserId:       ctxutil.UserID(l.ctx),
		AccessToken:  ctxutil.AuthToken(l.ctx),
		RefreshToken: ctxutil.RefreshToken(l.ctx),
	})
	if err != nil {
		return nil, err
	}
	return &types.Empty{}, nil
}
