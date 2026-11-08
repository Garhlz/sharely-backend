package logic

import (
	"context"
	"github.com/zeromicro/go-zero/core/logx"
	"interaction-rpc/interaction"
	"interaction-rpc/internal/store"
	"interaction-rpc/internal/svc"
)

type UnlikePostLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewUnlikePostLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UnlikePostLogic {
	return &UnlikePostLogic{ctx: ctx, svcCtx: svcCtx, Logger: logx.WithContext(ctx)}
}

func (l *UnlikePostLogic) UnlikePost(in *interaction.InteractionRequest) (*interaction.Empty, error) {
	err := mutate(l.ctx, l.svcCtx, store.Command{UserID: in.GetUserId(), TargetID: in.GetTargetId(), Kind: "LIKE", Remove: true})
	if err != nil {
		return nil, err
	}
	return &interaction.Empty{}, nil
}
