package logic

import (
	"context"
	"github.com/zeromicro/go-zero/core/logx"
	"interaction-rpc/interaction"
	"interaction-rpc/internal/store"
	"interaction-rpc/internal/svc"
)

type UncollectPostLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewUncollectPostLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UncollectPostLogic {
	return &UncollectPostLogic{ctx: ctx, svcCtx: svcCtx, Logger: logx.WithContext(ctx)}
}

func (l *UncollectPostLogic) UncollectPost(in *interaction.InteractionRequest) (*interaction.Empty, error) {
	err := mutate(l.ctx, l.svcCtx, store.Command{UserID: in.GetUserId(), TargetID: in.GetTargetId(), Kind: "COLLECT", Remove: true})
	if err != nil {
		return nil, err
	}
	return &interaction.Empty{}, nil
}
