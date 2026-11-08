package logic

import (
	"context"
	"github.com/zeromicro/go-zero/core/logx"
	"interaction-rpc/interaction"
	"interaction-rpc/internal/store"
	"interaction-rpc/internal/svc"
)

type RatePostLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewRatePostLogic(ctx context.Context, svcCtx *svc.ServiceContext) *RatePostLogic {
	return &RatePostLogic{ctx: ctx, svcCtx: svcCtx, Logger: logx.WithContext(ctx)}
}

func (l *RatePostLogic) RatePost(in *interaction.RateRequest) (*interaction.Empty, error) {
	err := mutate(l.ctx, l.svcCtx, store.Command{UserID: in.GetUserId(), TargetID: in.GetTargetId(), Kind: "RATE", Remove: false, Score: in.GetScore()})
	if err != nil {
		return nil, err
	}
	return &interaction.Empty{}, nil
}
