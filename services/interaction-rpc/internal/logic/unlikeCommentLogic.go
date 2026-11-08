package logic

import (
	"context"
	"github.com/zeromicro/go-zero/core/logx"
	"interaction-rpc/interaction"
	"interaction-rpc/internal/store"
	"interaction-rpc/internal/svc"
)

type UnlikeCommentLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewUnlikeCommentLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UnlikeCommentLogic {
	return &UnlikeCommentLogic{ctx: ctx, svcCtx: svcCtx, Logger: logx.WithContext(ctx)}
}

func (l *UnlikeCommentLogic) UnlikeComment(in *interaction.InteractionRequest) (*interaction.Empty, error) {
	err := mutate(l.ctx, l.svcCtx, store.Command{UserID: in.GetUserId(), TargetID: in.GetTargetId(), Kind: "COMMENT_LIKE", Remove: true})
	if err != nil {
		return nil, err
	}
	return &interaction.Empty{}, nil
}
