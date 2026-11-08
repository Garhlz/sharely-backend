package logic

import (
	"context"
	"github.com/zeromicro/go-zero/core/logx"
	"interaction-rpc/interaction"
	"interaction-rpc/internal/store"
	"interaction-rpc/internal/svc"
)

type LikeCommentLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewLikeCommentLogic(ctx context.Context, svcCtx *svc.ServiceContext) *LikeCommentLogic {
	return &LikeCommentLogic{ctx: ctx, svcCtx: svcCtx, Logger: logx.WithContext(ctx)}
}

func (l *LikeCommentLogic) LikeComment(in *interaction.InteractionRequest) (*interaction.Empty, error) {
	err := mutate(l.ctx, l.svcCtx, store.Command{UserID: in.GetUserId(), TargetID: in.GetTargetId(), Kind: "COMMENT_LIKE", Remove: false})
	if err != nil {
		return nil, err
	}
	return &interaction.Empty{}, nil
}
