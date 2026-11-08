package logic

import (
	"context"
	"errors"
	"interaction-rpc/internal/store"
	"interaction-rpc/internal/svc"

	"github.com/zeromicro/go-zero/core/logx"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func mutate(ctx context.Context, s *svc.ServiceContext, c store.Command) error {
	if err := store.Validate(c); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	id, _ := primitive.ObjectIDFromHex(c.TargetID)
	c.TargetID = id.Hex()
	if s.Interactions == nil {
		return status.Error(codes.Unavailable, "interaction storage unavailable")
	}
	_, err := s.Interactions.Apply(ctx, c)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return status.Error(codes.NotFound, "target not found")
		}
		logx.WithContext(ctx).Errorf("persist interaction: %v", err)
		return status.Error(codes.Unavailable, "persist interaction failed")
	}
	// 数据库提交后再失效缓存；失败由短 TTL 限制陈旧时间，不撤销已提交的业务。
	prefix := map[string]string{"LIKE": KeyPostLikeSet, "COLLECT": KeyPostCollectSet, "RATE": KeyPostRateHash, "COMMENT_LIKE": KeyCommentLikeSet}[c.Kind]
	if s.Redis != nil {
		if _, err := s.Redis.Del(ctx, prefix+c.TargetID).Result(); err != nil {
			logx.WithContext(ctx).Errorf("invalidate interaction cache: %v", err)
		}
	}
	return nil
}
