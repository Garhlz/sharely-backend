package logic

import (
	"context"
	"strconv"

	"interaction-rpc/interaction"
	"interaction-rpc/internal/svc"

	"github.com/zeromicro/go-zero/core/logx"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type BatchPostStatsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewBatchPostStatsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *BatchPostStatsLogic {
	return &BatchPostStatsLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *BatchPostStatsLogic) BatchPostStats(in *interaction.BatchPostStatsRequest) (*interaction.BatchPostStatsResponse, error) {
	if len(in.GetPostIds()) == 0 {
		return &interaction.BatchPostStatsResponse{Stats: map[string]*interaction.PostStats{}}, nil
	}

	// 这个接口是“帖子互动展示聚合口”：
	// 它不负责返回帖子正文，而是统一返回点赞数、收藏数、是否点赞、是否收藏、我的评分、是否关注作者等互动字段。
	// 网关在列表页、搜索页、详情页都会调用它，把互动视图收敛到 interaction-rpc。
	userId := in.GetUserId()
	userIdStr := strconv.FormatInt(userId, 10)
	stats := make(map[string]*interaction.PostStats, len(in.GetPostIds()))
	for _, postId := range in.GetPostIds() {
		// 初始化stats这个map中的每个元素
		stats[postId] = &interaction.PostStats{}
	}

	for _, postId := range in.GetPostIds() {
		// Ensure 内部先检查 Redis；只有缓存缺失才回源 Mongo，不再经过 Bloom 过滤。
		if err := EnsurePostCollectCache(l.ctx, l.svcCtx, postId); err != nil {
			l.Logger.Errorf("ensure post collect cache error: %v", err)
		}
		if err := EnsurePostLikeCache(l.ctx, l.svcCtx, postId); err != nil {
			l.Logger.Errorf("ensure post like cache error: %v", err)
		}
		if err := EnsurePostRateCache(l.ctx, l.svcCtx, postId); err != nil {
			l.Logger.Errorf("ensure post rate cache error: %v", err)
		}

		// 点赞数/收藏数统一从 interaction-rpc 维护的 Redis 集合读取，
		// 不再依赖 ES 中最终一致的冗余计数。
		collectCount, err := countSetWithoutDummy(l.ctx, l.svcCtx, KeyPostCollectSet+postId)
		if err != nil {
			l.Logger.Errorf("collect count error: %v", err)
		} else {
			stats[postId].CollectCount = int32(collectCount)
		}
		likeCount, err := countSetWithoutDummy(l.ctx, l.svcCtx, KeyPostLikeSet+postId)
		if err != nil {
			l.Logger.Errorf("like count error: %v", err)
		} else {
			stats[postId].LikeCount = int32(likeCount)
		}

		if userId > 0 {
			// 对当前登录用户，直接判断其 userId 是否存在于目标 Set 中，即可得到是否点赞/收藏。
			member, err := l.svcCtx.Redis.SIsMember(l.ctx, KeyPostCollectSet+postId, userIdStr).Result()
			if err != nil {
				l.Logger.Errorf("collect status error: %v", err)
			} else {
				stats[postId].IsCollected = member
			}
			liked, err := l.svcCtx.Redis.SIsMember(l.ctx, KeyPostLikeSet+postId, userIdStr).Result()
			if err != nil {
				l.Logger.Errorf("like status error: %v", err)
			} else {
				stats[postId].IsLiked = liked
			}
			// 我的评分来自评分 Hash 中 userId 对应的 field。
			// 如果查不到，说明当前用户还没给这篇帖子打过分。
			score, err := l.svcCtx.Redis.HGet(l.ctx, KeyPostRateHash+postId, userIdStr).Result()
			if err != nil {
				l.Logger.Errorf("rate status error: %v", err)
			} else if parsed, parseErr := strconv.ParseFloat(score, 64); parseErr == nil {
				stats[postId].MyScore = parsed
			}
		}
	}

	if l.svcCtx.Mongo != nil {
		// 评论数、评分总人数、评分均值目前仍由 Mongo posts 文档提供。
		// 这部分不是高频切换状态，不需要像点赞/收藏那样完全依赖 Redis Set/Hash。
		type postDoc struct {
			ID            primitive.ObjectID `bson:"_id"`
			CommentCount  int32              `bson:"commentCount"`
			RatingCount   int32              `bson:"ratingCount"`
			RatingAverage float64            `bson:"ratingAverage"`
		}
		ids := make([]primitive.ObjectID, 0, len(in.GetPostIds()))
		for _, id := range in.GetPostIds() {
			if objectID, err := primitive.ObjectIDFromHex(id); err == nil {
				ids = append(ids, objectID)
			}
		}
		cursor, err := l.svcCtx.Mongo.Collection("posts").Find(
			l.ctx,
			bson.M{"_id": bson.M{"$in": ids}},
			options.Find().SetProjection(bson.M{"commentCount": 1, "ratingCount": 1, "ratingAverage": 1}),
		)
		if err != nil {
			l.Logger.Errorf("mongo query error: %v", err)
		} else {
			defer cursor.Close(l.ctx)
			for cursor.Next(l.ctx) {
				var doc postDoc
				if err := cursor.Decode(&doc); err != nil {
					continue
				}
				if st, ok := stats[doc.ID.Hex()]; ok {
					st.CommentCount = doc.CommentCount
					st.RatingCount = doc.RatingCount
					st.RatingAverage = doc.RatingAverage
				}
			}
		}
	}

	if userId > 0 && l.svcCtx.Mongo != nil && len(in.GetPostAuthorMap()) > 0 {
		// 是否关注作者也在这里顺手聚合，避免网关或详情页再额外查一次关系表。
		authorSet := make(map[int64]struct{})
		for _, authorId := range in.GetPostAuthorMap() {
			if authorId > 0 {
				authorSet[authorId] = struct{}{}
			}
		}
		if len(authorSet) > 0 {
			authorIds := make([]int64, 0, len(authorSet))
			for id := range authorSet {
				authorIds = append(authorIds, id)
			}
			type followDoc struct {
				TargetUserId int64 `bson:"targetUserId"`
			}
			cursor, err := l.svcCtx.Mongo.Collection("user_follows").Find(
				l.ctx,
				bson.M{"userId": userId, "targetUserId": bson.M{"$in": authorIds}},
				options.Find().SetProjection(bson.M{"targetUserId": 1}),
			)
			if err != nil {
				l.Logger.Errorf("mongo follow query error: %v", err)
			} else {
				defer cursor.Close(l.ctx)
				followed := make(map[int64]struct{})
				for cursor.Next(l.ctx) {
					var doc followDoc
					if err := cursor.Decode(&doc); err != nil {
						continue
					}
					followed[doc.TargetUserId] = struct{}{}
				}
				for postId, authorId := range in.GetPostAuthorMap() {
					if st, ok := stats[postId]; ok {
						_, ok := followed[authorId]
						st.IsFollowed = ok
					}
				}
			}
		}
	}

	return &interaction.BatchPostStatsResponse{Stats: stats}, nil
}
