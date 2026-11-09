package logic

import (
	"context"
	appmetrics "interaction-rpc/internal/metrics"
	"interaction-rpc/internal/svc"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ==========================================
// 1. 常量定义
// ==========================================
const fillSetScript = "redis.call('DEL', KEYS[1]); for i=2,#ARGV do redis.call('SADD', KEYS[1], ARGV[i]) end; redis.call('EXPIRE', KEYS[1], ARGV[1]); return 1"
const fillHashScript = "redis.call('DEL', KEYS[1]); for i=2,#ARGV,2 do redis.call('HSET', KEYS[1], ARGV[i], ARGV[i+1]) end; redis.call('EXPIRE', KEYS[1], ARGV[1]); return 1"

const (
	// BizRedis Key 前缀。
	// MongoDB 保存互动事实，Redis 只保存可回填的短期读模型。
	KeyPostLikeSet    = "post:like:"
	KeyPostCollectSet = "post:collect:"
	KeyPostRateHash   = "post:rate:"
	KeyCommentLikeSet = "comment:like:"
	KeyCacheWarmLock  = "lock:cache:warm:"

	// DummyUserId/占位字段用于表示已经查询过 MongoDB，确认当前没有真实点赞数据。
	// 这样即使 Mongo 查询结果为空，也能在 Redis 中留下一个可见痕迹，避免缓存穿透
	DummyUserId = "-1"

	// 预热后的互动集合只保留有限时间，避免冷门帖子长期占用 Redis 内存。
	cacheTTLSeconds = 60
	// 预热时加一把短锁，避免同一个 key 在高并发下被多个请求同时回源 Mongo。
	cacheWarmLockSeconds = 5
	cacheWarmRetryCount  = 10
	cacheWarmRetryDelay  = 50 * time.Millisecond
)

// setCacheSpec/hashCacheSpec 把“某类互动缓存”的差异参数抽出来，
// 后面点赞、收藏、评论点赞、评分都共用一套预热逻辑。
// 这样做的目的是让缓存策略统一，避免每种互动都复制一套近似代码。
type setCacheSpec struct {
	redisKeyPrefix  string
	mongoCollection string
	targetField     string
}

type hashCacheSpec struct {
	redisKeyPrefix  string
	mongoCollection string
	targetField     string
	valueField      string
}

var (
	postLikeCacheSpec = setCacheSpec{
		redisKeyPrefix:  KeyPostLikeSet,
		mongoCollection: "post_likes",
		targetField:     "postId",
	}
	postCollectCacheSpec = setCacheSpec{
		redisKeyPrefix:  KeyPostCollectSet,
		mongoCollection: "post_collects",
		targetField:     "postId",
	}
	commentLikeCacheSpec = setCacheSpec{
		redisKeyPrefix:  KeyCommentLikeSet,
		mongoCollection: "comment_likes",
		targetField:     "commentId",
	}
	postRateCacheSpec = hashCacheSpec{
		redisKeyPrefix:  KeyPostRateHash,
		mongoCollection: "post_ratings",
		targetField:     "postId",
		valueField:      "score",
	}
)

// EnsurePostLikeCache / EnsurePostCollectCache / EnsureCommentLikeCache / EnsurePostRateCache
// 读取互动状态前按需回填缓存；写入路径提交 Mongo 事务后只删除缓存。
// 如果 Redis 中已存在 key，会直接返回；只有缓存缺失时才会回源 Mongo。
func EnsurePostLikeCache(ctx context.Context, svcCtx *svc.ServiceContext, postId string) error {
	return ensureSetCacheLoaded(ctx, svcCtx, postId, postLikeCacheSpec)
}

func EnsurePostCollectCache(ctx context.Context, svcCtx *svc.ServiceContext, postId string) error {
	return ensureSetCacheLoaded(ctx, svcCtx, postId, postCollectCacheSpec)
}

func EnsureCommentLikeCache(ctx context.Context, svcCtx *svc.ServiceContext, commentId string) error {
	return ensureSetCacheLoaded(ctx, svcCtx, commentId, commentLikeCacheSpec)
}

func EnsurePostRateCache(ctx context.Context, svcCtx *svc.ServiceContext, postId string) error {
	return ensureHashCacheLoaded(ctx, svcCtx, postId, postRateCacheSpec)
}

func setCacheMetricKind(spec setCacheSpec) string {
	switch spec.redisKeyPrefix {
	case KeyPostLikeSet:
		return "like"
	case KeyPostCollectSet:
		return "collect"
	case KeyCommentLikeSet:
		return "comment_like"
	default:
		return strings.Trim(spec.redisKeyPrefix, ":")
	}
}

// ensureSetCacheLoaded 在 Set 缓存缺失时，从 MongoDB 按需回填点赞、收藏等历史互动。
// 回填完整集合，避免只缓存部分用户导致互动状态与计数遗漏。
func ensureSetCacheLoaded(ctx context.Context, svcCtx *svc.ServiceContext, targetId string, spec setCacheSpec) error {
	start := time.Now()
	kind := setCacheMetricKind(spec)
	metricResult := ""
	// 统一在结束时记录可观测指标
	defer func() {
		if metricResult != "" {
			appmetrics.ObserveCacheWarm(kind, metricResult, time.Since(start))
		}
	}()

	key := spec.redisKeyPrefix + targetId

	exists, err := svcCtx.Redis.Exists(ctx, key).Result()
	if err != nil {
		metricResult = "redis_exists_error"
		return err
	}
	if exists > 0 {
		// key 已存在，说明这个目标的互动集合已经在 Redis 中建立好了，直接使用即可。
		metricResult = "cache_hit"
		return nil
	}

	// 按缓存 Key 加锁，减少同一目标的并发回源，缓解热点 Key 失效时的缓存击穿。
	lockKey := KeyCacheWarmLock + key
	// 仅在锁不存在时写入，并设置过期时间，避免请求异常退出后锁一直占用。
	locked, err := svcCtx.Redis.SetNX(ctx, lockKey, "1", time.Duration(cacheWarmLockSeconds)*time.Second).Result()
	if err != nil {
		metricResult = "lock_error"
		return err
	}
	if !locked {
		// 未获得锁时，有上限地轮询缓存，尽量复用其他请求的回填结果。
		for i := 0; i < cacheWarmRetryCount; i++ {
			time.Sleep(cacheWarmRetryDelay)
			exists, err = svcCtx.Redis.Exists(ctx, key).Result()
			if err != nil {
				metricResult = "wait_exists_error"
				return err
			}
			if exists > 0 {
				metricResult = "wait_hit"
				return nil
			}
		}
	} else {
		// 获得锁的请求负责回填，并在退出时尝试释放锁。
		defer func() {
			_, _ = svcCtx.Redis.Del(ctx, lockKey).Result()
		}()
	}

	// 再次检查缓存；等待超时的请求也会走到这里，此时不一定持有锁。
	exists, err = svcCtx.Redis.Exists(ctx, key).Result()
	if err != nil {
		metricResult = "double_check_error"
		return err
	}
	// 其他请求已经查好了数据库了，直接返回
	if exists > 0 {
		metricResult = "double_check_hit"
		return nil
	}

	if svcCtx.Mongo == nil {
		// 理论上互动持久化数据来自 Mongo；如果这里没有 Mongo，只能退化为“不预热直接返回”。
		// 这不是理想状态，但保留了服务可用性。
		metricResult = "mongo_unavailable"
		return nil
	}

	var likes []struct {
		UserId int64 `bson:"userId"`
	}

	coll := svcCtx.Mongo.Collection(spec.mongoCollection)
	cursor, err := coll.Find(ctx, bson.M{spec.targetField: targetId}, options.Find().SetProjection(bson.M{"userId": 1}))
	if err != nil {
		metricResult = "mongo_find_error"
		return err
	}
	defer cursor.Close(ctx)
	if err := cursor.All(ctx, &likes); err != nil {
		metricResult = "mongo_decode_error"
		return err
	}

	// 内容和 TTL 原子写入，避免请求中断留下永不过期的半成品缓存。
	args := []any{cacheTTLSeconds}
	if len(likes) == 0 {
		args = append(args, DummyUserId)
	} else {
		for _, like := range likes {
			args = append(args, strconv.FormatInt(like.UserId, 10))
		}
	}
	_, err = svcCtx.Redis.Eval(ctx, fillSetScript, []string{key}, args...).Result()
	if err != nil {
		metricResult = "redis_write_error"
		return err
	}
	if len(likes) == 0 {
		appmetrics.IncDummyOp(kind, "fill")
	}
	metricResult = "mongo_loaded"
	return nil
}

// ensureHashCacheLoaded 用于预热 Hash 结构缓存，目前服务于帖子评分。
// 它和 Set 版本的思路一致，只是把 userId -> score 作为 hash field/value 存储。
func ensureHashCacheLoaded(ctx context.Context, svcCtx *svc.ServiceContext, targetId string, spec hashCacheSpec) error {
	start := time.Now()
	kind := "post_rate"
	metricResult := ""
	defer func() {
		if metricResult != "" {
			appmetrics.ObserveCacheWarm(kind, metricResult, time.Since(start))
		}
	}()
	key := spec.redisKeyPrefix + targetId
	exists, err := svcCtx.Redis.Exists(ctx, key).Result()
	if err != nil {
		metricResult = "redis_exists_error"
		return err
	}
	if exists > 0 {
		metricResult = "cache_hit"
		return nil
	}

	lockKey := KeyCacheWarmLock + key
	locked, err := svcCtx.Redis.SetNX(ctx, lockKey, "1", time.Duration(cacheWarmLockSeconds)*time.Second).Result()
	if err != nil {
		metricResult = "lock_error"
		return err
	}
	if !locked {
		// 其他请求正在预热评分缓存，这里做短暂自旋等待，尽量避免并发回源。
		for i := 0; i < cacheWarmRetryCount; i++ {
			time.Sleep(cacheWarmRetryDelay)
			exists, err = svcCtx.Redis.Exists(ctx, key).Result()
			if err != nil {
				metricResult = "wait_exists_error"
				return err
			}
			if exists > 0 {
				metricResult = "wait_hit"
				return nil
			}
		}
	} else {
		defer func() {
			_, _ = svcCtx.Redis.Del(ctx, lockKey).Result()
		}()
	}

	exists, err = svcCtx.Redis.Exists(ctx, key).Result()
	if err != nil {
		metricResult = "double_check_error"
		return err
	}
	if exists > 0 {
		metricResult = "double_check_hit"
		return nil
	}

	if svcCtx.Mongo == nil {
		metricResult = "mongo_unavailable"
		return nil
	}

	type ratingDoc struct {
		UserId int64   `bson:"userId"`
		Score  float64 `bson:"score"`
	}

	var ratings []ratingDoc
	coll := svcCtx.Mongo.Collection(spec.mongoCollection)
	cursor, err := coll.Find(ctx, bson.M{spec.targetField: targetId}, options.Find().SetProjection(bson.M{"userId": 1, spec.valueField: 1}))
	if err != nil {
		metricResult = "mongo_find_error"
		return err
	}
	defer cursor.Close(ctx)
	if err := cursor.All(ctx, &ratings); err != nil {
		metricResult = "mongo_decode_error"
		return err
	}

	args := []any{cacheTTLSeconds}
	if len(ratings) == 0 {
		args = append(args, DummyUserId, "0")
	} else {
		for _, rating := range ratings {
			args = append(args, strconv.FormatInt(rating.UserId, 10), strconv.FormatFloat(rating.Score, 'f', 1, 64))
		}
	}
	_, err = svcCtx.Redis.Eval(ctx, fillHashScript, []string{key}, args...).Result()
	if err != nil {
		metricResult = "redis_write_error"
		return err
	}
	if len(ratings) == 0 {
		appmetrics.IncDummyOp(kind, "fill")
		metricResult = "mongo_empty"
		return nil
	}
	metricResult = "mongo_loaded"
	return nil
}

// countSetWithoutDummy 用于对外返回真实计数。
// Redis 中的 DummyUserId 只是防穿透占位节点，不能被算进点赞数/收藏数。
func countSetWithoutDummy(ctx context.Context, svcCtx *svc.ServiceContext, key string) (int64, error) {
	count, err := svcCtx.Redis.SCard(ctx, key).Result()
	if err != nil {
		return 0, err
	}
	hasDummy, err := svcCtx.Redis.SIsMember(ctx, key, DummyUserId).Result()
	if err != nil {
		return count, err
	}
	if hasDummy && count > 0 {
		count--
	}
	return count, nil
}
