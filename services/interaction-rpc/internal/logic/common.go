package logic

import (
	"context"
	"encoding/json"
	appmetrics "interaction-rpc/internal/metrics"
	"interaction-rpc/internal/svc"
	"strconv"
	"strings"
	"time"

	"github.com/rabbitmq/amqp091-go"
	goredis "github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc/metadata"
)

// ==========================================
// 1. 常量定义
// ==========================================
const (
	// BizRedis Key 前缀。
	// 这里的 Redis 不是单纯“查不到就算了”的旁路缓存，而是互动读链路的主读取来源。
	// Mongo/Java 侧更多承担持久化、异步聚合、搜索同步等职责。
	KeyPostLikeSet    = "post:like:"    // 对应 Java: POST_LIKE_SET
	KeyPostCollectSet = "post:collect:" // 对应 Java: POST_COLLECT_SET
	KeyPostRateHash   = "post:rate:"    // 对应 Java: POST_RATE_HASH
	KeyCommentLikeSet = "comment:like:" // 对应 Java: COMMENT_LIKE_SET
	KeyCacheWarmLock  = "lock:cache:warm:"

	// RabbitMQ 配置 (必须与 Java 端配置一致)
	ExchangeName     = "platform.topic.exchange"
	RoutingKeyCreate = "interaction.create"
	RoutingKeyDelete = "interaction.delete"

	// Bloom Filter 只做“快速判断某个目标是否可能存在互动数据”，
	// 它不是事实源，也不参与真实计数。
	// 一旦 Bloom 因为 Redis 重启丢失，系统仍然可以回退到 Mongo 预热。
	KeyBloomPostLike    = "bloom:post_likes"
	KeyBloomPostCollect = "bloom:post_collects"
	KeyBloomCommentLike = "bloom:comment_likes"
	KeyBloomPostRate    = "bloom:post_rates"

	// DummyUserId/占位字段用于表示已经查询过 MongoDB，确认当前没有真实点赞数据。
	// 这样即使 Mongo 查询结果为空，也能在 Redis 中留下一个可见痕迹，避免缓存穿透
	DummyUserId = "-1"

	// 预热后的互动集合只保留有限时间，避免冷门帖子长期占用 Redis 内存。
	cacheTTLSeconds = 24 * 60 * 60
	// 预热时加一把短锁，避免同一个 key 在高并发下被多个请求同时回源 Mongo。
	cacheWarmLockSeconds = 5
	cacheWarmRetryCount  = 10
	cacheWarmRetryDelay  = 50 * time.Millisecond
	bloomErrorRate       = 0.01
	bloomCapacity        = 1_000_000
)

// setCacheSpec/hashCacheSpec 把“某类互动缓存”的差异参数抽出来，
// 后面点赞、收藏、评论点赞、评分都共用一套预热逻辑。
// 这样做的目的是让缓存策略统一，避免每种互动都复制一套近似代码。
type setCacheSpec struct {
	redisKeyPrefix  string
	mongoCollection string
	targetField     string
	bloomKey        string
}

type hashCacheSpec struct {
	redisKeyPrefix  string
	mongoCollection string
	targetField     string
	valueField      string
	bloomKey        string
}

var (
	postLikeCacheSpec = setCacheSpec{
		redisKeyPrefix:  KeyPostLikeSet,
		mongoCollection: "post_likes",
		targetField:     "postId",
		bloomKey:        KeyBloomPostLike,
	}
	postCollectCacheSpec = setCacheSpec{
		redisKeyPrefix:  KeyPostCollectSet,
		mongoCollection: "post_collects",
		targetField:     "postId",
		bloomKey:        KeyBloomPostCollect,
	}
	commentLikeCacheSpec = setCacheSpec{
		redisKeyPrefix:  KeyCommentLikeSet,
		mongoCollection: "comment_likes",
		targetField:     "commentId",
		bloomKey:        KeyBloomCommentLike,
	}
	postRateCacheSpec = hashCacheSpec{
		redisKeyPrefix:  KeyPostRateHash,
		mongoCollection: "post_ratings",
		targetField:     "postId",
		valueField:      "score",
		bloomKey:        KeyBloomPostRate,
	}
)

// ==========================================
// 2. 事件结构体 (保持与 Java InteractionEvent 一致)
// ==========================================
type InteractionEvent struct {
	UserId   int64       `json:"userId"`
	TargetId string      `json:"targetId"`
	Type     string      `json:"type"`   // LIKE, COLLECT, RATE, COMMENT_LIKE
	Action   string      `json:"action"` // ADD, REMOVE
	Value    interface{} `json:"value"`  // 评分的分数 (Double/Float)
}

func interactionEventIfChanged(changed int, userID int64, targetID, interactionType, action string, value any) (*InteractionEvent, bool) {
	if changed <= 0 {
		return nil, false
	}
	return &InteractionEvent{
		UserId:   userID,
		TargetId: targetID,
		Type:     interactionType,
		Action:   action,
		Value:    value,
	}, true
}

// ==========================================
// 3. 通用 MQ 发送函数
// ==========================================
func publishEvent(ctx context.Context, channel *amqp091.Channel, routingKey string, event *InteractionEvent) error {
	// 1. 序列化为 JSON
	body, err := json.Marshal(event)
	if err != nil {
		appmetrics.IncMQPublish(routingKey, "marshal_error")
		return err
	}

	// 2. 发送消息
	// 注意：Context 用于控制超时，amqp091-go 支持带 Context 的 Publish
	requestID := requestIDFromContext(ctx)
	headers := amqp091.Table{
		"X-Request-Id":  requestID,
		"X-Trace-Id":    requestID,
		"X-Service":     "interaction-rpc",
		"X-Routing-Key": routingKey,
	}
	otel.GetTextMapPropagator().Inject(ctx, amqpHeaderCarrier(headers))
	err = channel.PublishWithContext(ctx,
		ExchangeName, // exchange
		routingKey,   // routing key
		false,        // mandatory
		false,        // immediate
		amqp091.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp091.Persistent, // 消息持久化
			Timestamp:    time.Now(),
			Body:         body,
			Headers:      headers,
		},
	)

	if err != nil {
		appmetrics.IncMQPublish(routingKey, "publish_error")
		logx.WithContext(ctx).Errorf("Failed to publish event: %v", err)
		return err
	}

	appmetrics.IncMQPublish(routingKey, "success")
	logx.WithContext(ctx).Infof("mq publish success routingKey=%s payload=%s", routingKey, string(body))
	return nil
}

func requestIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if values := md.Get("x-request-id"); len(values) > 0 && values[0] != "" {
			return values[0]
		}
		if values := md.Get("x-trace-id"); len(values) > 0 && values[0] != "" {
			return values[0]
		}
	}
	return strconv.FormatInt(time.Now().UnixNano(), 36)
}

type amqpHeaderCarrier amqp091.Table

func (c amqpHeaderCarrier) Get(key string) string {
	value, ok := amqp091.Table(c)[key]
	if !ok {
		return ""
	}
	switch v := value.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return ""
	}
}

func (c amqpHeaderCarrier) Set(key, value string) {
	amqp091.Table(c)[key] = value
}

func (c amqpHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range amqp091.Table(c) {
		keys = append(keys, k)
	}
	return keys
}

// EnsurePostLikeCache / EnsurePostCollectCache / EnsureCommentLikeCache / EnsurePostRateCache
// 都是在“真正读写 Redis 前，先确保这个 key 已经完成冷启动预热”。
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
// 先恢复完整集合再执行本次操作，避免缓存丢失后只保留新互动，导致读取遗漏历史数据。
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

	exists, err := svcCtx.Redis.ExistsCtx(ctx, key)
	if err != nil {
		metricResult = "redis_exists_error"
		return err
	}
	if exists {
		// key 已存在，说明这个目标的互动集合已经在 Redis 中建立好了，直接使用即可。
		metricResult = "cache_hit"
		return nil
	}

	// 按缓存 Key 加锁，减少同一目标的并发回源，缓解热点 Key 失效时的缓存击穿。
	lockKey := KeyCacheWarmLock + key
	// 仅在锁不存在时写入，并设置过期时间，避免请求异常退出后锁一直占用。
	locked, err := svcCtx.Redis.SetnxEx(lockKey, "1", cacheWarmLockSeconds)
	if err != nil {
		metricResult = "lock_error"
		return err
	}
	if !locked {
		// 未获得锁时，有上限地轮询缓存，尽量复用其他请求的回填结果。
		for i := 0; i < cacheWarmRetryCount; i++ {
			time.Sleep(cacheWarmRetryDelay)
			exists, err = svcCtx.Redis.ExistsCtx(ctx, key)
			if err != nil {
				metricResult = "wait_exists_error"
				return err
			}
			if exists {
				metricResult = "wait_hit"
				return nil
			}
		}
	} else {
		// 获得锁的请求负责回填，并在退出时尝试释放锁。
		defer func() {
			_, _ = svcCtx.Redis.DelCtx(ctx, lockKey)
		}()
	}

	// 再次检查缓存；等待超时的请求也会走到这里，此时不一定持有锁。
	exists, err = svcCtx.Redis.ExistsCtx(ctx, key)
	if err != nil {
		metricResult = "double_check_error"
		return err
	}
	// 其他请求已经查好了数据库了，直接返回
	if exists {
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

	if len(likes) > 0 {
		// Mongo 中存在真实互动记录，整批写回 Redis Set。
		args := make([]any, len(likes))
		for i, like := range likes {
			args[i] = strconv.FormatInt(like.UserId, 10)
		}
		_, err = svcCtx.Redis.SaddCtx(ctx, key, args...)
		if err == nil {
			// 一旦确认真实存在互动数据，就把 targetId 加入 Bloom。
			// 后续读请求可以先走 Bloom 做快速判断，减少不必要的 Mongo 预热尝试。
			AddBloom(ctx, svcCtx, spec.bloomKey, targetId)
		}
	} else {
		// Mongo 返回空集合时不能什么都不写。
		// 否则下一次读写这个目标时，仍然会认为“缓存不存在”，又去查 Mongo，形成穿透。
		// 即这个集合已经被预热过，但是不存在真实数据
		_, err = svcCtx.Redis.SaddCtx(ctx, key, DummyUserId)
		if err == nil {
			appmetrics.IncDummyOp(kind, "fill")
		}
	}
	if err != nil {
		metricResult = "redis_write_error"
		return err
	}

	// 预热后的 key 统一设置 TTL，避免 Redis 中残留大量长期无人访问的互动集合。
	_ = svcCtx.Redis.ExpireCtx(ctx, key, cacheTTLSeconds)
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
	exists, err := svcCtx.Redis.ExistsCtx(ctx, key)
	if err != nil {
		metricResult = "redis_exists_error"
		return err
	}
	if exists {
		metricResult = "cache_hit"
		return nil
	}

	lockKey := KeyCacheWarmLock + key
	locked, err := svcCtx.Redis.SetnxEx(lockKey, "1", cacheWarmLockSeconds)
	if err != nil {
		metricResult = "lock_error"
		return err
	}
	if !locked {
		// 其他请求正在预热评分缓存，这里做短暂自旋等待，尽量避免并发回源。
		for i := 0; i < cacheWarmRetryCount; i++ {
			time.Sleep(cacheWarmRetryDelay)
			exists, err = svcCtx.Redis.ExistsCtx(ctx, key)
			if err != nil {
				metricResult = "wait_exists_error"
				return err
			}
			if exists {
				metricResult = "wait_hit"
				return nil
			}
		}
	} else {
		defer func() {
			_, _ = svcCtx.Redis.DelCtx(ctx, lockKey)
		}()
	}

	exists, err = svcCtx.Redis.ExistsCtx(ctx, key)
	if err != nil {
		metricResult = "double_check_error"
		return err
	}
	if exists {
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

	if len(ratings) == 0 {
		// 评分也需要“空值占位”。
		// 这样即便该帖子从未被评分，也能阻止后续请求持续穿透到 Mongo。
		_, err = svcCtx.Redis.HsetnxCtx(ctx, key, DummyUserId, "0")
		if err != nil {
			metricResult = "redis_write_error"
			return err
		}
		appmetrics.IncDummyOp(kind, "fill")
		_ = svcCtx.Redis.ExpireCtx(ctx, key, cacheTTLSeconds)
		metricResult = "mongo_empty"
		return nil
	}

	for _, rating := range ratings {
		// 用一位小数字符串写入 Redis，保证和业务协议中的评分格式一致。
		scoreStr := strconv.FormatFloat(rating.Score, 'f', 1, 64)
		if err := svcCtx.Redis.HsetCtx(ctx, key, strconv.FormatInt(rating.UserId, 10), scoreStr); err != nil {
			metricResult = "redis_write_error"
			return err
		}
	}
	// 评分数据存在时，同样把帖子加入 Bloom，供后续快速判断使用。
	AddBloom(ctx, svcCtx, spec.bloomKey, targetId)
	_ = svcCtx.Redis.ExpireCtx(ctx, key, cacheTTLSeconds)
	metricResult = "mongo_loaded"
	return nil
}

// ShouldLoadSetCache / ShouldLoadHashCache 负责决定“是否值得尝试预热”。
// 返回 true 的含义不是“Redis 一定缺 key”，而是“这个目标可能有真实互动数据，或者 Bloom 本身不可用，需要兜底检查”。
func ShouldLoadSetCache(ctx context.Context, svcCtx *svc.ServiceContext, targetId string, spec setCacheSpec) bool {
	ready, exists := CheckBloom(ctx, svcCtx, spec.bloomKey, targetId)
	// bloom本身不可用
	if !ready {
		// Bloom 不可用时保守处理，允许走预热逻辑，避免漏掉真实数据。
		return true
	}
	// 返回当前redis key是否可能存在
	return exists
}

func ShouldLoadHashCache(ctx context.Context, svcCtx *svc.ServiceContext, targetId string, spec hashCacheSpec) bool {
	ready, exists := CheckBloom(ctx, svcCtx, spec.bloomKey, targetId)
	if !ready {
		return true
	}
	return exists
}

// CheckBloom 返回两个值：
// 1. ready: Bloom 本身是否可用
// 2. exists: 如果可用，这个 value 是否“可能存在”
//
// 这里故意把“不存在 Bloom key / RedisBloom 不可用 / 指令报错”都视为 ready=false，
// 让上层走保守兜底逻辑，而不是直接返回 false 造成假阴性。
func CheckBloom(ctx context.Context, svcCtx *svc.ServiceContext, bloomKey string, value string) (bool, bool) {
	kind := bloomMetricKind(bloomKey)
	if svcCtx.RawRedis == nil {
		appmetrics.IncBloomCheck(kind, "client_unavailable")
		return false, false
	}

	keyExists, err := svcCtx.RawRedis.Exists(ctx, bloomKey).Result()
	if err != nil || keyExists == 0 {
		appmetrics.IncBloomCheck(kind, "unavailable")
		return false, false
	}

	res, err := svcCtx.RawRedis.Do(ctx, "BF.EXISTS", bloomKey, value).Int64()
	if err != nil {
		appmetrics.IncBloomCheck(kind, "command_error")
		return false, false
	}
	if res == 1 {
		appmetrics.IncBloomCheck(kind, "hit")
	} else {
		appmetrics.IncBloomCheck(kind, "miss")
	}

	return true, res == 1
}

// AddBloom 在首次写入真实互动数据时，把目标加入 Bloom。
// BF.RESERVE 重复执行时会报“item exists”，这里显式吞掉这个错误。
func AddBloom(ctx context.Context, svcCtx *svc.ServiceContext, bloomKey string, value string) {
	if svcCtx.RawRedis == nil {
		return
	}

	_, err := svcCtx.RawRedis.Do(ctx, "BF.RESERVE", bloomKey, bloomErrorRate, bloomCapacity).Result()
	if err != nil && !isBloomAlreadyExistsErr(err) {
		logx.WithContext(ctx).Errorf("reserve bloom failed: %v", err)
		return
	}

	if _, err := svcCtx.RawRedis.Do(ctx, "BF.ADD", bloomKey, value).Result(); err != nil {
		logx.WithContext(ctx).Errorf("add bloom failed: %v", err)
	}
}

// countSetWithoutDummy 用于对外返回真实计数。
// Redis 中的 DummyUserId 只是防穿透占位节点，不能被算进点赞数/收藏数。
func countSetWithoutDummy(ctx context.Context, svcCtx *svc.ServiceContext, key string) (int64, error) {
	count, err := svcCtx.Redis.ScardCtx(ctx, key)
	if err != nil {
		return 0, err
	}
	hasDummy, err := svcCtx.Redis.SismemberCtx(ctx, key, DummyUserId)
	if err != nil {
		return count, err
	}
	if hasDummy && count > 0 {
		count--
	}
	return count, nil
}

// removeDummyUser / ensureDummyUserIfEmpty / removeDummyRate / ensureDummyRateIfEmpty
// 保证“空集合也有 key，有真实数据时又不会被 Dummy 干扰”。
// 这几个辅助函数是让 Set/Hash 在反复增删后仍然保持缓存语义稳定的关键。
func removeDummyUser(ctx context.Context, svcCtx *svc.ServiceContext, key string) {
	_, _ = svcCtx.Redis.SremCtx(ctx, key, DummyUserId)
	appmetrics.IncDummyOp(setKindFromKey(key), "remove")
	_ = svcCtx.Redis.ExpireCtx(ctx, key, cacheTTLSeconds)
}

func ensureDummyUserIfEmpty(ctx context.Context, svcCtx *svc.ServiceContext, key string) {
	count, err := svcCtx.Redis.ScardCtx(ctx, key)
	if err != nil {
		return
	}
	if count > 0 {
		_ = svcCtx.Redis.ExpireCtx(ctx, key, cacheTTLSeconds)
		return
	}
	_, _ = svcCtx.Redis.SaddCtx(ctx, key, DummyUserId)
	appmetrics.IncDummyOp(setKindFromKey(key), "fill")
	_ = svcCtx.Redis.ExpireCtx(ctx, key, cacheTTLSeconds)
}

func removeDummyRate(ctx context.Context, svcCtx *svc.ServiceContext, key string) {
	_, _ = svcCtx.Redis.HdelCtx(ctx, key, DummyUserId)
	appmetrics.IncDummyOp("post_rate", "remove")
	_ = svcCtx.Redis.ExpireCtx(ctx, key, cacheTTLSeconds)
}

func ensureDummyRateIfEmpty(ctx context.Context, svcCtx *svc.ServiceContext, key string) {
	size, err := svcCtx.Redis.HlenCtx(ctx, key)
	if err != nil {
		return
	}
	if size > 0 {
		_ = svcCtx.Redis.ExpireCtx(ctx, key, cacheTTLSeconds)
		return
	}
	_, _ = svcCtx.Redis.HsetnxCtx(ctx, key, DummyUserId, "0")
	appmetrics.IncDummyOp("post_rate", "fill")
	_ = svcCtx.Redis.ExpireCtx(ctx, key, cacheTTLSeconds)
}

func isBloomAlreadyExistsErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "item exists")
}

func isRedisNil(err error) bool {
	return err == goredis.Nil
}

func bloomMetricKind(bloomKey string) string {
	switch bloomKey {
	case KeyBloomPostLike:
		return "post_like"
	case KeyBloomPostCollect:
		return "post_collect"
	case KeyBloomCommentLike:
		return "comment_like"
	case KeyBloomPostRate:
		return "post_rate"
	default:
		return "unknown"
	}
}

func setKindFromKey(key string) string {
	switch {
	case strings.HasPrefix(key, KeyPostLikeSet):
		return "post_like"
	case strings.HasPrefix(key, KeyPostCollectSet):
		return "post_collect"
	case strings.HasPrefix(key, KeyCommentLikeSet):
		return "comment_like"
	default:
		return "unknown"
	}
}
