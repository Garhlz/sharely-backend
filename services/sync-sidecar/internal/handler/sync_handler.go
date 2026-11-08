package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"sync-sidecar/internal/event"
	"sync-sidecar/internal/model"
	"sync-sidecar/internal/obslog"
	"sync-sidecar/internal/service"
	"sync-sidecar/internal/telemetry"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	mongoOptions "go.mongodb.org/mongo-driver/mongo/options"
	"go.opentelemetry.io/otel"
	oteltrace "go.opentelemetry.io/otel/trace"
)

type SyncHandler struct {
	Infra   *service.Infra
	indexMu sync.Mutex

	// reindexMu 用来保护下面的 reindexRunning，避免同时触发多次全量回填。
	reindexMu      sync.Mutex
	reindexRunning bool
}

func (h *SyncHandler) Handle(d amqp.Delivery) {
	h.indexMu.Lock()
	defer h.indexMu.Unlock()
	ctx := telemetry.ExtractDeliveryContext(context.Background(), d)
	ctx, span := otel.Tracer("sync-sidecar").Start(ctx, "mq "+d.RoutingKey, oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	defer span.End()

	typeId, ok := d.Headers["__TypeId__"].(string)
	if !ok {
		d.Nack(false, false)
		return
	}
	className := typeId[strings.LastIndex(typeId, ".")+1:]

	var err error
	es := h.Infra.ES
	cfg := h.Infra.Cfg

	switch className {
	case "PostCreateEvent":
		// 未开启审核时，发帖即发布，直接根据事件写入 ES。
		if cfg.AuditEnable {
			obslog.DeliveryInfof(d, "es sync skip event=PostCreateEvent reason=audit_enabled")
			d.Ack(false)
			return
		}
		var e event.PostCreateEvent
		if err = json.Unmarshal(d.Body, &e); err == nil {
			obslog.DeliveryInfof(d, "es sync receive event=PostCreateEvent postId=%s", e.Id)
			if h.Infra.Mongo != nil {
				err = h.handleUpdate(ctx, es, h.Infra.Mongo.Database("rednote").Collection("posts"), event.PostUpdateEvent{PostId: e.Id})
			} else {
				err = h.handleCreate(ctx, es, e)
			}
		}
	case "PostAuditPassEvent":
		// 开启审核时，只有审核通过后才应进入 ES。这里直接回查 Mongo，拿到完整帖子数据。
		if !cfg.AuditEnable {
			obslog.DeliveryInfof(d, "es sync skip event=PostAuditPassEvent reason=audit_disabled")
			d.Ack(false)
			return
		}
		var e event.PostAuditPassEvent
		if err = json.Unmarshal(d.Body, &e); err == nil {
			obslog.DeliveryInfof(d, "es sync receive event=PostAuditPassEvent postId=%s", e.Id)
			postColl := h.Infra.Mongo.Database("rednote").Collection("posts")
			err = h.handleUpdate(ctx, es, postColl, event.PostUpdateEvent{PostId: e.Id})
		}
	case "PostDeleteEvent":
		var e event.PostDeleteEvent
		if err = json.Unmarshal(d.Body, &e); err == nil {
			obslog.DeliveryInfof(d, "es sync receive event=PostDeleteEvent postId=%s", e.PostId)
			err = h.handleDelete(ctx, es, e)
		}
	case "PostUpdateEvent":
		var e event.PostUpdateEvent
		if err = json.Unmarshal(d.Body, &e); err == nil {
			obslog.DeliveryInfof(d, "es sync receive event=PostUpdateEvent postId=%s", e.PostId)
			// 需要回查 Mongo 以获取最新完整数据
			postColl := h.Infra.Mongo.Database("rednote").Collection("posts")
			err = h.handleUpdate(ctx, es, postColl, e)
		}
	case "UserUpdateEvent":
		var e event.UserUpdateEvent
		if err = json.Unmarshal(d.Body, &e); err == nil {
			// 这里只负责 ES 的数据同步，Mongo 的归 UserHandler 管
			err = h.handleESUserUpdate(ctx, es, e)
		}
	default:
		obslog.DeliveryInfof(d, "es sync ignore unknownEvent=%s", className)
		d.Ack(false)
		return
	}

	if err != nil {
		obslog.DeliveryErrorf(d, "es sync handle error event=%s err=%v", className, err)
		// 合法消息的临时下游故障保留重试；非法 JSON 进入死信，避免毒消息阻塞。
		if json.Valid(d.Body) {
			time.Sleep(time.Second)
			d.Nack(false, true)
		} else {
			d.Nack(false, false)
		}
	} else {
		d.Ack(false)
	}
}

// --- 具体业务逻辑 ---

func (h *SyncHandler) handleCreate(ctx context.Context, es *elasticsearch.Client, e event.PostCreateEvent) error {
	resources := []string{}
	if len(e.Images) > 0 {
		resources = e.Images
	} else if e.Video != "" {
		resources = append(resources, e.Video)
	}
	// ES 时间格式化
	formattedTime := time.Now().In(h.Infra.Cfg.TimeLocation).Format("2006-01-02T15:04:05.000")

	doc := model.PostEsDoc{
		Id:           e.Id,
		UserId:       e.UserId,
		Title:        e.Title,
		Content:      e.Content,
		Tags:         e.Tags,
		Type:         e.Type,
		Resources:    resources,
		Cover:        e.Cover,
		CoverWidth:   e.CoverWidth,
		CoverHeight:  e.CoverHeight,
		UserNickname: e.UserNickname,
		UserAvatar:   e.UserAvatar,
		LikeCount:    0,
		CreatedAt:    formattedTime,
	}

	err := h.indexEs(ctx, es, doc.Id, doc)
	if err == nil {
		obslog.Infof("es sync indexed postId=%s", e.Id)
	}
	return err
}

func (h *SyncHandler) handleDelete(ctx context.Context, es *elasticsearch.Client, e event.PostDeleteEvent) error {
	req := esapi.DeleteRequest{
		Index:      h.Infra.Cfg.IndexName,
		DocumentID: e.PostId,
	}
	res, err := req.Do(ctx, es)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() && res.StatusCode != 404 {
		return fmt.Errorf("ES Delete Error: %s", res.Status())
	}
	obslog.Infof("es sync deleted postId=%s", e.PostId)
	return nil
}

func (h *SyncHandler) handleUpdate(ctx context.Context, es *elasticsearch.Client, coll *mongo.Collection, e event.PostUpdateEvent) error {
	objId, _ := primitive.ObjectIDFromHex(e.PostId)

	var mongoDoc model.PostDoc
	err := coll.FindOne(ctx, bson.M{"_id": objId}).Decode(&mongoDoc)

	// 如果 Mongo 里没了，或者状态不对，ES 也要删掉
	if err == mongo.ErrNoDocuments || mongoDoc.IsDeleted == 1 || mongoDoc.Status != 1 {
		obslog.Infof("es sync remove invalid postId=%s", e.PostId)
		return h.handleDelete(ctx, es, event.PostDeleteEvent{PostId: e.PostId})
	} else if err != nil {
		return err
	}

	formattedTime := mongoDoc.CreatedAt.In(h.Infra.Cfg.TimeLocation).Format("2006-01-02T15:04:05.000")

	esDoc := model.PostEsDoc{
		Id:           mongoDoc.ID.Hex(),
		UserId:       mongoDoc.UserId,
		Title:        mongoDoc.Title,
		Content:      mongoDoc.Content,
		Tags:         mongoDoc.Tags,
		Type:         mongoDoc.Type,
		Resources:    mongoDoc.Resources,
		Cover:        mongoDoc.Cover,
		CoverWidth:   mongoDoc.CoverWidth,
		CoverHeight:  mongoDoc.CoverHeight,
		UserNickname: mongoDoc.UserNickname,
		UserAvatar:   mongoDoc.UserAvatar,
		LikeCount:    mongoDoc.LikeCount,
		CreatedAt:    formattedTime,
	}

	err = h.indexEs(ctx, es, esDoc.Id, esDoc)
	if err == nil {
		obslog.Infof("es sync updated postId=%s", e.PostId)
	}
	return err
}

func (h *SyncHandler) handleESUserUpdate(ctx context.Context, es *elasticsearch.Client, e event.UserUpdateEvent) error {
	// 使用 Painless 脚本更新所有文档
	source := `ctx._source.userNickname = params.nickname; ctx._source.userAvatar = params.avatar;`

	req := esapi.UpdateByQueryRequest{
		Index: []string{h.Infra.Cfg.IndexName},
		Body: strings.NewReader(fmt.Sprintf(`{
            "script": { "source": "%s", "params": { "nickname": "%s", "avatar": "%s" } },
            "query": { "term": { "userId": %d } }
        }`, source, e.NewNickname, e.NewAvatar, e.UserId)),
	}

	res, err := req.Do(ctx, es)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.IsError() {
		return fmt.Errorf("ES UpdateByQuery Error: %s", res.Status())
	}

	obslog.Infof("es sync user batch updated userId=%d", e.UserId)
	return nil
}

func (h *SyncHandler) indexEs(ctx context.Context, es *elasticsearch.Client, id string, doc interface{}) error {
	data, err := json.Marshal(doc)
	if err != nil {
		return err
	}

	var lastErr error
	for attempt := 1; attempt <= 4; attempt++ {
		req := esapi.IndexRequest{
			Index:      h.Infra.Cfg.IndexName,
			DocumentID: id,
			Body:       bytes.NewReader(data),
		}

		res, err := req.Do(ctx, es)
		if err != nil {
			lastErr = err
		} else {
			body, _ := io.ReadAll(res.Body)
			_ = res.Body.Close()

			if !res.IsError() {
				return nil
			}

			// ES 在负载高、线程池写满、主分片暂不可用时可能返回 429。
			// 对这种瞬时错误做有限重试，避免单条增量同步直接丢失。
			if res.StatusCode == http.StatusTooManyRequests && attempt < 4 {
				wait := time.Duration(attempt*attempt) * 500 * time.Millisecond
				obslog.Errorf("es sync retry status=429 attempt=%d postId=%s wait=%s body=%s", attempt, id, wait, strings.TrimSpace(string(body)))
				time.Sleep(wait)
				lastErr = fmt.Errorf("ES Index Error [%s]: %s", res.Status(), strings.TrimSpace(string(body)))
				continue
			}

			return fmt.Errorf("ES Index Error [%s]: %s", res.Status(), strings.TrimSpace(string(body)))
		}

		if attempt < 4 {
			wait := time.Duration(attempt*attempt) * 500 * time.Millisecond
			obslog.Errorf("es sync retry request_failed attempt=%d postId=%s wait=%s err=%v", attempt, id, wait, lastErr)
			time.Sleep(wait)
		}
	}

	return lastErr
}

type reindexResponse struct {
	Code       int    `json:"code"`
	Message    string `json:"message"`
	Total      int    `json:"total,omitempty"`
	DurationMs int64  `json:"durationMs,omitempty"`
}

// HandleReindexPosts 提供一个仅供管理侧手动触发的全量回填入口。
// 它会清空 ES 中的旧帖子索引，再从 Mongo 中分页扫描 status=1 且 isDeleted=0 的帖子重新写入。
func (h *SyncHandler) HandleReindexPosts(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(w).Encode(reindexResponse{
			Code:    http.StatusMethodNotAllowed,
			Message: "仅支持 POST",
		})
		return
	}

	if r.Header.Get("X-Admin-Token") != h.Infra.Cfg.AdminToken {
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(reindexResponse{
			Code:    http.StatusForbidden,
			Message: "无权操作，Token 错误",
		})
		return
	}

	if !h.beginReindex() {
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(reindexResponse{
			Code:    http.StatusConflict,
			Message: "已有全量回填任务正在执行",
		})
		return
	}
	defer h.finishReindex()

	start := time.Now()
	// 全量回填不直接复用请求上下文，避免浏览器/代理提前断开后任务被意外取消。
	// 同时给它一个明确超时，避免 ES 或 Mongo 卡死时 curl 一直无响应。
	reindexCtx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	total, err := h.reindexPostsFromMongo(reindexCtx)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(reindexResponse{
			Code:       http.StatusInternalServerError,
			Message:    err.Error(),
			DurationMs: time.Since(start).Milliseconds(),
		})
		return
	}

	_ = json.NewEncoder(w).Encode(reindexResponse{
		Code:       http.StatusOK,
		Message:    "同步完成",
		Total:      total,
		DurationMs: time.Since(start).Milliseconds(),
	})
}

func (h *SyncHandler) beginReindex() bool {
	h.reindexMu.Lock()
	defer h.reindexMu.Unlock()

	if h.reindexRunning {
		return false
	}
	h.reindexRunning = true
	return true
}

func (h *SyncHandler) finishReindex() {
	h.reindexMu.Lock()
	defer h.reindexMu.Unlock()
	h.reindexRunning = false
}

func (h *SyncHandler) reindexPostsFromMongo(ctx context.Context) (int, error) {
	h.indexMu.Lock()
	defer h.indexMu.Unlock()
	obslog.Infof("reindex start source=mongo target=%s", h.Infra.Cfg.IndexName)
	obslog.Infof("reindex prepare clear index=%s", h.Infra.Cfg.IndexName)

	if err := h.clearEsIndex(ctx); err != nil {
		return 0, err
	}
	obslog.Infof("reindex scan start collection=posts")

	coll := h.Infra.Mongo.Database("rednote").Collection("posts")
	filter := bson.M{
		"status":    1,
		"isDeleted": 0,
	}

	opts := mongoOptions.Find().
		SetBatchSize(int32(h.Infra.Cfg.ReindexBatch)).
		SetSort(bson.D{{Key: "_id", Value: 1}})

	cursor, err := coll.Find(ctx, filter, opts)
	if err != nil {
		return 0, fmt.Errorf("Mongo 查询失败: %w", err)
	}
	defer cursor.Close(ctx)

	total := 0
	for cursor.Next(ctx) {
		var mongoDoc model.PostDoc
		if err := cursor.Decode(&mongoDoc); err != nil {
			return total, fmt.Errorf("Mongo 文档解码失败: %w", err)
		}

		formattedTime := mongoDoc.CreatedAt.In(h.Infra.Cfg.TimeLocation).Format("2006-01-02T15:04:05.000")
		esDoc := model.PostEsDoc{
			Id:           mongoDoc.ID.Hex(),
			UserId:       mongoDoc.UserId,
			Title:        mongoDoc.Title,
			Content:      mongoDoc.Content,
			Tags:         mongoDoc.Tags,
			Type:         mongoDoc.Type,
			Resources:    mongoDoc.Resources,
			Cover:        mongoDoc.Cover,
			CoverWidth:   mongoDoc.CoverWidth,
			CoverHeight:  mongoDoc.CoverHeight,
			UserNickname: mongoDoc.UserNickname,
			UserAvatar:   mongoDoc.UserAvatar,
			LikeCount:    mongoDoc.LikeCount,
			CreatedAt:    formattedTime,
		}

		if err := h.indexEs(ctx, h.Infra.ES, esDoc.Id, esDoc); err != nil {
			return total, fmt.Errorf("ES 写入失败, postId=%s: %w", esDoc.Id, err)
		}
		total++
		if total%100 == 0 {
			obslog.Infof("reindex progress indexed=%d", total)
		}
	}

	if err := cursor.Err(); err != nil {
		return total, fmt.Errorf("Mongo 游标遍历失败: %w", err)
	}

	obslog.Infof("reindex completed indexed=%d", total)
	return total, nil
}

func (h *SyncHandler) clearEsIndex(ctx context.Context) error {
	refresh := true
	obslog.Infof("reindex clearing index=%s", h.Infra.Cfg.IndexName)
	req := esapi.DeleteByQueryRequest{
		Index:   []string{h.Infra.Cfg.IndexName},
		Refresh: &refresh,
		Body:    strings.NewReader(`{"query":{"match_all":{}}}`),
	}

	res, err := req.Do(ctx, h.Infra.ES)
	if err != nil {
		return fmt.Errorf("清空 ES 索引失败: %w", err)
	}
	defer res.Body.Close()

	// ES 索引不存在时允许继续，后续 IndexRequest 会自动建索引。
	if res.StatusCode == http.StatusNotFound {
		obslog.Infof("reindex index missing index=%s", h.Infra.Cfg.IndexName)
		return nil
	}
	if res.IsError() {
		return fmt.Errorf("清空 ES 索引失败: %s", res.Status())
	}

	obslog.Infof("reindex cleared index=%s", h.Infra.Cfg.IndexName)
	return nil
}
