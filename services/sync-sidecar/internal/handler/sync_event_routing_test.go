package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"sync-sidecar/internal/config"
	"sync-sidecar/internal/event"
	"sync-sidecar/internal/service"

	elasticsearch "github.com/elastic/go-elasticsearch/v8"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
)

// fakeESServer 启动一个假 ES 服务器，记录收到的请求路径，并返回指定状态码。
func fakeESServer(t *testing.T, statusCode int) (*httptest.Server, *[]string) {
	t.Helper()
	var paths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(statusCode)
		_, _ = w.Write([]byte(`{"result":"created","_id":"test-id"}`))
	}))
	t.Cleanup(srv.Close)
	return srv, &paths
}

// makeSyncHandler 创建一个 SyncHandler，使用假 ES（无 Mongo，无 RabbitMQ）。
func makeSyncHandler(t *testing.T, esSrv *httptest.Server) *SyncHandler {
	t.Helper()
	esClient, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{esSrv.URL},
		Transport: esSrv.Client().Transport,
	})
	if err != nil {
		t.Fatalf("创建 ES client 失败: %v", err)
	}
	loc, _ := time.LoadLocation("Asia/Shanghai")
	return &SyncHandler{
		Infra: &service.Infra{
			ES:    esClient,
			Mongo: nil,
			Cfg: &config.AppConfig{
				AuditEnable:  false,
				IndexName:    "posts",
				TimeLocation: loc,
			},
		},
	}
}

// buildDelivery 构造一条带有 __TypeId__ 的 AMQP 消息。
func buildDelivery(typeId string, body any) amqp.Delivery {
	bodyBytes, _ := json.Marshal(body)
	return amqp.Delivery{
		Headers: amqp.Table{
			"__TypeId__": typeId,
		},
		Body: bodyBytes,
	}
}

// ackTracker 包装 amqp.Delivery，追踪 Ack/Nack 调用次数（通过记录标志位）。
// 注意：amqp.Delivery 的 Ack/Nack 在无真实 Channel 时会返回错误，但不会 panic，
// 我们测的是 Handle() 是否走到正确分支——通过 ES 是否被调用来判断。
func TestHandle_PostCreateEvent_CallsESIndex(t *testing.T) {
	esSrv, paths := fakeESServer(t, http.StatusOK)
	h := makeSyncHandler(t, esSrv)

	e := event.PostCreateEvent{
		Id:    "post-001",
		Title: "测试帖子",
		Tags:  []string{"go"},
	}
	d := buildDelivery("com.example.PostCreateEvent", e)
	h.Handle(d)

	// PostCreateEvent 应该触发一次 ES index 调用（PUT /posts/_doc/post-001）
	found := false
	for _, p := range *paths {
		if p == "/posts/_doc/post-001" {
			found = true
		}
	}
	assert.True(t, found, "PostCreateEvent 应调用 ES index 接口，路径 /posts/_doc/post-001，实际路径: %v", *paths)
}

func TestHandle_PostDeleteEvent_CallsESDelete(t *testing.T) {
	esSrv, paths := fakeESServer(t, http.StatusOK)
	h := makeSyncHandler(t, esSrv)

	e := event.PostDeleteEvent{PostId: "post-999"}
	d := buildDelivery("com.example.PostDeleteEvent", e)
	h.Handle(d)

	// PostDeleteEvent 应该触发 ES delete 调用（DELETE /posts/_doc/post-999）
	found := false
	for _, p := range *paths {
		if p == "/posts/_doc/post-999" {
			found = true
		}
	}
	assert.True(t, found, "PostDeleteEvent 应调用 ES delete 接口，实际路径: %v", *paths)
}

func TestHandle_UnknownEvent_NoESCall(t *testing.T) {
	esSrv, paths := fakeESServer(t, http.StatusOK)
	h := makeSyncHandler(t, esSrv)

	// 未知事件类型应被忽略，不调用 ES
	d := amqp.Delivery{
		Headers: amqp.Table{
			"__TypeId__": "com.example.SomeUnknownEvent",
		},
		Body: []byte(`{}`),
	}
	h.Handle(d)

	assert.Empty(t, *paths, "未知事件类型不应调用 ES，实际路径: %v", *paths)
}

func TestHandle_MissingTypeIdHeader_NoESCall(t *testing.T) {
	esSrv, paths := fakeESServer(t, http.StatusOK)
	h := makeSyncHandler(t, esSrv)

	// 缺少 __TypeId__ 头应直接 Nack 并不处理
	d := amqp.Delivery{
		Headers: amqp.Table{},
		Body:    []byte(`{}`),
	}
	h.Handle(d)

	assert.Empty(t, *paths, "缺少 __TypeId__ 不应调用 ES")
}

func TestHandle_PostCreateEvent_AuditEnabled_SkipsES(t *testing.T) {
	esSrv, paths := fakeESServer(t, http.StatusOK)
	esClient, _ := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{esSrv.URL},
		Transport: esSrv.Client().Transport,
	})
	loc, _ := time.LoadLocation("Asia/Shanghai")
	// 开启审核模式：PostCreateEvent 应被跳过，不写入 ES
	h := &SyncHandler{
		Infra: &service.Infra{
			ES:    esClient,
			Mongo: &mongo.Client{},
			Cfg: &config.AppConfig{
				AuditEnable:  true,
				IndexName:    "posts",
				TimeLocation: loc,
			},
		},
	}

	e := event.PostCreateEvent{Id: "post-002", Title: "待审核帖子"}
	d := buildDelivery("com.example.PostCreateEvent", e)
	h.Handle(d)

	assert.Empty(t, *paths, "开启审核模式时 PostCreateEvent 应跳过 ES 写入")
}

func TestHandle_InvalidJSON_DoesNotCallES(t *testing.T) {
	esSrv, paths := fakeESServer(t, http.StatusOK)
	h := makeSyncHandler(t, esSrv)

	// body 是非法 JSON，json.Unmarshal 会失败，Handle 应 Nack 并不调用 ES
	d := amqp.Delivery{
		Headers: amqp.Table{"__TypeId__": "com.example.PostCreateEvent"},
		Body:    []byte(`{not valid json`),
	}
	h.Handle(d)

	// 非法 JSON 下 ES 不应被调用（json.Unmarshal 失败会让 err != nil，走 Nack 分支）
	// 注意：go-elasticsearch 的 IndexRequest 即使 ES 被调用也会有路径，这里验证 ES 不被触碰
	assert.Empty(t, *paths, "非法 JSON body 不应触发 ES 调用")
}

func TestHandle_PostDeleteEvent_ES404_DoesNotError(t *testing.T) {
	// ES 返回 404 时（文档已不存在），handleDelete 应正常处理，不返回错误
	esSrv, _ := fakeESServer(t, http.StatusNotFound)
	h := makeSyncHandler(t, esSrv)

	e := event.PostDeleteEvent{PostId: "non-existent-post"}
	d := buildDelivery("com.example.PostDeleteEvent", e)

	// Handle() 内部处理 404 时应 Ack 而不是 Nack（不 panic，不返回错误）
	// 验证方式：调用不 panic，对外无 panic
	assert.NotPanics(t, func() { h.Handle(d) }, "ES 404 时 Handle 不应 panic")
}

func TestHandleUpdate_MongoNoDocuments_CallsESDelete(t *testing.T) {
	// handleUpdate 直接单元测试：Mongo FindOne 返回 ErrNoDocuments 时
	// 应走 handleDelete 分支，调用 ES DELETE。
	// 用 mtest 精确模拟 "no documents in result"，用带产品检测头的 fake ES 捕获 DELETE 调用。
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))

	mt.Run("FindOne 返回 ErrNoDocuments 时调用 ES DELETE", func(mt *mtest.T) {
		var paths []string
		fakeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			paths = append(paths, r.URL.Path)
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"result":"deleted","_id":"post-xyz"}`))
		}))
		defer fakeSrv.Close()

		esClient, err := elasticsearch.NewClient(elasticsearch.Config{
			Addresses: []string{fakeSrv.URL},
			Transport: fakeSrv.Client().Transport,
		})
		if err != nil {
			t.Fatalf("创建 ES client 失败: %v", err)
		}

		loc, _ := time.LoadLocation("Asia/Shanghai")
		h := &SyncHandler{
			Infra: &service.Infra{
				ES: esClient,
				Cfg: &config.AppConfig{
					IndexName:    "posts",
					TimeLocation: loc,
				},
			},
		}

		// mtest mock：FindOne 返回空游标 → mongo-driver 解析为 ErrNoDocuments
		mt.AddMockResponses(mtest.CreateCursorResponse(0, "rednote.posts", mtest.FirstBatch))

		coll := mt.DB.Collection("posts")
		err = h.handleUpdate(context.Background(), esClient, coll,
			event.PostUpdateEvent{PostId: "post-xyz"})

		// handleUpdate 遇到 ErrNoDocuments 应调用 handleDelete，不返回 error
		assert.NoError(t, err, "ErrNoDocuments 应走 handleDelete 分支，不返回错误")
		assert.NotEmpty(t, paths, "handleDelete 应调用 ES DELETE")
		assert.Contains(t, paths[0], "post-xyz", "ES DELETE 路径应含帖子 ID")
	})
}
