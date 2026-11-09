package logic

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	appmetrics "search-rpc/internal/metrics"
	"search-rpc/internal/svc"
	"search-rpc/search"

	"github.com/zeromicro/go-zero/core/logx"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type SearchLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewSearchLogic(ctx context.Context, svcCtx *svc.ServiceContext) *SearchLogic {
	return &SearchLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 解析 Sidecar 写入的 ES 帖子文档。
type PostEsDoc struct {
	Id           string   `json:"id"`
	Title        string   `json:"title"`
	Content      string   `json:"content"`
	Cover        string   `json:"cover"`
	Type         int32    `json:"type"`
	LikeCount    int32    `json:"likeCount"`
	UserId       int64    `json:"userId"`
	UserNickname string   `json:"userNickname"`
	UserAvatar   string   `json:"userAvatar"`
	Tags         []string `json:"tags"`
	CoverWidth   int32    `json:"coverWidth"`
	CoverHeight  int32    `json:"coverHeight"`
	CreatedAt    string   `json:"createdAt"`
}

func (l *SearchLogic) Search(in *search.SearchRequest) (*search.SearchResponse, error) {
	start := time.Now()
	// 登录用户的搜索历史由本服务异步写 Mongo，不再经过旧 Java 搜索事件。
	if in.UserId > 0 && in.Keyword != "" {
		go l.recordSearchHistory(in.UserId, in.Keyword)
	}

	// 2. 构建 ES 查询 Body (对应 Java: Step 3)
	// 使用 Map 构建 JSON 结构
	queryBool := map[string]interface{}{}
	mustClauses := []map[string]interface{}{}
	filterClauses := []map[string]interface{}{}

	// A. 关键词匹配 (MultiMatch)
	if in.Keyword != "" {
		mustClauses = append(mustClauses, map[string]interface{}{
			"multi_match": map[string]interface{}{
				"query":  in.Keyword,
				"fields": []string{"title^3", "title.pinyin^1.5", "content", "tags^2", "tags.pinyin^1.0"},
			},
		})
	} else {
		mustClauses = append(mustClauses, map[string]interface{}{
			"match_all": map[string]interface{}{},
		})
	}

	// B. 标签过滤 (Term)
	if in.Tag != "" {
		filterClauses = append(filterClauses, map[string]interface{}{
			"term": map[string]interface{}{
				"tags.keyword": in.Tag,
			},
		})
	}

	queryBool["must"] = mustClauses
	if len(filterClauses) > 0 {
		queryBool["filter"] = filterClauses
	}

	// 3. 构建排序与最终 Query (对应 Java: Step 3.2)
	var finalQuery map[string]interface{}
	var sortRule []map[string]interface{}

	if in.Sort == "hot" {
		// [F] 综合热度: Function Score
		nowStr := time.Now().Format("2006-01-02T15:04:05.000")
		finalQuery = map[string]interface{}{
			"function_score": map[string]interface{}{
				"query": map[string]interface{}{"bool": queryBool},
				"functions": []map[string]interface{}{
					{
						// 点赞加分
						"filter": map[string]interface{}{"match_all": map[string]interface{}{}},
						"field_value_factor": map[string]interface{}{
							"field":    "likeCount",
							"modifier": "log1p",
							"factor":   1.0,
							"missing":  0.0,
						},
					},
					{
						// 高斯衰减
						"filter": map[string]interface{}{"match_all": map[string]interface{}{}},
						"gauss": map[string]interface{}{
							"createdAt": map[string]interface{}{
								"origin": nowStr,
								"scale":  "3d",
								"offset": "1d",
								"decay":  0.5,
							},
						},
					},
				},
				"boost_mode": "multiply",
				"score_mode": "sum",
			},
		}
		// 排序: 分数倒序, 时间倒序
		sortRule = []map[string]interface{}{
			{"_score": "desc"},
			{"createdAt": "desc"},
		}
	} else {
		// 普通查询，不需要 Function Score
		finalQuery = map[string]interface{}{"bool": queryBool}

		switch in.Sort {
		case "new":
			sortRule = []map[string]interface{}{{"createdAt": "desc"}}
		case "old":
			sortRule = []map[string]interface{}{{"createdAt": "asc"}}
		case "likes":
			sortRule = []map[string]interface{}{{"likeCount": "desc"}}
		default:
			if in.Keyword != "" {
				sortRule = []map[string]interface{}{{"_score": "desc"}, {"createdAt": "desc"}}
			} else {
				sortRule = []map[string]interface{}{{"createdAt": "desc"}}
			}
		}
	}

	// 4. 组装完整请求体
	from := (in.Page - 1) * in.PageSize
	if from < 0 {
		from = 0
	}

	reqBody := map[string]interface{}{
		"query": finalQuery,
		"sort":  sortRule,
		"from":  from,
		"size":  in.PageSize,
		"_source": []string{
			"id", "title", "content", "cover", "coverWidth", "coverHeight",
			"type", "tags", "userId", "userNickname", "userAvatar", "likeCount", "createdAt",
		},
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(reqBody); err != nil {
		appmetrics.ObserveRequest("search", "encode_error", time.Since(start))
		return nil, err
	}

	// 5. 执行 ES 搜索
	esStart := time.Now()
	res, err := l.svcCtx.Es.Search(
		l.svcCtx.Es.Search.WithContext(l.ctx),
		l.svcCtx.Es.Search.WithIndex("posts"), // 索引名
		l.svcCtx.Es.Search.WithBody(&buf),
		l.svcCtx.Es.Search.WithTrackTotalHits(true),
	)
	if err != nil {
		appmetrics.ObserveESQuery("search", time.Since(esStart))
		appmetrics.ObserveRequest("search", "es_error", time.Since(start))
		return nil, err
	}
	defer res.Body.Close()
	appmetrics.ObserveESQuery("search", time.Since(esStart))

	if res.IsError() {
		appmetrics.ObserveRequest("search", "es_status_error", time.Since(start))
		return nil, fmt.Errorf("ES error: %s", res.String())
	}

	// 6. 解析结果 (对应 Java: searchHits 解析)
	var esResponse struct {
		Hits struct {
			Total struct {
				Value int64 `json:"value"`
			} `json:"total"`
			Hits []struct {
				Source    PostEsDoc           `json:"_source"`
				Highlight map[string][]string `json:"highlight"`
			} `json:"hits"`
		} `json:"hits"`
	}

	if err := json.NewDecoder(res.Body).Decode(&esResponse); err != nil {
		appmetrics.ObserveRequest("search", "decode_error", time.Since(start))
		return nil, err
	}

	// 7. 转换为 Proto 响应
	var items []*search.SearchPostVO
	for _, hit := range esResponse.Hits.Hits {
		doc := hit.Source

		summary := buildSearchSummary(doc.Content, hit.Highlight)

		item := &search.SearchPostVO{
			Id:          doc.Id,
			Title:       doc.Title,
			Content:     summary,
			Cover:       doc.Cover,
			Type:        doc.Type,
			LikeCount:   doc.LikeCount,
			Tags:        doc.Tags,
			CoverWidth:  doc.CoverWidth,
			CoverHeight: doc.CoverHeight,
			Author: &search.UserVO{
				UserId:   fmt.Sprintf("%d", doc.UserId),
				Nickname: doc.UserNickname,
				Avatar:   doc.UserAvatar,
			},
			// 个性化互动状态由 Go 网关调用 interaction-rpc 批量补齐。
			IsLiked:   false,
			CreatedAt: doc.CreatedAt,
		}
		items = append(items, item)
	}

	appmetrics.ObserveRequest("search", "success", time.Since(start))
	return &search.SearchResponse{
		Items: items,
		Total: esResponse.Hits.Total.Value,
	}, nil
}

// 辅助方法：记录搜索历史 (Upsert)
func (l *SearchLogic) recordSearchHistory(userId int64, keyword string) {
	collection := l.svcCtx.Mongo.Collection("search_histories")

	filter := bson.M{"userId": userId, "keyword": keyword}
	update := bson.M{
		"$set": bson.M{
			"updatedAt": time.Now(),
		},
		"$setOnInsert": bson.M{
			"userId":    userId,
			"keyword":   keyword,
			"createdAt": time.Now(),
		},
	}
	opts := options.Update().SetUpsert(true)

	_, err := collection.UpdateOne(context.Background(), filter, update, opts)
	if err != nil {
		logx.Errorf("Failed to record search history: %v", err)
	}
}

func buildSearchSummary(content string, highlight map[string][]string) string {
	for _, field := range []string{"content", "title", "tags"} {
		if frags, ok := highlight[field]; ok && len(frags) > 0 && strings.TrimSpace(frags[0]) != "" {
			return frags[0]
		}
	}

	summary := strings.TrimSpace(content)
	if summary == "" {
		return ""
	}
	if len([]rune(summary)) > 50 {
		return string([]rune(summary)[:50]) + "..."
	}
	return summary
}
