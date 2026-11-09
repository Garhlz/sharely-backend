package logic

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"search-rpc/search"

	"github.com/stretchr/testify/require"
)

func TestSearchQueryAndResponseContract(t *testing.T) {
	for _, sort := range []string{"new", "likes", "hot"} {
		t.Run(sort, func(t *testing.T) {
			requests := make(chan map[string]any, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					http.Error(w, err.Error(), 400)
					return
				}
				requests <- body
				w.Header().Set("X-Elastic-Product", "Elasticsearch")
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{"hits": map[string]any{"total": map[string]any{"value": 2}, "hits": []any{
					map[string]any{"_source": map[string]any{"id": "first", "title": "深圳", "content": strings.Repeat("长", 80), "likeCount": 10, "userId": int64(7)}},
					map[string]any{"_source": map[string]any{"id": "second", "title": "校园", "content": "正文"}, "highlight": map[string]any{"content": []string{"<em>校园</em>片段"}}},
				}}})
			}))
			defer server.Close()
			// 匿名查询避免启动与本用例无关的搜索历史协程。
			resp, err := NewSearchLogic(context.Background(), makeSearchSvcCtx(t, server)).Search(&search.SearchRequest{Keyword: "深圳", Tag: "校园", Sort: sort, Page: 2, PageSize: 10})
			require.NoError(t, err)
			var body map[string]any
			select {
			case body = <-requests:
			default:
				t.Fatal("ES query was not captured")
			}
			require.Equal(t, float64(10), body["from"])
			require.Equal(t, float64(10), body["size"])
			sorts := body["sort"].([]any)
			query := body["query"].(map[string]any)
			switch sort {
			case "new":
				require.Equal(t, map[string]any{"createdAt": "desc"}, sorts[0])
			case "likes":
				require.Equal(t, map[string]any{"likeCount": "desc"}, sorts[0])
			case "hot":
				require.Equal(t, map[string]any{"_score": "desc"}, sorts[0])
				score := query["function_score"].(map[string]any)
				require.Len(t, score["functions"], 2)
				query = score["query"].(map[string]any)
			}
			boolean := query["bool"].(map[string]any)
			match := boolean["must"].([]any)[0].(map[string]any)["multi_match"].(map[string]any)
			require.Equal(t, "深圳", match["query"])
			require.Contains(t, match["fields"], "title.pinyin^1.5")
			filters := boolean["filter"].([]any)
			require.Equal(t, map[string]any{"term": map[string]any{"tags.keyword": "校园"}}, filters[0])
			require.EqualValues(t, 2, resp.Total)
			require.Len(t, resp.Items, 2)
			require.Equal(t, "first", resp.Items[0].Id, "preserve ES order; this is not a real ES sorting test")
			require.Equal(t, strings.Repeat("长", 50)+"...", resp.Items[0].Content)
			require.Equal(t, "<em>校园</em>片段", resp.Items[1].Content)
			require.Equal(t, "7", resp.Items[0].Author.UserId)
		})
	}
}
