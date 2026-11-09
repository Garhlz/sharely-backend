# 测试、验收与覆盖率

测试命令、验证记录与历史覆盖率统一维护在本文；实现与切换条件见 [架构与迁移](architecture.md)，未完成项见 [TODO](todo-list.md)。

## 分层与默认执行

| 层次 | 范围 | 依赖 |
|---|---|---|
| 单元/组件 | DTO、鉴权、Logic、Handler、事务命令 | fake store、miniredis、mtest、httptest |
| 真实中间件 | Mongo 事务、MQ 恢复、通知重放、Redis 缓存 | 显式配置的隔离容器 |
| Smoke | 网关到数据库、MQ、Java、搜索的完整业务链路 | 完整集群与测试账号 |

Go 服务为独立模块，在相应目录执行：

```bash
go test ./...
go vet ./...
```

CI 对 gateway-api、user-rpc、interaction-rpc、search-rpc、comment-rpc、notification-rpc、sync-sidecar 分别执行测试与 vet，另执行 Java 默认测试和 Compose 配置检查。普通 CI 不启动完整中间件，真实集成用例缺少显式环境变量时 Skip。

Java 在 services/platform-java 执行：

```bash
mvn test
mvn test -Pintegration
RUN_EXTERNAL_TESTS=true EXTERNAL_TEST_EMAIL=your-test-mailbox@example.com mvn test -Pexternal
```

默认测试不连接真实中间件；integration 使用独立数据库；external 仅使用专用测试账号，可能发送邮件或上传 OSS。完整 Smoke 可设置 `AI_ENABLED=false` 关闭 AI 调用。

## 真实中间件测试

仅将以下变量指向隔离实例，禁止使用共享开发库或 Broker。测试创建随机 sharely_*_test_* 数据库、随机队列与 Key，并清理自己的数据；不得执行共享数据的 dropCollection/deleteAll/FLUSHDB。

```bash
# 在 services/interaction-rpc 执行；MQ 测试同时需要前两个变量。
export SHARELY_TEST_MONGO_URI='mongodb://localhost:27017/?directConnection=true'
export SHARELY_TEST_RABBITMQ_URL='amqp://test-user:test-password@localhost:5672/'
go test ./internal/store -count=3 -v

# 在 services/notification-rpc 执行，复用隔离实例。
go test ./internal/consumer -count=3 -v

# 在 services/interaction-rpc 执行；普通 Redis 即可，不依赖 RedisBloom。
SHARELY_TEST_REDIS_ADDR=localhost:6379 go test ./internal/logic -run TestUnifiedRedisCacheWithRealRedis -v
```

| 模块 | 关键回归 |
|---|---|
| Gateway | Header/Cookie、Token 版本匹配/撤销/缺失、匿名降级、Java 代理、响应映射 |
| User | 登录注册状态机、Refresh 轮换/JTI/黑名单、验证码缺失、Token TTL |
| Interaction | 重复点赞/取消、改分、评论点赞落库、并发统计、删除目标、Outbox 失败回滚与恢复、无 Bloom 的缓存回填、Lua/锁 TTL |
| Notification | Upsert/列表/未读、事件重放不重置已读、旧版本、临时失败重试、非法消息死信 |
| Comment | 创建失败补偿、权限、根评论软删、子评论硬删、回复计数、分页 |
| Search | 建议词去重/原词策略、摘要与高亮、ES 错误 |
| Sidecar | 事件路由、索引更新/删除、重建互斥、管理鉴权、MQ 重订阅 |

具体测试文件以各服务 `*_test.go` 与 Java `src/test` 为准，不再维护第二份逐文件清单。

## 旧 Java 场景与迁移后覆盖

此表用于迁移验收，不将“旧测试已禁用”视为“新实现已有覆盖”。两个原文件保留为场景参考；它们仍指向旧 Java 路由、旧 Redis Key 或共享库，禁止直接取消 Disabled 后运行。旧 RedisKey/PostRateDTO 已下沉至 Java 测试目录，不再进入生产包。

旧 ES 实体、Repository 与 es-settings.json 同样仅作测试夹具，Elasticsearch starter 为 test 范围。DataSyncTest.syncAllMongoToEs 是会清空索引的旧手工工具，保留源文件但禁用；当前重建入口及相关回归在 Sidecar，不以运行该旧工具作为验收。

| 旧用例 | 当前对应位置 | 验证范围与差异 |
|---|---|---|
| InteractionIntegrationTest.testLikeAndUnlikePost | [TestMongoTransactions](../services/interaction-rpc/internal/store/interactions_test.go)、[缓存与写入回归](../services/interaction-rpc/internal/logic/like_post_logic_test.go) | 真实隔离 Mongo 验证明细、计数与取消；缓存从“先加成员”改为提交后失效 |
| InteractionIntegrationTest.testRatePost | TestMongoTransactions、[TestRateCacheLoadsFactsWithoutBloom](../services/interaction-rpc/internal/logic/cache_read_test.go) | 首次评分与改分的均值/人数、Hash 回填；不使用旧 Redis 写后立即读断言 |
| InteractionIntegrationTest.testCommentLike | TestMongoTransactions | 已补真实评论点赞明细、评论计数、重复请求与取消 |
| InteractionIntegrationTest.testDuplicateLike | TestMongoTransactions、[事务命令回归](../services/interaction-rpc/internal/store/transaction_test.go) | 重复点赞不增加事实或 Outbox；真实集成与 mock 命令测试分开统计 |
| SearchModuleTest.testSuggestionWithPinyin | [建议词逻辑测试](../services/search-rpc/internal/logic/suggest_integration_test.go)、[建议词组装](../services/search-rpc/internal/logic/suggest_dedup_test.go) | 假 ES 验证原词顺序、高亮、去重；不能代替真实拼音分析器测试，旧场景暂保留 |
| SearchModuleTest.testSearchSorting | [TestSearchQueryAndResponseContract](../services/search-rpc/internal/logic/search_contract_test.go) | 验证 new/likes/hot 查询、分页、标签与响应顺序，不宣称实际 ES 排名已验证 |
| SearchModuleTest.testSearchContentFilter | TestSearchQueryAndResponseContract、[摘要测试](../services/search-rpc/internal/logic/search_score_test.go) | 当前契约返回高亮或最多 50 字摘要，不再要求 content 为空；明确为契约演进 |

本轮另外补充 [Java FOLLOW 通知适配测试](../services/platform-java/src/test/java/com/szu/afternoon3/platform/listener/InteractionEventListenerTest.java)，验证关注通知仍转交 Go、已迁移互动不会再次由 Java 处理。真实 ES 拼音召回和排序仍需隔离 ES 的用例，不能用请求体断言冒充。

## 跨服务 Smoke

在专用测试集群执行 `./scripts/smoke/run_all.sh http://localhost:8090`。脚本会创建业务数据并修改测试账号通知状态，默认使用 tech@test.com / photo@test.com，账号参数见 [公共脚本](../scripts/smoke/lib.sh)。

| 脚本 | 验证链路 |
|---|---|
| 01_post_to_es | 发帖 → Sidecar → 搜索与建议词 |
| 02_comment_soft_delete | 根评论软删除后显示占位 |
| 03_notification_unread | 点赞 → 通知 → 未读增加 → 全部已读 |
| 04_sub_comment_reply_count | 子评论删除后 replyCount 扣减 |
| 05_auth_cookie_vs_header | Header/Cookie 鉴权与匿名访问 |

fail() 必须返回非零；只看最终“通过”文字不足以判断结果。异步断言使用有上限的轮询。

## 验证记录与范围

- **2026-11-09 迁移残留清理**：七个 Go 模块的默认测试、build、vet 通过；隔离 Mongo 事务测试三轮及真实 Redis 缓存测试通过。Java `mvn clean package` 的 14 个默认测试通过，生产 JAR 检查确认不含旧 RedisKey/PostRateDTO/ES 夹具与 ES 客户端依赖，FOLLOW Listener 保留。两份 Compose 配置及文档链接检查通过；未重跑完整 Smoke，未验证真实 ES 拼音与排名。旧互动/搜索测试未删除，仍禁用并保留场景对照。
- **2026-11-09 可观测性改进**：网关、互动、通知、Sidecar 默认测试与 vet 通过；新增积压快照、Span 父子关系、消费确认透传及结构化日志回归。隔离 Mongo/MQ 的互动与通知集成各三轮通过，验证 Broker 不可达时积压采集仍工作、恢复发布后排空、请求标识与 Trace 透传。Prometheus/Promtail 配置校验、告警规则单元测试与 Dashboard JSON 检查通过；未启动完整 Grafana/Loki/Jaeger 集群或验证外部告警通知。
- **2026-11-09 文档整理前回归**：gateway-api、user-rpc、interaction-rpc 的 `go test ./...` 与 `go vet ./...` 通过；未配置真实中间件的集成用例按约定 Skip，本轮未重新执行完整 Smoke。
- **2026-11-08 互动迁移**：互动、通知、网关、Sidecar 的 Go 全量测试/vet，Java 10 个默认测试及两份 Compose 检查通过。真实 Mongo/MQ 集成各三轮通过；修正 Smoke 失败退出码后五条完整 Smoke 两轮通过。
- **2026-11-08 故障演练**：实际停止隔离 MQ 后，HTTP 点赞成功、Mongo 明细存在、Outbox 积压两条；恢复后通知与 ES 自动补齐。发现并修复 Sidecar 未重订阅；旧评论服务仍需重新建立连接，不能将结论扩大到整个旧集群。
- **Redis 客户端统一**：网关、用户、互动全量测试/vet 通过；miniredis 验证空值、TTL、锁和连接配置。独立 Redis Stack 验证 Lua、TTL、Bloom，共用原生客户端；修复 BF.EXISTS 的 RESP3 布尔回复兼容。未实测真实 Cluster/TLS 部署。

该 RedisBloom 验证是历史记录；当前迁移清理已移除无生产用途的 Bloom，现行真实 Redis 用例验证 Lua、Set/Hash 与 TTL。
- **提交前审查**：Java 重建代理默认端口对齐 18088；真实事务测试覆盖 int64 删除标记拒绝互动，相关回归通过。

Smoke 使用当时的互动/通知/Sidecar 二进制和 Java，其余未修改 Go 服务复用本地镜像。测试关闭 AI、OSS，未改动原开发卷。Java 测试使用独立输出目录规避编辑器与 Maven 同时生成 target 的冲突。后续 Redis 客户端替换未重新执行完整 Smoke，不能将上一轮 Smoke 视为对之后所有修改的验证。

## 历史覆盖率：2026-07-24

下表早于互动持久化迁移和 Redis 客户端统一，**不是当前覆盖率**。当时七模块全量测试通过，按语句加权为 13.5%（881/6521）。生成代码、装配和启动代码也计入分母，业务包比全仓比例更有参考价值。

| 模块 | 当时核心包覆盖率 | 当时模块覆盖率 |
|---|---:|---:|
| gateway-api | proxy/middleware/ctxutil/response：49.3%～82.1% | 16.5% |
| user-rpc | logic：31.9% | 10.1% |
| interaction-rpc | logic：12.4% | 6.1% |
| search-rpc | logic：41.4% | 12.8% |
| comment-rpc | logic/model：62.3% / 100% | 20.9% |
| notification-rpc | logic/model：61.3% / 96.3% | 14.8% |
| sync-sidecar | handler：30.7% | 18.9% |

当时 Gateway 关键包：Java Proxy 77.0%、中间件 49.3%、Context 82.1%、响应 76.9%、Auth DTO 18.2%、Comment DTO 33.3%、帖子写代理 22.9%。旧报告中的 Redis 先写/MQ 失败仍成功场景已被新事务测试替代。

重新统计时，在仓库根目录准备 `build/coverage`，再分别进入服务目录：

```bash
go test ./... -covermode=atomic -coverprofile=../../build/coverage/<module>.out
go tool cover -func=../../build/coverage/<module>.out
go tool cover -html=../../build/coverage/<module>.out -o ../../build/coverage/<module>.html
```

产物不提交。新覆盖率必须标记日期与统计范围；暂不设包含生成代码的全仓 80% 门槛。后续优先补真实故障窗口、BFF 聚合、缓存回填竞态与用户同步，不为凑数字逐个测试生成 Handler。
