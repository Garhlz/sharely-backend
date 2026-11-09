# sync-sidecar

`sync-sidecar` 是项目的异步同步旁路服务，核心职责是消费 RabbitMQ 事件，将主库数据增量同步到 Elasticsearch，并提供手动全量重建能力。

## 服务职责

| 职责 | 说明 |
|------|------|
| ES 增量同步 | 消费帖子创建、更新、审核通过事件，回查 Mongo 后写入 ES |
| ES 全量重建 | 管理接口触发，分批扫描 Mongo → 批量写入 ES |
| 用户资料冗余同步 | 消费 `UserUpdateEvent`，刷新 ES 中帖子文档的作者冗余字段 |
| 日志落库 | 消费 `log.#` 事件，将操作日志写入 Mongo 供后台审计 |

## 管理接口

独立指标入口为容器内 `:40097/metrics`，监听地址通过 `METRICS_ADDR` 配置，默认不映射到宿主机。消费指标、面板与告警见 [可观测性](../../docs/observability.md)。

该服务不对外暴露业务接口，仅提供以下管理端点（需 `X-Admin-Token` 验证）：

| 路径 | 方法 | 说明 |
|------|------|------|
| `/admin/reindex/posts` | `POST` | 触发 Mongo → ES 全量重建 |

**调用示例**（Docker 本地环境，宿主机端口 18088）：

```bash
curl -X POST 'http://127.0.0.1:18088/admin/reindex/posts' \
  -H 'X-Admin-Token: <your-admin-token>'
```

> `AdminToken` 通过环境变量 `ADMIN_TOKEN` 注入，默认值仅供开发调试使用。

## MQ 消费拓扑

| 队列 | Routing Key | 处理逻辑 |
|------|-------------|----------|
| `platform.es.sync.queue` | `post.#` | 帖子增量写入 / 软删除同步 ES |
| `platform.es.sync.queue` | `user.update` | 刷新 ES 中帖子的作者冗余字段 |
| `platform.es.sync.queue` | `post.audit.pass` | 审核通过后将帖子写入 ES（仅审核模式） |
| `platform.log.queue` | `log.#` | 操作日志写入 Mongo |
| `platform.user.queue` | `user.#` | 用户数据同步到 Mongo 多集合冗余字段 |

## handleUpdate 分支逻辑

`PostUpdateEvent` 触发 `handleUpdate`，回查 Mongo 后按以下规则路由：

- `ErrNoDocuments`（文档不存在）→ 走 `handleDelete`，在 ES 中删除该帖子
- `IsDeleted == 1` 或 `Status != 1` → 同上，同步删除
- 正常文档 → 重新索引（upsert）

这确保了即使帖子在 Mongo 中被标记删除，ES 侧也会最终保持一致。

## 全量重建流程

1. 校验 `X-Admin-Token`
2. 互斥锁保证同时只有一次重建任务运行（并发请求返回 `409 Conflict`）
3. 清空 ES 目标索引
4. 分批扫描 Mongo `posts`（`status=1` 且 `isDeleted=0`）
5. 批量写入 ES

适用场景：初次接入 ES、历史增量事件丢失后修复索引、调整映射策略后重建。

## 本地运行

```bash
# 构建
cd services/sync-sidecar
go build ./...

# 通过 Docker Compose 启动（推荐）
# 返回仓库根目录后执行
cd ../..
docker compose up -d --build sync-sidecar
```

服务启动后监听：
- 内部 HTTP 管理端口：`:8088`
- Docker 宿主机映射：`127.0.0.1:18088`

## 关键依赖

- `RabbitMQ`：事件消费
- `MongoDB`：主数据源与日志落库
- `Elasticsearch`：搜索索引写入
