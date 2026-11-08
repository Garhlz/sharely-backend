# interaction-rpc

`interaction-rpc` 负责帖子和评论的高频互动能力，是项目里 Redis 读模型最集中的服务。

## 服务职责

- 帖子点赞 / 取消点赞
- 帖子收藏 / 取消收藏
- 帖子评分
- 评论点赞 / 取消点赞
- 批量查询帖子互动状态
- 用户主页统计数据聚合
- 事务 Outbox 投递互动通知和搜索刷新任务

## 接口规范

主要通过 `gateway-api` 暴露 HTTP。

| 网关路径 | 方法 | 说明 |
| --- | --- | --- |
| `/api/interaction/like/post` | `POST` | 点赞帖子 |
| `/api/interaction/unlike/post` | `POST` | 取消点赞帖子 |
| `/api/interaction/collect/post` | `POST` | 收藏帖子 |
| `/api/interaction/uncollect/post` | `POST` | 取消收藏帖子 |
| `/api/interaction/rate/post` | `POST` | 给帖子评分 |
| `/api/interaction/like/comment` | `POST` | 点赞评论 |
| `/api/interaction/unlike/comment` | `POST` | 取消点赞评论 |

调用示例：

```bash
curl -X POST 'http://localhost:8090/api/interaction/like/post' \
  -H 'Authorization: Bearer <access-token>' \
  -H 'Content-Type: application/json' \
  -d '{"targetId":"69cd52ee9535d61382e92951"}'
```

## 技术实现

### Redis 读模型

- 帖子点赞、收藏、评论点赞使用 `Set`
- 帖子评分使用 `Hash`
- 查询帖子互动状态时不直接扫 Mongo，而是优先从 Redis 读

### 缓存保护策略

服务使用短期缓存与回填保护：

- Dummy 占位
  - 避免空 key 被重复回源
- 短锁预热
  - 减少同 Key 的并发回源，等待超时后仍可能并发加载
- Lua 原子回填集合与 60 秒 TTL；Bloom 不再用于跳过事实数据查询

### 事件发布

- MongoDB 事务同时提交互动明细、统计投影和 Outbox，提交后失效 Redis
- 后台投递使用持久化消息、mandatory、Confirm，失败保留任务并重连
- notification-rpc 消费独立队列生成通知，Sidecar 负责 ES 刷新
- 启动需要 MongoDB 副本集；切换与验证见 [迁移说明](../../docs/architecture.md)

### 用户主页统计

`GetUserStats` 会聚合：

- followCount
- fanCount
- receivedLikeCount
- isFollowed

其中关注关系和累计获赞主要来自 Mongo 聚合。

## 技术亮点

- MongoDB 为互动事实源，Redis 为可重建的短期读模型
- 事务统计和 Outbox 避免已确认互动因消息发送失败而丢失
- 批量接口 `BatchPostStats` 能一次性补齐帖子列表页需要的多个互动字段
- Outbox 保留 trace 上下文，支持异步传播

## 关键依赖

- `Redis`
- `MongoDB`
- `RabbitMQ`

## 本地运行

```bash
cd services/interaction-rpc
go build ./...
go test ./...
```

通过 Docker 启动：

```bash
# 在仓库根目录执行
docker compose up -d --build interaction-rpc
```
