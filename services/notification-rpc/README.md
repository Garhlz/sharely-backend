# notification-rpc

`notification-rpc` 是通知读写中心，负责通知列表、未读数、已读更新，以及状态型通知的去重 upsert。

## 服务职责

- 维护 MongoDB 中的 `notifications` 集合
- 提供未读数、通知列表、已读能力
- 提供普通通知插入和状态型通知 upsert
- 提供历史重复通知清理接口

## 接口规范

主要通过 `gateway-api` 暴露给前端，写接口则供 Java listener 或其他内部服务调用。

### 面向前端的核心 HTTP 接口

| 网关路径 | 方法 | 说明 |
| --- | --- | --- |
| `/api/message/unread-count` | `GET` | 获取未读通知数 |
| `/api/message/notifications` | `GET` | 获取通知列表 |
| `/api/message/read` | `POST` | 标记全部已读 |
| `/api/message/read` | `PUT` | 批量已读 |

调用示例：

```bash
curl 'http://localhost:8090/api/message/unread-count' \
  -H 'Authorization: Bearer <access-token>'
```

### 内部 gRPC 接口

- `CreateNotification`
  - 评论、回复、系统通知等逐条插入
- `UpsertNotification`
  - 点赞、收藏、评分、关注等状态型通知去重写入
- `CleanDuplicateNotifications`
  - 清理历史重复通知

## 技术实现

### 1. 普通通知与状态型通知分流

- 普通通知
  - 评论
  - 回复
  - 系统通知
  - 保留完整历史
- 状态型通知
  - 点赞帖子
  - 收藏帖子
  - 评分帖子
  - 点赞评论
  - 关注用户
  - 相同 `receiverId + senderId + type + targetId` 只保留一条

### 2. Mongo 唯一索引 + Partial Filter

状态型通知通过唯一索引保证去重：

- 只对状态型通知生效
- 评论、回复这类历史通知不受影响
- 老环境若已有脏数据导致建索引失败，服务只记录告警，不阻断启动

### 3. 未读与列表查询

- 未读数通过 `receiverId + isRead` 查询
- 列表按 `createdAt` 倒序
- 已读更新支持全部已读和批量已读两种模式

## 技术亮点

- 用“通知类型分层”解决了消息中心最常见的刷屏问题
- Partial unique index 只约束状态型通知，兼顾幂等性和业务语义
- 面向迁移老数据的环境，唯一索引失败不会直接把服务启动打崩
- 互动通知由本服务消费独立队列生成，eventId 收据与通知同事务提交

## 与其他服务的关系

- `gateway-api`
  - 直接读取通知未读数和通知列表
- `platform-java`
  - 保留关注、评论、审核等通知编排
  - 最终通过 gRPC 调用本服务写入通知
- `interaction-rpc`
  - 通过 Outbox 投递互动事件，本服务查询目标与发送者资料并生成通知
  - 配置及副本集要求见 [迁移说明](../../docs/architecture.md)

## 本地运行

```bash
cd services/notification-rpc
go build ./...
go test ./...
```

通过 Docker 启动：

```bash
# 在仓库根目录执行
docker compose up -d --build notification-rpc
```
