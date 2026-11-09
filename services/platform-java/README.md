# platform-java

`platform-java` 是当前系统的主业务服务，负责帖子主写、关注关系、后台管理和多个异步 listener 的业务副作用处理。

虽然项目已经把评论、通知等能力逐步迁到了 Go 服务，但 Java 仍然是业务主干。

## 服务职责

- 帖子发布、编辑、删除、详情
- 首页推荐流、关注流、用户帖子列表
- 关注 / 取关、粉丝 / 关注列表、好友关系
- 后台审核、日志、统计
- MQ 事件消费
  - 更新帖子计数
  - 生成通知语义
  - 触发 AI 回复

## 接口规范

该服务对外接口通常经由 `gateway-api` 暴露，以下列出当前仍主要由 Java 实现的核心能力。

| 网关路径 | 方法 | 说明 |
| --- | --- | --- |
| `/api/post/` | `POST` | 发布帖子 |
| `/api/post/:postId` | `GET` | 帖子详情 |
| `/api/post/list` | `GET` | 推荐流 / 关注流 |
| `/api/post/user/:userId` | `GET` | 用户帖子列表 |
| `/api/user/follows/:userId` | `GET` | 关注列表 |
| `/api/user/fans/:userId` | `GET` | 粉丝列表 |
| `/api/user/follow` | `POST` | 关注用户 |
| `/api/user/unfollow` | `POST` | 取消关注 |
| `/admin/**` | 多种 | 后台管理、审核、日志导出、统计 |

调用示例：

```bash
curl -X POST 'http://localhost:8090/api/post/' \
  -H 'Authorization: Bearer <access-token>' \
  -H 'Content-Type: application/json' \
  -d '{
    "title": "测试帖子",
    "content": "分享一次完整的后端重构经历",
    "images": ["https://example.com/a.png"],
    "tags": ["Go", "微服务"],
    "coverWidth": 1080,
    "coverHeight": 1350,
    "type": 2
  }'
```

## 技术实现

### 1. 帖子主写

- 帖子、评论、通知等内容型数据主存储在 Mongo
- 发帖成功后发布 `post.create` 或审核相关事件
- 推荐流依赖 Mongo / ES / Redis 的组合查询

### 2. 关注关系

- 关注关系使用 Mongo `UserFollowDoc`
- 关注流不是走 ES 搜索，而是先查关注关系，再查对应作者帖子
- 适合“我关注的人发了什么”的关系型 Feed

### 3. 异步 listener

Java 当前仍保留多个关键 listener：

- `InteractionEventListener`
  - 处理点赞、收藏、关注、评分等副作用
- `CommentEventListener`
  - 处理评论数、回复数、评论通知
- `PostEventListener`
  - 处理帖子事件相关业务
- `PostAuditListener`
  - 处理审核通过/驳回通知

这些 listener 会继续把通知写入委托给 `notification-rpc`，把评论核心能力委托给 `comment-rpc`。

### 4. 后台与运营能力

- 用户管理
- 内容审核
- 操作日志导出
- 访问统计与热门内容统计

这部分暂时仍集中在 Java，更适合 Spring Boot + 后台脚手架式开发。

## 技术亮点

- 采用“保留 Java 主业务 + 渐进式迁出 Go 子域服务”的演进路径，风险可控
- 关注流和推荐流分开建模，分别走关系链查询和搜索/排序逻辑
- 通过 MQ listener 把通知、计数、审核等副作用从主请求链路中剥离
- Java 不再直接承担所有读写接口，而是开始与 Go RPC 形成协同边界

## 本地运行

首次启动先准备本地配置：

```bash
cp services/platform-java/.env.example services/platform-java/.env
```

在仓库根目录执行复制命令。默认配置连接 Docker 暴露到宿主机的 MySQL、MongoDB、Redis 和 RabbitMQ。检索与 ES 写入由 Go 承接，生产 Java 不再直接初始化 Elasticsearch；旧 ES 依赖与实体仅保留在测试范围。第三方能力使用禁用占位值；只有验证邮件、OSS、微信或 AI 时才填写专用测试凭证。

```bash
cd services/platform-java
SPRING_PROFILES_ACTIVE=dev mvn spring-boot:run
```

开启 Jaeger 链路追踪：

```bash
./scripts/run-platform-java-with-otel.sh
```

该脚本从仓库根目录执行，会按需下载 OpenTelemetry Java Agent；如本机未安装 `tspin`，可先使用普通 `mvn spring-boot:run` 启动。

## 测试分层

Java 测试使用 JUnit 5 Tag 隔离外部依赖和真实副作用：

```bash
# 默认：只运行不依赖中间件、不会访问外部服务的快速测试
mvn test

# 集成：使用隔离的 MySQL、MongoDB、Redis、RabbitMQ；旧 Java 搜索与 ES 重建用例仍禁用
mvn test -Pintegration

# 真实外部服务：可能发送邮件或上传 OSS，必须显式设置安全开关
RUN_EXTERNAL_TESTS=true \
EXTERNAL_TEST_EMAIL=your-test-mailbox@example.com \
mvn test -Pexternal
```

默认测试覆盖统一响应、业务错误码、用户 ThreadLocal、JWT 和 FOLLOW 通知适配。
带 `integration` / `external` Tag 的历史测试不会进入默认执行。旧互动/搜索测试完整保留，但不能直接启用其中的旧路由及共享库清理夹具；[迁移场景对应表](../../docs/testing.md) 区分已有覆盖与真实 ES 验证缺口。

标签约定：

- 无标签：可在本机和 CI 中安全、快速运行
- `integration`：依赖本地基础设施，允许写入测试数据
- `external`：访问真实第三方服务；同时也属于 `integration`

普通开发和 CI 不应设置 `RUN_EXTERNAL_TESTS`。真实外部测试应使用专用测试账号和测试 Bucket，不能使用生产凭证。
