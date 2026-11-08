# 互动域迁移与切换

## 实现状态

互动写入口已迁移到 Go：点赞、收藏、评论点赞和评分均由 interaction-rpc 同步持久化 MongoDB。原集合名、字段、联合唯一索引保持兼容。

一次有效变更在同一个 MongoDB 事务内完成：

1. 写入或删除互动明细，重复操作不产生新事件；修改评分只有分值改变才写入。
2. 从事务快照重新计算对应计数/平均分，更新 posts 或 comments 的互动字段和 interactionVersion。同目标并发事务通过父文档写冲突重试。
3. 写 interaction_outbox：通知事件和帖子搜索刷新任务。

事务提交后才失效 Redis，缓存失败不撤销业务。Redis 缓存回填使用 Lua 一次写入完整集合与 60 秒 TTL；缓存回填与写入交错仍允许短期陈旧，不能视为强一致。原 Bloom 不再用作跳过 Mongo 回填的依据。

Outbox 后台任务在 MQ 断连时保留数据并重连。使用持久化消息、mandatory 和 Publisher Confirm；收到确认且未被退回后删除任务。发布成功但删除任务前退出会重复投递。

通知使用独立持久化队列 interaction.notification.v1，通过 MySQL 只读查询发送者资料、Mongo 查询目标作者；通知与 eventId 收据同事务写入。重复事件不会重置未读，同目标旧版本不会覆盖新通知。发送失败时消费者保持消息未确认并重试；非法消息进入 interaction.notification.dead。

搜索刷新投递到现有 platform.es.sync.queue，使用 PostUpdateEvent 载荷。Sidecar 回查 Mongo 当前状态；ES 队列限定单消费者、单 Worker，重建与增量共享本进程锁。消费者断线后重连并重新声明队列、绑定和订阅，停机时取消重连。仅支持一个 Sidecar ES 消费实例，不宣称多实例水平扩展。Java 后台重建入口转发到 Sidecar。

## 保留的边界

- Java 保留帖子内容、审核、关注、评论事件及 AI 编排；InteractionEventListener 仅保留 FOLLOW 通知。
- 为兼容旧 Java 查询，Go 在事务内更新 posts/comments 的互动统计字段。Java 编辑帖子改为内容字段更新，禁止整实体替换覆盖统计。
- 帖子删除、评论删除和用户注销的历史级联清理暂保留原实现，属于生命周期兼容路径，不是普通互动写入入口。本轮没有重写整套内容删除链路。
- 通知消费者对 MySQL 用户表是只读依赖；可后续替换为 user-rpc 查询，不新增服务。
- Receipt 暂不自动过期，避免过期后历史消息重放重置未读；Outbox 确认后删除。积压与死信需监控，长期失败任务会阻塞本实例后续投递。

## 启动条件与维护窗口

MongoDB 必须支持事务。Compose 已配置单节点 rs0 和 mongo-init；现有数据卷保留，不执行删除或重建数据卷。单节点副本集提供事务能力，不提供高可用。

1. 备份现有业务数据，暂停网关互动写入；保持旧 Java Listener 运行到 platform.interaction.queue 的旧非 FOLLOW 事件处理完成，检查死信。
2. 停止旧互动生产者后，再重启 Mongo 进入副本集模式。先确认当前实例不是已有不同配置的副本集；初始化脚本不做 reconfig。
3. 从仓库根目录执行 `docker compose up -d mongo mongo-init`，确认 mongo-init 退出码为 0，再启动新服务。生产使用 deploy/docker-compose.prod.yml 的对应命令。
4. 宿主机 Java 的 MONGO_URI 使用 `mongodb://localhost:27017/rednote?directConnection=true`，避免解析容器成员名 mongo。不要将本机绝对路径或真实凭据提交。
5. 为通知配置 NOTIFICATION_RABBITMQ_URL 与 NOTIFICATION_MYSQL_DSN（Compose 开发默认值已提供，生产需设置专用只读账号）。宿主机直接运行通知服务时也必须提供这些变量。
6. Java 后台重建代理配置 SYNC_SIDECAR_URL、SYNC_SIDECAR_TOKEN；本机默认地址 http://localhost:18088，容器内使用 http://sync-sidecar:8088，Token 与 Sidecar ADMIN_TOKEN 一致。
7. 构建并协调发布 Go 与 Java；运行五条 Smoke，检查 Outbox 消退、通知未读和 ES 计数。旧 Redis 缓存可能仍有 24 小时 TTL，切换时需要针对 post:like:*、post:collect:*、post:rate:*、comment:like:* 做受控缓存清理或等待旧 TTL，不能用 FLUSHDB。

不要将新 Go 生产者与旧 Java 互动持久化消费者混跑。回退前停止写入和新投递，处理已提交的 Outbox；不能直接回滚二进制后重放全部事件。

## 验证

2026-11-08：interaction-rpc、notification-rpc、sync-sidecar、gateway-api 的全量 Go 测试与静态检查通过；Java 默认 10 个测试通过；两份 Compose 配置检查通过。事务命令级 Mock 测试覆盖提交与 Outbox 失败时的 abort。

OrbStack 隔离环境验证已完成：

- 真实 Mongo 副本集：重复点赞/取消、改分、并发计数、Outbox 插入失败回滚，通过。
- 真实 RabbitMQ：连接失败后任务保留、恢复投递、mandatory 无法路由时任务不删除，通过。
- 通知消费：临时查询失败后重试、重复投递去重、旧版本不重置已读、非法 JSON 进入死信，通过。
- 上述互动存储和通知集成测试各连续运行三轮通过；数据库与队列均按测试随机隔离。
- 完整集群五条 Smoke 在修正断言退出码后两轮通过，Outbox 已清空，通知收据已落库。
- 实际停止 Smoke RabbitMQ 后，HTTP 点赞仍成功，Mongo 明细存在且 Outbox 积压两条；恢复后通知和 ES 自动补齐，修复后的 Sidecar 无需手动重启。

Smoke 使用当前 Go 互动/通知/Sidecar 二进制和当前 Java；其余未修改 Go 服务复用本地既有镜像。未升级原开发项目，也未操作原有业务数据卷。测试关闭 AI、OSS 等外部能力。Java 默认 10 个单测另在独立输出目录运行通过，避免编辑器与 Maven 并发生成 target 文件。

本轮修复：测试 collMod 命令使用有序 BSON；Smoke 的 fail() 返回非零，消除假通过；Sidecar 增加断线重订阅。临时 Ubuntu 测试镜像缺少时区库时使用 UTC，正式服务镜像已有 tzdata。

提交前审查修正：Java 重建代理的本机默认端口对齐为 18088；互动查询使用 Mongo 数值比较拒绝已删除目标，覆盖 int64 删除标记回归。相关 Go 全量测试/静态检查、真实事务测试及 Java 10 个默认测试重新通过。

故障恢复范围限于本轮迁移的互动、通知和 Sidecar。未改动的旧 comment-rpc 在 MQ 重启后仍需重新建立连接，最终 Smoke 前已重启该测试服务；不能据此宣称整个旧集群均支持自动恢复。

普通测试：分别在 interaction-rpc、notification-rpc、sync-sidecar 执行 `go test ./...`，Java 执行 `mvn test`。

真实事务测试需显式指定隔离副本集，测试仅创建并删除随机 sharely_interaction_test_* / sharely_notification_test_* 数据库：

```bash
cd services/interaction-rpc
SHARELY_TEST_MONGO_URI='mongodb://localhost:27017/?directConnection=true' go test ./internal/store -run TestMongoTransactions -v

cd ../notification-rpc
SHARELY_TEST_MONGO_URI='mongodb://localhost:27017/?directConnection=true' go test ./internal/consumer -run TestMongoNotificationReplay -v
```

覆盖重复点赞/取消、改分、并发计数、Outbox 插入失败回滚，以及通知重投/旧版本不重置已读。未提供环境变量时这些测试明确 Skip。

MQ 测试另外设置 SHARELY_TEST_RABBITMQ_URL，且必须指向独立测试 Broker；在对应模块执行 `go test ./internal/store -count=3` 或 `go test ./internal/consumer -count=3`。队列采用随机后缀，避免重复运行互相抢消息。

尚未覆盖的独立故障注入：精确在 Confirm 后退出生产进程、ES 进程停止后恢复、缓存回填与失效的高并发交错。重复通知与旧版本已通过重放测试验证，但不等同于所有进程崩溃窗口均已演练。
