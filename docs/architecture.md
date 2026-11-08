# 服务架构、Redis 与迁移

本文合并服务边界草案、互动迁移记录和 Redis 客户端说明；描述当前代码。测试结果统一见 [测试与覆盖率](testing.md)，未完成工作见 [TODO](todo-list.md)。

## 服务与数据归属

| 服务 | 职责与写入范围 |
|---|---|
| gateway-api | HTTP 入口、鉴权、BFF 聚合、Java 代理，不持有业务事实数据 |
| user-rpc | MySQL 账号资料、认证及 Redis Token 状态 |
| interaction-rpc | MongoDB 点赞、收藏、评分、评论点赞明细；互动统计投影与 Outbox |
| comment-rpc | 评论、回复树、删除规则 |
| notification-rpc | 通知存储、未读状态、互动通知消费与去重 |
| search-rpc | ES 查询、建议词与搜索历史 |
| sync-sidecar | ES 写入、重建与历史用户事件同步 |
| platform-java | 帖子、关注、审核、后台、部分评论事件及 AI 编排 |

MongoDB 保存内容与互动事实，Redis 和 ES 是可重建读模型。为兼容 Java 查询，Go 在事务内更新 posts/comments 的互动字段，Java 编辑帖子只更新内容字段。历史帖子删除、评论删除和用户注销的级联清理暂保留原路径，尚未完全收口为单服务写入。

## 点赞与事件链路

```text
HTTP → Gateway 鉴权 → interaction-rpc
  → MongoDB 事务：互动明细 + 统计投影 + Outbox
  → 事务提交后失效 Redis → 返回成功

Outbox → RabbitMQ
  ├→ interaction.notification.v1 → notification-rpc → 通知 + 事件收据
  └→ platform.es.sync.queue → Sidecar 回查 Mongo → ES
```

- 重复点赞、重复取消不产生新变更；修改评分仅在分值变化时写入。
- 同目标的统计文档写冲突触发事务重试；计数与均值从事务快照计算，不靠重放消息反复增减。
- Outbox 使用持久化消息、mandatory 与 Publisher Confirm；确认且未被退回后删除任务。断连重试保留任务，确认后进程退出可能造成重复投递。
- 通知在同一事务内写 eventId 收据与通知。同目标旧版本不覆盖新通知，重投不将已读恢复为未读；业务通知合并键与事件去重键分别处理。
- Sidecar 的 ES 队列采用单消费者、单 Worker，重建与增量共用本进程锁。断线后重新建立连接、声明和订阅；当前不支持多个 ES 消费实例并行扩展。
- Java InteractionEventListener 仅保留 FOLLOW 通知；后台索引重建接口转发给 Sidecar。

早期实现为 Redis 先写、MQ 触发 Java 持久化，存在已确认互动因消息失败而丢失的缺口。最终选择由 Go 同事务维护明细、统计和 Outbox，未采用草案中的 Java 异步统计方案。

### 事件载荷

互动通知包含 eventId、schemaVersion=1、userId、targetId、type、action、occurredAt、notify、version；type 为 LIKE/COLLECT/RATE/COMMENT_LIKE，action 为 ADD/REMOVE。载荷定义见 [Go 存储实现](../services/interaction-rpc/internal/store/interactions.go)。

搜索刷新使用 `{"postId":"..."}`，消息 Header `__TypeId__=PostUpdateEvent`。用户事件仍使用旧发布器，契约见 [用户事件](user_events.md)，不能将互动 Outbox 的可靠性保证套用到全部事件。

## Redis 客户端与缓存

gateway-api、user-rpc、interaction-rpc 的业务调用统一使用 go-redis/v9，ServiceContext.Redis 为 UniversalClient。普通互动命令与 RedisBloom 共用客户端，不再维护 RawRedis。

保留 YAML 的 Redis/BizRedis 和 Host、Type、User、Pass、Tls、NonBlock、PingTimeout 字段，仅复用 go-zero RedisConf 配置结构：

- node 使用单机客户端；cluster 显式启用集群模式，Host 可包含逗号分隔的种子地址。
- 账号、密码、TLS 传给原生客户端；NonBlock=true 惰性建连，false 按 PingTimeout 检查连接。
- 三个独立 Go Module 各有少量连接装配代码，未增加公共模块或业务命令包装层。服务退出时关闭连接池。
- 原 go-zero Redis 封装的熔断与专用 Hook 不再自动应用；HTTP/gRPC/MQ 观测和业务指标保留。

命令使用请求 Context 与 Result()/Err()。Exists 返回数量；Get/HGet 缺失返回 redis.Nil：验证码校验失败、受保护接口拒绝无版本 Token、公开接口降级匿名。SetEx/SetNX/Expire 使用 time.Duration，Token 版本通过 Set(..., 0) 保持无 TTL。BF.EXISTS 使用 Bool() 兼容 RESP2 整数与 RESP3 布尔回复。

缓存为 Set/Hash，Mongo 查询为空时保留 Dummy 占位；按 Key 加短锁减少并发回源。Lua 原子写入完整集合与 60 秒 TTL。写入与回填交错仍可能短期陈旧，不保证 Redis 与 Mongo 强一致。Bloom 不再作为跳过事实查询的依据，Key 与验证码/Token 有效期保持原约定。

## 切换与启动条件

MongoDB 必须支持事务。Compose 使用单节点 rs0 与 mongo-init；单节点副本集提供事务能力，不提供高可用。快速启动见 [主 README](../README.md)。

1. 备份数据，暂停互动写入；旧 Java Listener 排空 platform.interaction.queue 中的非 FOLLOW 事件，并检查死信。
2. 停止旧互动生产者，再将 Mongo 切换为副本集。确认不是已有不同配置的副本集；初始化脚本不执行 reconfig、不删除数据卷。
3. 在仓库根目录执行 `docker compose up -d mongo mongo-init`，确认 mongo-init 退出码为 0，再协调启动新 Go/Java。生产使用 deploy/docker-compose.prod.yml 对应命令。
4. 宿主机 Java 使用 `MONGO_URI=mongodb://localhost:27017/rednote?directConnection=true`，避免解析容器成员名 mongo。
5. 通知配置 NOTIFICATION_RABBITMQ_URL、NOTIFICATION_MYSQL_DSN；开发 Compose 已提供默认值，生产使用专用只读账号。启用消费者要求副本集与通知唯一索引，不满足时启动失败。
6. Java 重建代理配置 SYNC_SIDECAR_URL、SYNC_SIDECAR_TOKEN。本机默认 http://localhost:18088，容器内 http://sync-sidecar:8088，Token 与 Sidecar ADMIN_TOKEN 一致。
7. 旧 Redis 缓存可能还有 24 小时 TTL，切换时受控清理 post:like:*、post:collect:*、post:rate:*、comment:like:* 或等待旧 TTL；不使用 FLUSHDB。执行 Smoke 并检查 Outbox 消退、通知收据与 ES 统计。

禁止新 Go 生产者与旧 Java 互动持久化消费者混跑。回退前停止写入与新投递，处理已提交 Outbox；只回滚二进制不足以恢复旧语义。

## 已知边界

- 通知消费者只读访问 MySQL 用户表，尚未改成 user-rpc 查询。
- 收据暂不自动过期，避免历史重放重置未读；Outbox 确认后删除。长期失败任务可能阻塞本实例后续投递，需监控积压与死信。
- 旧 comment-rpc 的 MQ 重启恢复仍需补齐，不能宣称全系统自动恢复。
- Go 互动事务与评论补偿采用不同策略；用户资料、帖子事件等旧链路尚未全面采用 Outbox。
