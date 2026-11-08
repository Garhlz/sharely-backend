# 用户事件契约

本文对应 user-rpc 当前实现，不适用互动域的版本化 Outbox 事件。互动事件见 [架构](architecture.md)。

## 路由与载荷

默认 Topic Exchange 为 `platform.topic.exchange`，路由可由 user-rpc MQ 配置覆盖。消息体是扁平 JSON，没有统一 `payload` 包装。

| 路由 | 消息类型 | 消息体 |
|---|---|---|
| `user.update` | `UserUpdateEvent` | `{"userId":123,"newNickname":"昵称","newAvatar":"https://example.com/a.png"}` |
| `user.delete` | `UserDeleteEvent` | `{"userId":123}` |

类型通过 AMQP Header `__TypeId__` 标识，值分别为：
- `com.szu.afternoon3.platform.event.UserUpdateEvent`
- `com.szu.afternoon3.platform.event.UserDeleteEvent`

其他 Header 包括 `X-Request-Id`、`X-Trace-Id`、`X-Service=user-rpc`、`X-Routing-Key`，并注入 OpenTelemetry 上下文。
这里的自定义 `X-Trace-Id` 使用请求标识，不应直接等同于 OTel Trace ID。

当前没有独立的用户状态事件。不能按旧文档假设存在 `user.event.*` 或 `user.status.update` 路由。

## 消费者

| 队列 | 绑定 | 职责 |
|---|---|---|
| `platform.user.queue` | `user.#` | Sidecar 更新 MongoDB 冗余用户资料、处理用户删除副作用 |
| `platform.es.sync.queue` | 包含 `user.update` | Sidecar 更新 ES 文档中的用户资料 |

删用户的具体清理范围以 UserHandler 为准；该事件并不自动保证所有历史互动、通知和外部资源均被清除。

## 可靠性边界

- 用户资料的数据库变更与 MQ 发布不在同一事务中。
- 当前 Publisher 未使用 Outbox、Publisher Confirm、mandatory 路由检查或持久消息设置，也没有自动重连。
- MQ 不可用时，启动阶段可能禁用 Publisher；更新资料发布失败会记录日志，删除路径忽略发布错误。主业务成功不代表副作用一定完成。
- 消息没有 `eventId` 或业务版本，不能宣称具备互动事件相同的去重与乱序保护。
- 消费操作的具体幂等性、重试及死信行为需结合消费者检查；修复发布端之前，应保留人工核对与重建手段。

## 代码入口

- [载荷定义](../services/user-rpc/internal/event/)
- [Publisher](../services/user-rpc/internal/mq/publisher.go)
- [资料更新](../services/user-rpc/internal/logic/updateProfileLogic.go)
- [删除公共逻辑](../services/user-rpc/internal/logic/user_delete_shared.go)
- [MongoDB 消费](../services/sync-sidecar/internal/handler/user_handler.go)
- [ES 消费](../services/sync-sidecar/internal/handler/sync_handler.go)
