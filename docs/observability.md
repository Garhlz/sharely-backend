# 可观测性与排障

本文件合并原 Grafana 面板教程，统一记录当前日志、Trace 与指标入口。启动方式见根目录 [README](../README.md)，验证方式见 [测试](testing.md)。

## 入口与配置

| 组件 | 本地地址 | 用途 |
|---|---|---|
| Grafana | http://localhost:3001 | 查询 Prometheus 与 Loki |
| Prometheus | http://localhost:9091 | 检查采集目标、直接运行 PromQL |
| Jaeger | http://localhost:16686 | 查看跨服务 Trace |

Grafana 数据源由 [配置](../deploy/grafana/provisioning/datasources/datasources.yml) 自动创建：Prometheus 指向容器内 `http://prometheus:9090`，Loki 指向 `http://loki:3100`，不要填写宿主机映射端口。固定数据源 UID 用于关联仓库中的 [核心链路 Dashboard](../deploy/grafana/dashboards/sharely-core.json)，启动后位于 Sharely 文件夹，无需手动导入。

修改后在根目录执行以下命令重建应用并应用新增挂载；仅 restart 不会添加新的 Dashboard/规则挂载：

```bash
docker compose up -d --build gateway-api interaction-rpc notification-rpc sync-sidecar prometheus grafana promtail
```

Sidecar 的独立指标端口为容器内 40097，由 `METRICS_ADDR` 配置，默认不映射到宿主机。本仓库的采集和面板自动配置针对开发 Compose；生产编排需另外部署监控并配置网络。

Go 通过服务配置接入追踪。宿主机 Java 如需完整链路，使用 [OTel 启动脚本](../scripts/run-platform-java-with-otel.sh)；普通 Maven 启动不等同于已加载 Agent。日志收集还依赖 Promtail 的容器发现、Java 日志挂载及实际文件路径。

## 排障顺序

1. 在网关确定接口、响应码、请求标识及失败时间。
2. 在 Loki 按服务和标识查询，定位 RPC、Java 代理或 MQ 消费错误。
3. 在 Jaeger 选择对应服务与时间段，检查慢 Span 和错误；只对已采样、已接入的链路有效。
4. 在 Prometheus 检查目标是否 UP，再看错误率、耗时及缓存回源。
5. 异步故障继续检查 RabbitMQ 队列与消费者、MongoDB Outbox 和消费凭据，不能仅凭 HTTP 成功判断同步完成。

常用日志字段为 `service`、`requestId`、`traceId`、`routingKey`。Go 原生日志还可能使用 `trace` 字段；请求标识、自定义 Trace Header 与 OTel Trace ID 不是天然相同的值。

Loki 仅索引 service/level/routingKey 等低基数标签，不再索引 requestId、traceId、trace、span；这些字段留在日志正文，查询时提取。Sidecar 输出 JSON 并参与同一采集规则。Java 异常堆栈按时间行合并，启动日志保留静态 service 标签。

互动事务已持久化 OTel 上下文和请求标识；Outbox 重投创建独立 Producer Span，通知与 ES 消费恢复该上下文。无追踪头的旧事件允许形成新链路，不伪造原 Trace ID。旧 X-Trace-Id 响应头仍兼容请求标识，不等同于真实 OTel Trace ID。

## Loki 查询

在 Grafana Explore 中选择 Loki：

```logql
# 单个服务
{service="gateway-api"}

# 跨服务查找已知标识；.+ 避免选择器匹配空标签
{service=~".+"} |= "实际请求标识"

# Go JSON 错误日志
{service="user-rpc"} | json | level="error"

# Java 日志按实际输出文本过滤
{service="platform-java"} |= "ERROR"

# 按具体消息标识定位异步处理
{service=~"sync-sidecar|notification-rpc"} |= "实际messageId"

# 最近五分钟 Go 服务错误日志速率
sum(rate({service="user-rpc"} | json | level="error" [5m]))
```

查不到日志时，先检查时间范围、服务标签及 Promtail 采集状态；不要先假设业务没有执行。

## Prometheus 与面板

在 Grafana 选择 Prometheus。下列指标按实际有流量的路径使用；标签值以采集结果为准。

| 目标 | 指标 |
|---|---|
| 网关请求与延迟 | `gateway_http_requests_total`、`gateway_http_request_duration_seconds_bucket` |
| Java 代理错误与延迟 | `gateway_java_proxy_errors_total`、`gateway_java_proxy_duration_seconds_bucket` |
| 互动缓存预热 | `interaction_cache_warm_total`、`interaction_cache_warm_duration_seconds_bucket` |
| 认证与验证码 | `user_auth_requests_total`、`user_auth_request_duration_seconds_bucket`、`user_email_code_requests_total` |
| 搜索 RPC 与 ES 查询 | `search_rpc_requests_total`、`search_rpc_request_duration_seconds_bucket`、`search_es_query_duration_seconds_bucket` |
| 网关错误分类 | `gateway_response_errors_total{class="business或server"}`，仅统计网关统一错误处理，不含 Java 透传 |
| Outbox 积压与采集 | `interaction_outbox_pending`、`interaction_outbox_oldest_age_seconds`、`interaction_outbox_collection_up`、`interaction_outbox_snapshot_timestamp_seconds` |
| Outbox 发布 | `interaction_outbox_publisher_up`、`interaction_outbox_publish_total`、`interaction_outbox_publish_duration_seconds_bucket` |
| 通知消费 | `notification_consumer_enabled`、`notification_consumer_up`、`notification_consume_total`、`notification_consume_duration_seconds_bucket` |
| Sidecar 消费 | `sidecar_consumer_up`、`sidecar_consume_total`、`sidecar_consume_duration_seconds_bucket`，按固定配置队列区分 |

常用查询：

```promql
# 网关每秒请求数
sum(rate(gateway_http_requests_total[5m]))

# 网关 P95，保留 le 桶标签后计算分位数
histogram_quantile(0.95,
  sum by (le) (rate(gateway_http_request_duration_seconds_bucket[5m]))
)

# 代理错误按原因分组
sum by (kind) (rate(gateway_java_proxy_errors_total[5m]))

# 缓存预热按类型与结果分组
sum by (kind, result) (rate(interaction_cache_warm_total[5m]))

# 认证按动作和结果分组
sum by (action, result) (rate(user_auth_requests_total[5m]))
```

Counter 使用 rate/increase，不直接把累计值作为实时吞吐；Histogram 桶聚合需要保留 `le`。低流量时 P95 可能没有样本，不等同于零延迟。

Outbox 每 15 秒独立采集，单轮超时 5 秒，不依赖 MQ 连接。采集失败保留旧值并标记失败；面板只展示成功且 60 秒内更新的快照，避免旧值被当成当前值。聚合会扫描待发任务，长期积压时应关注查询成本。

通知 result=success 表示一次业务处理返回成功，acked 表示 Ack 调用成功；同一条消息可以增加多个结果，不能把全部结果求和当作消息数。Sidecar 记录的是确认结果，旧 Handler 可能忽略未知事件或在局部失败后 Ack，不能将 acked 描述为全部副作用成功。dead_letter 仅表示成功调用不重入队的拒绝操作，不是 Broker 已持久保存死信的证明，也不是 DLQ 当前深度。

## 告警与验证

[规则文件](../deploy/prometheus/rules/sharely.yml) 自动加载，Prometheus 的 Alerts 页面可查看 Pending/Firing：

- 采集目标不可用持续 2 分钟。
- 最老 Outbox 事件超过 120 秒且持续 2 分钟，并要求快照有效。
- Outbox 采集失败或快照过期、消费者未订阅持续 2 分钟。
- 最近 5 分钟出现不重入队拒绝。
- 网关 5xx 超过 5%，且 5 分钟至少 20 次请求，持续 2 分钟。

未配置通知消费者时不触发断连告警。网关明确的 RPC/Context 超时映射为 504，未分类 gRPC 内部错误为 500；普通非结构化错误仍按原参数解析契约返回 400，应逐步用 AppError/gRPC 状态表达内部故障，不能声称所有业务错误已自动正确分类。

阈值是本地演示起点，不是生产 SLO。当前没有配置 Alertmanager 或外部通知渠道，Firing 不等于已发送邮件/消息。Counter 在首次采集前发生的事件可能无法通过 increase 检出，死信排查仍需核对 Broker 队列。

```bash
docker run --rm --network none --entrypoint promtool -v "$PWD/deploy/prometheus:/etc/prometheus:ro" prom/prometheus:latest check config /etc/prometheus/prometheus.yml
docker run --rm --network none --entrypoint promtool -v "$PWD/deploy/prometheus:/etc/prometheus:ro" -w /etc/prometheus/tests prom/prometheus:latest test rules sharely.test.yml
```

规则测试已接入 CI。隔离集成测试验证 Broker 不可达时积压仍可观测、恢复发布后清零，以及事件请求标识/Trace 透传；不将它等同于完整监控集群的实测告警通知。

## 当前限制

- 无生产用途的 `interaction_mq_publish_total` 与 `interaction_bloom_checks_total` 已移除；使用 Outbox 指标判断发布状态。
- Dummy 指标仍由空结果回填触发，当前不再有原地写缓存的 remove 路径；没有样本不代表没有空结果风险。
- Sidecar ES 同步当前采用单消费者串行处理，避免旧事件覆盖新状态；吞吐瓶颈应先测量，再考虑按键有序并发。
- 指标与 Trace 不能替代业务一致性校验。集群重启、MQ 故障及重复事件应使用隔离集成测试验证。
