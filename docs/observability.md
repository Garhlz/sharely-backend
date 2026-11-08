# 可观测性与排障

本文件合并原 Grafana 面板教程，统一记录当前日志、Trace 与指标入口。启动方式见根目录 [README](../README.md)，验证方式见 [测试](testing.md)。

## 入口与配置

| 组件 | 本地地址 | 用途 |
|---|---|---|
| Grafana | http://localhost:3001 | 查询 Prometheus 与 Loki |
| Prometheus | http://localhost:9091 | 检查采集目标、直接运行 PromQL |
| Jaeger | http://localhost:16686 | 查看跨服务 Trace |

Grafana 数据源由 [配置](../deploy/grafana/provisioning/datasources/datasources.yml) 自动创建：Prometheus 指向容器内 `http://prometheus:9090`，Loki 指向 `http://loki:3100`，不要填写宿主机映射端口。Dashboard 需自行创建或导入，数据源就绪不代表已有业务面板。

Go 通过服务配置接入追踪。宿主机 Java 如需完整链路，使用 [OTel 启动脚本](../scripts/run-platform-java-with-otel.sh)；普通 Maven 启动不等同于已加载 Agent。日志收集还依赖 Promtail 的容器发现、Java 日志挂载及实际文件路径。

## 排障顺序

1. 在网关确定接口、响应码、请求标识及失败时间。
2. 在 Loki 按服务和标识查询，定位 RPC、Java 代理或 MQ 消费错误。
3. 在 Jaeger 选择对应服务与时间段，检查慢 Span 和错误；只对已采样、已接入的链路有效。
4. 在 Prometheus 检查目标是否 UP，再看错误率、耗时及缓存回源。
5. 异步故障继续检查 RabbitMQ 队列与消费者、MongoDB Outbox 和消费凭据，不能仅凭 HTTP 成功判断同步完成。

常用日志字段为 `service`、`requestId`、`traceId`、`routingKey`。Go 原生日志还可能使用 `trace` 字段；请求标识、自定义 Trace Header 与 OTel Trace ID 不是天然相同的值。

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

# 异步链路按路由关键词定位，依赖日志确实输出该字段
{service=~"sync-sidecar|notification-rpc"} |= "post.like"

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

## 当前限制

- 旧的 `interaction_mq_publish_total` 不能代表新 Outbox 发布状态；当前需结合 Outbox 集合、日志和 RabbitMQ 判断积压。
- Bloom 与 dummy 相关指标不一定有生产路径持续调用；空面板不能证明命中率为零或逻辑正确。
- Sidecar ES 同步当前采用单消费者串行处理，避免旧事件覆盖新状态；吞吐瓶颈应先测量，再考虑按键有序并发。
- 指标与 Trace 不能替代业务一致性校验。集群重启、MQ 故障及重复事件应使用隔离集成测试验证。
