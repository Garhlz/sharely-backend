# Sharely 仓库协作指南

本文件是本仓库面向编码代理的唯一规范。修改代码、配置、测试或文档前先阅读本文件。

## 沟通与变更

- 使用简体中文回答；新增注释优先使用简洁中文。
- 未经用户明确要求，不执行 `git commit`、`git push`，不覆盖用户已有改动。
- 修改前先确认当前实现和接口契约，不根据旧文档或旧脚本猜测。
- 配置、路由、事件结构变更时，同步检查调用方、测试和文档，避免迁移后契约漂移。

## 项目定位

Sharely（分享派）是内容社区练手项目，由课程单体项目演进为 Go + Java 双栈微服务：

```text
前端
  -> gateway-api (:8090)
       -> user / interaction / search / comment / notification RPC
       -> platform-java (:8080，代理尚未迁移的业务与管理接口)

RabbitMQ -> sync-sidecar -> Elasticsearch / MongoDB 等异步副作用
Etcd     -> Go RPC 服务发现
```

核心边界：

- `gateway-api`：统一 HTTP 入口、鉴权、聚合和 Java 反向代理。
- Go RPC：已迁移的认证、互动、搜索、评论和通知领域。
- `platform-java`：帖子主写、管理后台及尚未迁移的复杂业务。
- `sync-sidecar`：消费领域事件，完成 ES 和冗余数据同步。
- MySQL 存账号与认证；MongoDB 存内容和社交数据；Redis 存令牌状态、缓存及互动读模型；Elasticsearch 负责检索；RabbitMQ 负责最终一致性。

## 端口

| 服务 | 端口 |
|---|---:|
| platform-java | 8080 |
| gateway-api | 8090 |
| interaction / search / user RPC | 8081 / 8082 / 8083 |
| notification / comment RPC | 8084 / 8085 |
| sync-sidecar | 18088（宿主机）/ 8088（容器） |
| MySQL / MongoDB / Redis | 3306 / 27017 / 6379 |
| RabbitMQ / 管理页 | 5672 / 15672 |
| Elasticsearch / Etcd | 9200 / 2379 |
| Jaeger / Grafana / Prometheus | 16686 / 3001 / 9091 |

## 本地运行

基础设施和 Go 服务：

```bash
docker compose up -d
docker compose ps
```

Java 服务在宿主机运行，并通过根配置中的本机端口连接容器：

```bash
cd services/platform-java
mvn spring-boot:run
```

若需 OpenTelemetry Java Agent，再使用 `scripts/run-platform-java-with-otel.sh`；普通开发和排错不依赖该脚本。

网关默认地址为 `http://localhost:8090`，Java Actuator 健康检查为：

```bash
curl --noproxy '*' http://localhost:8080/actuator/health
```

## 构建与测试

```bash
# Go：在具体服务目录执行
go test ./...
go build ./...
go vet ./...

# Java：默认单元测试不得依赖真实中间件
cd services/platform-java
mvn test

# 完整集群启动后的端到端冒烟测试
./scripts/smoke/run_all.sh http://localhost:8090
```

- 集成测试只能使用隔离数据库；禁止对共享开发数据执行 `dropCollection`、`deleteAll` 或大范围 Redis 清理。
- 外部测试可能发送邮件或上传 OSS，仅在显式设置测试凭据后运行。
- 修复测试时先判断是产品缺陷、契约漂移还是测试夹具问题，不用放宽断言掩盖真实失败。
- 异步链路使用有上限的轮询等待，避免依赖固定 `sleep`。

## 当前 API 契约

Go 网关统一响应：

```json
{
  "code": 200,
  "message": "操作成功",
  "data": {}
}
```

关键约定：

- 登录令牌路径为 `data.tokens.accessToken`。
- 分页请求字段为 `page`、`size`，分页结果集合为 `data.records`。
- 发帖：`POST /api/post/`。
- 评论创建/删除：`POST /api/comment/`、`DELETE /api/comment/:id`。
- 点赞：`POST /api/interaction/like/post`，JSON 请求体为 `{"targetId":"..."}`。
- 网关同时兼容 `Authorization: Bearer <token>` 与 `accessToken` Cookie。

修改 `services/gateway-api/gateway.api` 后，应同步生成代码并检查前端、冒烟脚本和服务文档。

## 开发约定

Go：

- 遵循 go-zero 的 `internal/{config,svc,logic,model,server,mq}` 分层。
- 业务逻辑放在 `logic`，依赖通过 `ServiceContext` 注入。
- 使用 `logx.WithContext(ctx)` 保留链路上下文；测试优先采用表驱动。

Java：

- 保持 DTO、VO、Entity 分离，使用明确 import。
- 业务异常使用 `BusinessException`，接口使用 `Result<T>`。
- 按场景使用 `@Valid`、`@Transactional` 和 `@RestController`。

跨服务：

- 容器间调用使用 Compose 服务名；宿主机 Java 与容器网关通过映射端口或 `host.docker.internal` 通信。
- 修改 MQ routing key、事件载荷或 Proto 时，必须同时检查 Go/Java 生产者、消费者及生成代码。
- Java Spring Data 与 Go 启动建索引共用 MongoDB 时，索引字段、选项和名称必须一致。
- 日志尽量保留 `service`、`traceId`、`requestId`、`routingKey`。

## 代码生成

```bash
cd services/gateway-api
goctl api go -api gateway.api -go .

cd services/<rpc-service>
goctl rpc protoc ../../proto/<name>/<name>.proto \
  --go_out=. --go-grpc_out=. --zrpc_out=. --style=go_zero
```

生成后检查 diff，避免覆盖已手工维护的兼容逻辑。
