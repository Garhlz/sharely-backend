# Sharely 文档导航

根目录 [README](../README.md) 负责项目概览与启动；详细说明按主题维护，不再另设迁移计划、实施记录或 Grafana 教程副本。

| 文档 | 内容 |
|---|---|
| [架构与迁移](architecture.md) | 服务职责、数据事实源、互动事务与 Outbox、Redis 客户端、迁移与回滚 |
| [测试与覆盖率](testing.md) | 测试分层、执行命令、集成测试隔离、验证记录及历史覆盖率 |
| [可观测性](observability.md) | 日志、Trace、Prometheus 指标与 Grafana 查询 |
| [用户事件契约](user_events.md) | 实际事件载荷、路由、消费者及可靠性边界 |
| [业务错误码](business_codes.md) | 统一响应与业务码 |
| [开发 TODO](todo-list.md) | 尚未完成的工作及优先级 |
| [项目复盘与面试](resume_project_summary.md) | 简历描述、技术选型与问题复盘 |
| [个人复习笔记](intern.md) | 通用基础与其他项目的历史学习笔记，不作为本仓库实现规范 |

接口定义见 [Proto](../proto/) 与 [网关 API](../services/gateway-api/gateway.api)；服务运行细节见各 [服务目录](../services/)。
仓库协作规范以根目录 [AGENTS.md](../AGENTS.md) 为准。

原迁移计划、互动迁移记录和 Redis 说明已合并到架构文档；原覆盖率报告合并到测试文档；原 Grafana 教程合并到可观测性文档。
