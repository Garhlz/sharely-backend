# MongoDB → MySQL 全量迁移计划

状态：**评估草案，尚未批准实施**。本文只提出目标方案，不改变 [当前架构](architecture.md)。用户已确认现有数据均可重建，但尚未决定迁库；本轮不执行 DDL、删库、清卷、换连接或重建 ES。

## 1. 目标、前提与非目标

目标是移除生产 MongoDB，让 Go/Java 使用更容易理解和验证的关系模型、SQL 与本地事务，并减少一个数据库的运行成本。不是保证换库后性能自动提高，也不同时推翻服务拆分。

- 现有 mock 账号、内容、通知、历史可重建；不做旧 Mongo 数据转换、双写、CDC 或在线灰度。
- 保留 Go + Java 职责、网关入口、HTTP/Proto 字段类型、响应包装、分页、权限与业务码。
- 保留 Redis Cache Aside、RabbitMQ、Elasticsearch、Outbox、消费幂等和现有观测指标。
- 当前未提交清理应先独立提交，迁移另开分支；每个迁移阶段独立提交并记录验收。
- 不把“可以重建数据”理解为允许立即删除旧资源。实施时先创建新数据库和隔离运行环境，旧卷保留到新版本验收且再次确认清理范围后。
- 不改前端技术栈，不新增业务功能，不把所有旧事件顺便改成 Outbox，也不借机重写现有认证模块。

迁移收益主要是可理解性与维护成本。若 PoC 后只是把 BSON DSL 换成更难懂的 ORM DSL，应暂停，而不是为完成迁移继续扩张。

## 2. 当前影响范围

按当前源码粗查，约 61 个生产文件直接使用 Mongo 驱动、MongoTemplate/Repository 或文档模型，另有测试、配置和脚本。数量仅用于规模判断，不是精确工期。

| 部分 | 当前 Mongo 用途 | 迁移工作 |
|---|---|---|
| platform-java | 帖子、关注、个人历史、后台统计、审核、日志、级联删除、部分评论副作用 | MyBatis-Plus Entity/Mapper 和显式 SQL；核对事务代理与字段更新范围 |
| interaction-rpc | 互动明细、统计、Outbox、缓存回填、用户统计 | SQL Store、目标行锁、唯一键、事务、发布器/积压采集 |
| comment-rpc | 评论树、权限、回复数、点赞读取与删除、MQ 失败补偿 | SQL Repository；保持根评论占位、子评论删除与补偿语义 |
| notification-rpc | 通知、未读、归并、版本与消费收据 | SQL 事务、条件唯一键、并发去重与已读保护 |
| search-rpc | 搜索历史、热门词回退 | SQL 历史存取；ES 搜索主体不因迁库重写 |
| sync-sidecar | 回查帖子建 ES 文档、重建、用户资料冗余同步、审计日志 | 从 SQL 回源；保留串行 ES 消费、重连与重建互斥 |
| gateway-api / user-rpc | 网关聚合；账号已在 MySQL | 主要改配置和联调，不重写鉴权、路由或用户模型 |
| 部署/测试 | Mongo 副本集、初始化、mtest、索引、mock 造数 | MySQL 隔离测试、版本化 DDL、造数、监控和运行说明 |

不能只迁移互动：它的事务还更新 posts/comments，而 Java 与 Sidecar 也读写这些数据。相关链路必须在同一阶段组成可验证闭环。

## 3. Go 数据访问：可以用 ORM，但限制使用方式

### 3.1 推荐与备选

| 方案 | 优点 | 成本 | 本项目判断 |
|---|---|---|---|
| GORM + 显式 SQL | 简单 CRUD、结构体映射、Context/事务入口统一；复杂查询仍可 Raw/Exec | 需要掌握默认命名、零值、软删除、关联和事务行为 | **新迁移模块首选 PoC**，前提是代码实际更好读 |
| 现有 go-zero/core/stores/sqlx | 已在 user-rpc 使用；直接写 SQL、少一种依赖 | DTO 映射和 CRUD 较手工 | 现有认证保持；PoC 不满意时的主要备选。不是 jmoiron/sqlx |
| database/sql | 明确、标准、可控 | 扫描与事务传递样板更多 | 可用于少量关键查询，不作为另一套并行封装 |
| sqlc / Ent 等代码生成方案 | 类型检查、结构化访问 | 多一套工具或生成约定 | 暂不引入，避免替换数据库时又增加学习面 |

PoC 对同一条“点赞事务”比较 GORM 与现有 sqlx 风格。只保留一种新模块主方案；不为比较搭建公共 DAO 框架。Java 保持已有 MyBatis-Plus，不再引入 JPA/Hibernate。

### 3.2 GORM 使用规则

- 显式 TableName/column，Entity、DTO、Proto 分离；不直接序列化数据库模型。
- 不嵌入 gorm.Model：其 ID 类型、时间和默认软删除不能自动决定既有业务语义。
- 不使用 AutoMigrate 管理共享表；版本化 SQL 是唯一结构事实源，不能让 Java 和各 Go 服务各自改表。
- 普通 CRUD 用有限的链式调用；锁、复杂聚合、条件更新、Outbox 用清晰可审查的 SQL。
- 每个服务一个业务连接池，GORM 与 Raw SQL 共用；不因框架选择额外维护平行连接池。
- 统一 WithContext(ctx)，有超时；事务回调只使用传入的 tx，不夹用全局 db 或另一个 sqlx 池。
- 禁用隐式关联保存和业务 Hook；不会自动 Preload 全关系。列表用批量查询，避免 N+1。
- 不使用 Save 更新整篇帖子，防止内容编辑覆盖 Go 维护的计数；Updates 用明确字段集合。
- false、0、空字符串需要实际写入时使用 map/Select，不能依赖默认忽略零值的结构体更新。
- 不全局关闭默认事务以追逐性能；先确认显式事务边界，再按测量结果局部优化。
- 不吞掉 duplicate key 或忽略所有插入错误。只把预期唯一键冲突归为幂等，其余错误必须回滚。
- ORM Debug 日志仅开发启用，不输出密码、Token、完整个人信息或带凭据的 DSN。

ORM 减少的是映射和 CRUD 样板，不替代索引、锁、隔离级别、事务设计或 EXPLAIN。

## 4. 目标数据库、ID 与通用约定

### 4.1 新库与结构管理

- 开发迁移默认使用新库 sharely_mysql_preview，同库包含账号和内容，原 user_db/Mongo 保持不动；服务 DSN 必须一起对齐。
- users 沿用现有表结构、字段类型与认证语义，通过新造数生成账号，不复制历史 dump 中的真实邮箱或个人资料。
- InnoDB、utf8mb4；业务 ID 使用明确的二进制/区分大小写比较规则，文本字段单独选择排序规则。
- 时间持久化统一 UTC 的 DATETIME(6)，Go/Java 显式转换；HTTP/Proto 时间格式保持现状，不让会话时区改变排序。
- 使用版本化 SQL（建议 db/migrations/ + 单一迁移执行器），记录版本及校验值；具体工具在 PoC 固定，不同时接入多套迁移框架。
- 同一数据库是为简化本项目部署，不宣称 database-per-service。明确表/字段写入归属；生产部署再按归属收紧账号权限。
- 不在启动函数中“缺什么就建什么”，不混跑旧 scripts/migrations 与新建库脚本。

### 4.2 ID 决策

默认先保留帖子、评论、通知、事件的 **24 位小写十六进制字符串外观**，数据库用 CHAR(24)；新 ID 由应用使用安全随机数生成，不再依赖 BSON/ObjectID。唯一约束冲突必须处理，不把随机 ID 当作绝对无碰撞。

这样避免第一轮迁移同时修改前端 ID 校验、MQ 载荷和 URL 形状。账号继续使用现有 BIGINT ID。仅内部 Outbox 排序可使用 BIGINT AUTO_INCREMENT，另保留独立 message_id/event_id。

这不是 MySQL 性能最优主键的承诺。若希望改为自增业务 ID，必须在阶段 0 审计前端/脚本所有格式校验并另行确认，继续以 JSON 字符串返回，避免 JavaScript 大整数精度问题。不能边写表边临时混用两种格式。

## 5. 建表蓝图

最终 DDL 在 PoC 后冻结。下表是必须覆盖的实体、约束与查询路径，不是可直接执行的建表脚本。

| 表 | 核心数据/唯一约束 | 主要索引与语义 |
|---|---|---|
| users | 复用账号、邮箱、密码、token_version、删除状态 | 保留账号现有约束与认证查询 |
| posts | 作者、标题、正文、类型、resources JSON、封面、审核/删除状态、互动计数、interaction_version、时间 | (user_id,created_at,id)、(status,is_deleted,created_at,id)；内容与计数字段分开更新 |
| tags / post_tags | 标签名唯一；关联 (post_id,tag_id) 唯一 | (tag_id,post_id)；统一标签筛选/热词，不在 Go/Java 使用不同 JSON 比较规则 |
| comments | post_id、作者快照、content、parent_id、reply_to、reply_count、like_count、删除占位、interaction_version | (post_id,parent_id,created_at,id)、(parent_id,created_at,id)；根 parent_id 为 NULL，出参仍为空串 |
| post_likes | (user_id,post_id) 唯一 | (post_id,user_id) 支持目标计数/缓存回填；按用户查询补时间索引 |
| post_collects | (user_id,post_id) 唯一 | 同上，支持个人收藏分页 |
| post_ratings | (user_id,post_id) 唯一、score DECIMAL(2,1) | (post_id,user_id)；明确只允许 0.5～5.0 的半分值，不使用浮点比较决定幂等 |
| comment_likes | (user_id,comment_id) 唯一 | (comment_id,user_id) |
| user_follows | (user_id,target_user_id) 唯一，保留现有展示快照 | 正向/反向查询索引；重复关注不再次加计数或发通知 |
| post_view_histories | (user_id,post_id) 唯一、view_time | (user_id,view_time,id)，再次浏览更新时间 |
| search_histories | (user_id,keyword) 唯一、updated_at | (user_id,updated_at,id)、keyword 前缀查询索引；限定输入长度与大小写语义 |
| notifications | 接收者、发送者快照、type、target_id、is_read、时间、last_event_at/source/version | (receiver_id,is_read,created_at,id)、(receiver_id,created_at,id)；状态型通知的条件唯一键见下 |
| notification_event_receipts | event_id 主键、processed_at | 去重；首版不自动过期，不改变历史重放语义 |
| interaction_outbox | 自增排序 ID、message_id 唯一、目标队列、type_id、payload JSON、headers JSON、created_at | (created_at,id)；确认后删除，失败保留，保持原路由与事件版本 |
| post_audit_logs | 帖子/标题快照、管理员、动作、原因、时间 | (post_id,created_at,id)、(operator_id,created_at,id) |
| api_logs | 操作分类、业务对象、用户、service/trace/request、结果、耗时、时间 | 按后台真实过滤组合建索引，不复制全部单列索引 |

资源只整组读写，首版用 JSON 保留顺序，不强制拆图片表；账号、关系、分页条件和约束不藏在 JSON 中。后续需要独立操作资源时再拆表。

MySQL 没有 Mongo TTL 索引的同等自动行为：api_logs 需显式保留策略和按时间/主键分批清理任务。当前 Java 注释写“90 天”，实际 TTL 为 2592000 秒（30 天）；首版按实际 30 天设计，并在实施时确认。不能把通知去重收据一并套用日志 TTL。

### 5.1 通知条件唯一键，不能遗漏

当前仅 LIKE_POST / COLLECT_POST / RATE_POST / LIKE_COMMENT / FOLLOW 唯一归并；COMMENT / REPLY / SYSTEM 等保留完整历史。

建议使用生成列 state_type：状态型时等于 type，否则为 NULL，再建 UNIQUE(receiver_id,sender_id,state_type,target_id)。其余三个键为 NOT NULL，避免意外 NULL 绕过约束；历史型依靠 state_type=NULL 允许多条。生成列表达式和类型由同一份 DDL 定义，ORM 不写该列。

必须测试：同一状态键归并成功、两条相同评论通知都可存在、系统消息不被误合并、并发首次创建不产生重复状态记录。

### 5.2 冗余数据与删除

首轮保留现有作者/发送者快照及计数，减少“换库 + 改展示语义”的叠加。资料更新/删除的 Sidecar 消费改成 SQL，并逐表列出更新/清理范围。以后可单独评估哪些展示字段改成 JOIN，哪些必须保留历史快照。

不使用 ORM 自动级联删除或跨服务全量 Save。帖子删除、用户注销、根评论占位、子评论物理删除、关联明细清理分别有明确操作顺序。必要外键默认 RESTRICT/NO ACTION，不用 CASCADE 偷偷替代事件和业务清理；多态 target_id 不强建指向某一实体的外键。

## 6. 表/字段写入归属与事务

| 归属 | 写入职责 |
|---|---|
| user-rpc | users 的账号、资料与认证状态；管理后台继续通过已有 user RPC |
| platform-java | posts 内容/审核/删除、标签关系、关注、浏览与审核日志；保留现有生命周期清理入口 |
| interaction-rpc | 互动关系、posts/comments 的互动计数和版本、interaction_outbox |
| comment-rpc | 评论与根 reply_count；首版不擅自接管 Java Listener 的帖子 comment_count |
| notification-rpc | notifications 与 notification_event_receipts |
| sync-sidecar | ES 写入、api_logs 和保留的用户资料/删除副作用 SQL |

同一表的字段级归属是当前双栈现实边界，必须测试 Java 编辑不会覆盖 Go 计数。生命周期删除是明确的协调例外，不是“各服务任意写别人数据”。

### 6.1 互动事务

首版优先保持当前算法，先把 COUNT/AVG 换成有索引的 SQL，不在迁库同一阶段强行优化成增量计数。

1. BEGIN；按主键 SELECT 目标 posts/comments FOR UPDATE，检查存在、删除/可互动状态。
2. 查询互动唯一关系；重复点赞/重复取消/同分评分直接结束，不产生新 Outbox。
3. 插入、更新或删除明细；在目标锁保护下重算目标计数/均值并更新 interaction_version。
4. 同一事务插入通知与搜索任务，包含现有 schemaVersion、eventId、Trace 和请求标识。
5. COMMIT；之后再失效 Redis，缓存失败不撤销已提交业务。

固定“帖子 → 根评论 → 子评论 → 互动明细”等锁序，并按操作实际涉及的资源保持一致；批量删除按 ID 排序。数据库错误分类明确，死锁/必要的锁等待超时只重试整个事务，有次数、退避和总超时上限；MQ/HTTP/邮件不放在重试回调中。

验收后再比较增量计数：仅在关系确实变化时 +1/-1，评分维护半分整数总量与人数。此为可选优化，不是切换前置条件；先有正确性与 EXPLAIN/压测基线。

### 6.2 通知消费事务

- 保留无需通知、自赞、目标不存在等原过滤语义，不强制给无副作用事件写收据。
- 需处理的事件在同一事务中写收据与通知，依靠 event_id 唯一键协调并发；只将该预期冲突视为“已处理”。
- 状态通知的首次并发创建要由唯一约束及受控 UPSERT/重试保护，不能只做无锁的“先查再插”。
- 同一源对象比较 interaction version，不同源按既有时间语义归并；不能跨源盲目比较版本。
- 重放不能把已读改回未读，旧事件不能覆盖新状态；提交后才 Ack。
- MySQL 不提供跨服务自动事务：Java 调 notification RPC 不属于调用方的 @Transactional。

### 6.3 评论与 Java Listener

当前 comment-rpc 负责创建、根 reply_count 与部分补偿，Java CommentEventListener 还维护帖子 comment_count、删除副作用、通知及 AI 机器人回复。先列清每条写入来源，尤其机器人直接写评论的路径。

首版将这些操作逐项翻译为 SQL，保留消息发布失败补偿与根软删/子硬删语义，不同时改变成功时点。避免 Go 和 Java 都扣 reply_count/comment_count，或提前删除明细后让消费者找不到补偿依据。

是否把评论及事件也收口到 SQL 事务 Outbox，应在该阶段单独决策和提交；若不做，明确保留的 MQ 失败窗口，不能因为数据库统一就宣称评论也获得互动的保证。

### 6.4 Outbox、Redis 与 ES

- 首版仍每进程串行发布；保持持久消息、mandatory、Confirm、退回检查和重连，重复投递语义不变。
- 不持有 MySQL 行锁等待 Broker 确认。多实例抢占/租约是后续需求，不为本地练手先引入 SKIP LOCKED 多 Worker。
- Outbox 积压 COUNT/MIN 独立于 MQ 连接采集，保留成功标记、更新时间与过期快照保护。
- Redis 仍为短 TTL、空值占位、Lua 完整回填；回源从 Mongo 改 SQL，不回退到缓存先写，不重引入 Bloom。
- ES mapping/索引/建议词与 HTTP 契约不变；Sidecar 从 SQL 读取当前状态，保留串行消费和重建互斥。资源/标签用批量查询组装，避免逐帖 N+1。
- 历史用户事件仍为原 best-effort 链路，迁库不会自动修复其发布可靠性；发布失败恢复另列 TODO。

## 7. 分阶段实施与退出条件

阶段结束可以编译/测试，不等于允许新旧混合栈接入同一业务库。必要的中间版本只在隔离测试中运行，最终整体切换。

| 阶段 | 产出与改动范围 | 必须通过的关卡 |
|---|---|---|
| 0. 决策与 PoC | 固定 ORM、ID、时间/删除/通知唯一规则；实现“最小帖子 + 点赞 + Outbox”试验 | 能解释生成 SQL；事务中途失败全回滚；重复请求、并发计数、锁超时可验证；用户确认再开展全量 |
| 1. DDL 与模型 | 新库、版本化建表、Go/Java 字段字典、连接池/只读查询装配 | 空库可初始化；第二次执行不重置数据；Java/Go 读写同一行一致，索引与约束可检查 |
| 2. 帖子/互动/Outbox/Sidecar | Java 帖子主写、Go 互动与回填、SQL Outbox、ES 回源/重建 | 发帖→点赞→SQL 明细与计数→Outbox→搜索；Java 编辑不覆盖统计；Mongo 不参与此闭环 |
| 3. 评论/通知 | SQL 评论树/权限/补偿、Java 评论 Listener、状态/历史通知与收据 | 根软删/子硬删、reply_count、通知去重/版本/已读、机器人路径；无重复计数 |
| 4. 其余业务 | 关注、资料同步、浏览/搜索历史、审核、日志、后台列表/统计、注销/删帖清理 | 逐项对照旧路由；分页、排序、空值、权限、统计及清理结果一致 |
| 5. 造数与全栈验收 | 新 mock 生成器、隔离运行配置、完整 Smoke、重启/MQ 故障、查询基线 | 从空库一键生成可演示数据，重启后状态保持，MQ 恢复补齐，ES 可从 SQL 重建 |
| 6. 下线 Mongo | 删除生产驱动/配置/副本集初始化、更新 CI/文档/AGENTS、审核依赖与镜像 | 不启动 Mongo 也能完成全链路；静态扫描和生产包均无 Mongo；用户确认后才清理旧资源 |

若阶段 0 的理解成本没有降低，或阶段 2 需要大量额外抽象才能工作，可以停止迁移，保留目前已经验证的 Mongo 版本。不要因已写几张表就继续投入。

## 8. 测试迁移与验收清单

旧场景按 [现有对应表](testing.md) 迁移，不把“移除 Mongo mock 测试”当作覆盖完成。保留业务断言，替换夹具与查询实现；真实 MySQL 测试补足 mock 无法验证的锁、隔离级别和索引行为。不用 SQLite 冒充 InnoDB。

- [ ] 七个 Go Module test/build/vet、Java 默认测试与 package 通过；默认测试无真实依赖。
- [ ] 真实集成只使用随机命名的测试库，带名字/实例检查；DDL、回滚和清理不会落到共享库。
- [ ] 事务在明细/统计/Outbox 任一步失败全部回滚；并发重复/取消/改分/删除目标均正确。
- [ ] Java/Go 同帖并发更新不覆盖对方字段；死锁重试无额外通知、事件或负计数。
- [ ] false/0/空值更新有效；逻辑删除筛选、parent NULL 与响应空串正确。
- [ ] 通知状态唯一、历史可重复；重复与旧事件不重置已读；消费者重启重放安全。
- [ ] MQ 中断时互动事实和 Outbox 保留；恢复后自动投递；Ack 后退出仍允许安全重投。
- [ ] 评论补偿、占位、权限、根/子计数和机器人路径各有断言；不静默扩大一致性保证。
- [ ] 用户资料变化、注销、帖子删除不留下可见孤儿关系；ES 同步/重建后的过滤与计数可核对。
- [ ] 完整五条 Smoke 均通过；FAIL 必须退出非零，异步只使用有上限轮询。
- [ ] 真实 ES 拼音召回和排序独立验收，fake ES 请求体测试不能替代。
- [ ] 关键 SQL 用 EXPLAIN 核对索引；普通/热点点赞、列表、评论、通知记录耗时与连接池等待，未测量不写性能提升百分比。
- [ ] Prometheus/Loki/Trace 仍工作；更新 mongo_error 等失效标签和 runbook，但不无故更改现有指标名称。
- [ ] 生产构建不含 Mongo 驱动；源码无 BSON/primitive.ObjectID 验证依赖；旧测试如保留则归档/明确 test-only，不重新拉回生产依赖。

## 9. 重建数据与安全切换

### 9.1 造数方案

新脚本建议 scripts/dev/seed-mysql，目标是“生成可复现的演示场景”，不是复制旧 dump。

1. 检查目标为明确的本地/测试环境、新库和配置指纹；普通 seed 只幂等补齐，不执行 DROP/TRUNCATE。reset 是独立命令，要求明确目标及二次确认。
2. 生成固定 mock 用户、管理员和 AI 机器人，密码使用当前 BCrypt 规则，凭据由测试环境显式提供；机器人 ID 与配置一致。
3. 沿用 Smoke 专用账号邮箱契约；不发送真实验证码、邮件、OSS 或 AI 请求。
4. 生成确定性的图文/视频/纯文帖子、标签和占位资源；保证发布状态与前端实际校验一致。
5. 生成关注、点赞/收藏/评分、根评论/回复、通知和历史；优先走新业务入口。必须直写的夹具在 SQL 事务中维护匹配计数/版本，不只堆关系行。
6. 等待 Outbox 排空或显式执行 Sidecar 重建，再用 API 核对关系、计数、未读和搜索。保存 seed manifest 供验证/定向清理。
7. 连续执行两次不增加重复关系或改变已经确认的已读状态；正常 seed 不重置已有用户密码。

资源 URL 使用本地占位文件或稳定的专用 fixture，不能依赖旧 OSS 对象仍然存在。

### 9.2 最终切换

- 记录旧代码提交及配置，不覆盖本地 .env；新连接放在独立示例/覆盖文件。
- 停止旧生产者/消费者并检查待发与在途任务。mock 数据可以放弃，但“放弃旧 Outbox/队列”的具体目标仍需确认。
- 使用独立 RabbitMQ vhost（保持逻辑队列/路由名）、独立 Redis 实例和新 MySQL 库；新 Compose 项目/端口覆盖需避免与旧环境冲突。不让旧消费者处理新库事件。
- 更新所有 Go/Java 的 DSN，尤其 user-rpc 和通知读取账号的连接，不能账号留旧库、内容进新库却仍假设同库事务。
- 新环境执行建表、造数、ES 重建、Smoke；新 ES 也优先使用隔离实例/索引别名方案，不能先清空原共享索引再验证。
- 验收后默认启动不再包含 Mongo；首次确认可在旧卷仍保留时完成。清理原卷、Broker 任务及旧 ES 数据需单独列出精确目标，禁止 docker compose down -v 一把删除全部资源。

不引入长期双写。回退是停新栈、切回旧提交与旧隔离资源，放弃新 mock 数据；不是让旧 Mongo 程序读取新 SQL 表。恢复旧资源仍有疑问时重新生成旧 mock，不能盲目混连。

## 10. 风险、工作量与开始条件

主要风险：共享表字段覆盖、字符串 ID/时间语义、通知条件唯一约束、评论计数多写入方、生命周期清理、ORM 零值/隐式操作、消息残留跨版本、测试只验 mock 不验 InnoDB。

不保留旧数据可以省掉转换和在线迁移，但不会省掉业务存储改写及整链路验证。粗略按专注开发估算：

- PoC/表结构与契约冻结：1～2 个工作日。
- 主要存储与事务闭环：4～7 个工作日。
- 剩余后台/历史/日志、造数、配置与故障回归：3～5 个工作日。
- 合计约 8～14 个工作日量级；这是规划区间，不是交付承诺。AI 可以减少样板输入，不替代审查、并发测试或本人理解，兼职复习的日历时间可能更长。

只有在确认以下三项后开始阶段 0：
- 愿意把这视为新的中等规模迭代，而不是最后几处修补。
- 接受 ORM 仅简化 CRUD，关键 SQL/锁仍需自己能解释。
- 当前清理版本已独立提交且可运行，可以随时停止迁移。

本轮只提交计划和清理，默认不开始阶段 0、不安装 GORM、不新增数据库。

## 参考资料

- [GORM 事务](https://gorm.io/docs/transactions.html)：多操作事务需显式组织，回调内使用 tx。
- [GORM 更新语义](https://gorm.io/docs/update.html)：Save/零值更新需要谨慎，不等同于按业务字段局部更新。
- [GORM 迁移说明](https://gorm.io/docs/migration.html)：AutoMigrate 会改结构，不作为本项目共享表的唯一迁移机制。
- [GORM 锁查询](https://gorm.io/docs/advanced_query.html)：可表达行锁，但并不替代锁顺序设计。
- [MySQL InnoDB 锁](https://dev.mysql.com/doc/refman/8.0/en/innodb-locking.html) 与 [死锁处理](https://dev.mysql.com/doc/refman/8.0/en/innodb-deadlocks.html)：使用索引、短事务、一致锁序及失败重试。
