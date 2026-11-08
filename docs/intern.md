# 个人实习复习笔记

本文保留通用基础与其他项目的历史学习笔记，部分运行时实现细节具有版本依赖，不作为 Sharely 当前实现规范。项目介绍、取舍与案例统一见 [项目复盘](resume_project_summary.md)。

---

## 一、八股文精准押题

不要背几百页的文档，只看下面这些"必死题"。

### 1. Go 语言（重中之重）

简历有 Go 项目，这部分一定会被深问。

**GMP 调度模型**（腾讯/字节必问）

- G（Goroutine）、M（OS 线程）、P（逻辑处理器）分别是什么
- P 的数量默认等于 CPU 核数，M 和 P 动态绑定
- Work Stealing：P 的本地队列空了会去其他 P 的队列偷任务
- 抢占式调度：1.14 之前基于函数调用抢占，1.14 之后基于信号量异步抢占

**GC 垃圾回收**

- 三色标记法：白色（待扫描）→ 灰色（已发现未展开）→ 黑色（已处理）
- 写屏障（Write Barrier）：并发标记期间，防止黑色对象引用新增白色对象被漏扫
- STW 发生两次：标记开始前和标记结束时，现代 Go 已压缩到毫秒级

**Channel**

- 底层结构是环形队列 `hchan`，有缓冲时发送方直接写队列，无缓冲时发送方阻塞等接收方
- 向关闭的 channel 写数据：**panic**
- 从关闭的 channel 读数据：返回零值和 `false`，不 panic

**Map**

- 线程不安全，并发读写会 panic（可用 `sync.Map` 或加 `sync.RWMutex`）
- 扩容：等量扩容（整理空洞）或翻倍扩容，采用渐进式迁移避免单次大 STW
- 底层是哈希桶 + 溢出链，每个桶存 8 个 kv

**Slice**

- 底层是 `{array, len, cap}` 三元组，切片是对底层数组的引用
- 扩容：Go 1.18 之前容量 < 1024 时翻倍，≥1024 时按 1.25 倍增长；1.18 之后平滑过渡
- 注意：对切片的修改可能影响原数组，append 超容量后才会分配新数组

---

### 2. 操作系统（基于 xv6 重点展开）

面试官问 OS，把话题往 xv6 引。

**虚拟内存**

> "在 xv6 中，每个进程有独立的页表（pagetable），用户态映射低地址，内核态映射高地址（TRAMPOLINE / KSTACK）。虚拟内存的核心价值是进程隔离：进程 A 读不到进程 B 的物理页，哪怕它们的虚拟地址相同。"

**Copy-on-Write (COW)**

- `fork()` 时不复制物理页，只复制页表并将 PTE 设为只读
- 父子进程任意一方写入时触发 Page Fault，内核才分配新物理页
- xv6 中 `usertrap()` 捕获这个异常，检查 PTE 的 COW 标志位后执行实际复制

**进程切换**

- `context switch` 保存 callee-saved registers（ra、sp、s0-s11）
- 用户态 → 内核态 → 目标进程内核态 → 目标进程用户态，经过两次上下文切换

---

### 3. 数据库（通用原理为主）

**索引**

- B+ 树而不是红黑树：B+ 树是多路树，高度低，一次磁盘 IO 读一个 node 能覆盖更多 key
- 聚簇索引：叶节点存整行数据；非聚簇索引：叶节点存主键，需要"回表"

**事务与 MVCC**

- MySQL InnoDB：通过 undo log 保存历史版本，Read View 判断可见性，实现 MVCC
- PostgreSQL：通过每行的 `xmin` / `xmax` 字段判断哪个事务可见，不需要 undo log，但需要 VACUUM 清理过期版本
- 隔离级别：RC（每次 select 创建新 Read View）vs RR（事务开始时创建一次 Read View）

**WAL（Write Ahead Log）**

- 修改数据前先写日志，保证崩溃后可以通过 redo 恢复
- 顺序写日志比随机写数据页快得多，这是 WAL 性能优势的来源

---

### 4. 计算机网络

**TCP 三次握手**

- 第一次（SYN）丢失：客户端超时重传 SYN
- 第三次（ACK）丢失：服务端重传 SYN-ACK，客户端再次发 ACK；若客户端已开始发数据，服务端会用数据包中的 ACK 确认连接

**HTTPS TLS 握手（记住区别）**

- RSA：客户端用服务端公钥加密预主密钥，服务端私钥解密 → 不支持前向安全
- ECDHE：双方各自生成临时密钥对，交换公钥后独立计算共享密钥 → 支持前向安全，服务端私钥泄露不影响历史会话

**I/O 多路复用**

- select/poll：每次调用需遍历所有 fd，O(n)
- epoll：内核维护就绪列表，`epoll_wait` 只返回就绪 fd，O(1)
- LT（水平触发）：fd 就绪时反复通知，直到处理完毕
- ET（边缘触发）：只在状态变化时通知一次，需要一次性读完，Go netpoller 底层基于 epoll ET

---

## 二、算法题战术

**语言**：现代 C++（`auto`、`vector`、`lambda` 随便用）。

**刷题范围**：只刷 LeetCode Hot 100，不要分散精力。

**分类击破顺序**：

| 类型 | 必刷题 | 时间要求 |
|------|--------|---------|
| 链表 | 反转链表、环检测、合并有序链表 | 5 分钟内 bug-free |
| 二叉树 | 层序遍历、最近公共祖先（LCA） | 能口述递归思路 |
| Top K | 堆（priority_queue）、快速选择 | 两种方法都要会 |
| LRU Cache | `list` + `unordered_map` 手写 | 必须能白板手写 |
| 动态规划 | 子序列、背包、跳跃游戏 | 会写状态转移方程 |

---

## 三、项目面试策略

### Sharely 内容社区

项目职责、技术选型和可展开案例统一见 [项目复盘与面试](resume_project_summary.md)，代码边界见 [架构](architecture.md)。复习重点为登录鉴权、互动事务 Outbox、通知幂等及搜索最终一致性，不再维护第二份项目介绍。

### MIT 6.S081 xv6（防守反击）

面试官问 OS，主动往 xv6 引：

> "我在实现 xv6 内核的 Lazy Allocation 时，忘记在 `uvmunmap` 里处理未分配的页面，导致 kernel panic。这让我理解了 PTE 的 valid 标志位不仅用于 TLB miss 的地址翻译，也用于 unmap 时判断是否需要释放物理页。"

这个 bug 能体现你对页表项状态位的真实理解，而不是只背过概念。

---

### PostgreSQL Symmetric Hash Join（奇兵）

- 放在"课程项目"里，不要放在"实习/工程经历"里（降低面试官期望）
- 只准备原理，不准备代码细节
- 核心亮点：**Symmetric Hash 适合流式数据**，不需要等整张表构建完 Hash 表就能出结果，适合无界流（和 B+ 树、普通 Hash Join 形成三角对比）
- 被问太深时：诚实划定边界，"这是课程大作业，主要关注算法逻辑，对 PG 存储引擎底层的 dirty page 落盘机制涉猎不深"

---

## 四、Go 深度学习路线（面试前按优先级刷）

### 第一梯队：必看（三座大山）

1. **GMP 调度器**：P 和 M 的关系？Work Stealing？抢占式调度？
2. **GC 垃圾收集器**：三色标记？写屏障解决什么问题？STW 什么时候发生？
3. **Channel 底层**：`hchan` 结构？有缓冲 vs 无缓冲的阻塞逻辑？

### 第二梯队：项目强相关

1. **Context**：gRPC / HTTP 超时控制和链路追踪都依赖 Context，面试常问"Context 怎么取消 Goroutine"
2. **Netpoller**：为什么 Go 写高并发网络服务性能好——goroutine + epoll 的组合
3. **defer**：执行顺序（LIFO）、defer 和 return 的执行顺序（return 先赋值，defer 后执行）
4. **Interface**：`iface` vs `eface`，接口为 nil 和接口内值为 nil 的区别（经典坑）

### 第三梯队：有空看

- `for / range` 循环变量的坑（新版本已修复，但老代码还有）
- `select` 的随机选择机制
- 内存分配器 TCMalloc 思想（`mcache / mcentral / mheap` 记住这三个名词）

### 不用看

- 编译原理、机器码生成（除非面编译器组）
- 插件系统、元编程、标准库源码（JSON/HTTP/DB 等）
- 字符串、数组基础章节（扫一眼即可）

**推荐阅读顺序**：GMP → GC → Channel → Map/Slice → Context → Netpoller

---

## 五、数据库深度学习路线（配合 CMU 15-445）

### 配合项目实战的重点章节

**第 14 章：索引**
- B+ 树（Bustub 核心）
- **LSM Tree（必看）**：你用到的 Milvus、RocksDB 底层都是 LSM。核心 Trade-off：写优化（批量顺序写）vs 读代价（可能需要查多层 Level）

**第 18 章：并发控制**
- MVCC 实现（MySQL vs PostgreSQL 的差异）
- 快照隔离（Snapshot Isolation）vs 可重复读（Repeatable Read）

**第 19 章：恢复系统**
- WAL：先写日志再写磁盘
- ARIES 算法：配合 Project 4（Recovery）精读

**第 21、23 章：分布式**
- 数据分区与一致性哈希
- 2PC（两阶段提交）
- Raft（比 Paxos 易懂，推荐配合 Raft 论文或 MIT 6.824 视频）

### 可以跳过

- 第 1-5 章（SQL 基础，你会写 Bustub 说明没问题）
- 第 6-7 章（ER 图、范式，实战中基本反范式）
- 第 8-9 章（XML、Servlet，上个世纪的技术）
- 第 10-11 章（大数据分析，不如直接看 DDIA）
