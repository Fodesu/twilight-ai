# Twilight Cloud Agent 组件边界

本文档定义 cloud agent 的部署组件：每个组件拥有什么状态、对外暴露什么协议、依赖 Agent Core 的哪些端口接口。它只划分边界，不规定实现语言之外的技术选型；`agent-runtime.md` §11 的两种部署形态中，本文档展开的是"云端"一栏。编号前缀 `CLD-`。

Agent Core 的协议规则（`agent-run.md`、`agent-runtime.md`）在所有组件中不变。组件之间只经这些规则已经定义的端口通信；本文档新增的协议只有一条：Worker 与 Backend 之间的 wire（CLD-WIR）。

## 1. 划分依据

**CLD-CMP-1（划分标准）** 组件按契约与状态归属划分，不按进程划分。两个组件可以部署在同一进程或同一 Pod，边界不变；一个组件不得跨两个 authority 持有状态。判定一个候选是否为独立组件的三个条件：它拥有其他组件不持有的状态或凭证；它的伸缩单位与相邻组件不同；它与相邻组件之间已有或需要一条 message-shaped 的接口。三者满足其一即分开。

**CLD-CMP-2（清单）** 共七个组件。

| 组件 | 拥有的状态 / 凭证 | 对外协议 | 依赖的 Core 端口 | 伸缩单位 |
|---|---|---|---|---|
| executor worker | execution ledger 的租约与 fencing（authority 在共享 store）；进程内 `ProgressHub`、`SettlementHub` | `effect.ExecutionPort` 的 HTTP 绑定（`executor/http`） | `executionstore.Store`；`ExecutionBackend`（经 CLD-WIR） | ledger 吞吐 |
| model backend | provider 凭证、base URL、限流配额；in-flight 表 | CLD-WIR 的 Backend 协议 | `sdk`、`provider/*`、`loop.ModelCatalog` | provider 并发 |
| tool sandbox backend | sandbox 生命周期、workspace materialization；in-flight 表 | CLD-WIR 的 Backend 协议 | `loop.ToolCatalog`、`agentcore/environment`、`agentcore/workspace` | sandbox 资源，按 session 或 tenant 隔离 |
| owner service | Session 租约（`session.OpenOptions`）；Writer 内存投影 | 命令面（Send、Turn 状态、Fork 等 `app.Application` 的方法）；观察流（`observe.Bus`） | `session.Backend`、`artifact.ContentStore`、`owner.Artifacts`、`process.Store`、`effect.ExecutionPort`（`executor/http.Client`）、`effect.SettlementPort` | Session 数 |
| controller | 无持久状态；策略参数（放弃时限、扫描间隔） | 内部 | `session.Store.ListLeases`（SES-OWN-5）、`effect.ExecutionPort.Attach`、`effect.Recoverer`、Worker 的 `Dispose`、owner 的 Open | 单实例或 leader 选举 |
| gateway | 无持久状态；认证会话 | 面向用户的 HTTP/WebSocket | owner 的命令面与观察流 | 连接数 |
| 共享存储 | 全部 durable 事实：Session ledger 与 lineage、execution ledger 与租约、dispatch ledger、artifact binding 与 claim、CAS 正文 | SQL 与对象存储 | `session.Backend`、`executionstore.Store`、`process.Store`、artifact stores 的实现 | 存储容量 |

**CLD-CMP-3（不在清单中的）** Reconciler、Loop、Driver、Watcher 都是 owner service 进程内的组成，不是组件：它们没有自己的状态归属，也没有 message-shaped 的对外接口。Responder（包括 spawn 子代理）同样在 owner 内。preset 注册表按 OWN-SCP-2 可以是进程内或共享服务，本文档把它归入 owner service，共享化留作后续。

## 2. 各组件

### 2.1 executor worker

**CLD-EXE-1** 组件即 `executor.Worker` 加 `executor/http.Server`。它持有 `executionstore.Store` 的连接，路由表（`executor.Route`）的每个 provider 指向一个经 CLD-WIR 连接的 backend。进程内没有 provider 凭证，也没有工具实现；`loop.NewLocalExecutor` 不在此组件中。

**CLD-EXE-2** 对 owner 暴露的端点为现有的 `/validate`、`/dispatch`、`/attach`、`/abort`、`/status`、`/outcome`、`/cancel`、`/recover`、`/dispose`、`/acknowledge`、`/progress`、`/settlements`（RUN-EXE-3、RUN-EXE-12、RUN-EXE-17）。多副本时任一副本可应答任一 key：Attach、GetOutcome、Abort 只读 ledger；Dispatch 的重放与 RecoverExecution 由 ledger 的 Seq 0 竞争与租约裁决（RUN-EXE-14、RUN-EXE-16）。owner 对每个 Worker 副本保持一条 `/settlements` 订阅（CLD-OWN-3）。

**CLD-EXE-3** Worker 副本的 `WorkerOptions.ID` 在重启后不得复用（现有约束）；k8s 下取 Pod 名加启动时间戳。`SettlementHub` 的 epoch 随之变化，订阅者按 RUN-EXE-17 重读。

### 2.2 model backend

**CLD-MDL-1** 组件承载 `sdk` 与 `provider/*`：把 `effect.ModelAssignment` 内联的冻结请求交给 provider，流式 delta 作为进度帧上行，结果作为 Outcome。provider 凭证、base URL、region、配额只存在于此。今天这部分逻辑在 `loop.LocalExecutor.runModel` 与 `invokeModel` 中；组件化后它们移入 model backend，`LocalExecutor` 保留为单进程形态的 in-process backend。catalog 是 `agent/models`（不在 agentcore）：`Entry{Ref, Kind, Model, BaseURL, APIKeySecret | AuthTokenSecret}` 把 preset 冻结的逻辑 ModelRef 映射到一个 provider 的物理 model id；条目只命名密钥、不携带密钥值，`Build(ctx, entries, resolver)` 经 `secrets.Resolver.Lookup(name)` 解析，并按 (Kind, BaseURL, 凭证) 共享 provider 实例。`agent/secrets` 是部署层的密钥读取抽象，local 与 cloud 共用，models 只是消费者，后续需要凭证的工具、workspace、backend 同样消费它：cloud agent 用 `secrets.Dir`（k8s Secret 挂载为目录，每个 key 一个文件，值只去掉一个结尾换行，名字须为单个路径元素），local agent 用 `secrets.Static`（从本地配置装入），有 vault 的宿主自行实现 `Lookup`；catalog 与二进制都不读进程环境变量。catalog 文档本身是一个 JSON（`models.Load`，`{"models":[...]}`，恰好一个文档，含字面凭证或尾随内容即拒绝），在 k8s 中来自 ConfigMap，同一份文档在两种部署下不变。`*sdk.Model` 本身满足 `loop.ModelInvoker` 与 `StreamingModelInvoker`，适配只做一件事：把请求里的逻辑名清空，由 sdk 填入物理 id。catalog 只是查找表，不做路由、回退、配额与 key 轮换；需要它们时把 Entry 的 BaseURL 指向一个网关。

**CLD-MDL-2** Ref 是 backend 自己的 in-flight 标识。`Prepare` 按 AssignmentKey 派生（不分配任何资源，RUN-EXE-3）；`Start` 分配 in-flight 条目并发起 provider 调用；`Attach` 按 Ref 查 in-flight 表，条目存在为 active 或 terminal，不存在为 missing。model backend 无持久状态，重启后所有 in-flight 都是 missing，Worker 按 RUN-EXE-9 对模型 Assignment 重派，`Restart` 返回新 Ref。

**CLD-MDL-3** 限流、key 轮换、按 tenant 的配额是本组件的策略；Worker 与 owner 看到的只是 Outcome 里的失败分类与 `RetryDisposition`（RUN-EXE-11）。

### 2.3 tool sandbox backend

**CLD-TOL-1** 组件承载 `loop.ToolCatalog` 的实现与 `agentcore/environment`、`agentcore/workspace`：在 Assignment 携带的 Target 上 materialize 工具运行环境并执行。Target 的解析在 owner 侧完成（`loop.TargetResolver`，APP-TGT），backend 只接收已解析的 `run.TargetRef`。

**CLD-TOL-2** Ref 是 sandbox 内一次执行的标识；`Attach` 按 sandbox 状态回答，sandbox 仍在但执行记录不在为 missing，sandbox 本身不可达为 orphaned（不得回答 missing，RUN-EXE-3）。工具是否可重派由 Assignment 的 Replay 声明决定，Worker 在调用 `Restart` 前判定（RUN-EXE-9、TRN-DUR-4）；backend 不做这个判断。

**CLD-TOL-3** sandbox 的生命周期（按 session 创建、空闲回收、配额）是本组件的策略，与 Run 协议无关。sandbox 回收后其内的 Ref 全部 missing。

### 2.4 owner service

**CLD-OWN-1** 组件即 `app.Application`：`owner.New` 组装的 Owner、Driver、按 PresetRef 缓存的 Loop、每个 Session 的 Reconciler，加 `observe.Bus`。它持有 Session 租约（`session.OpenOptions.LeaseDuration`、Writer 心跳续租，SES-OWN-1）和 Writer 的内存投影；一切 durable 事实在共享存储。

**CLD-OWN-2** 端口实现：`session.Backend`、`artifact.ContentStore`、`owner.Artifacts`、`process.Store` 为共享存储的客户端；`effect.ExecutionPort` 为 `executor/http.Client`。owner 进程内没有模型客户端与工具实现（OWN-PRT-2）。

**CLD-OWN-3** Driver 持有一个 `effect.Watcher`（RUN-EXE-17）；多个 Worker 副本共用一个 owner 可见的地址时（k8s Service），一条订阅落在一个副本上，只收到该副本的结算通知。因此 Watcher 的周期性重读是必要路径而非兜底；或者 owner 对每个 Worker 副本各持一个 Watcher，`http.Client` 需要能枚举副本（headless Service）。本文档选后者作为目标，前者作为过渡。

**CLD-OWN-4** 多副本 owner：一个 Session 在任一时刻只有一个 owner 副本持有其租约。哪个副本 Open 哪个 Session 由 gateway 的路由与 controller 的接管决定（CLD-GWY-2、CLD-CTL-2）；owner 本身不做副本间协调，`OpenOptions.Takeover` 是唯一的接管开关。

### 2.5 controller

**CLD-CTL-1** controller 承担 RUN-EXE-6 明确归部署的决定：何时、对哪些、由谁触发恢复与放弃。它没有持久状态，全部依据来自共享存储与端口的读取。

**CLD-CTL-2（Session 接管）** 发现来源是 Session 租约过期：`session.Store.ListLeases` 返回每个有持有者的租约（SES-OWN-5），controller 对照自己的时钟挑出 `UntilUnixMilli` 已过的。对过期租约的 Session，controller 选择一个 owner 副本执行 `Open(sid, Takeover: true)`；Open 内部的 `RecoverInterrupted` 完成该 Session 全部 Run 的效果处置（RUN-CMT-7）。

**CLD-CTL-3（效果恢复与放弃）** owner 存活时，其 Reconciler 已按 `OrphanProbe` 对自己 Run 的 orphaned 效果调用 `RecoverExecution`。controller 只处理 owner 不存在时的情形（随 CLD-CTL-2 的 Open 一起完成）与放弃：一个效果 orphaned 超过放弃时限后调用 Worker 的 `Dispose`，owner 下一次读到 Unknown 后按 RUN-CMT-7 处置。放弃时限是 controller 的参数，协议层无界。

**CLD-CTL-4** controller 的所有动作都是幂等的端口调用；两个 controller 实例同时运行只造成重复调用，不造成状态分歧。单实例部署即可，leader 选举为可选。

### 2.6 gateway

**CLD-GWY-1** gateway 是用户面：认证、把用户输入转为 owner 命令面的调用（Send、Turn 状态、Fork、Withdraw）、把 `observe.Bus` 的事件推送给客户端（SSE 或 WebSocket）。它不持有 Session 状态，不读共享存储。

**CLD-GWY-2** 路由：一个 Session 的命令必须到达持有其租约的 owner 副本。gateway 经 `session.Store.LeaseOf` 读取当前租约的 `Owner`（SES-OWN-5）（`OpenOptions.Owner` 记录的进程标识需要能映射到网络地址）或维护一张 session→owner 的路由表；租约无持有者时选择一个 owner 副本 Open。命令到达错误副本时 owner 返回 conflict（OWN-HDL-2），gateway 重查路由后重试一次。

### 2.7 共享存储

**CLD-STO-1** 今天所有 durable store 只有 SQLite（`agentcore/store/sqlite`：execution ledger 与租约、dispatch ledger、artifact binding 与 claim）与 filestore（Session ledger、CAS 正文）实现。两者依赖本地文件与文件锁，无法跨 Pod 共享。cloud 形态需要以下接口的共享实现，这是工作量最大的一项：

| 接口 | 内容 | 目标后端 |
|---|---|---|
| `session.Backend`（`LedgerStore` + `SessionStore` + `CreateSession`） | Session ledger、lineage、租约 | Postgres |
| `executionstore.Store` | execution ledger、租约 | Postgres |
| `process.Store` | dispatch ledger | Postgres |
| artifact binding、retention claim | 引用与回收 | Postgres |
| `artifact.ContentStore`（CAS 正文） | frozen 请求、大对象 | 对象存储或 Postgres 大对象 |

**CLD-STO-2（接口审查项）** 移植前需确认接口在 Postgres 语义下可满足：`executionstore.Store.Acquire` 的租约事务（读 fold、读租约行、写租约行、追加 claimed 事件在一个事务内）；Seq 0 的 acceptance 与 abort 竞争依赖唯一约束而非乐观锁（RUN-EXE-16）；`ListOwned` 需要租约行按 owner 的索引；Session ledger 的 `Append` 以 `(segment, seq)` 唯一约束实现 ErrConflict；CAS 的 digest 去重。审查结果记入本节。

**CLD-STO-3（租约读取）** controller 与 gateway 读取 Session 租约的接口为 `session.Store.LeaseOf` 与 `ListLeases`，定义于 SES-OWN-5（`agent-session.md`），filestore 已实现，共享存储实现随 CLD-STO-1 一起提供。

## 3. Worker 与 Backend 的 wire

**CLD-WIR-1（Worker 与 Backend 的 wire，已定）** model backend 与 tool sandbox backend 都是远端后，Worker 进程内不再有任何 backend，`ExecutionBackend` 必须有一条 wire。两个方案曾被比较：

方案 A，Worker 链。backend 自身是一个 Worker，前置 Worker 经 `executor.PortBackend` 把远端 `ExecutionPort` 适配为 backend。不需要新协议，进度与结算通知经 `PortBackend` 中继。代价：同一 effect 在两级 Worker 各有一条 ledger 与租约，模型与工具都远端后为三份；backend 侧需要 `executionstore.Store`；`Restart` 对 Port-shaped backend 返回同一 Ref，RUN-EXE-9 的新一代 Ref 语义在链上退化。

方案 B，Backend 协议。把 `ExecutionBackend` 直接做成 HTTP 协议：`/prepare`、`/start`、`/restart`、`/attach`、`/status`、`/cancel` 按 Ref；`Outcome` 不再是阻塞读（现有接口注释"阻塞到终态"在网络上重现 RUN-EXE-17 解决过的问题），改为 `/outcome` 纯读加 backend 侧的结算通知流，Worker 的 watch 循环订阅它；进度帧由 Worker 从 backend 的 `/progress` 拉取并发布到自己的 `ProgressHub`。backend 无 ledger，只有 in-flight 表；ledger 只在 Worker 一处。

选定方案 B：`ExecutionBackend.Outcome` 在 Go 接口上即为一次读取（RUN-EXE-17），Backend 可选实现 `notice.Source` 提供按 Ref 的结算通知流，等待归 Worker；Backend 协议只是一对无状态适配器（`agentcore/executor/backendhttp`）。`Server` 把一个进程内 backend 暴露为 `/validate`、`/prepare`、`/start`、`/restart`、`/attach`、`/status`、`/outcome`、`/cancel`、`/progress`、`/notices`，每个端点都是对 backend 的一次调用，Server 自己不持有任何表：`/outcome` 转发 backend 的读取（未结算为 204，未知 Ref 为 404，永不可读为 410），`/notices` 把 backend 的 `Settled` 流以 SSE 转发（首个事件前的淘汰为 410；backend 无通知源时回答立即结束的空流）。`Client` 实现 `ExecutionBackend`、`effect.ProgressPort` 与 `notice.Source`，同样无状态：`Outcome` 是一次 `/outcome` 读，`Settled` 是一次 `/notices` 流，谁在等由驱动它的 Worker 决定，Worker 对每个 Backend 只保持一条 `Settled` 订阅。`Start` 的 400 为 backend 的确定拒绝，传输失败与其他状态为 `ErrDispatchUnknown`；`Attach` 的传输失败为 error 而非观察。local agent 不经这条 wire：Worker 进程内直接挂 `LocalExecutor`，一行不改。ledger 只在 Worker 一处（RUN-EXE-10），backend 无状态，Server 重启后 backend 对它持有过的每个 Ref 回答 missing，Worker 按 RUN-EXE-9 重派或按 Replay 声明结算。

**CLD-WIR-2** 无论哪个方案，`Prepare` 都不得在 backend 侧分配资源（RUN-EXE-3 与 RUN-EXE-16：Abort 可能抢先），资源分配在 `Start`。

## 4. 开发集群

**CLD-DEV-1** 开发与单机检验使用 k3d。一个 namespace 内：worker Deployment 2 副本、model-backend Deployment 1 副本、tool-backend Deployment 1 副本、owner Deployment 1 副本（多副本在 CLD-GWY-2 的路由完成后）、gateway 1 副本、controller 1 副本、Postgres StatefulSet 1 副本，对象存储可选（MinIO）。Worker 以 headless Service 暴露，供 owner 枚举副本（CLD-OWN-3）。

**CLD-DEV-2（故障检验）** 集群上要检验的三类故障，每类对应协议中已定义的恢复路径：

| 故障 | 期望路径 |
|---|---|
| 删除持有效果租约的 worker Pod | owner 的 Watcher 在 `OrphanProbe` 内观察到 orphaned，`RecoverExecution` 由另一副本接管；backend 侧执行仍在则 Attach 后继续观察，结算经新副本的 `/settlements` 到达 |
| 删除持有 Session 租约的 owner Pod | 租约过期后 controller 触发另一副本 `Open(Takeover)`，`RecoverInterrupted` 对 Executing 效果 Attach 并 keep/defer/dispose |
| 删除 backend Pod | Worker 的 watch 读到 Attach missing：模型 Assignment 经 `Restart` 重派为新一代；工具 Assignment 按 Replay 声明重派或以 Unknown 结算 |

## 5. 顺序

1. CLD-WIR-1 的 Backend 协议适配器对（`agentcore/executor/backendhttp`）：已完成。
2. `cmd/worker`、`cmd/model-backend`、`cmd/tool-backend`、`cmd/owner` 四个二进制，单机多进程跑通 Dispatch、GetOutcome、RecoverExecution 与 Session 接管。此步仍用 SQLite 与 filestore，各进程共享同一文件路径只用于单机验证。
3. CLD-STO 的 Postgres 与对象存储实现，先做 CLD-STO-2 的接口审查。
4. k3d 清单与 CLD-DEV-2 的故障检验。
5. controller 与 gateway。

## 6. 未决

- `OpenOptions.Owner` 到网络地址的映射方式（CLD-GWY-2）：写入租约行，或由 owner 副本向注册表登记。
- preset 注册表是否共享化（CLD-CMP-3）。
- tool sandbox 的隔离粒度（按 session 还是按 tenant）与 workspace 的持久化位置。
