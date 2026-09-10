# Twilight Agent Runtime 重构记录

状态：迁移记录，非协议规范

当前 Run/Loop 协议的唯一 authority 是 [agent-run.md](agent-run.md)。本文只保留重构背景、已接受的 package 决策、完成状态与后续迁移工作；实现与本文冲突时，以各领域正式规范为准。

正式规范：

| 领域 | authority |
|---|---|
| Run Machine、Runtime、Loop | [agent-run.md](agent-run.md) |
| Session ES kernel | [agent-session.md](agent-session.md)（草案） |
| Artifact Core | [agent-artifact.md](agent-artifact.md)（草案） |
| Session Module Framework | [agent-session-extension.md](agent-session-extension.md)（草案） |
| Chatlog ontology/projection | [agent-session-chatlog.md](agent-session-chatlog.md)（草案） |
| Turn→Run coordination/materialization | [agent-turn.md](agent-turn.md)（草案） |
| 参考组装（Agent/Profile / Planner / Input / Session 宿主） | [agent-reference-assembly.md](agent-reference-assembly.md)（草案） |

## 1. 背景

重构前的 agent execution 代码混合了 SDK transport、Run state、Loop、history、queue 与 application policy，导致：

- 单次模型调用和多步 Agent execution 边界不清；
- state mutation、event persistence 和恢复路径缺少统一 authority；
- local 与 durable execution 使用不同抽象；
- queue、Session history 和 Run progress 容易形成多份长期事实；
- package 边界无法表达不同变化周期。

本次重构把 Agent Core 收敛为相互独立的 Run、Session、Artifact、Session Module、Chatlog 和 Turn 协议，并保留 `sdk` 作为单次 provider transport boundary。

## 2. 已接受的架构决策

### 2.1 authority

```text
Session stream    唯一 authority：twilight/turn、twilight/chatlog、twilight/run 事件同在一条 stream，一行一个 event
Session 所有权     一个 Session 同一时刻一个 Writer 进程；Epoch fencing 拒绝旧写者
writer.Writer  进程内唯一写入口：串行、重放判定、admission、claim、投影
MachineState      Run 的语义状态投影（twilight/run/machine），投影缓存为可丢弃缓存
Runtime           Run command 的提交入口：Writer 内 Decide、Evolve、companion，一次 Append
FrozenValueStore  内容寻址旁存：模型请求本体（含工具定义）
```

2026-09-04 之前的设计为两条 ES（Run 独立的 `RunHeader + TransitionRecord[]`，Turn 把 Run 事实 materialize 到 Session）。该设计已被第 6 节记录的决定取代；第 7 节记录 2026-09-04 架构审查后的修订（多写者临界区、控制面 KV、lease），第 8 节记录 2026-09-08 的修订（Session 级单写者、扁平事件）。上表为第 8 节之后的形态。

### 2.2 package layout

```text
agent/es                  shared ES primitives
agent/jsonstable          immutable canonical JSON
agent/run                 Run Machine、frozen values、fact codec、fold、Runtime 与 Companion contract
agent/run/loop            in-process model/tool interpreter 与 observation ports
agent/session             追加日志 kernel（Create、Header、Open 所有权与 Epoch、Append 整组、Read）；Memory 与文件 adapter
agent/session/extension   Session Module Framework：first-party Registry、payload 版本、admission、Writer、ProjectionReader 与缓存
agent/session/chatlog     first-party Message ontology
agent/session/run         first-party Run module：EventDefinition、machine projection、Runtime 实现（经 Writer 写入）、接管处置、FrozenValueStore
agent/artifact            Ref、Binding、自持久化的两态 RetentionLedger、回收前核对
agent/turn                Turn 生命周期、attempt、CompanionV1
```

文件用于提高同一 package 内的导航性；subpackage 只用于依赖限制和独立变化轴。Loop 因依赖 SDK execution、streaming、并发和工具 ports 而独立成 `agent/run/loop`。Machine、protocol 与 Runtime contract 保持在根 `agent/run`；Runtime 实现与 adapter 在 `agent/session/run`，与 `chatlog` 同级。

依赖方向为：

```text
Application        -> agent/turn + agent/run/loop + agent/session/run + adapters
agent/run/loop     -> agent/run + agent/session（identity）+ sdk
agent/run          -> agent/es + agent/jsonstable + agent/session（identity、Store 类型）+ sdk
agent/session/run  -> agent/run + agent/session + agent/session/extension
agent/turn         -> agent/run + agent/session + agent/session/extension + agent/session/chatlog
```

run、turn、chatlog 三个模块构成一个 agent 领域，耦合方向固定为 turn → run、turn → chatlog；它们保持三个包与三个 EventType 命名空间，因为读侧投影按命名空间筛选事件。可插拔的通用框架（Application Source、Catalog 构建、RuntimeRegistry）没有第二个消费者，推迟到出现时再做（extension 附录 B）。

根 `agent/run` 不提供 Loop alias、wrapper 或 façade。

### 2.3 boundary decisions

- `sdk.Request`、`sdk.ModelResult` 和 tool definitions 在 Runtime 前冻结为 run-owned persisted values。
- Queue、steer/follow-up、fixed-model policy、权限、provider registry 和 MCP lifecycle 属于 Application。
- Session kernel 保持 payload-opaque、Artifact-free。
- Chatlog Message 原生支持 first-party Artifact references；`sdk.Message` 只是 materialized provider transport。
- Turn Coordinator 从 `twilight/turn/surface` 与 `twilight/run/machine` 投影重建，不保存隐藏的长期状态。
- Run 事实与其对话内容（companion）在同一组（一次 Append）写入；没有 Run→Session materialization、coverage 水位或 outbox。
- 只有一条写入路径：`writer.Writer`。Run 的 Runtime、Turn 的 Coordinator 都经它写入，companion 与 Attach 事件与其他 producer 一样经 admission；artifact claim 在 Append 之前建立，孤儿由回收前核对释放。
- 一个 Session 同一时刻一个 Writer 进程（Session 级所有权，Epoch fencing）；没有按目标的 lease、grant 或 durable ClaimStore。ExecutionClaim 只在 worker 内存中；投影缓存与 FrozenValueStore 是派生或旁存数据，不进入 stream。
- 接管者对全部 Executing 目标一次性处置（模型回 Prepared、工具记 Unknown），不逐目标等待或恢复。
- Run fact 只保存执行状态与内容 digest；请求本体（含工具定义）在 FrozenValueStore，模型输出与工具输出在 chatlog 事件。
- kernel `ProtocolVersion` 只覆盖行结构与 digest；payload 版本由模块携带（`v` 字段），Run 保留自己的 `SchemaVersion`。

## 3. 已完成迁移

| 工作 | 状态 |
|---|---|
| SDK single-call boundary 与 run-owned frozen model data | 完成 |
| shared `agent/es` 与 RFC 8785 canonical JSON | 完成 |
| Decide/Evolve/Next Run Machine | 完成 |
| RunHeader、TransitionRecord、wire codec、fold/golden tests | 完成 |
| 第 6.4 节的 `agent/run` 修改（digest-only fact、Owner/Attempt、RunCreated、Withdraw、任意状态入队） | 完成，2026-09-07；golden 重新冻结 |
| per-Run `Store`、`stored_runtime`、`sqlitestore`、`RunHeader`、`TransitionRecord` | 已删除，2026-09-07 |
| Session kernel Memory Store（`agent/session`） | 完成，2026-09-07；第 7 节 conformance 完成，2026-09-10 |
| `agent/session/extension`（Registry、Writer/Writers、ProjectionReader、MemoryProjectionCache） | 完成，2026-09-07；第 7 节 conformance 完成，2026-09-10（多版本 codec 共存、并发串行、binding admission、current 版本必须有 codec 的校验）；`Admission` 缺失由 error 报告而非 `CommitInvalid` |
| 投影缓存接入 Writer：`rebuild` 从缓存条目续折（组对齐校验 + 失效回退）、`CachePolicy`/`CacheEvery`/`Exclude` 只管写入、`Close` 刷新 | 完成，2026-09-10；EXT-PRJ-3/5/6/7 与 REF-MEM-2 新增 |
| 拆分 `agent/session/extension`：写入路径迁到 `agent/session/writer`（与 `extension` 平级）（`Writer`/`Writers`/`Admission`/`WritersConfig`/`View`），声明与投影引擎留在 `extension` | 完成，2026-09-10；EXT-SCP-4 新增，写入路径的包内实现类型改名以避免与包名同名 |
| 文件 adapter（`agent/session/filestore`）的持久化投影缓存（`Store.ProjectionCache()`，`<sid>/projections/<id>/<v>.json`）与跨进程重启 conformance | 完成，2026-09-10；`agent/ref` 经 `ProjectionCacheProvider` 选中它 |
| 提交历史索引归 kernel（SES-REP-3/4）：Writer 不再持有 CommitID → 行的索引，文件 adapter 记录每组的字节区间并按区间读取 | 完成，2026-09-10；重开后 Writer 的常驻内存与日志长度无关（守卫见 `agent/session/writer/retained_test.go`），query conformance 由内存与文件两个 adapter 同跑 |
| `agent/session/chatlog`（事件、parts codec、Surface、Context） | 完成，2026-09-07；checkpoint 完成，2026-09-09（CHT-EVT-3 转正，宿主策略见 REF-CKP-1/2） |
| `agent/artifact`（Ref、Binding、Memory BindingStore、两态 KV ledger） | 完成，2026-09-07；Resolver/Store/Promoter 未实现 |
| `agent/session/run`（module descriptor、machine 投影、Runtime、RecoverExpired） | 完成，2026-09-07 |
| `agent/run/loop` 绑定 Session（`Run(ctx, runtime, sessionID, runID, sink)`、RunPosition、SessionCommit 观察） | 完成，2026-09-07 |
| `agent/turn` 重写（Coordinator、CompanionV1、surface 投影） | 完成，2026-09-07；旧实现已删除 |
| 参考组装 `agent/ref`（Agent 配置面（原 ExecutionBinding，2026-09-09 改名 Profile）、ContextPlanner、Memory 组装、SessionDriver、Session 宿主、崩溃恢复 example） | 完成，2026-09-07 |
| Runtime conformance（RUN-CMP-2，`agent/session/run/runtimetest`，以 `session.Store` 为参数） | 完成，2026-09-07；对 Memory Store 通过。kernel 与 extension 的 conformance 见 2026-09-10 各行 |
| 第 8 节规范修订（session、extension、run、turn、chatlog、artifact、参考组装按单写者与扁平事件改写） | 完成，2026-09-08 |
| 第 8 节代码重构（kernel 收缩、Writer、Runtime 去 lease/grant、接管处置、conformance 重建） | 完成，2026-09-08；`agent/` 下 9 个测试包全部通过，kernel 与 RUN-CMP-2 的 conformance 均以 Store 为参数 |
| 文件 adapter（`agent/session/filestore`）、live 模型接入 | kernel 落盘、崩溃残尾恢复与投影缓存已完成（2026-09-10）；live 模型接入未开始 |
| kernel wire golden fixtures（header digest、行 digest 链、行 canonical JSON 形状、落盘字节） | 冻结，2026-09-09；`-update` 重生成 |
| `agent/session` kernel conformance（第 7 节，含崩溃残尾恢复） | 完成，2026-09-10；Memory 与 filestore 跑同一套 |
| `agent/session` 版本隔离（digest domain 携带 profile 版本） | 完成，2026-09-10 |
| `agent/es` 收敛到 canonical identity 与 digest 职责 | 完成，2026-09-10；删除 superseded 的 record/fold |
| `agent/jsonstable` JCS 契约测试（键序、binary64 数字格式、拒绝面、canonical-by-construction） | 完成，2026-09-10；`Value` 仅未导出 `raw`，唯一赋值点在 `Parse` 内 |
| Fork、ancestry、canonical import（agent-session.md 第 8 节） | 未实现；kernel 当前拒绝非 nil `ParentFork` |
| `agent/session/chatlog` 开放项 | wire field names、输入 limits、golden fixtures 未冻结 |
| `agent/artifact` | Ref、Binding、Memory BindingStore、BindingSetBuilder、两态 ledger 完成；Resolver、Store、Promoter、scheme registry 未实现 |
| `agent/artifact` 保留子系统（claim、ledger、Reconcile） | 尚无真实内容存储与 GC 消费者；conformance 随第一个真实内容存储冻结，在此之前允许修订；`Prepared` 状态、provider 迁移 fence、archive import/export 未实现 |
| `agent/turn` 第 8 节 conformance | 部分实现，当前由 `agent/ref` 的测试覆盖 Start、Deliver、Stop 与新 Turn 的开启 |

2026-09-07 的代码行是第 8 节修订前的形态，已于 2026-09-08 按第 8 节重写。当前正式调用形态为 `agent/ref` 的 Memory 组装：`ref.New` 返回 Store、Registry、Writers、Runtime、Coordinator 与 Agents；宿主对每个 Session 先 `Memory.Open`（取所有权并接管处置）再经 `SessionDriver.Send` 投递输入。Coordinator 只做协议提交与状态读取（Status），驱动编排（解析 profile、调用 driver、组装结果、取消）在宿主层的 `Memory.Drive`（REF-DRV-1）。Loop 不保存 authority state；Runtime 不读取 queue 或 planner context。

已决定（2026-09-07）：终态 Run 从 `twilight/run/machine` 投影移除后，`Runtime.Load` 对该 Run 按 RunID 过滤 replay 后折叠返回终态，`ErrRunNotFound` 只用于不存在的 RunID（RUN-CMT-1）。该路径为兜底：Loop 在模型结算返回终态 snapshot 时直接结束，不再 Load（RUN 第 7 节）；Coordinator 的 Deliver 与 Stop 从 turn surface 的 `AttemptView.SchemaVersion` 构造 envelope，不读 machine 投影（TRN-DLV-2、TRN-STP-1）。曾考虑在投影保留终态 Run 的最小记录，因投影会随历史增长而未采用。

已决定（2026-09-07）：Runtime 不为 command digest 另设控制面索引；同 CommandID 一律按重放处理，幂等只由 kernel 的 `(SessionID, CommitID)` 承担（RUN-CMT-5）。曾实现过 `twilight/run/command` 索引，用于把同 ID 不同内容判为冲突；该判定只覆盖 approve/reject 撞 ID 与同一 attempt 两次结算两种情形，前者由调用方读投影覆盖，后者属于实现错误，故删除。

## 4. 后续实施工作

### 4.1 Core reference implementations

第 6、7 节的全部条目已于 2026-09-07 完成；第 8 节修订后的实施顺序见 8.5。kernel `ProtocolVersion` 1 与各模块 payload 版本 1 的 golden fixtures 已于 2026-09-09 冻结：kernel（header/行 digest 链/行与落盘字节形状）、extension（`v` 注入后的 payload 字节）、chatlog（checkpoint 摘要域与 payload wire）、run（派生身份表 13 项，envelope/fact 既有 golden）、turn（PlanDigest/StartOperationDigest/DeriveRunID）、ref（Profile digest 及 SystemPrompt 边界）、artifact（Ref identity/Binding digest/RefSet digest）。

### 4.2 durable adapters

- 文件 adapter `agent/session/filestore`：已完成——一个 Session 一个目录（header.json、log.jsonl 一行一个 event、owner.json 承载 epoch 与 owned），每次 Append 一次 fsync，打开时校验摘要链并截掉不完整尾组，接管走 `Takeover`；
- 数据库 adapter（SQLite / PostgreSQL）：sessions（header、epoch）、events 两张表，Append 一个事务；只在多会话服务需要时做；
- 收紧 Session authority tables 的 immutable RLS policy；
- 需要远程 Store 或跨存储 claim 时，实现 extension 附录 C 与 artifact 附录的两阶段路径；
- 内容寻址旁存合并：FrozenValueStore 与 artifact 的 `cas` scheme 是同一抽象的两份定义，长期把模型请求本体旁存实现为 authority 固定的 artifact `cas` 存储实例，随第一个真实内容存储（artifact Store/Resolver）一起做。文件后端（`filestore.NewFrozenValues`）已落地，补齐了进程重启后模型中断恢复的重放路径；合并方向不变。

### 4.3 Application migration

- 组合 model/tool registries、permission、queue admission 与 `agent/run/loop` driver；
- 构建 Session Surface/Context projections 与 API；
- 逐步把 `bot_history_messages` 降为兼容 read model；
- 在完整 materialization、terminal settlement 与 retention closure 后执行归档/GC。

## 4.3 Run 内部整理（已完成）

- envelope digest 不匹配从 `ErrCommandConflict` 改为不可重试错误，调用方不再对构造错误 reload 重试（该自校验其后整体删除：envelope 只经 `BuildEnvelope` 构造，不再携带 digest）；
- Evolve 对重复 `InputAccepted` 报错而非静默去重；
- `decideSubmitModelResult` 拆为 binding 校验与 ToolStep 派生两步；
- `RunEnded` wire 改为 tagged union，与 Go sealed union 对称；
- `ModelCatalog.ResolveModel` / `ToolCatalog.ResolveTool`，一个类型可同时实现两者；
- `ProtocolV1` 改为函数，不可被重新赋值。

## 4.4 sdk.Request 作为冻结类型的评估

结论：请求层保留 `run.ModelRequest` 镜像，但把镜像的理由收窄到具体字段；消息层与结果层必须保留镜像。

| 层 | sdk 类型中的开放字段 | 能否直接冻结 |
|---|---|---|
| `sdk.Request` 顶层标量与 `Tools`、`ToolChoice`、`StopSequences` | 无 | 能 |
| `sdk.Request.ProviderOptions` | `map[string]json.RawMessage` | 不能：值未 canonical 化，digest 依赖调用方字节 |
| `sdk.Request.ResponseFormat.JSONSchema` | `*jsonschema.Schema`（第三方结构体） | 不能：其 JSON 形状由外部库版本决定，不受本协议冻结 |
| `sdk.Request.Messages[].Content` | `[]MessagePart` 接口，`ToolCallPart.Input any`、`ToolResultPart.Result any`、各 part 的 `ProviderMetadata map[string]any` | 不能 |
| `sdk.ModelResult` | `ToolCalls[].Input any`、`TextProviderMetadata map[string]any`、`Response *ResponseMetadata` 含 `map[string]any` | 不能 |

因此“让 `sdk.Request` 直接作为冻结请求类型”在当前 sdk 形状下不成立：顶层有两个字段（`ProviderOptions`、`ResponseFormat.JSONSchema`）阻止直接冻结，消息层整体阻止。若要消除请求层镜像，需要先在 sdk 侧完成三项修改：`ProviderOptions` 改为 `map[string]jsonstable.Value`；`ResponseFormat.JSONSchema` 改为 canonical JSON 而非第三方结构体；`MessagePart` 从接口改为闭合的 tagged struct，`Input` / `Result` / `ProviderMetadata` 改为 canonical JSON。这三项都是 sdk 公共 API 变更，影响全部 provider 实现，不在本轮范围内。本轮的处置为：保留镜像，`sdk.Request` 注释中“参与 DigestRequest、无排除字段”的表述已不准确，digest 定义在 `run.ModelRequest` 上；后续若 sdk 完成上述闭合，再删除 `model_data.go` 中请求层的镜像与对应 clone。

## 5. 完成标准

重构在以下条件全部成立时结束：

- Memory 与 PostgreSQL Session Store adapters 通过相同 Session 与 Runtime conformance；
- Turn 的 Start、Retry、Stop、Settle 与 Run commit 对 crash、重复提交和 unknown response 可恢复；
- Session/Artifact/Session Module/Chatlog reference implementations 通过各自 conformance；
- production request context 和 UI surface 由 Session projections 提供；
- legacy history 不再承担 canonical write authority；
- Run、Session 与 Artifact 的 durable integrity/recovery paths 有持续 CI 覆盖。

## 6. 单一 Session ES 决定（2026-09-04）

### 6.1 决定

Run 从独立的 Event Sourcing 存储改为 first-party Session Module。Run 事实以 `twilight/run/` 事件进入 Session stream；`MachineState` 是投影；per-Run 的 `RunHeader`、`TransitionRecord`、Run `Store` 与 Turn 的 materialization 层删除。

### 6.2 依据

先前"两条 ES"的两个理由是 run 事实的写入量与生命周期。对一次两步 live run 的记录按字节拆分：

| 组成 | 占事件字节 | 增长方式 |
|---|---|---|
| `ModelStepPrepared.Request.Messages`（完整历史每步重存） | 9%（两步）；随步数平方增长 | 平方 |
| 工具定义，每步重复且在 fact 内存两份 | 13% | 每步 × 工具数 |
| 事件外壳（digest 与 identity） | 42% | 每事件约 365 字节 |
| 其余执行状态与业务载荷 | 35% | 线性 |

平方项与工具定义重复的原因是存储形状（fact 携带内容本体），与日志条数无关。fact 只留 digest、本体进内容寻址旁存后，run 事实每步约 4 KB、线性，约为同一 run 在 transcript 级事件的 6 倍。生命周期问题由 checkpoint 之下前缀转冷存储解决，不需要删除事件。剩余差距不足以支撑第二条 ES 的成本：跨存储的结算双写、Turn 与 Run 的 linkage 与 coverage 水位、两套提交合同与 conformance。

### 6.3 单一 ES 带来的变化

- 一个 Run command 恰产生一个 SessionCommit；`CommitID = CommandID`，幂等与 conflict 由 Session kernel 的 `(SessionID, CommitID)` 判定；
- Run 事实与其对话内容在同一 commit：companion 映射由 turn 模块提供，`run.Runtime` 在临界区内调用；
- `RunEnded(completed)` 与 `twilight/turn/completed` 同 commit；`RunEnded(failed)` 不结算 Turn，Turn 进入 `attempt_failed`，由 Retry 或 Settle 决定；
- Turn:Run 为 1:N，`RunID = Digest("twilight/turn/run", SessionID, TurnID, Attempt)`；
- Session kernel 新增 `CommitIn` 临界区合同，`Commit`（CAS）保留；
- 崩溃后同一 ModelStep 的 Recovered 仍重发同一冻结请求，本体按 `RequestDigest` 从 FrozenValueStore 取回。

### 6.4 代码迁移清单

保留（对存储位置无假设）：`decide.go`、`evolve.go`、`next.go`、`ids.go`、`fact.go`、`state.go`、`model_data.go`、`clone.go`、`snapshot.go` 的 MachineState codec、`agent/run/loop` 的执行逻辑。

修改：

| 项 | 内容 |
|---|---|
| `state.go` | 新增 `OwnerID`、`RunPosition`、`ModuleEvent`、`Companion` 接口；`MachineState` 加 `Owner`、`Attempt`，删 `LastModelResult`；`ModelStep.Request` 改为 `RequestDigest`；`ToolSpec` 删 `Definition`；`RunResult` 删 `Model` |
| `fact.go` | 删 `RunHeader`、`TransitionRecord`、`AgentEvent`；`ModelStepCompleted` 改为 `{StepID, Usage, FinishReason, ResultDigest}`；`ToolCallCompleted`/`ToolCallAnswered` 改为 digest；新增 `RunCreated{SchemaVersion, RunID, Owner, Attempt, CausationID}`；fact codec 输出 `jsonstable.Value`，payload `v` 由 Registry 加入 |
| `decide.go` | Prepare 校验 command 携带的本体 digest 后只写 digest；SubmitModelResult/SubmitToolResult 计算 ResultDigest/OutputDigest；`AcceptInput` 前置放宽为任意非终态；无 call 且有 pending 输入的 SubmitModelResult 回到 Open；新增 `WithdrawPreparedStep` |
| `evolve.go` / `next.go` | `ModelStepWithdrawn` 折叠为 Open；`Next` 在 Model Prepared 且有 pending 输入时返回 `WithdrawPrepared` |
| `commit.go` | `EvaluateCommit` 改为在 `SemanticTx` 内执行：LookupCommit、snapshot 加 tail fold、以 `RunPosition` 做 prepare hard CAS、从控制面 KV 读 lease、Decide、Evolve、companion、Attach、SourceDigest 校验，返回 `SemanticGroup` 与 lease ops、snapshot 决定 |
| `ids.go` | `DeriveModelRequestCommandID` 以 `RunPosition` 为 preimage；新增 run 事件 EventID 派生 |
| `agent/run/loop` | `Run(ctx, runtime, sessionID, runID, sink)`；Start 前 `Runtime.FrozenRequest`；ClaimStore key 加 SessionID；EventSink 的 `Committed` 改为 SessionCommit |
| `agent/turn` | 删 `mapper.go` 的 MaterializeAll、`ResultReference`、`MemoryLog`；`TurnID` 留在 turn，写入 Run 时转为 `OwnerID`；新增 `CompanionV1`、`Deliver`、`Retry`、`Settle`、surface 投影 |

删除：`store.go`、`memory_store.go`、`stored_runtime.go`、`sqlitestore/`、`header.go`、`transition.go` 的 per-Run wire、`example_run_test.go` 的 per-Run Store 用法（改写为 Session Store 版本）。

新增：`agent/session` Memory Store（Commit、`CommitIn`、Types 过滤 replay、snapshot、控制面 KV 含条件写与 deadline 枚举）、`agent/session/extension`（FirstPartyRegistry、payload 版本、admission、SemanticAppender、Lease）、`agent/artifact` 两态 ledger 的 KV 实现、`agent/session/run`（module descriptor、machine projection、Runtime 实现、Memory FrozenValueStore、SnapshotPolicy）、golden fixtures 重新冻结。

## 7. 2026-09-04 修订（架构审查后）

### 7.1 采纳的修正

| 审查意见 | 处理 | 位置 |
|---|---|---|
| Runtime 直写绕过 binding admission，companion 中的 ReferencePart 没有 claim | 只保留一条写入路径。`SemanticAppender` 增加临界区入口 `AppendSemanticIn`，Runtime 经它写入；companion 与 Attach 事件与其他 producer 一样经 codec、admission，claim 在同一事务建立 | EXT-SCP-1、EXT-APP-3、RUN-CMT-3、TRN-CMP-1 |
| commit 与 lease 的原子性无法由 SessionTx 实现；lease 丢失会使 Run 停滞 | Session Store 增加控制面 KV，`SessionTx` 内可读写、与 commit 同事务；lease、grant、artifact claim 都放在 KV。KV 与 stream 同一事务域，不会单独丢失。续期用 kernel 的条件写（见 7.6） | SES-API-3、RUN-CMT-7、RUN 5.1 |
| 单一 ProtocolVersion 重新耦合模块变更周期 | kernel 版本只覆盖 envelope、commit、snapshot envelope、digest profile；payload 第一层携带 `v`，Registry 按 `(EventType, v)` 选 codec 并永久保留旧版本；Run 恢复 `created.SchemaVersion` | SES-VER-1/2、EXT-REG-2、RUN-WIR-2、RUN-CMT-8 |
| v1 范围过大，Fork 等能力先于纵向切片 | Fork、ancestry、canonical import、resolved replay 移入 session 附录 A；Application module 与通用 Catalog 移入 extension 附录 B；两阶段 journal 移入附录 C；artifact 的 Prepared 状态、reconciler、迁移 fence、import/export 移入附录。v1 conformance 只覆盖 Memory Store 与纵向切片 | SES-SCP-2、EXT-SCP-2、ART-SCP-2 |
| 持久结构数量与一致性等级未写明 | 见 7.3 | 本节 |
| snapshot 每次 commit 重写且 `Results` 无界 | snapshot 改为可丢弃缓存，写入由 `SnapshotPolicy` 决定，`Load` 为 snapshot 加过滤 tail；终态 Run 从投影移除，结果由 `Record` 与 turn surface 的 `AttemptView.End` 提供 | SES-SNP-1/2、RUN-CMT-2、TRN-PRJ-1 |
| Retry 语义把产品策略写进协议 | 协议只保证失败 attempt 的内容留在 stream 与 ContextFold 输出中；是否进入请求由 Planner 决定，参考 Planner 的策略是全部纳入 | TRN-RTY-3、CHT-LIF-1、REF-PLN-6 |
| Prepare 对 chatlog 写入不敏感未声明 | 写明为有意选择，新鲜度由 Application 经 PlanningToken 负责，Run 不校验它 | RUN-CMT-4 |
| Run 内出现 TurnID | Run 只保留 opaque `OwnerID`，turn 以 TurnID 填充；run 不依赖 turn | RUN-SCP-2、TRN-SCP-1 |
| append fingerprint 含时间戳，崩溃重试得到 conflict | fingerprint 不再覆盖 `RecordedAtUnixMilli` | SES-APP-1 |
| Record 需要按 RunID 筛事件，Replay 无过滤 | `ReplayRequest.Types` 与 `SessionTx.Tail(types)` 前缀过滤；adapter 维护类型前缀到 revision 的索引 | SES-REP-2 |
| CoverageDigest 逐次重算 | `Through.Digest` 即 coverage 证明，删除独立 CoverageDigest | SES-SNP-1 |

### 7.2 部分采纳

审查意见"三个 first-party module 实际是一个领域，应合为一个实现"。耦合证据成立，但它们指向的是固定的分层顺序（turn → run、turn → chatlog），可以用包依赖表达。合成一个包会失去读侧收益：投影按 EventType 命名空间筛选，Context 只读 chatlog、machine 只读 run。因此保留三个包与三个命名空间，推迟的是可插拔框架（附录 B），不是模块划分。

### 7.3 持久结构与一致性等级（第 8 节修订前；修订后见 8.4）

| 结构 | 等级 | 写入点 | 丢失或不一致时 |
|---|---|---|---|
| Session commit 与 head | authority | `Commit` / `CommitIn` | 不可恢复；digest chain 使损坏可检测 |
| 控制面 KV：`twilight/run/lease`（经 extension.Lease） | 同事务控制面 | `AcquireLease`（start）、`ReleaseLease`（settlement、recovery）、`Leases.Renew`（续期，条件写） | 与 stream 同事务域，不会单独丢失；被运维误删时该 target 不再被过期枚举发现，需人工提交 Recover 命令 |
| 控制面 KV：`twilight/run/claim`（durable ClaimStore） | 同事务控制面 | Loop 经 Store | 退化为 lease 过期恢复 |
| 控制面 KV：`twilight/artifact/claim` | 同事务控制面 | `SemanticAppender` | GC 可能提前回收该 commit 引用的内容；可从 stream 中的 Binding 引用重建 |
| 投影 snapshot | 派生缓存 | `SnapshotPolicy` | 从 stream 重折 |
| FrozenValueStore | 旁存，生命周期为 ModelStep | 进入事务前 `Put` | Executing/Prepared step 的重发失败为不可重试错误，Application 决定 Retry；已终结 step 不受影响 |
| Artifact content store 与 BindingStore | 外部内容 | artifact owner | resolve 失败按 ART-CAP-1 分类 |

v1 只有两类恢复动作：`RecoverExpired`（按 deadline 枚举过期 lease）与 Application 的 artifact GC（按 Active claim）。extension 附录 C 的 journal 扫描与 artifact 附录的 Prepared reconciler 不在 v1。

### 7.4 后续加固

lease 的第二条出路：grant 由 `(Claim, start CommitID)` 派生，start fact 记录 `ClaimDigest`，settlement 携带 Claim 时由 Runtime 从 stream 验证所有权。这样 lease 表完全退化为 deadline 缓存。改动涉及 fact wire 与 grant 签发，留待 v1 跑通后评估。

### 7.5 实施顺序

1. Session kernel Memory Store（Commit、CommitIn、Types 过滤、snapshot、控制面 KV 含条件写与 deadline 枚举）与 conformance；
2. extension FirstPartyRegistry、payload 版本、admission、SemanticAppender 两个入口、Lease；artifact 两态 ledger；
3. `agent/session/run`：module descriptor、machine projection、Runtime 实现（消费 SemanticAppender 与 Lease）、FrozenValueStore、RecoverExpired；
4. `agent/run` 按 6.4 修改，golden 重新冻结；
5. `agent/turn` attempt 模型、CompanionV1、surface 投影；
6. 参考组装跑通 Input → Turn → Run → Session 纵向切片，再接 live 模型。

### 7.6 租约的层次（2026-09-04，随后调整）

审查后最初把 lease 写成 run 模块对 opaque KV 的约定，续期与结算存在竞争，并补了一个投影兜底扫描。随后考虑过把类型化的 lease 原语放进 kernel，被否决：kernel 不应持有"持有者"这类模块语义。最终切法：

| 层 | 提供 |
|---|---|
| kernel（agent/session） | KV 条件写 `ControlCompareAndPut`、条目 deadline 字段、`ControlExpired` 枚举。不出现 lease 概念 |
| Module Framework（agent/session/extension） | 类型化的 `Lease`：Acquire / Release 在 `SemanticTx` 内、Renew 用条件写、Expired 用 deadline 枚举、Token 由 CommitID 派生。API 不含任何模块的概念 |
| run 模块（agent/session/run） | `Lease` 的第一个消费者：grant 即 Token，ExecutionClaim 即 Holder，过期后提交哪个 command |

`Lease` 与 `SemanticAppender`、artifact claim 适配同属 EXT-SCP-3 定义的"kernel 机制之上的共用设施"。它现在只有一个消费者，仍放在 extension 而非 run 模块，原因是它的 API 不含 run 的概念、实现只依赖 kernel 原语，放在 run 里只会让下一个消费者（审批超时等）复制一份。由此删除：投影兜底扫描（KV 与 stream 同事务域，不会单独丢失）、"续期借 Session 锁"的方案、run 模块自己的 lease 值编码。

### 7.7 模块间依赖的声明（2026-09-05）

此前模块间依赖只以 Go import 表达，Registry 不知道 turn 的投影消费 run 的事件，也不知道它能处理 run 事件的哪个版本。现在 `ModuleDescriptor` 增加 `Requires []ModuleRequirement`，Registry 构建时校验：被依赖模块已注册、依赖图无环、投影消费的事件类型在本模块或 `Requires` 范围内、被依赖事件的当前版本在声明的可处理版本内（EXT-REG-4）。三个模块的声明见 EXT-SCP-4。

曾考虑再加一对 `Needs` / `Provides` 表达"run 需要一个 Companion、turn 提供它"。否决：Companion 是 run Runtime 的构造参数，为 nil 时启动即失败，框架层的声明防不住任何额外的失效。`Requires` 只表达事件消费依赖。

### 7.8 回合中途追加输入（2026-09-05）

`PendingInputs` 已是持久化队列，缺的是入队入口与消费时机。改动：Turn 增加 `Deliver`，每条输入一个 Run commit（`AcceptInput` 加 Attach 的 `input_delivered`）；`AcceptInput` 前置从 `Open` 放宽为任意非终态；模型无 tool call 但有 pending 输入时 Run 回到 `Open` 而不结束；新增 `WithdrawPreparedStep`，Prepared 期间入队的输入使 `Next` 返回 `WithdrawPrepared`，Loop 放弃已冻结但未发出的请求并重规划。Executing 与 ToolStep 期间的输入等待该步结算，在随后的 `Open` 被 Prepare 一次消费，与 Codex、Claude Code 的注入点一致。Deliver 不打断进行中的调用；打断用 Stop。turn surface 消费 `run/input_accepted` 以跟踪全部输入，Retry 重放它们。

对照 pi 与 DeepSeek harness 的 inbox 模型后补齐了 session 级的路由：pi 的 steering 在当前 step 的工具结果之后注入、不中断生成也不跳过剩余 tool call，follow-up 只在 agent 本来要停下时取用；DeepSeek harness 的 inbox 是 `next-step` 与 `next-turn` 两条持久化列表，steer 在最近的 step 边界消费，turn 关闭前做最后一次 drain。twilight 的对应：`PendingInputs` 即 next-step；chatlog 中已 submitted 未 delivered 的输入即 next-turn；缺的"空闲时被唤醒、turn 结束后自动取下一条"由参考组装的 `SessionDriver` 提供（REF-DRV），协议不变。Stop 后 Retry 等价于 `cancel(keepInbox)`，Settle 等价于默认 cancel（TRN-STP-1）。

## 8. 2026-09-08 修订：Session 级单写者与扁平事件

### 8.1 起因

第 7 节形态的 Memory 栈跑通后（第 3 节），对照 dsh 与 Codex 的 session 日志实现发现：twilight 比它们多出的全部机制（`CommitIn` 临界区、`Commit` 的 CAS、控制面 KV、按目标的 lease 与 grant、`RenewLease` 心跳、`RecoverExpired` 按 deadline 枚举、commit 与 KV 同事务）都源于同一个假设：同一个 Session 可以有多个并发写者，包括不同进程。该假设没有部署需求支撑：Memoh 作为服务把一个 Session 固定到一个 worker，failover 走锁接管，不会两个 worker 同时写同一 Session；本地宿主是单进程。dsh 的做法（每 session 一个 write handle，进程内独占加跨进程 `flock`，第二个写者直接被拒）说明单写者足以支撑同类需求。

同时发现 `SessionCommit` 容器在读侧只是一层没有语义的嵌套（`ReplayPage.Commits[].Events[]`），它承担的三个作用中，幂等与 CAS 单位随单写者上移到进程内，commit 级元数据可以摊到每行，只剩"整组原子可见"一条，而这条只需要 append 以组为单位并在读侧不暴露不完整组，不需要嵌套类型。

### 8.2 决定

| 项 | 修订前 | 修订后 |
|---|---|---|
| 写者 | 多写者，`CommitIn` 回调式临界区，`Commit` CAS | 一个 Session 同一时刻一个 `Writer`（SES-OWN-1）；`Open` 取所有权，Epoch 加一并持久化；落后 Epoch 的 `Append` 被拒（SES-OWN-2） |
| 写入单位 | `SessionCommit{Events[]}`，`(Revision, Index)` 定位 | 一行一个 `SessionEvent`，全局 `Seq`；同一次 `Append` 的行共用 `CommitID`，`Index`/`Last` 标记组；整组原子，不读不完整组（SES-APP-1/2） |
| 幂等 | kernel 按 `(SessionID, CommitID)` 加 fingerprint | kernel 只拒绝重复 CommitID；`writer.Writer` 以内存索引判定 AlreadyApplied / Conflict（EXT-WRT-2） |
| digest | header、event、commit、snapshot 四套，`ProtocolProfile` 12 个方法 | 每行一个 digest，覆盖本行与前一行（SES-WIR-2） |
| EventID | `Digest(Type, CommitID, index)` | 删除；`Seq` 即身份，`SourceSeqs` 引用 Seq |
| replay | `ReplayCursor{After: EventPosition, Token}` 分页 | `Read(sid, From, Types, Limit)`，Limit 在组边界截断 |
| `SourceEvents` 校验、`CausationID`/`CorrelationID` | kernel 校验引用存在；commit 级字段进 digest | 引用语义归声明它的模块；commit 级字段删除 |
| snapshot | kernel 的 `LoadSnapshot`/`SaveSnapshot`，与 commit 同事务 | 移到 extension 的可选 `ProjectionCache`，不与 Append 同事务（EXT-PRJ-3） |
| 控制面 KV、`extension.Lease`、grant | lease 按目标、TTL、条件写续期、deadline 枚举 | 全部删除。Session 所有权即执行所有权（RUN-CMT-6） |
| 恢复 | `RecoverExpired` 按过期 lease 逐目标恢复 | 接管者 `RecoverInterrupted` 对全部 Executing 目标一次性处置，Claim 为 `TakeoverClaim(SessionID, Epoch)`（RUN-CMT-7） |
| Loop | `RenewLease` 心跳、durable `ClaimStore`、grant 校验 | 都删除；Claim 只在 worker 内存中用于派生 CommandID；`ErrOwnershipLost` 为终止性错误（RUN-LOP-5） |
| artifact claim | `ActivateIn(kv)` 与 commit 同事务 | ledger 自持久化，`Activate` 在 Append 之前；孤儿 claim 由回收前核对释放（EXT-WRT-3、ART-RET-3） |
| `RequireComplete`、`ModuleForEvent` 按前缀猜模块 | 投影对未注册事件按模块归属拒绝 | 写者声明 `Ignorable`；范围内不可忽略的 Unknown 使 fold 失败，范围外跳过（EXT-PRJ-2） |
| extension 其他 | `JSONPointer` 提取、双入口 Appender、`LoadIn`/`SaveSnapshotIn` | 删除 |

保留：Registry 的 `Requires` 校验（启动期）、`Types` 前缀过滤（读取优化）、canonical JSON 要求（digest 与跨 adapter 一致性的前提）、Run 的 `SchemaVersion`、companion 同组、Prepare 的 hard CAS、终态 Run 的 Load 兜底。

### 8.3 失去与得到

失去：同一 Session 的不同工具调用由不同进程并发执行（没有消费者）；claim 与 commit 的同事务一致性（降为先 claim 后 append，孤儿由核对清理）；修订前 conformance 中 grant 隔离、跨 Run grant、lease 续期的十几项断言。

得到：kernel 接口从 15 个方法降到 4 个，adapter 只需实现独占、追加与读，JSONL 成为一等实现；Runtime 去掉 lease/grant 两套校验；Loop 去掉心跳与 ClaimStore；与 dsh、Codex 的心智模型一致（一个 session 同一时刻一个写者）。

### 8.4 持久结构与一致性等级（修订后）

| 结构 | 等级 | 写入点 | 丢失或不一致时 |
|---|---|---|---|
| Session header 与 event 行 | authority | `Writer.Append` | 不可恢复；按行 digest 链使损坏可检测；不完整尾组在打开时截掉 |
| 所有权记录（Epoch） | 控制 | `Open` | 接管由 Open 的 `Takeover` 声明，旧写者被 Epoch fencing |
| 投影缓存 | 派生缓存 | `SnapshotPolicy` | 从 stream 重折 |
| Writer 内存：投影状态、head | 派生 | `OpenWriter` 重建 | 随进程消失，重开时从日志重建 |
| kernel 句柄内存：CommitID → 该组的行区间或字节区间 | 派生 | `Open` 从日志重建（第 11 节） | 随进程消失，重开时从日志重建 |
| FrozenValueStore | 旁存，生命周期为 ModelStep | Commit 之前 `Put` | Executing/Prepared step 的重发失败为不可重试错误 |
| artifact claim | 独立持久 | `Activate`，Append 之前 | 孤儿 claim 由回收前核对释放；不可能出现无 claim 的引用 |
| Artifact content store 与 BindingStore | 外部内容 | artifact owner | resolve 失败按 ART-CAP-1 分类 |

v1 只有两类恢复动作：`RecoverInterrupted`（新 owner 一次性处置 Executing 目标）与 Application 的 artifact GC（回收前核对加按 Active claim 计算 root）。

### 8.5 实施顺序

1. `agent/session`：重写 Memory Store（Create、Header、Open/Epoch/Takeover、Append 整组、Read 过滤）与 conformance；删除 CommitIn、CAS、控制面 KV、snapshot、四套 digest、EventID、ReplayCursor；
2. `agent/session/extension`：`Writer`（OpenWriter 重建、Commit 串行、幂等索引、claim 先于 Append、ErrOwnershipLost 失效）、`Writers`、`ProjectionReader` 与 `ProjectionCache`、`Ignorable`；删除 SemanticAppender、Lease、LoadIn/SaveSnapshotIn、JSONPointer、ModuleForEvent 推断；
3. `agent/artifact`：ledger 改为自持久化 `Activate`，加 `OwnerVerifier` 与回收前核对；
4. `agent/run` 与 `agent/session/run`：`RunPosition = Seq`、`CommitResult.Events`、删除 grant/lease/RenewLease/RecoverExpired，新增 `RecoverInterrupted` 与 `TakeoverClaim`，machine 投影加 `Ended`；
5. `agent/run/loop`：删除 LeaseRenewInterval、ClaimStore、grant 路径；`ErrOwnershipLost` 处理；EventSink 携带 `[]SessionEvent`；
6. `agent/turn`：Coordinator 改为 `Writers`，Seq 定位，恢复表按 TRN-REC-2；
7. `agent/session/chatlog`：位置类型改 Seq；
8. `agent/ref`：按参考组装第 5 节重组，崩溃恢复 example 改为"关闭 Writer、以新 Epoch 打开、RecoverInterrupted、Resume"；
9. RUN-CMP-2 conformance 按修订后的清单重建；随后写文件 adapter，用 session 与 runtimetest 两套 conformance 验收。

后续协议修改直接更新对应正式规范；本文只更新迁移状态和历史决策，不再承载 wire、Machine、Runtime 或 Loop 算法。

## 9. 2026-09-10 修订：正式规范只承载目标设计

### 9.1 起因

正式规范里混入了四类不属于目标设计的内容，使规范同时充当状态文档，读者无法分辨哪一句是目标、哪一句是现状：

1. **实现状态**："已由 `agent/session/extension` 实现并通过第 7 节的测试"、"`agent/ref` 已实现…"、"第 8 节 conformance 尚未完整实现"；
2. **进度与开放项**："wire 在文件 adapter 通过前不冻结"、"v1 freeze 前开放项：wire field names、输入 limits"、"payload 字段尚未冻结"；
3. **范围裁剪**："不进入 v1"、附录 A/B/C、"v1 实现返回 `ErrUnsupported`"、"目前没有规范内的消费者"；
4. **实现分析与测试方法**："参考实现为 MemoryStore 与文件 adapter"、"参考实现在 `OpenWriter` 完成日志重建之后…核对一次"、"conformance 以可选能力 `CrashTail` 注入崩溃"。

### 9.2 决定

1. 七份正式规范（run、session、session-extension、session-chatlog、artifact、turn、参考组装）只写**目标设计**：不写实现状态、进度、开放项、迁移记录、范围裁剪与实现分析。
2. 规范首行的"状态"只声明**文档自身**的成熟度（草案／设计规范）并指向本文；是否实现不写在那里。
3. **设计内容不因尚未实现而从规范中删除。** 范围裁剪只改变"什么已经实现"的记录位置，不改变目标设计本身。因此 kernel 的 fork／ancestry／canonical import 设计保留为 [agent-session.md](agent-session.md) 第 8 节，只去掉"预留能力（不进入 v1）"与"v1 返回 `ErrUnsupported`"的表述。
4. 规范描述的目标与实现之间的差距，记在 §3 的表里，不在规范内声明。

### 9.3 落点

- 七份规范的状态行：改为文档成熟度加指向本文的指针，其中属于设计的句子（Writer 串行、Coordinator 只做协议、Runtime 无 lease/grant 等）保留；
- [agent-session.md](agent-session.md)：删附录 A 标题与"不进入 v1"，Fork／ancestry／canonical import 转为第 8 节；SES-APP-2 去掉参考实现与 conformance 注入手段；删"参考实现为…"与 golden fixture 冻结段（该约束为工程约束，见 §3）；SES-SCP-3 的 conformance 断言随之删除；
- [agent-artifact.md](agent-artifact.md)：第 7 节去掉"附录，不进入 v1"与"实现返回 `ErrUnsupported`"；ART-RET-3 去掉"参考实现"表述；
- [agent-session-extension.md](agent-session-extension.md)：清单去掉 claim 断言的冻结说明；"通用 `Catalog` 仍不进入 v1"改为职责边界表述；`OpenWriter` 签名改为携带 `Admission{Bindings, Ledger}`（原签名只有 ledger，无法满足 EXT-REF-1/2 的 scheme/durability admission）；补上 `CommitResult` 的 outcome／error 分工，以及"payload 含引用而 admission 未配置即返回 error"的双向契约；
- [agent-session-chatlog.md](agent-session-chatlog.md)：删"v1 freeze 前开放项"；checkpoint 失效段去掉"相对早期草案的修订"的历史框架；
- [agent-run.md](agent-run.md)：RUN-CMP-1 去掉 fixture 状态，只保留版本演进规则。

### 9.4 后续

§7.1 的"v1 范围过大，Fork 等能力先于纵向切片"是当时范围裁剪的决定；本文不追溯改写该行。范围裁剪仍然有效（Fork 仍未实现，见 §3），改变的是它**不被写进目标规范**。

## 10. 2026-09-10 修订：包边界（写入路径独立，折语义留在 `extension`）

### 10.1 起因

`agent/session/extension` 同时承载三件事：模块声明与其索引（`Registry`）、投影折引擎与投影缓存、进程内的写入路径。投影引擎还被实现为 `Registry` 的方法（`scopeFor`／`fold`），使规范早就划出的缝——第 5 节 Writer 对第 6 节 pure projection 与缓存——在代码里完全不可见。

### 10.2 决定

1. **判据**：一个包够格独立，当且仅当 (a) 它有独立于原包的变化理由，(b) 它与原包之间是单向且窄的依赖。两条都满足才拆；只有一条满足时，拆出去换来的独立性由导出内部件或新增层次来偿付。
2. **写入路径满足两条**：其变化理由是 Session 写入协议（进程内串行、幂等重放、claim 顺序、admission），与"模块声明了什么"无关；它只单向消费 framework 的公开面。故独立为 `agent/session/writer`，且与 `extension` **平级**。
3. **依赖由 import 表达，不由目录嵌套表达。** 一个 import `extension` 的包是它的兄弟而不是子包；`extension` 的消费者——`agent/session/writer`、`agent/session/chatlog`、`agent/session/run`、`agent/session/filestore`——一律平级在 `agent/session/` 下。把消费者嵌进被消费者内部会使目录结构反向陈述依赖。
4. **折引擎不满足 (a)，留在 `extension`。** EXT-PRJ-2 的范围规则不是一套独立规则，它是 `Registry` 已声明的 Requires 图与事件索引在日志上的解释：`applyRow` 逐行读 `Registry` 的事件索引与模块归属，`ScopeFor` 从 Requires 闭包算出该投影的范围。把它搬出去，只能把规则的数据与规则的计算分到两个包，或改成逐行经访问器重查索引。**这与投影引擎"与 extension 耦合"是两回事：它在 `extension` 内部，且与 Registry 共享唯一的变化理由，那是内聚。**
5. **缓存与 reader 满足两条，但记为未做的候选**（见 10.4）。它与折引擎的取舍点在于**纯度**：折是 `Scope` 加 rows 到 state 的纯计算，缓存与 reader 碰 `session.Store`。

### 10.3 落点

- 新增 `agent/session/writer`：`Writer`／`Writers`／`View`／`SemanticGroup`／`TypedEvent`／`CommitFn`／`CommitResult`／`Admission`／`WritersConfig`、claim 身份派生与幂等指纹；写入路径的测试与缓存基准随其迁移；
- `extension` 导出 `ProjectionScope`／`ScopeFor`／`Fold` 作为折引擎的公开面，原先只服务于包内 Writer 的 `projectionScope`／`scopeFor`／`fold` 收束到该面之下；
- [agent-session-extension.md](agent-session-extension.md) 新增 **EXT-SCP-4**：两包平级与依赖方向，并记录声明为何必须同包——`ModuleDescriptor` 声明 `ProjectionDefinition`，而 `ProjectionDefinition.Apply` 消费带模块身份的 `DecodedEvent`，二者互相引用，只有同包才不成环；
- 七份规范中以 `extension.` 限定的 Writer 类型（18 处）改为 `writer.`。

### 10.4 后续

投影缓存与 `ProjectionReader` 可按纯度独立为 `extension` 的兄弟包（仅依赖 Registry 公开面）。收益是"派生状态怎么存、怎么读"这一变化理由独立出来（memory → 文件 → SQLite 不动折语义）；代价是给 `Scope` 补一个类型前缀访问器，并迁移引用点。**未做**：当前 `extension` 的三个部分共享"模块协议"这一变化理由，切分收益小于偿付。

## 11. 2026-09-10 修订：提交历史索引归 kernel

### 11.1 起因

EXT-WRT-1 要求 `OpenWriter` 读取整条日志，重建三样内存状态，其中第一样是**幂等索引**（CommitID → 该组的行与 fingerprint）。实测（`agent/session/writer/retained_test.go`）表明这条要求本身就是 O(N) 常驻内存的来源：

- 用一个状态为 O(1) 的投影测量，重开 Writer 的常驻堆在 1600 行与 12800 行上分别是 0.007 MB 与 0.002 MB——即与日志长度无关；把整条日志重新钉住（模拟旧索引）后，12800 行变成 1.588 MB，守卫失败。
- 用状态随日志增长的投影测量，6400 行时改前为 **2.22 MB（全量 fold）／2.24 MB（命中缓存）**，改后为 0.32／0.27 MB。**缓存条目对内存没有任何帮助**：它省的是 fold 时间。这一条此前没有被意识到。

机制有两层。索引每项持有该组的行，而每组的行是 `rebuild` 那一次"整条日志读取"结果的**子切片**；子切片与底层数组共享存储，于是只要索引里还留着一组，整条日志就不能被回收。更深一层，这份索引是 kernel 索引的**第二份拷贝**：两个 adapter 为了拒绝重复 CommitID（SES-APP-3）本来就持有它（内存 adapter 存行区间，文件 adapter 存成员集合），文件 adapter 还在 `Open` 把整条日志解析了一遍之后只留下成员集合、丢掉行，而 Writer 紧接着又解析了一遍并永久钉住。

### 11.2 决定

1. **CommitID 索引归 kernel**，也就是归日志的所有者。`Append` 必须拒绝重复 CommitID，kernel 因此本来就持有该索引；新增 SES-REP-3/4 把它的读侧显式化。规范里"Writer 重建幂等索引"是错的定位，改的是规范而不只是代码。
2. **成员与行分开问**：`Committed(CommitID) bool` 只查索引，不碰存储；`LookupCommit(CommitID)` 命中才取行。文件 adapter 在 `Open` 记录每组的**字节区间**，`LookupCommit` 只读该区间，代价与日志长度无关。两层问法都出现在 `View` 上，调用点自己陈述代价：协调器只问"是否已提交"，Run 的重放还要行。
3. **Writer 不再持有提交历史**：命中才向 kernel 取行（重放是罕见路径），未命中是一次索引查找。常驻内存只随已注册投影数增长。
4. **fingerprint 在命中时重算**：kernel 存的是行不是 fingerprint。值与重建时存的相同——fingerprint 只取 Type、SourceSeqs、Payload，不含 Seq/Digest/Index/Last——所以重放与冲突的判定不变，代价只落在命中。
5. 这条决定不改 SES-APP-3 的职责划分：kernel 仍然**不比对**重复 CommitID 的内容，也不返回"已应用"；它只是把"我有哪些 CommitID"这件它已经知道的事说出来。

### 11.3 落点

- [agent-session.md](agent-session.md)：`Writer` 接口与新增 **SES-REP-3/4**；SES-APP-3 末句由"重放由 `writer.Writer` 以内存索引完成"改为经 `LookupCommit` 取行完成。
- [agent-session-extension.md](agent-session-extension.md)：EXT-WRT-1 改为"重建投影状态与 head，不保留日志与提交历史索引"；EXT-WRT-2 与 `View` 块随之；conformance 清单补"重开后常驻内存不随日志长度增长"。
- `agent/session/sessiontest`：新增 **query** conformance，内存与文件两个 adapter 同跑，覆盖"当前句柄刚追加的组"与"重开后从日志重建的组"，并要求返回的行不暴露内部存储。
- `agent/session/writer/retained_test.go`：守卫常驻内存与日志长度无关。
- 两个 adapter：内存 adapter 直接从行区间复制；文件 adapter 记录并读取字节区间。
- §2.1 的形态表与 §8.4 的持久结构表随之：Writer 的内存不再含索引，派生状态表新增 kernel 句柄的 CommitID 索引（两者的重建点分别是 `OpenWriter` 与 `Open`）。

### 11.4 后续（未做，代价已知）

**文件 adapter 的 `Read` 每次调用都全量解析日志**（`readLog` 无起读点），因此下列读取的代价都随日志长度增长，与只需读到多少行无关：(a) 全量 fold 的那次读取；(b) 校验缓存条目落在组边界上的那次读取（每个投影一次）；(c) `storeReader.Load` 的每次读取——它先 `isPrefix` 再 `Read`，即热缓存下每次 `Load` 都是**两次全量解析**。要让"热缓存下重开 Writer 不读整条日志"成立，需要让 `Read` 支持按字节区间起读（文件 adapter 已有每组的字节区间，缺的是行到字节的定位表），这同时会解决 (c)。本次改动只消除**常驻**内存，不改变这些读取的代价，故 11.1 的"读整条日志"在文件 adapter 上仍然发生，只是读完即释放。

另有两处相邻的健壮性问题，均未改动：`storeReader.isPrefix` 只比对行不存在与 digest，不要求该行是组尾（`writer.coversGroupBoundary` 要求），对系统自身写出的条目无影响，对不正确的外部条目偏弱；`Agent` 层与 reader 的缓存写入策略分离（`runmod.WriterCachePolicy` 排除机器投影、Runtime 的 `SnapshotPolicy` 负责它）是刻意的，不在本次修订内。

## 12. 2026-09-11 修订：provider 接缝改用 `sdk.Request` → `ModelResult`/`ModelStream`

### 12.1 起因

provider 接缝说的是旧类型：入参 `GenerateParams`，出参 `*GenerateResult` / `*StreamResult`。这带来三个问题。

1. **编排状态与单次调用边界挤在同一个类型里**。`GenerateResult` 同时承载单次调用的产出（text、tool calls、usage）和多步编排的产出（`Steps`、`Messages`、`ToolResults`、`DeferredToolApproval`），`StreamResult` 也一样。provider 从不填编排字段，但类型上无法阻止它去填，于是"什么可以过接缝"只能靠约定。
2. **装配存在多份实现，同一批 parts 可以得出不同结果**。core 有一份（`StreamText` 的步骤累积）、`StreamResult.ToResult` 有第二份、`StreamText` 的 `MaxSteps == 0` 快速路径直接绕过装配返回 provider 的流。三者的语义并不一致：`ToResult` 只从 `FinishStepPart` 取 `Response`，不取 finish reason 与 usage。实测到的外部症状是 responses 的 generate 与 stream 对同一时间戳给出不同表示（一个带本地时区，一个 UTC）——两条路径对同一份元数据不一致。
3. **旧 adapter 不看 ctx**。消费者中途离开（取消）后，转发 goroutine 会永久阻塞在无人接收的 send 上。provider 侧本身是正确的（`streamProcessor.send` 都 select `ctx.Done()`），泄漏完全在 core 适配层。

### 12.2 决定

1. **接缝类型是 `Request` → `ModelResult` / `<-chan StreamPart`**，只承载一次模型调用的边界。provider 只产 `StreamPart`，part→结果的折叠只在 core 有一处（`assembleStream`）。`Steps`/`Messages`/`ToolResults`/`DeferredToolApproval` 在接缝类型上不存在，因此"编排状态不过界"从约定变成类型事实。
2. **转换单向**。legacy→新只保留 `RequestFromGenerateParams`（legacy 便利 API 构造 `Request` 的唯一入口）；新→legacy 只保留客户端上转换 `GenerateResultFromModelResult`，它只服务客户端结果格式化，绝不向内跨接缝。向下的 `ModelStreamFromStreamResult`、`GenerateParamsFromRequest`、`ModelResultFromGenerateResult`、`ToolChoice.Legacy()` 全部删除。
3. **`GenerateResult` 与 `ModelResult` 是两个类型，不是内嵌关系**。`Response` 在接缝侧是指针、在客户端侧是值，内嵌会产生同名字段提升冲突；拆开之后编排字段在类型上就不可能过界。
4. **取消语义**：ctx 结束后装配器**停止记录**、继续排空上游，并让 `Result()` 立刻返回 `ctx.Err()`。等上游 channel 关闭才返回，会把一次取消变成一次挂死（实测：一个不关闭 channel 的 provider 让 `Result()` 挂了 10 分钟）。排空是为了让正 mid-send 的 provider 不被阻塞；两者合起来才是两个方向都不阻塞。
5. **两条路径的出口统一过 `hardenResult`**：调用方拿到自己的副本，且流式与非流式对同一份元数据给出相同表示（响应时间戳归一化到 UTC 就在这一处）。此前只有流式路径做了归一化。
6. **只有流式传输的后端用 `sdk.CollectStream` 回答非流式调用**（codex：`DoGenerate` = `DoStream` + `CollectStream`）。折叠仍是 SDK 的唯一实现，provider 不再写第二份。
7. **legacy `StreamResult.ToResult` 也路由到同一个装配器**；tool results 是编排，由 legacy 包装层在穿流时自己收集。于是全仓只剩一处 part→结果 的折叠。
8. **`ProviderOptions` 的契约落在 SDK，语义留在 provider**。它是 `map[namespace]JSON object`，namespace 就是 `Provider.Name()`；对象成员按名合并进该 provider 的 wire request（因此可以覆盖 SDK 设的字段），合并由 `sdk.ApplyProviderOptions` 完成——SDK 定"选项放在哪、怎么施加"，"选项是什么意思"仍是 provider 的属性，因为被解码的目标是 provider 自己的类型。未知成员报错而不是静默忽略：被悄悄丢掉的选项与从未设置无法区分，而这正是接缝要消灭的失效模式。provider 在 `buildRequest` 末尾调用它，所以选项能覆盖 SDK 已设的字段。
9. **验收靠 conformance 而不是靠方法名**。`provider/providertest` 只经 `sdk.Generate`/`sdk.Stream` 到达 provider，因此断言的是行为：请求确实上wire（system/user/tool 三类 marker 在 URL 或 body 中出现）、单次调用产出、流式产出与之一致、错误不被吞成空成功。套件在被打假四次后才被信任（抽掉 system 消息 → request/generate 失败；只在流式路径抽掉 → 仅 stream 失败；让流式文本发散 → 仅 stream 失败），6 个 chat provider 各有 text 与 tool-call 两个 fixture，复用各自既有的 wire 数据。

### 12.3 落点

- [providers.md](../providers.md)：`Provider` 接口签名与"接缝只跨单次调用边界"的表述；`DoStream` 的契约（关闭 channel、尊重 ctx、流中失败以 `ErrorPart` 报告）；自定义 provider 示例改为新接缝。
- [api-reference.md](../api-reference.md)：新增 `Request` 与 `ModelResult` 两个接缝类型的条目，并写明 `GenerateParams`/`GenerateResult` 与它们的关系是单向投影。
- `sdk/provider.go`、`sdk/model_call.go`、`sdk/model_stream.go`：接缝签名、`assembleStream`（唯一折叠点）、`CollectStream`（只支持流式的后端）、`hardenResult`（两条路径的共同出口）。
- `sdk/stream.go`：`StreamResult.ToResult` 改为经 `assembleStream`；`sdk/generate_text.go`、`sdk/stream_text.go`：legacy 循环走 `Request`/`ModelResult`，删除"快速路径"（它绕过了装配）。
- `sdk/request.go`：`ProviderOptions` 的命名空间与合并契约，`ApplyProviderOptions`；6 个 chat provider 在 `buildRequest` 末尾各调用一次。
- `provider/providertest`：`Fixture.Options` 与 `wantOptionsOnWire` 断言——声明了选项的 provider，其选项必须出现在请求体里（去掉任一 provider 的接线即失败，已打假）。
- `sdk/request_adapter.go`：只留 `RequestFromGenerateParams`（入）与 `GenerateResultFromModelResult`（客户端出），其余向下转换器删除。
- `provider/providertest/providertest.go` 与 6 个 `provider/*/conformance_test.go`：接缝一致性套件与 fixture。
- 6 个 chat provider 的 `DoGenerate`/`DoStream`/`buildRequest`/`convertTools`/tool-choice 转换；`agent/run/loop/contract.go` 的 `ModelInvoker`/`StreamingModelInvoker` 成为唯一声明（`sdk` 侧重复声明与 `cmd/twilight-agent` 的 `providerModel` shim 删除）。

### 12.4 后续（未做，代价已知）

1. **provider 单测仍以 legacy 参数构造请求再投影一次**（`mustJSON` 辅助函数只是把 Go schema 值变成"接缝上已经解析好的 JSON"的等价写法）。接缝原生形状由 conformance 覆盖，把剩余约 150 处字面量改写成原生 `Request` 是待办，不是行为缺口。
2. **google provider 丢弃 wire 上携带的 tool-call id**，总是自己 mint 一个（`provider/google/generativeai/types.go` 的 `functionCall` 没有 id 字段）。对照 `provider/openai/completions` 是保留 wire id、缺失才 `generateID`。
3. **OpenAI 形状的 tool-choice 编码在 4 个 provider 里各有一份**（completions、copilot、codex、responses）。这是刻意的：wire 形状属于 provider；若后续确认四处永远一致，可抽成 internal helper。
