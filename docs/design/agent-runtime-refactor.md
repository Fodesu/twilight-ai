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
| 参考组装（Binding / Planner / Input） | [agent-reference-assembly.md](agent-reference-assembly.md)（草案） |

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
extension.Writer  进程内唯一写入口：串行、幂等索引、admission、claim、投影
MachineState      Run 的语义状态投影（twilight/run/machine），投影缓存为可丢弃缓存
Runtime           Run command 的提交入口：Writer 内 Decide、Evolve、companion，一次 Append
FrozenValueStore  内容寻址旁存：模型请求本体（含工具定义）
```

2026-09-04 之前的设计为两条 ES（Run 独立的 `RunHeader + TransitionRecord[]`，Turn 把 Run 事实 materialize 到 Session）。该设计已被第 6 节记录的决定取代，第 7 节记录审查后的第二次修订（多写者临界区、控制面 KV、lease），第 8 节记录 2026-09-08 的第三次修订（Session 级单写者、扁平事件）。上表为第 8 节之后的形态。

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
- 只有一条写入路径：`extension.Writer`。Run 的 Runtime、Turn 的 Coordinator 都经它写入，companion 与 Attach 事件与其他 producer 一样经 admission；artifact claim 在 Append 之前建立，孤儿由回收前核对释放。
- 一个 Session 同一时刻一个 Writer 进程（Session 级所有权，Epoch fencing）；没有按目标的 lease、grant 或 durable ClaimStore。ExecutionClaim 只在 worker 内存中；投影缓存与 FrozenValueStore 是派生或旁存数据，不进入 stream。
- 接管者对全部 Executing 目标一次性处置（模型回 Prepared、工具记 Unknown），不等待 TTL 按目标恢复。
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
| Session kernel Memory Store（`agent/session`） | 完成，2026-09-07；conformance 部分实现 |
| `agent/session/extension`（Registry、SemanticAppender、Lease、ProjectionReader） | 完成，2026-09-07；conformance 部分实现 |
| `agent/session/chatlog`（事件、parts codec、Surface、Context） | 完成，2026-09-07；checkpoint 未实现 |
| `agent/artifact`（Ref、Binding、Memory BindingStore、两态 KV ledger） | 完成，2026-09-07；Resolver/Store/Promoter 未实现 |
| `agent/session/run`（module descriptor、machine 投影、Runtime、RecoverExpired） | 完成，2026-09-07 |
| `agent/run/loop` 绑定 Session（`Run(ctx, runtime, sessionID, runID, sink)`、RunPosition、SessionCommit 观察） | 完成，2026-09-07 |
| `agent/turn` 重写（Coordinator、CompanionV1、surface 投影） | 完成，2026-09-07；旧实现已删除 |
| 参考组装 `agent/ref`（ExecutionBinding、ContextPlanner、Memory 组装、SessionDriver、崩溃恢复 example） | 完成，2026-09-07 |
| Runtime conformance（RUN-CMP-2，`agent/session/run/runtimetest`，以 `session.Store` 为参数） | 完成，2026-09-07；对 Memory Store 通过。kernel 与 extension 的 conformance 部分实现 |
| 第 8 节规范修订（session、extension、run、turn、chatlog、artifact、参考组装的第二版） | 完成，2026-09-08；代码未动 |
| 第 8 节代码重构（kernel 收缩、Writer、Runtime 去 lease/grant、接管处置、conformance 重建） | 完成，2026-09-08；`agent/` 下 9 个测试包全部通过，kernel 与 RUN-CMP-2 的 conformance 均以 Store 为参数 |
| 文件 adapter（`agent/session/filestore`）、live 模型接入 | 未开始 |

2026-09-07 的代码行是第一版形态，已于 2026-09-08 按第 8 节重写为第二版。当前正式调用形态为 `agent/ref` 的 Memory 组装：`ref.New` 返回 Store、Registry、Writers、Runtime、Coordinator 与 Bindings；宿主对每个 Session 先 `Memory.Open`（取所有权并接管处置）再经 `SessionDriver.Send` 投递输入。Loop 不保存 authority state；Runtime 不读取 queue 或 planner context。

已决定（2026-09-07）：终态 Run 从 `twilight/run/machine` 投影移除后，`Runtime.Load` 对该 Run 按 RunID 过滤 replay 后折叠返回终态，`ErrRunNotFound` 只用于不存在的 RunID（RUN-CMT-1）。该路径为兜底：Loop 在模型结算返回终态 snapshot 时直接结束，不再 Load（RUN 第 7 节）；Coordinator 的 Deliver 与 Stop 从 turn surface 的 `AttemptView.SchemaVersion` 构造 envelope，不读 machine 投影（TRN-DLV-2、TRN-STP-1）。曾考虑在投影保留终态 Run 的最小记录，因投影会随历史增长而未采用。

已决定（2026-09-07）：Runtime 不为 command digest 另设控制面索引；同 CommandID 一律按重放处理，幂等只由 kernel 的 `(SessionID, CommitID)` 承担（RUN-CMT-5）。曾实现过 `twilight/run/command` 索引，用于把同 ID 不同内容判为冲突；该判定只覆盖 approve/reject 撞 ID 与同一 attempt 两次结算两种情形，前者由调用方读投影覆盖，后者属于实现错误，故删除。

## 4. 后续实施工作

### 4.1 Core reference implementations

第一版（第 6、7 节）的全部条目已于 2026-09-07 完成；第 8 节修订后的实施顺序见 8.5。完成后再冻结 kernel `ProtocolVersion` 1 与各模块 payload 版本 1 的 golden fixtures。

### 4.2 durable adapters

- 文件 adapter `agent/session/filestore`：一个 Session 一个目录，`stream.jsonl` 一行一个 event，`session.lock` 为 `flock` 目标，每次 Append 一次 fsync，打开时截掉不完整尾组；
- 数据库 adapter（SQLite / PostgreSQL）：sessions（header、epoch、owner deadline）、events 两张表，Append 一个事务，`Heartbeat` 推后 deadline；只在多会话服务需要时做；
- 收紧 Session authority tables 的 immutable RLS policy；
- 需要远程 Store 或跨存储 claim 时，实现 extension 附录 C 与 artifact 附录的两阶段路径。

### 4.3 Application migration

- 组合 model/tool registries、permission、queue admission 与 `agent/run/loop` driver；
- 构建 Session Surface/Context projections 与 API；
- 逐步把 `bot_history_messages` 降为兼容 read model；
- 在完整 materialization、terminal settlement 与 retention closure 后执行归档/GC。

## 4.3 Run 内部整理（已完成）

- envelope digest 不匹配从 `ErrCommandConflict` 改为不可重试错误，调用方不再对构造错误 reload 重试；
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

## 7. 第二次修订（2026-09-04，架构审查后）

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

### 7.3 持久结构与一致性等级（第一版；第 8 节之后见 8.4）

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

### 7.6 租约的层次（2026-09-04 第三次修订）

审查后的第一版把 lease 写成 run 模块对 opaque KV 的约定，续期与结算存在竞争，并补了一个投影兜底扫描。随后考虑过把类型化的 lease 原语放进 kernel，被否决：kernel 不应持有"持有者"这类模块语义。最终切法：

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

## 8. 第三次修订（2026-09-08）：Session 级单写者与扁平事件

### 8.1 起因

第一版 Memory 栈跑通后（第 3 节），对照 dsh 与 Codex 的 session 日志实现发现：twilight 比它们多出的全部机制（`CommitIn` 临界区、`Commit` 的 CAS、控制面 KV、按目标的 lease 与 grant、`RenewLease` 心跳、`RecoverExpired` 按 deadline 枚举、commit 与 KV 同事务）都源于同一个假设：同一个 Session 可以有多个并发写者，包括不同进程。该假设没有部署需求支撑：Memoh 作为服务把一个 Session 固定到一个 worker，failover 走锁接管，不会两个 worker 同时写同一 Session；本地宿主是单进程。dsh 的做法（每 session 一个 write handle，进程内独占加跨进程 `flock`，第二个写者直接被拒）说明单写者足以支撑同类需求。

同时发现 `SessionCommit` 容器在读侧只是一层没有语义的嵌套（`ReplayPage.Commits[].Events[]`），它承担的三个作用中，幂等与 CAS 单位随单写者上移到进程内，commit 级元数据可以摊到每行，只剩"整组原子可见"一条，而这条只需要 append 以组为单位并在读侧不暴露不完整组，不需要嵌套类型。

### 8.2 决定

| 项 | 第一版 | 第二版 |
|---|---|---|
| 写者 | 多写者，`CommitIn` 回调式临界区，`Commit` CAS | 一个 Session 同一时刻一个 `Writer`（SES-OWN-1）；`Open` 取所有权，Epoch 加一并持久化；落后 Epoch 的 `Append` 被拒（SES-OWN-2） |
| 写入单位 | `SessionCommit{Events[]}`，`(Revision, Index)` 定位 | 一行一个 `SessionEvent`，全局 `Seq`；同一次 `Append` 的行共用 `CommitID`，`Index`/`Last` 标记组；整组原子，不读不完整组（SES-APP-1/2） |
| 幂等 | kernel 按 `(SessionID, CommitID)` 加 fingerprint | kernel 只拒绝重复 CommitID；`extension.Writer` 以内存索引判定 AlreadyApplied / Conflict（EXT-WRT-2） |
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

失去：同一 Session 的不同工具调用由不同进程并发执行（没有消费者）；claim 与 commit 的同事务一致性（降为先 claim 后 append，孤儿由核对清理）；第一版 conformance 中 grant 隔离、跨 Run grant、lease 续期的十几项断言。

得到：kernel 接口从 15 个方法降到 4 个，adapter 只需实现独占、追加与读，JSONL 成为一等实现；Runtime 去掉 lease/grant 两套校验；Loop 去掉心跳与 ClaimStore；与 dsh、Codex 的心智模型一致（一个 session 同一时刻一个写者）。

### 8.4 持久结构与一致性等级（第二版）

| 结构 | 等级 | 写入点 | 丢失或不一致时 |
|---|---|---|---|
| Session header 与 event 行 | authority | `Writer.Append` | 不可恢复；按行 digest 链使损坏可检测；不完整尾组在打开时截掉 |
| 所有权记录（Epoch，数据库 adapter 另有 deadline） | 控制 | `Open`、`Heartbeat` | 文件 adapter 随进程释放；数据库 adapter 过期后可接管 |
| 投影缓存 | 派生缓存 | `SnapshotPolicy` | 从 stream 重折 |
| Writer 内存：幂等索引、投影状态、head | 派生 | `OpenWriter` 重建 | 随进程消失，重开时从日志重建 |
| FrozenValueStore | 旁存，生命周期为 ModelStep | Commit 之前 `Put` | Executing/Prepared step 的重发失败为不可重试错误 |
| artifact claim | 独立持久 | `Activate`，Append 之前 | 孤儿 claim 由回收前核对释放；不可能出现无 claim 的引用 |
| Artifact content store 与 BindingStore | 外部内容 | artifact owner | resolve 失败按 ART-CAP-1 分类 |

v1 只有两类恢复动作：`RecoverInterrupted`（新 owner 一次性处置 Executing 目标）与 Application 的 artifact GC（回收前核对加按 Active claim 计算 root）。

### 8.5 实施顺序

1. `agent/session`：按第二版重写 Memory Store（Create、Header、Open/Epoch/Heartbeat、Append 整组、Read 过滤）与 conformance；删除 CommitIn、CAS、控制面 KV、snapshot、四套 digest、EventID、ReplayCursor；
2. `agent/session/extension`：`Writer`（OpenWriter 重建、Commit 串行、幂等索引、claim 先于 Append、ErrOwnershipLost 失效）、`Writers`、`ProjectionReader` 与 `ProjectionCache`、`Ignorable`；删除 SemanticAppender、Lease、LoadIn/SaveSnapshotIn、JSONPointer、ModuleForEvent 推断；
3. `agent/artifact`：ledger 改为自持久化 `Activate`，加 `OwnerVerifier` 与回收前核对；
4. `agent/run` 与 `agent/session/run`：`RunPosition = Seq`、`CommitResult.Events`、删除 grant/lease/RenewLease/RecoverExpired，新增 `RecoverInterrupted` 与 `TakeoverClaim`，machine 投影加 `Ended`；
5. `agent/run/loop`：删除 LeaseRenewInterval、ClaimStore、grant 路径；`ErrOwnershipLost` 处理；EventSink 携带 `[]SessionEvent`；
6. `agent/turn`：Coordinator 改为 `Writers`，Seq 定位，恢复表按 TRN-REC-2；
7. `agent/session/chatlog`：位置类型改 Seq；
8. `agent/ref`：按参考组装第 5 节重组，崩溃恢复 example 改为"关闭 Writer、以新 Epoch 打开、RecoverInterrupted、Resume"；
9. RUN-CMP-2 conformance 按第二版清单重建；随后写文件 adapter，用 session 与 runtimetest 两套 conformance 验收。

后续协议修改直接更新对应正式规范；本文只更新迁移状态和历史决策，不再承载 wire、Machine、Runtime 或 Loop 算法。
