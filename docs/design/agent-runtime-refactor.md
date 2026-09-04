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
Session stream    唯一 authority：twilight/turn、twilight/chatlog、twilight/run 事件同在一条 stream
MachineState      Run 的语义状态投影（twilight/run/machine），snapshot 为派生缓存
Runtime           Run command 的提交入口：Session commit 临界区内 Decide、Evolve、追加
FrozenValueStore  内容寻址旁存：模型请求与工具定义本体
```

2026-09-04 之前的设计为两条 ES（Run 独立的 `RunHeader + TransitionRecord[]`，Turn 把 Run 事实 materialize 到 Session）。该设计已被第 6 节记录的决定取代。

### 2.2 package layout

```text
agent/es                  shared ES primitives
agent/jsonstable          immutable canonical JSON
agent/run                 Run Machine、frozen values、fact codec、fold、Runtime contract
agent/run/loop            in-process model/tool interpreter 与 observation ports
agent/session             Event-first Session kernel（Commit CAS 与 CommitIn 临界区）
agent/session/extension   Session Module Framework：static modules、codec、semantic append、projection
agent/session/chatlog     first-party Message ontology
agent/session/run         first-party Run module：EventDefinition、machine projection、Runtime 实现、FrozenValueStore 与 lease adapter
agent/artifact            Ref、Binding、RetentionClaim
agent/turn                Turn 生命周期、attempt、companion 映射
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

根 `agent/run` 不提供 Loop alias、wrapper 或 façade。

### 2.3 boundary decisions

- `sdk.Request`、`sdk.ModelResult` 和 tool definitions 在 Runtime 前冻结为 run-owned persisted values。
- Queue、steer/follow-up、fixed-model policy、权限、provider registry 和 MCP lifecycle 属于 Application。
- Session kernel 保持 payload-opaque、Artifact-free。
- Chatlog Message 原生支持 first-party Artifact references；`sdk.Message` 只是 materialized provider transport。
- Turn Coordinator 从 `twilight/turn/surface` 与 `twilight/run/machine` 投影重建，不保存隐藏的长期状态。
- Run 事实与其对话内容（companion）在同一 SessionCommit 写入；没有 Run→Session materialization、coverage 水位或 outbox。
- lease 与其 recovery（`RenewLease`、`RecoverExpired`）是 `run.Runtime` public contract 的一部分，由 [agent-run.md](agent-run.md) 第 5 节定义；lease、grant、ExecutionClaim、ClaimStore、投影 snapshot 与 FrozenValueStore 是控制面或派生数据，不进入 stream。
- Run fact 只保存执行状态与内容 digest；请求与工具定义本体在 FrozenValueStore，模型输出与工具输出在 chatlog 事件。

## 3. 已完成迁移

| 工作 | 状态 |
|---|---|
| SDK single-call boundary 与 run-owned frozen model data | 完成 |
| shared `agent/es` 与 RFC 8785 canonical JSON | 完成 |
| Decide/Evolve/Next Run Machine | 完成 |
| RunHeader、TransitionRecord、wire codec、fold/golden tests | 完成 |
| RunID-addressed `Runtime.Create/Load/Commit/Record` | 完成；待按第 6 节改为 Session module 形态 |
| multi-Run Store-backed Runtime 与 Runtime conformance | 完成；conformance 待迁移到 Session Store 之上 |
| 追加式 per-Run `Store` 合同、snapshot codec、lease 续期 | 完成；per-Run Store 将由 Session Store 的 `CommitIn` 取代，lease 表与 snapshot codec 保留 |
| SQLite per-Run Store adapter | 完成；随 per-Run Store 退役，SQLite 实现改为 Session Store adapter |
| `agent/run/loop` package extraction | 完成 |
| Turn 最小实现（`agent/turn`：Start / Resume / Stop、v1 FactMapper、append-only MemoryLog、崩溃后 Resume） | 完成；FactMapper 与 MaterializeAll 将改为 companion，Log 由 Session Store 取代 |
| 单一 Session ES 的设计文档（agent-run 第 5 节、agent-turn、session CommitIn、extension/chatlog 修订） | 完成，2026-09-04 |
| Session/Artifact/Session Module protocols | 草案，无实现；wire 在 Memory 纵向切片跑通前不冻结 |
| Chatlog protocol | 草案，payload 与 golden 尚未冻结 |
| 参考组装 | 草案 |
| PostgreSQL durable Run adapter（旧接口） | 历史 prototype，迁移未完成 |

当前正式调用形态为 Application 组合 shared `run.Runtime` 与 `loop.Loop`。Loop 不保存 authority state；Runtime 不读取 queue 或 planner context。

## 4. 后续实施工作

### 4.1 Core reference implementations

- 冻结 Session、Artifact 与 Chatlog v1 wire profiles、domain separators、wire-size validation 和 golden fixtures；Run fact 的 wire 与 golden 并入同一 ProtocolVersion；
- 实现 Session kernel Memory Store（含 `CommitIn`）、Artifact、Session Module、Chatlog 的 Memory implementations 与 conformance；
- 按第 6 节把 `agent/run` 的存储层改为 Session module；
- 把 `agent/turn` 改为 attempt 模型与 companion，`Log` 替换为 Session Store 与 `extension.SemanticAppender`；
- 加 Chatlog Context projection 与参考 Planner；`PlanningHint` 只提供边界事实。

### 4.2 durable adapters

- Session Store 的 SQLite 与 PostgreSQL adapter（commit、CommitIn 事务、snapshot、lease 表、FrozenValueStore）；
- 收紧 Session authority tables 的 immutable RLS policy；
- 实现过期 execution recovery scanner；
- 设计 Artifact Binding/Claim 与 semantic append intent tables。

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
| `state.go` | 新增 `TurnID`、`RunPosition`、`Companion` 接口；`MachineState` 加 `Turn`、`Attempt`，删 `LastModelResult`；`ModelStep.Request` 改为 `RequestDigest`；`ToolSpec` 删 `Definition`；`RunResult` 删 `Model` |
| `fact.go` | 删 `RunHeader`、`TransitionRecord`、`AgentEvent`；`ModelStepCompleted` 改为 `{StepID, Usage, FinishReason, ResultDigest}`；`ToolCallCompleted`/`ToolCallAnswered` 改为 digest；新增 `RunCreated`；fact codec 输出 `jsonstable.Value` |
| `decide.go` | Prepare 校验 command 携带的本体 digest 后只写 digest；SubmitModelResult/SubmitToolResult 计算 ResultDigest/OutputDigest |
| `commit.go` | `EvaluateCommit` 改为 SessionTx 形态：LookupCommit、读投影、以 `RunPosition` 做 prepare hard CAS、Decide、Evolve、companion、Attach、SourceDigest 校验，返回 AppendRequest 与 lease ops |
| `ids.go` | `DeriveModelRequestCommandID` 以 `RunPosition` 为 preimage；新增 run 事件 EventID 派生 |
| `agent/run/loop` | `Run(ctx, runtime, sessionID, runID, sink)`；Start 前 `Runtime.FrozenRequest`；ClaimStore key 加 SessionID；EventSink 的 `Committed` 改为 SessionCommit |
| `agent/turn` | 删 `mapper.go` 的 MaterializeAll、`ResultReference`、`MemoryLog`；新增 `CompanionV1`、`Retry`、`Settle`、surface 投影 |

删除：`store.go`、`memory_store.go`、`stored_runtime.go`、`sqlitestore/`、`header.go`、`transition.go` 的 per-Run wire、`example_run_test.go` 的 per-Run Store 用法（改写为 Session Store 版本）。

新增：`agent/session` Memory Store（含 `CommitIn`）、`agent/session/run`（module descriptor、machine projection、Runtime 实现、Memory FrozenValueStore 与 lease）、golden fixtures 重新冻结。

后续协议修改直接更新对应正式规范；本文只更新迁移状态和历史决策，不再承载 wire、Machine、Runtime 或 Loop 算法。
