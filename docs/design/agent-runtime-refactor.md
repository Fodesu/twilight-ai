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
MachineState      Run 的语义状态投影
TransitionRecord  canonical Run commit record
Runtime           Run authority、RunID-addressed access 与 atomic commit boundary
Session Events    跨 Run 的长期语义事实
```

Run snapshot 支持直接恢复执行；immutable header 与 transition log 支持 verified replay、materialization 与审计。Session 只接收经 Turn materialization 的长期语义 events。

### 2.2 package layout

```text
agent/es                  shared ES primitives
agent/jsonstable          immutable canonical JSON
agent/run                 Run Machine、persisted protocol、Runtime、Store、MemoryStore
agent/run/loop            in-process model/tool interpreter 与 observation ports
agent/session             Event-first Session kernel
agent/session/extension   Session Module Framework：static modules、codec、semantic append、projection
agent/session/chatlog     first-party Message ontology
agent/artifact            Ref、Binding、RetentionClaim
agent/turn                Turn→Run coordination 与 Run→Session materialization
```

文件用于提高同一 package 内的导航性；subpackage 只用于依赖限制和独立变化轴。Loop 因依赖 SDK execution、streaming、并发和工具 ports 而独立成 `agent/run/loop`。Machine、Runtime、protocol 与 Store 保持在根 `agent/run`，避免 sealed variants、codec、Decide/Evolve 和 adapter internals 之间形成 cycle 或镜像 DTO。

依赖方向为：

```text
Application -> agent/turn + agent/run/loop + adapters
agent/run/loop -> agent/run + sdk
agent/run      -> agent/es + agent/jsonstable + sdk
agent/turn     -> agent/run + agent/session + agent/session/chatlog + session modules
```

根 `agent/run` 不提供 Loop alias、wrapper 或 façade。

### 2.3 boundary decisions

- `sdk.Request`、`sdk.ModelResult` 和 tool definitions 在 Runtime 前冻结为 run-owned persisted values。
- Queue、steer/follow-up、fixed-model policy、权限、provider registry 和 MCP lifecycle 属于 Application。
- Session kernel 保持 payload-opaque、Artifact-free。
- Chatlog Message 原生支持 first-party Artifact references；`sdk.Message` 只是 materialized provider transport。
- Turn Coordinator 从 Session facts 与 `Runtime.Record` 重建，不保存隐藏的长期状态。
- Run→Session materialization 按完整 revision coverage 和 stable identity exactly-once 收敛。
- lease 与其 recovery（`RenewLease`、`RecoverExpired`）是 `run.Runtime` public contract 的一部分，由 [agent-run.md](agent-run.md) 第 5 节定义；`Store` 只持久化 lease 记录，不解释它。durable owner/fence/outbox 仍是 adapter control plane，不进入 `run.Runtime`。

## 3. 已完成迁移

| 工作 | 状态 |
|---|---|
| SDK single-call boundary 与 run-owned frozen model data | 完成 |
| shared `agent/es` 与 RFC 8785 canonical JSON | 完成 |
| Decide/Evolve/Next Run Machine | 完成 |
| RunHeader、TransitionRecord、wire codec、fold/golden tests | 完成 |
| RunID-addressed `Runtime.Create/Load/Commit/Record` | 完成 |
| multi-Run Store-backed Runtime 与 Runtime conformance | 完成 |
| 追加式 `Store` 合同（LoadHead / LoadLog / LoadRecord 单一致读 / Commit critical section / ExpiredLeases）、snapshot codec、lease 续期 | 完成 |
| SQLite Store adapter（snapshot 加日志、lease 表）通过 Runtime / recovery / renewal conformance | 完成 |
| `agent/run/loop` package extraction | 完成 |
| Turn 最小实现（`agent/turn`：Start / Resume / Stop、v1 FactMapper、append-only MemoryLog、崩溃后 Resume） | 完成；Session kernel、Extension、Artifact 未接入，Log 为纵向切片的替身 |
| Session/Artifact/Session Module protocols | 草案，无实现；wire 在 Memory 纵向切片跑通前不冻结 |
| Chatlog protocol | 草案，payload 与 golden 尚未冻结 |
| 参考组装 | 草案 |
| PostgreSQL durable Run adapter（旧接口） | 历史 prototype，迁移未完成 |

当前正式调用形态为 Application 组合 shared `run.Runtime` 与 `loop.Loop`。Loop 不保存 authority state；Runtime 不读取 queue 或 planner context。

## 4. 后续实施工作

### 4.1 Core reference implementations

- 冻结 Session、Artifact 与 Chatlog v1 wire profiles、domain separators、wire-size validation 和 golden fixtures；
- 实现 Session、Artifact、Session Module、Chatlog 的 Memory implementations 与 conformance；
- 把 `agent/turn` 的 `Log` 替换为 Session kernel 的 `Store` 与 `extension.SemanticAppender`，事件 identity 从 Seq 改为 spec 的派生 EventID / CommitID；
- 加 Chatlog Context projection 与参考 Planner，让 Run 的 `PlanningHint` 只提供边界事实。

### 4.2 durable adapters

- 将 PostgreSQL durable Run adapter 迁移到统一 `run.Runtime`；
- 收紧 Run authority tables 的 immutable RLS policy；
- 将 recovery record 绑定具体 lease/fence 并原子消费；
- 实现过期 execution recovery scanner；
- 增加 transition delivery outbox，作为 Turn materialization 的 delivery 优化；
- 设计 Session Store、projection snapshot、Artifact Binding/Claim 与 semantic append intent tables。

### 4.3 Application migration

- 组合 model/tool registries、permission、queue admission 与 `agent/run/loop` driver；
- 构建 Session Surface/Context projections 与 API；
- 逐步把 `bot_history_messages` 降为兼容 read model；
- 在完整 materialization、terminal settlement 与 retention closure 后执行归档/GC。

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

- Memory 与 PostgreSQL adapters 通过相同 Runtime conformance；
- Turn materialization 对 crash、重复 delivery 和 unknown response 可恢复；
- Session/Artifact/Session Module/Chatlog reference implementations 通过各自 conformance；
- production request context 和 UI surface 由 Session projections 提供；
- legacy history 不再承担 canonical write authority；
- Run、Session 与 Artifact 的 durable integrity/recovery paths 有持续 CI 覆盖。

后续协议修改直接更新对应正式规范；本文只更新迁移状态和历史决策，不再承载 wire、Machine、Runtime 或 Loop 算法。
