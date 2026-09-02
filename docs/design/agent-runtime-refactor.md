# Twilight Agent Runtime 重构记录

状态：迁移记录，非协议规范

当前 Run/Loop 协议的唯一 authority 是 [agent-run.md](agent-run.md)。本文只保留重构背景、已接受的 package 决策、完成状态与后续迁移工作；实现与本文冲突时，以各领域正式规范为准。

正式规范：

| 领域 | authority |
|---|---|
| Run Machine、Runtime、Loop | [agent-run.md](agent-run.md) |
| Session ES kernel | [agent-session.md](agent-session.md) |
| Artifact Core | [agent-artifact.md](agent-artifact.md) |
| Session Module Framework | [agent-session-extension.md](agent-session-extension.md) |
| Chatlog ontology/projection | [agent-session-chatlog.md](agent-session-chatlog.md) |
| Turn→Run coordination/materialization | [agent-turn.md](agent-turn.md) |
| 参考组装（Binding / Planner / Input） | [agent-reference-assembly.md](agent-reference-assembly.md) |

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
- Durable owner/fence/lease/recovery/outbox 是 adapter control plane，不进入 `run.Runtime` public contract。

## 3. 已完成迁移

| 工作 | 状态 |
|---|---|
| SDK single-call boundary 与 run-owned frozen model data | 完成 |
| shared `agent/es` 与 RFC 8785 canonical JSON | 完成 |
| Decide/Evolve/Next Run Machine | 完成 |
| RunHeader、TransitionRecord、wire codec、fold/golden tests | 完成 |
| RunID-addressed `Runtime.Create/Load/Commit/Record` | 完成 |
| multi-Run Store-backed Runtime 与 Runtime conformance | 完成 |
| `agent/run/loop` package extraction | 完成 |
| Session/Artifact/Session Module/Turn protocols | 规范草案完成，实施待完成 |
| Chatlog protocol | 草案，payload 与 golden 尚未冻结 |
| 参考组装 | 草案 |
| PostgreSQL durable Run adapter（旧接口） | 历史 prototype，迁移未完成 |

当前正式调用形态为 Application 组合 shared `run.Runtime` 与 `loop.Loop`。Loop 不保存 authority state；Runtime 不读取 queue 或 planner context。

## 4. 后续实施工作

### 4.1 Core reference implementations

- 冻结 Session、Artifact 与 Chatlog v1 wire profiles、domain separators、wire-size validation 和 golden fixtures；
- 实现 Session、Artifact、Session Module、Chatlog 的 Memory implementations 与 conformance；
- 实现 `turn.Coordinator`、first-party Turn module、`FactMapper`、`MaterializeAll` 和 settlement recovery；
- 跑通 Input → Turn → Context → Run/Loop → Session Events → replay 的最小 vertical slice。

### 4.2 durable adapters

- 将 PostgreSQL durable Run adapter 迁移到统一 `run.Runtime`；
- 让 `Record` 在单一数据库 read snapshot 内读取并验证 header/state/log；
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

## 5. 完成标准

重构在以下条件全部成立时结束：

- Memory 与 PostgreSQL adapters 通过相同 Runtime conformance；
- Turn materialization 对 crash、重复 delivery 和 unknown response 可恢复；
- Session/Artifact/Session Module/Chatlog reference implementations 通过各自 conformance；
- production request context 和 UI surface 由 Session projections 提供；
- legacy history 不再承担 canonical write authority；
- Run、Session 与 Artifact 的 durable integrity/recovery paths 有持续 CI 覆盖。

后续协议修改直接更新对应正式规范；本文只更新迁移状态和历史决策，不再承载 wire、Machine、Runtime 或 Loop 算法。
