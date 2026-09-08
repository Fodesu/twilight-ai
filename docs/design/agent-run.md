# Twilight Agent Run Protocol

状态：设计规范，第二版（2026-09-08）。Machine、command/fact 规则与 Loop 的执行逻辑已有实现并在第一版栈上通过测试；第 5 节的 Runtime（`agent/session/run`）与 Loop 已于 2026-09-08 按本版实现：Runtime 经 `extension.Writer` 写入，无 lease/grant，`RecoverInterrupted` 为接管处置；RUN-CMP-2 conformance 在 `agent/session/run/runtimetest` 以 Store 为参数，对 Memory Store 通过。本版依据 [agent-session.md](agent-session.md) 第二版（Session 级单写者、一行一个 event）与 [agent-session-extension.md](agent-session-extension.md) 第二版（`extension.Writer`）；实施记录见 [agent-runtime-refactor.md](agent-runtime-refactor.md) 第 8 节。

本文定义 `agent/run`、`agent/run/loop` 与 Run 作为 Session Module 的存储形态。文中的"必须""不得""应该"是协议约束；canonical JSON、JCS 与 domain-separated digest 使用 `agent/jsonstable` 和 `agent/es` 的通则。

## 1. 范围与 authority

```text
Session stream               唯一 authority：twilight/run/ 事实与 turn、chatlog 事件同在一条 stream，一行一个 event
MachineState                 Run 的语义状态投影（twilight/run/machine）；投影缓存为可丢弃的派生缓存
Runtime                      Run 的 command 入口：在 Session 的 extension.Writer 内 Decide、Evolve、companion，一次 Append
loop.Loop                    当前进程的 execution interpreter
FrozenValueStore             内容寻址旁存：模型请求本体（含工具定义），按 digest 存取
```

`MachineState` 决定 Run 当前可执行动作。每次接受的 command 产生一组同 CommitID 的 Session event，其中的 `twilight/run/` 事件经该 Run 版本的 `Protocol.Evolve` 从 `twilight/run/created` 重放后必须得到同一 `MachineState`。

Run 的职责分成五个相互独立的层面：

```text
Agent Machine   = Run/Step 状态与合法转移（Decide、Evolve、Next）
Agent Loop      = Machine effect 的进程内解释器
Runtime         = command 到 Session event 组的原子提交边界、接管处置
Model / Tool    = 一次模型请求或一次工具调用的 effect 执行器
Request Planner = Session context 到 sdk.Request 的投影器
```

Machine 处理已冻结的值和已提交的事实；Loop 解释 `Next` 产生的 transient effect；Runtime 保存并验证 Machine 的推进；Model/Tool 执行一次外部 effect；Request Planner 组装下一次模型请求。

`Step` 是 Run 的持久化恢复边界；`execution attempt` 表示某个 Loop 进程对该 Step 或 ToolCall 的一次易失执行。一个 Step 可以有多个 attempt。执行所有权是 Session 级的（SES-OWN-3）：持有该 Session `Writer` 的进程拥有其中全部执行，Run 不设按目标的 grant 或 lease。attempt 的 identity 由 start command 的 `ExecutionClaim` 表达；它不进入 stream。

**RUN-SCP-1** `agent/run` 拥有 Run identity、persisted frozen values、Machine、command/fact protocol、fact codec、fold 与 `Runtime`、`Companion` contract；它依赖 `agent/session` 的 identity 与 wire 类型，不依赖 loop、turn 或 extension。`agent/run/loop` 拥有 planner/model/tool ports、streaming、并发执行、EventSink 与 Loop policy。`agent/session/run` 是 Run 的 Session Module 实现：EventDefinition（按 SchemaVersion 的 codec）、`twilight/run/machine` projection、`Runtime` 实现（经 `extension.Writer.Commit` 写入）、接管处置、FrozenValueStore adapter。

**RUN-SCP-2** Run 是 first-party Session Module（Source `twilight`，ModuleID `run`）。Run 不解释它的上层实体：`OwnerID` 是 opaque 字符串，由 turn 模块以 TurnID 填充。本模块的 `Requires`（EXT-REG-4）为空；`Companion` 是 Runtime 的构造参数，由组装代码注入，为 nil 时构造失败，不作为模块依赖声明。Turn 的创建、attempt 归属与结算、Run 事实到对话内容的 companion 映射由 [agent-turn.md](agent-turn.md) 定义；对话内容 ontology 由 [agent-session-chatlog.md](agent-session-chatlog.md) 定义；stream、所有权、组追加与投影机制由 [agent-session.md](agent-session.md) 与 [agent-session-extension.md](agent-session-extension.md) 定义。Artifact、queue、provider registry、权限与产品 policy 分别由其 package 或 Application 拥有。

## 2. identity、persisted values 与 wire

```go
type RunID string
type OwnerID string // 上层实体标识，Run 不解释
type StepID string
type CallID string
type CommandID string
type ResponseID string
type InputID string
type ToolRef string
type ModelRef string
type PlanningToken string
type ExecutionClaim string
type Digest = es.Digest
```

**RUN-WIR-1** identity 必须非空、稳定且为有效 UTF-8。`ExecutionClaim` 由 Loop 为一次 start command 生成并在该 command 的重试中保持不变，用于把同一执行尝试的 start 与 settlement 派生为确定的 CommandID；接管处置使用 `TakeoverClaim = Digest("twilight/run/takeover", SessionID, Epoch)`（RUN-CMT-7）。Claim 不进入 fact。Run 跨 domain causation 记录在 `twilight/run/created` 的 `CausationID`。

Run 持久化协议保存 run-owned frozen values。模型请求、模型结果、消息、工具定义、usage、provider metadata 与所有动态 JSON 在进入 command 前，分别经 `FreezeModelRequest`、`FreezeModelResult`、`FreezeToolDefinition`、`FreezeToolCallInput` 等入口转为纯数据和 immutable `CanonicalJSON`。Runtime 接收 agent-owned value；调用方负责在边界前完成冻结。

**RUN-WIR-2** Run 事实是 Session event：EventType 为 `twilight/run/<name>`，payload 为 canonical JSON object，第一层携带 `runId` 与 payload 版本字段 `v`（SES-VER-1、EXT-REG-2）。`v` 等于该 Run 的 `SchemaVersion`：由 `twilight/run/created` 记录，同一 Run 的全部事实使用同一值，Registry 永久保留每个已发布版本的 codec、Decide 与 Evolve。行字段（Seq、CommitID、Index、Last、digest）由 Session kernel 提供，Run 不另设 envelope。fact codec 必须拒绝 unknown type、duplicate key、unknown field、trailing data、非法 UTF-8、非 canonical-equivalent wire。精确 identity 和 digest 使用 JSON string，整数字段使用 Session profile 的整数 wire shape。

```go
type CommandEnvelope struct {
    SchemaVersion uint16 // 必须等于该 Run 的 created.SchemaVersion
    Type string
    SessionID session.SessionID
    RunID RunID
    ID CommandID
    Digest Digest      // 覆盖 schema、type 与完整 command，含 transient 内容
    Command AgentCommand
}
```

command 不持久化。`CommandEnvelope.ID` 就是该 command 产生的 event 组的 `CommitID`；`Digest` 只用于 Runtime 校验 envelope 构造完整（不匹配为不可重试错误），不参与重放判定。

**RUN-WIR-3** 一个 command 恰产生一组事件（一次 `Append`，同一 CommitID）；其 `twilight/run/` 事件在组内 Index 从 0 连续递增，companion 事件（TRN-CMP）与调用方附加事件（`CommitRequest.Attach`）依次紧随其后。事件没有独立 EventID，`Seq` 即身份（SES-WIR-1）。Runtime 提交的组其 CommitID 等于 CommandID，Coordinator 写入的 Start 与 Retry 组使用该组自己的 CommitID。`RecordedAtUnixMilli` 由写入方的时钟填入，是 metadata，不参与 Run 的任何派生，也不进入 Writer 的幂等 fingerprint（EXT-WRT-2）。构造 command 必须使用该 Run 版本的 `Protocol.BuildEnvelope`（Loop 通过 `RuntimeSnapshot.Protocol()` 取得）。`agent/run` 不提供隐式选择版本的包级 `BuildEnvelope`、`Decide`、`Evolve` 或 `Digest*` 函数；新 Run 与测试显式使用 `ProtocolV1()`。

**RUN-WIR-4** 内容与执行状态分离。fact 只保存执行状态与内容 digest，内容本体落在两处：

| 内容 | fact 中的字段 | 本体位置 |
|---|---|---|
| 冻结模型请求 `ModelRequest`（含工具定义） | `ModelStepPrepared.RequestDigest` | `FrozenValueStore`，key 为 RequestDigest |
| 工具定义 `ToolDefinition` | `ToolSpec.DefinitionDigest`，只用于执行前校验 | 请求本体内；不另设存储 |
| 模型输出文本、reasoning、tool call 列表 | `ModelStepCompleted.ResultDigest` | 同组的 `twilight/chatlog/assistant`，其 `SourceDigest` 等于 ResultDigest |
| 工具输出 | `ToolCallCompleted.OutputDigest` / `ToolCallAnswered.ResponseDigest` | 同组的 `twilight/chatlog/tool_result`，其 `SourceDigest` 等于该 digest |
| tool call 参数 | `ToolCallBinding.Arguments` | fact 本身（执行不得依赖 chatlog 解码） |

companion 与 Attach 事件与 Run 事实一起经 Module Framework 的 admission（EXT-REF-2）：它们可以携带 `ReferencePart`，其 Binding 的 claim 由 Writer 在 `Append` 之前建立（EXT-WRT-3）。`FrozenValueStore` 是内容寻址存储：`Put(digest, bytes)` 幂等，`Get(digest)`。请求本体的有效期是该 ModelStep 从 Prepared 到终结；step 终结后 adapter 可按保留策略删除或归档，Record 校验不依赖本体。

下列 identity 稳定派生并由 Commit 验证：

| identity | preimage |
|---|---|
| PrepareModelRequest CommandID | RunID、loaded `RunPosition`（该 RunID 最后一条事件的 Seq） |
| ModelStep StepID | RunID、prepare CommandID、model/request/tools binding digest |
| ToolStep StepID | source ModelStepID、ordered binding-set digest |
| CallID | source ModelStepID、该 call 在模型结果 `ToolCalls` 中的位置 |
| ResponseID | RunID、ToolStepID、CallID、ResponseKind |
| response CommandID | RunID、StepID、CallID、ResponseID |
| input CommandID | RunID、InputID |
| withdraw CommandID（WithdrawPreparedStep） | RunID、StepID |
| start CommandID（StartModelExecution / StartToolCall） | RunID、StepID、CallID（model 为空）、Claim |
| owner settlement CommandID（model result/failure/reject、tool result/failure） | RunID、StepID、CallID、Claim |
| Pending Known failure CommandID | RunID、StepID、CallID、空 Claim |
| model recovery CommandID（RecoverModelExecution） | RunID、StepID、Claim |
| tool recovery CommandID（接管处置的 Unknown） | RunID、StepID、CallID、TakeoverClaim |

派生 identity 使同 CommandID 即同一 command：内容差异只可能出现在 identity 有意不覆盖内容的两族（同一 ResponseID 的 approve 与 reject、同一 attempt 的两次结算），Runtime 对它们按精确重放处理，调用方从投影读取实际生效的结果。`PlanningToken` 是 Application-owned opaque freshness token，属于 prepare command identity 内容；Run 不校验它的语义（RUN-CMT-4）。

## 3. 创建与 canonical record

```go
type NewRun struct {
    SchemaVersion uint16
    RunID RunID
    Owner OwnerID
    Attempt uint32
    CausationID es.CausationID
}
type RunCreated struct {
    SchemaVersion uint16
    RunID RunID
    Owner OwnerID
    Attempt uint32
    CausationID es.CausationID
}
type RunRecord struct {
    Created session.Seq
    Snapshot RuntimeSnapshot
    Events []session.SessionEvent // 该 RunID 的全部 twilight/run/ 事件，按 Seq 顺序
}
```

**RUN-NEW-1** `twilight/run/created` 是 Run 的第一个事实。v1 初始状态恰为：相同 RunID、Owner、Attempt、`RunActive`、`Current=Open`、无 pending input、零 model step、零 usage、无 result。初始输入随后以 `twilight/run/input_accepted` 进入同一组（TRN-STR-2）。`Protocol.BuildCreateGroup(NewRun, []AgentInput)` 返回 `created` 与 `input_accepted` 的 facts，编码为 Session event 由 `agent/session/run` 完成，Coordinator 不自行编码。同一 RunID 第二条 `created` 为 Evolve 错误。

**RUN-NEW-2** `FoldRun(events)` 按 Seq 顺序折叠该 RunID 的完整事件序列，第一条必须是 `created`，并按其 `SchemaVersion` 绑定 `Protocol`。Fold 过程执行纯状态重建。import、诊断与 `Runtime.Record` integrity verification 都经 FoldRun；投影缓存通过 FoldRun 结果校验。

## 4. Machine

```go
type Current interface{ current() }
type Open struct{}
func (Open) current() {}
func (ModelStep) current() {}
func (ToolStep) current() {}

type MachineState struct {
    RunID RunID
    Owner OwnerID
    Attempt uint32
    Status RunStatus
    Current Current
    PendingInputs []AgentInput
    ModelSteps int
    LastToolStep *ToolStep
    Usage Usage
    Result *RunResult
}

type RunResult struct {
    Status  RunStatus
    Reason  RunReason
    Failure *RunFailure
    UncertainCalls []CallID
    UncertainModel StepID
    Usage   Usage
}

type ModelStepStatus uint8 // Prepared | Executing
type ModelStep struct {
    RefValue StepRef
    RequestDigest Digest // 本体在 FrozenValueStore
    Model ModelRef
    Tools []ToolSpec
    ToolsDigest Digest
    Status ModelStepStatus
    Rejects int // 已接受的 ModelStepRejected 次数；不进入 RefValue.Digest
}
type ToolSpec struct {
    Ref ToolRef
    DefinitionDigest Digest // 本体在请求内
    Policy ResponsePolicy
}
type ToolScheduleMode string // "parallel" | "sequential"；空值按 parallel 解释
type ToolScheduling struct {
    Mode ToolScheduleMode
    MaxParallel int // 0 表示当前 Start 批次全部 Pending call 可并行
}
type ToolStep struct {
    RefValue StepRef
    Source StepID
    Calls []ToolCallState
    Scheduling ToolScheduling
}
```

`Status` 是 Run 的生命周期：`RunActive | RunCompleted | RunStopped | RunFailed`。后三者是终态。`RunStatus` 表示当前 MachineState 的投影；终态 fact 使用 RunEnd union 表达具体结果。

`Current` 是 Active 期间的内容。`Open` 是规划区间：可提交 `PrepareModelRequest`，`Next` 返回 `NeedModelRequest`。`ModelStep` 与 `ToolStep` 表示正在进行的步骤。`AcceptInput` 在任意非终态都被接受，只把输入追加到 `PendingInputs`；`PendingInputs` 是回合中途追加输入的持久化队列，在下一次 Prepare 时被一次消费。终态的 `Current` 为空；终态由 `Status` 表达，不另设 Current variant。Active 的 `Current` 不得为空。`Step` 仍只有 `ModelStep` 与 `ToolStep`，提供 `Ref()`。

MachineState 不保存模型输出与工具输出本体。上一步的内容由 Planner 从 chatlog fold 读取（REF-PLN），MachineState 只提供 `LastToolStep` 作为 Run 边界事实。

终态 fact 使用 Go 的 sealed-union 形式，终态结构由合法的 RunEnd variant 构成：

```go
type RunEnd interface{ runEnd() }

type RunCompletedEnd struct{}
type RunStoppedEnd struct {
    Reason RunReason
    UncertainCalls []CallID
    UncertainModel StepID
}
type RunFailedEnd struct {
    Reason  RunReason
    Failure RunFailure
}

func (RunCompletedEnd) runEnd() {}
func (RunStoppedEnd) runEnd() {}
func (RunFailedEnd) runEnd() {}

type RunEnded struct { End RunEnd }
```

`RunEnded.End` 必须恰好是上述三个 variant 之一；`RunStoppedEnd.Reason` 必须非空，`RunFailedEnd.Reason` 必须是失败原因，`RunFailedEnd.Failure.Class` 必须非空。`RunEnded` 是 terminal 组中最后一个 `twilight/run/` 事实。RunStatus、RunResult 等读取模型从该 union 派生。v1 wire 是 tagged union：`{"completed":{}}`、`{"stopped":{reason, uncertainCalls?, uncertainModel?}}` 或 `{"failed":{reason, failure}}`，恰有一个 variant key；codec 拒绝零个或多个 variant、缺失字段与多余字段。Cancel 时仍 Executing 的 tool call 与 model step 必须写入 `RunStoppedEnd` 并投影到 `RunResult`。

```text
ModelStep: Prepared -> Executing -> Completed
             |           |             |
             |           +-> Recovered-+  (回到同一 frozen request 的 Prepared)
             |           +-> Rejected     (retry 回到 Prepared，或同组失败 Run)
             +-> Withdrawn -> Open        (Prepared 期间有 pending input，放弃该请求并重规划)

ToolCall:
  Pending -> Executing -> Completed
     |          |
     |          +-> Failed(Known|Unknown)
     +-> Failed(Known)
  Waiting(Approval)         -> Pending | Failed(Known)
  Waiting(ExternalResponse) -> Completed | Failed(Known)
```

Recovered 回到 Prepared 后，下一次 Start 重发同一 `RequestDigest` 的请求，Loop 经 `Runtime.FrozenRequest` 取回本体。这是冻结请求被重用的唯一情形；step 终结后下一步由 Planner 重新组装。Prepared 期间到达的输入使该请求不再完整，`Next` 改为返回 `WithdrawPrepared`，Loop 提交 `WithdrawPreparedStep` 后回到 `Open` 重规划；Executing 期间到达的输入等待该步结算，在随后的 `Open` 被消费。

**RUN-MCH-1** MachineState 保存 Run 的 execution semantics。`LastToolStep` 保存最近一个经 Evolve 关闭路径写下的 ToolStep 只读投影，必须与事件序列折叠出的最后关闭 step 一致，供下一次 planner 定位 `SourceStep`。Cancel 经 `RunEnded` 把 `Current` 置空、不走关闭路径时不改写 `LastToolStep`。terminal state 吸收所有未幂等命令；`RunEnded` 建立唯一 terminal result。

**RUN-MCH-2** `ToolCallBinding` 冻结 CallID、ProviderCallID、ToolRef、definition digest、canonical arguments、response policy 与 binding digest。`CallID` 由 Run 派生（`DeriveCallID(source, index)`），是 Run 内的持久化 identity，进入 fact、派生 CommandID 与 chatlog；`ProviderCallID` 是模型发出的 `tool_call_id`，只用于 Planner 回传工具结果时与模型配对，Run 不以它为键，也不要求它唯一或非空。Decide 校验每个 binding 的 CallID 等于派生值、ProviderCallID 等于模型结果中对应位置的 id。已知工具使用匹配 frozen ToolSpec 的 ref/digest/policy；未知工具保留为同名 unresolved DirectExecution binding，并在执行前收束为已知 lookup failure。approval/external response 的 `ResponseRequest` 由 Decide 稳定派生。Unknown outcome 使用 class `effect_unknown`，只把该 Executing call 记为 `ToolCallFailed(Unknown)`。Run 保持 Active；同 step 其他 call 继续。全部 call 进入 Completed 或 Failed 后 Evolve 关闭 ToolStep。

`AgentCommand` 与 `Fact` 都是 sealed interface。v1 的 command→fact 规则为：

| command | precondition / facts |
|---|---|
| `AcceptInput` | 任意非终态；`InputAccepted`，追加到 `PendingInputs`。同一 InputID 重复接受为错误 |
| `PrepareModelRequest` | `Open`，完整有序消费 PendingInputs，request/tools digests 有效；`ModelStepPrepared`。command 携带请求本体，fact 只留 digest，本体由 Runtime 写入 FrozenValueStore |
| `WithdrawPreparedStep` | Model Prepared 且 `PendingInputs` 非空；`ModelStepWithdrawn`，`Current` 回到 `Open`，该请求本体可释放 |
| `StartModelExecution` | Model Prepared；`ModelStepStarted`。command 必须携带本次 start 的 `ExecutionClaim` |
| `RecoverModelExecution` | Model Executing；`ModelStepRecovered`。携带该 attempt 的 `Claim`，接管处置时为 `TakeoverClaim` |
| `SubmitModelResult` | Model Executing；`ModelStepCompleted{Usage, FinishReason, ResultDigest}`。有 calls 时随后 `ToolStepOpened`（携带冻结的 `Scheduling` 与 bindings）；无 calls 且 `PendingInputs` 为空时随后 `RunEnded(completed)`；无 calls 且 `PendingInputs` 非空时 `Current` 回到 `Open`，Run 继续。command 携带冻结 `ModelResult` 本体，companion 写 `twilight/chatlog/assistant` |
| `SubmitModelFailure` | Model Executing；`RunEnded(failed/provider_failure)` |
| `RejectModelResult` | Model Executing；`ModelStepRejected`，由调用方显式选择回到 Prepared 或在同一组追加 `RunEnded(failed/malformed_model_result)` |
| `StartToolCall` | Tool Pending；`ToolCallStarted`。command 必须携带本次 start 的 `ExecutionClaim` |
| `SubmitToolResult` | Tool Executing；`ToolCallCompleted{OutputDigest}`。command 携带输出本体，companion 写 `tool_result`。Evolve 后若全部 call 已 terminal，则关闭 ToolStep |
| `SubmitToolFailure(Known)` | Tool Pending/Executing；`ToolCallFailed(Known)`。Evolve 后若全部 call 已 terminal，则关闭 ToolStep |
| `SubmitToolFailure(Unknown)` | Tool Executing；`ToolCallFailed(Unknown)`。Evolve 后若全部 call 已 terminal，则关闭 ToolStep |
| `ApproveToolCall` | Waiting(Approval)；`ToolCallApproved` |
| `RejectToolCall` | Waiting(Approval) 记 `ToolCallFailed(Known/permission_denied)`；Waiting(ExternalResponse) 记 `ToolCallFailed(Known/response_rejected)`。Evolve 后若全部 call 已 terminal，则关闭 ToolStep |
| `SubmitToolResponse` | Waiting(ExternalResponse)；`ToolCallAnswered{ResponseDigest}`。Evolve 后若全部 call 已 terminal，则关闭 ToolStep |
| `CancelRun` | active；先把仍 Executing 的 tool call 记 `ToolCallFailed(Unknown)`，随后 `RunEnded(stopped/cancelled)`，并在 `RunStoppedEnd` / `RunResult` 上列出 `UncertainCalls` 与 `UncertainModel`。Waiting call 无论有无 Executing sibling 都不记 Failed；`RunEnded` 把 `Current` 置空。若这批 Unknown 使全部 call 进入终态，折叠会走 ToolStep 关闭路径并写入 `LastToolStep`；仍有 Waiting 或 Pending 时不走关闭路径，`LastToolStep` 保持原值。 |

没有独立的 `ToolStepClosed` fact。最后一个 ToolCall 进入 Completed 或 Failed 时，`Evolve` 在折叠该 fact 后若全部 call 已 terminal，则把 Current 设为 `Open` 并写入 `LastToolStep`；下一次 `PlanningHint.SourceStep` 取自 `LastToolStep.RefValue.ID`。Cancel 的 Unknown fact 同样走这条关闭规则；`RunEnded` 再把 Current 置空。

**RUN-MCH-3** `Protocol.Decide(state, command)` 执行全部验证与 derived consequence，一次返回该组的完整 ordered fact group；验证成功后返回完整 facts。`Protocol.Evolve(state, fact)` 机械折叠 fact，依赖 fact 携带的完整执行状态数据。accepted facts 必须 self-contained；若该组 terminalize，`RunEnded` 必须是 Decide 输出的最后一个 fact。

启动 command 的最小公共形状为：

```go
type StartModelExecution struct {
    StepID StepID
    Claim  ExecutionClaim
}
type StartToolCall struct {
    StepID StepID
    CallID CallID
    Claim  ExecutionClaim
}
type RecoverModelExecution struct {
    StepID StepID
    Claim  ExecutionClaim
}
```

一次执行 attempt 的全部 command identity 都从其 `Claim` 派生：start、owner settlement、model recovery 的 CommandID 分别按上表计算，Commit 对 start 强制校验该派生。因此 Loop 的 worker 只需在内存中保留 `Claim` 一个值直到 settlement 完成：提交返回非 sentinel 错误时，以同一 Claim 重放得到同一 CommandID，Writer 对精确重放返回 AlreadyApplied（RUN-LOP-5）。Claim 不需要持久化：进程崩溃后由接管者按 RUN-CMT-7 处置全部 Executing 目标，不依赖前一进程的 Claim。

`Next(state)` 最多返回一个 transient `Effect`：

| state | effect |
|---|---|
| terminal | 返回 `ErrRunTerminal`，没有 effect |
| `Open` | `NeedModelRequest{PlanningHint}` |
| Model Prepared 且 `PendingInputs` 非空 | `WithdrawPrepared` |
| Model Prepared | `StartModelCall` |
| Model Executing | `Idle` |
| ToolStep 有 Pending calls | `StartToolCalls` |
| ToolStep 无 Pending、仍有 Waiting 或 Executing | `Idle` |

Waiting call 上的 `ResponseRequest` 由 `WaitingCalls(state)` 读取。Executing call 由 `ExecutingCalls(state)` 读取。`NeedsRecovery(state)` 在 Model Executing 或 ToolStep 无 Pending 且仍有 Executing 时为 true。这些查询不是 Effect。

**RUN-MCH-4** Effect 由调用方每次 Load 后重新派生。`AcceptInput` 在任意非终态入队，Decide 不因 Run 正在执行而拒绝它；`PendingInputs` 只在 `Open` 的 Prepare 中被消费。`PrepareModelRequest.InputIDs` 必须与当前 PendingInputs 等长、同顺序、逐项相同；prepare 接受后一次消费全部 pending input。ToolStep 的 Waiting call 禁止 Start，同一 step 中的 Pending call 仍可执行。没有可执行 Start 时 `Next` 返回 `Idle`。Application 从投影读取 `WaitingCalls` 并提交 `ApproveToolCall` / `RejectToolCall` / `SubmitToolResponse`。Executing 目标在当前 owner 进程内由其 worker 结算；owner 崩溃后由接管者按 `NeedsRecovery` 一次性处置（RUN-CMT-7）。

## 5. Runtime、投影与 Commit

```go
type Runtime interface {
    Load(context.Context, session.SessionID, RunID) (RuntimeSnapshot, error)
    Commit(context.Context, session.SessionID, CommitRequest) (CommitResult, error)
    Record(context.Context, session.SessionID, RunID) (RunRecord, error)
    FrozenRequest(context.Context, Digest) (ModelRequest, error)
    // RecoverInterrupted 是接管处置：对该 Session 投影中全部 Executing 目标各提交一个 recovery command，
    // 返回提交数。宿主在 OpenWriter 之后、驱动任何 Run 之前调用一次（RUN-CMT-7）。
    RecoverInterrupted(context.Context, session.SessionID) (int, error)
}
// RunPosition 是该 RunID 最后一条 twilight/run/ 事件的 Seq；只有这个 Run 自己的事件会移动它。
type RunPosition = session.Seq
type RuntimeSnapshot struct {
    State MachineState // detached in-process view
    Position RunPosition
    Head session.Head  // 读取时的 Session head
    SchemaVersion uint16 // created.SchemaVersion
}

// ModuleEvent 是其他模块的 typed event，由 agent/session/run 经 Registry 编码。
type ModuleEvent struct {
    Type session.EventType
    Value any
}
// Companion 把一组 Run facts 与 command 携带的 transient 内容映射为
// 其他模块的事件（对话内容、Turn completed）。实现由 agent/turn 提供（TRN-CMP）。
type CompanionRequest struct {
    Session session.SessionID
    Owner OwnerID
    RunID RunID
    Command AgentCommand
    Facts []Fact
    State MachineState // Evolve 后
    RecordedAtUnixMilli int64
}
type Companion interface {
    Version() string
    Map(CompanionRequest) ([]ModuleEvent, error)
}
type Protocol struct {
    // ProtocolFor 一次绑定该 SchemaVersion 的函数。方法不再接受 version 参数。
}
func ProtocolFor(schemaVersion uint16) (Protocol, error)
func (RuntimeSnapshot) Protocol() (Protocol, error)
func (Protocol) Version() uint16
func (Protocol) DigestRequest(ModelRequest) (Digest, error)
func (Protocol) DigestToolDefinition(ToolDefinition) (Digest, error)
func (Protocol) DigestToolSpecs([]ToolSpec) (Digest, error)
func (Protocol) DigestModelStepBinding(ModelRef, Digest, Digest) (Digest, error)
func (Protocol) DigestModelResult(ModelResult) (Digest, error)
func (Protocol) DigestToolOutput(CanonicalJSON) (Digest, error)
func (Protocol) DigestToolResponseDecision(ResponseKind, ResponseDecision, string) (Digest, error)
func (Protocol) DigestCommand(typ string, command AgentCommand) (Digest, error)
func (Protocol) EncodeFact(typ string, fact Fact) (jsonstable.Value, error) // 不含 v；Registry 加入
func (Protocol) DecodeFact(typ string, wire jsonstable.Value) (Fact, error)
func (Protocol) Decide(MachineState, AgentCommand) ([]Fact, error)
func (Protocol) Evolve(MachineState, Fact) (MachineState, error)
func (Protocol) BuildEnvelope(session.SessionID, RunID, CommandID, AgentCommand) (CommandEnvelope, error)
func (Protocol) BuildCreateGroup(NewRun, []AgentInput) ([]Fact, error)
func (Protocol) EncodeMachineState(*MachineState) (jsonstable.Value, error)
func (Protocol) DecodeMachineState(jsonstable.Value) (MachineState, error)
func ProtocolV1() Protocol

type CommitRequest struct {
    Base RunPosition // Load 时的 Position；PrepareModelRequest 为 hard CAS，其他 command 可为零值
    Command CommandEnvelope
    Attach []ModuleEvent // 调用方附加事件，追加在 companion 之后；例如 Coordinator.Stop 的 twilight/turn/failed
}
type CommitResult struct {
    Status CommitStatus // CommitAccepted | CommitAlreadyApplied
    Snapshot RuntimeSnapshot
    Events []session.SessionEvent // 本次 command 的完整组：run facts、companion、Attach
}
```

Runtime 由组装代码以 `extension.Writers`（EXT-WRT-6）、`FrozenValueStore`、`Companion` 与 `SnapshotPolicy` 构造；它按 SessionID 取得该 Session 的 `Writer`，全部读写经该 Writer。

**RUN-CMT-1** Runtime 按 `(SessionID, RunID)` 寻址。Run 由 Coordinator 的 Start 组创建（TRN-STR-2），Runtime 没有 `Create`。`ErrRunNotFound` 只用于该 Session 中不存在的 RunID。已终结的 Run 不在 `twilight/run/machine` 投影中（RUN-CMT-2），`Load` 对它以 `Types=[twilight/run/]` 过滤 `Read`、按 RunID 筛出全部事实后 FoldRun，返回终态 snapshot；`Commit` 对它返回 `ErrRunTerminal`。这条路径是兜底：正常流程中 Loop 从结算返回的 snapshot 读到终态（第 7 节），Coordinator 从 turn surface 的 `AttemptView` 取终态与 SchemaVersion（TRN-PRJ-1），都不依赖它。

**RUN-CMT-2** 投影 `twilight/run/machine` 消费全部 `twilight/run/` 事件，其他模块的事件按 EXT-PRJ-2 跳过，状态为：

```go
type MachineProjection struct {
    Active map[RunID]MachineState   // 非终态 Run
    Positions map[RunID]RunPosition // 非终态 Run 的最后事件位置
    Ended map[RunID]struct{}        // 已终结的 RunID，只用于拒绝第二条 created
}
```

终态 Run 在 `RunEnded` 折叠后从 `Active` 与 `Positions` 移除，只在 `Ended` 保留 RunID 用于拒绝同一 RunID 的第二条 `created`（RUN-NEW-1）；终态结果由 `Record` 与 turn surface 提供，投影大小与活动 Run 数成正比，加上已终结 RunID 的集合。`Load` 经 `Writer.Projections()` 读取 Writer 内存中的投影（EXT-PRJ-4）；独立进程的观察者经 `extension.NewProjectionReader` 从 Store 读取，投影缓存（EXT-PRJ-3）是可丢弃的派生数据，写入策略由 `agent/session/run` 的 `SnapshotPolicy` 决定，默认在 Run 的 `Current` 回到 `Open` 或 Run 终结时写入，并可按组计数补充。`Record` 以 `Types=[twilight/run/]` 过滤 `Read` 读取该 RunID 的全部事件（SES-REP-2），FoldRun 重建；该 Run 仍在投影中时与投影状态比对，divergence 必须失败。

**RUN-CMT-3** Commit 经 `extension.Writer.Commit` 在该 Session 的 Writer 互斥区内完成（EXT-WRT-1）。所有 Runtime implementation 在 fn 内调用同一个 pure `EvaluateCommit`，顺序固定为：

```text
writer.Commit(func(view):
  1  validate envelope SessionID/RunID/schema/type/digest（digest 不匹配为不可重试错误）
  2  view.LookupCommit(CommitID = CommandID)
  3  found -> fn 返回 nil（Writer 记 Noop）；Runtime 以查到的行与当前投影构造 CommitAlreadyApplied
  4  derived CommandID check
  5  state = view.Projection(twilight/run/machine).Active[RunID]
     不在 Active：在 Ended 或过滤 Read 到该 RunID 的事件 -> ErrRunTerminal；否则 ErrRunNotFound
     schema 不等于 created.SchemaVersion -> 不可重试错误
  6  validate hard CAS（prepare 的 Base == Positions[RunID]）/ target state
  7  facts = Protocol.Decide(state, command) exactly once
  8  Protocol.Evolve in order；facts -> ModuleEvent（Type twilight/run/<name>，v = SchemaVersion）
  9  companion = Companion.Map(...)；校验 SourceDigest（TRN-MAP-3）；追加 request.Attach（不得为 twilight/run/ 事件）
  10 return SemanticGroup{CommitID: CommandID, Events: run ++ companion ++ attach}
)
// Writer 完成 codec、Binding admission、claim（先于 Append）、Append 与投影折叠（EXT-WRT-1/3）。
// Runtime 在 Commit 返回后按 SnapshotPolicy 写投影缓存；缓存写入失败不影响 commit 结果。
```

FrozenValueStore 的 `Put` 幂等且内容寻址，在进入 Writer 之前完成；Commit 失败时留下的本体无害，可由保留策略回收。

**RUN-CMT-4** `PrepareModelRequest` 是 hard-CAS command：`Base` 必须等于投影记录的该 Run 的 `Position`。这是有意选择：同一 Session 内其他模块的写入（用户提交新输入、summary、checkpoint、其他 Turn 的事件）不移动 Position，因此不使 Prepare 失效；Plan 与 Prepare 之间发生的 chatlog 写入不会被本次请求包含，新鲜度由 Application 经 `PlanningToken` 与 Planner 自行负责，Run 不校验 `PlanningToken` 的语义。其他 command 通过当前 target state 做 call-local rebase，`Base` 可为零值或过期值；stale Base 本身不阻止无冲突的 ingress/control/settlement。相同 command 的 replay 判定先于 terminal check，因此 terminal Run 仍能返回原组。

**RUN-CMT-5** 幂等键为 Writer 的 `(SessionID, CommitID)` 索引（EXT-WRT-2），CommitID 等于 CommandID，Runtime 不另设幂等索引。同 CommandID 的重放返回 `CommitAlreadyApplied`、当前 snapshot 与原完整组，且不得再次 Decide 或产生外部 effect；command 不持久化，Runtime 不比对重放 command 的内容，同 CommandID 视为同一 command。对于 `StartModelExecution` 和 `StartToolCall`，claim 是 CommandID 的 preimage，不同 claim 即不同 command：其 start 按当前 target state 评估，target 已是 Executing 时返回 `ErrStaleRuntime`。

**RUN-CMT-6** 执行授权与所有权失效。Runtime 不签发 grant，也不校验按目标的执行授权：Session 所有权（SES-OWN-1）即执行所有权，同一进程内同一 Run 至多一个 Loop 在驱动（第 7 节的 driver slot），Executing 目标的 settlement 只可能来自该 Loop 的 worker 或接管处置。跨进程的迟到写入由 kernel 的 Epoch fencing 拒绝（SES-OWN-2）：Writer 返回 `ErrOwnershipLost` 时 Runtime 原样返回该错误，Loop 必须取消全部 worker、放弃 settlement 并以该错误返回（RUN-LOP-5）；Coordinator 同样放弃该 Session（TRN-REC-2）。

**RUN-CMT-7** 接管处置。新 owner 取得 Writer 后，在驱动任何 Run 之前调用一次 `RecoverInterrupted`：对投影中每个 Executing 的 ModelStep 提交 `RecoverModelExecution{Claim: TakeoverClaim}`，对每个 Executing 的 tool call 提交 `SubmitToolFailure{Outcome: Unknown}`（CommandID 以 TakeoverClaim 派生，第 2 节 identity 表）；Pending call 不处置（start barrier 证明它从未运行，由下一次 Loop 启动）；Waiting call 不处置。每个处置是一次普通 Commit，companion 在同组写入 status=`unknown` 的 `tool_result`（TRN-CMP-2）；Run 保持 Active，同一 RunID 继续。`TakeoverClaim` 由 Writer 的 Epoch 派生，因此同一 owner 重复调用幂等（同 CommandID 得到 AlreadyApplied），不同 owner 的处置各自成为新 command。宿主在 `RecoverInterrupted` 返回后才 Resume 各 Turn（TRN-REC-1）。

**RUN-CMT-8** 每个 Run 的协议版本是 `created.SchemaVersion`，创建时冻结。`RuntimeSnapshot.SchemaVersion` 等于该值；`ProtocolFor(schemaVersion)` 返回绑定该版本 digest/codec/Decide/Evolve 的 `Protocol`。`EvaluateCommit` 接受 command 当且仅当 `CommandEnvelope.SchemaVersion` 等于该 Run 的版本。新 Run 由 `NewRun.SchemaVersion` 决定版本；同一 Session 内不同 Run 可以使用不同版本；v1 Run 的 replay 必须继续使用 `ProtocolV1()`。Run 的版本与 Session kernel 的 `ProtocolVersion` 无关（SES-VER-1）。

### 5.1 不进入 stream 的数据

`ExecutionClaim` 只存在于持有它的 worker 内存中；投影缓存是可丢弃的派生数据（EXT-PRJ-3）；`FrozenValueStore` 是内容寻址旁存。三者都不是 authority，丢失后的后果分别为：该 attempt 无法在本进程内重放（由 RUN-LOP-5 的一次重试之外的路径处理，或随进程崩溃由接管处置覆盖）、投影从 stream 重折、Executing/Prepared step 的重发失败为不可重试错误（Application 决定 Retry）。第一版的控制面 KV、lease、grant 与 durable ClaimStore 已全部删除，见 [agent-runtime-refactor.md](agent-runtime-refactor.md) 第 8 节。

## 6. Loop ports 与 policy

```go
// package agent/run
type PlanningHint struct {
    Session session.SessionID
    Owner OwnerID
    RunID RunID
    SourceStep StepID
    Inputs []AgentInput
}
// package agent/run/loop
type RequestPlanner interface {
    Plan(context.Context, run.PlanningHint) (RequestPlan, error)
}
type RequestPlan struct {
    Model run.ModelRef
    Request sdk.Request
    InputIDs []run.InputID
    PlanningToken run.PlanningToken
    Tools []run.ToolSpec // 与 Request.Tools 一一对应；DefinitionDigest 由 Loop 校验
}
type ModelCatalog interface { ResolveModel(run.ModelRef) (ModelInvoker, error) }
type ModelInvoker interface { Generate(context.Context, sdk.Request) (sdk.ModelResult, error) }
type StreamingModelInvoker interface { Stream(context.Context, sdk.Request) (sdk.ModelStream, error) }
type ToolCatalog interface { ResolveTool(run.ToolRef) (ExecutableTool, error) }
type ExecutableTool interface {
    Ref() run.ToolRef
    Definition() sdk.ToolDefinition
    ResponsePolicy() run.ResponsePolicy
    ValidateArguments(run.CanonicalJSON) error
    Execute(context.Context, ToolExecutionRequest) ToolExecutionOutcome
}
```

`ToolExecutionOutcome` 是 sealed interface：`ToolExecutionSucceeded`、`ToolExecutionFailed`（明确未完成）或 `ToolExecutionUnknown`（可能已发生）。`ValidateArguments` 在 start barrier 前运行，并保持无外部 effect。

```go
type ToolExecutionMode string

const (
    ToolExecutionParallel  ToolExecutionMode = "parallel"
    ToolExecutionSequential ToolExecutionMode = "sequential"
)

type ExecutionPolicy struct {
    ToolExecution ToolExecutionMode
    MaxParallel int
    OnMalformedModelResult func(run.ModelStep, run.StepFailure) run.ModelRejectDisposition
}
type LoopResult struct {
    Disposition LoopDisposition // LoopWaiting | LoopFinished
    Reason WaitReason           // 仅 ExecutionRecovery 时为 execution_recovery；否则为空
    ExecutionRecovery bool
    Result *run.RunResult
}
func New(models ModelCatalog, tools ToolCatalog, planner RequestPlanner, policy ExecutionPolicy, streaming bool) (*Loop, error)
func (*Loop) Run(context.Context, run.Runtime, session.SessionID, run.RunID, EventSink) (LoopResult, error)
```

**RUN-LOP-1** `ExecutionPolicy` 是 Loop 的本地执行策略。`ToolExecution` 与 `MaxParallel` 在 `SubmitModelResult` 时写入 `ToolStepOpened.Scheduling` 并冻结在该 ToolStep 上；后续 Loop 必须按冻结值调度，不得改用当时进程的 ExecutionPolicy。未指定 `ToolExecution` 时冻结为 `parallel`，`MaxParallel` 零值表示当前 Start 批次全部 Pending call 可并行。空 Mode 按 parallel 解释，不得在 normalize 时填入默认字符串。nil handler 时结构错误的模型结果选择 `ModelRejectFailRun`；重试由 handler 明确返回 `ModelRejectRetry`。`streaming` 表示是否请求可用的流式模型端口；两种模式都产生同一完整 `sdk.ModelResult`。Loop 没有租约续期与 durable ClaimStore：执行所有权由 Session Writer 承担（RUN-CMT-6）。

**RUN-LOP-7** `ModelRef` 是冻结请求中的执行身份。`ModelCatalog.ResolveModel` 在同一 Run 生命周期内必须把同一 `ModelRef` 解析为等价的执行语义。provider 绑定不进入 frozen request，因此 Catalog 不得把同一 ref 改绑到不同实现。

`LoopResult` 的语义固定为：`LoopWaiting` 时 `Result` 为 nil，表示没有可执行 effect、Run 仍为 active。`ExecutionRecovery` 等于 `NeedsRecovery(state)`。该值为 true 表示存在本进程未持有 Claim 的 Executing 目标（只在崩溃后、接管处置之前出现），`Reason` 为 `execution_recovery`；否则 `Reason` 为空。Waiting call 不进入 `LoopResult`；Application 通过投影的 `WaitingCalls` 读取。`LoopFinished` 时 `Result` 非 nil，并等于 terminal Run 的 `RunResult`。

`RequestPlanner` 从 `PlanningHint` 接收 Run 边界事实；它从 Session 的 chatlog fold 读取对话内容（上一步的 assistant 与 tool_result 已随 Run fact 同组提交），并使用自己注入的 memory、attachments 与 product policy 组装 `sdk.Request`。Runtime 验证并冻结 planner 返回的 request，Planner 管理 application context。

## 7. Loop execution

```text
Loop.Run(ctx, runtime, sessionID, runID, sink):
  repeat:
    snapshot = Runtime.Load(sessionID, runID)
    if terminal: emit observational run_finished; return Finished(snapshot.Result)
    effect = run.Next(snapshot.State)
    dispatch effect
    // 模型结算（无 tool call 的 SubmitModelResult、SubmitModelFailure、FailRun 的 RejectModelResult）
    // 可能终结 Run；此时 CommitResult.Snapshot 已是终态，Loop 直接 emit run_finished 并
    // return Finished(snapshot.Result)，不再 Load。工具结算不会终结 Run。
```

每个 `Loop` 实例为每个 `(SessionID, RunID)` 分配一个本地 driver slot。同一实例对同一 Run 的并发 `Run` 调用返回 `ErrRunAlreadyRunning`；不同 Run 可以并行驱动。宿主必须保证一个 Session 在一个进程内只有一个 Loop 实例驱动它的 Run（与 `Writer` 一一对应）。

**RUN-LOP-2** `NeedModelRequest` 调用 Planner，冻结 sdk.Request，验证 model、ordered InputIDs 与 ToolSpecs，计算 request/tools/binding digests 和 derived CommandID/StepID，再提交 Prepare（command 携带本体）。prepare stale 后重新 Load；同 Position 的内容拒绝不得 livelock 重试。业务停止统一使用 `CancelRun`。

**RUN-LOP-8** `WithdrawPrepared` 时 Loop 提交 `WithdrawPreparedStep{StepID}`，随后重新 Load；被放弃请求的本体在 FrozenValueStore 中可立即释放。Loop 不为输入做任何其他事：Executing 与 ToolStep 期间到达的输入留在 `PendingInputs`，由随后 `Open` 的 `NeedModelRequest` 经 `PlanningHint.Inputs` 交给 Planner。

**RUN-LOP-3** `StartModelCall` 先 Commit start barrier；`CommitAccepted`，或以同一 Claim 重试得到的 `CommitAlreadyApplied`（RUN-LOP-5 的一次重放），表示本 Loop 拥有该 execution。worker 在内存中保留该 attempt 的 Claim 直到完成 settlement，其余 identity 按需派生。调用使用 `Runtime.FrozenRequest(snapshot.State.Current.RequestDigest)` 取回的本体的 detached SDK materialization；本体缺失为不可重试错误，交由 Application 处理。streaming 与 non-streaming 必须产生同一种完整 `sdk.ModelResult`；delta 只发 EventSink。`ModelCatalog.ResolveModel` 失败或返回 nil 时提交 `RecoverModelExecution` 并返回错误，不得把 Run 记为 `provider_failure`：尚未发生模型调用。provider 调用失败提交 `SubmitModelFailure`；ctx cancellation 提交 `RecoverModelExecution`；结构、binding 或 freeze 失败提交 `RejectModelResult`，并由调用方显式选择 retry 或 fail-run。成功结果只提交一次 `SubmitModelResult`。

**RUN-LOP-4** Tool execution 先按 frozen binding resolve tool，并验证 Ref、definition digest、response policy 和 arguments。lookup/definition/argument failure 在 Pending 状态提交 `SubmitToolFailure(Known)`，不得跨越 start barrier。通过验证后逐 call 提交 `StartToolCall`；只有 start 被接受的 worker 可执行。冻结的 `ToolStep.Scheduling` 决定 `parallel` 或 `sequential` 以及 `MaxParallel`；不得改用 Loop 进程当前的 ExecutionPolicy。每个结果以自己的 Claim 派生 CommandID 提交。同一 ToolStep 中 DirectExecution 的 Pending call，在外层 ctx 未取消时于本次 `Run` 内按冻结 Scheduling 分批 Start 并结算；ctx 已取消时停止再 Start，只结算已 Start 的 call。`Next` 返回 `Idle` 时 Loop 返回 `LoopWaiting`，并用 `NeedsRecovery(state)` 设置 `ExecutionRecovery`。Loop 不解释 Waiting call，也不携带 `ResponseRequest`。Application 从投影读取 `WaitingCalls`，提交 `ApproveToolCall` / `RejectToolCall` / `SubmitToolResponse` 之后再次 `Run`。tool panic 或 effect 状态无法确定的错误转为对该 call 的 Unknown，并提交 `SubmitToolFailure(Unknown)`。该 settlement 不取消同批 sibling workers，也不结束 Run。`CancelRun` 先把仍 Executing 的 call 记为 `ToolCallFailed(Unknown)`，再 `RunEnded(stopped/cancelled)`，并把这些 CallID 与仍 Executing 的 ModelStep 写入 `RunStoppedEnd` / `RunResult` 的 `UncertainCalls`、`UncertainModel`。Waiting call 无论有无 Executing sibling 都不记 Failed。已接受 start 的 worker 必须在收到外层取消后返回并尝试 settlement；settlement 使用独立 control context。lookup/definition/argument failure 只允许发生在 Pending。

**RUN-LOP-5** model 与 tool worker 都接收外层 ctx；Loop 对已接受 effect 使用独立 control context 完成 known/unknown outcome settlement。Application 的业务停止顺序为先 Commit `CancelRun`，再取消 Loop ctx。非 sentinel Commit error 以同 CommandID 重放一次；仍未知时返回错误，由后续 Load/Record 查询 authority。stale/terminal/conflict 触发 reload/drop，旧 external effect 保持单次执行尝试。`ErrOwnershipLost` 是终止性错误：Loop 取消全部 worker 的 ctx，不再提交任何 settlement（提交也会被 kernel 拒绝），以该错误返回；已发生的外部 effect 由接管者按 RUN-CMT-7 记为 Unknown。工具实现配合 context 返回；永久阻塞由 application 处理。

Waiting call 的批准与外部结果由 Application 提交。Loop 不生成、不返回、不解释 `ResponseRequest`。Application 以投影中的 stable ResponseID、derived CommandID 与 payload/decision digest 提交 `ApproveToolCall`、`RejectToolCall` 或 `SubmitToolResponse`；随后再次运行 Loop。

## 8. EventSink 与边界

```go
type EventSink interface { Emit(context.Context, Event) error }
type Event struct {
    Session session.SessionID
    RunID run.RunID
    StepID run.StepID
    CallID run.CallID
    Sequence uint64
    Kind EventKind
    Durability EventDurability
    Payload json.RawMessage
    Committed []session.SessionEvent // EventAgentCommitted 携带本次 command 的完整组
}
```

`Sequence` 仅用于同一临时观察流内的顺序（例如 ToolProgress），从 1 开始；committed observation 的权威顺序由 Session `Seq` 表达，未提供临时序号时保持 0。

**RUN-LOP-6** EventSink 提供 realtime observation，Loop 通过序列化调用向 sink 发送事件。`EventAgentCommitted` 携带 accepted 组；text/reasoning delta、tool progress、tool lifecycle 与 run-finished observation 可丢失、重复或断流。sink failure 保持 Commit 结果；恢复与审计读取 Session stream，EventSink gap 通过 stream 对账。

`AcceptInput` 在任意非终态提交，`PendingInputs` 就是回合中途输入的队列；Loop 不解释 queue 或 steer：`Open` 时立刻 `NeedModelRequest`，Prepare 一次消费全部 pending input。Application 负责 admission；Turn 创建、attempt、中途投递、结算与 companion 映射由 [agent-turn.md](agent-turn.md) 定义。

## 9. compatibility 与 conformance

**RUN-CMP-1** 当前 pre-release schema v1 的 command/fact discriminator、wire fields、canonical digest、derived ID 和 `ProtocolV1().Evolve` 由 golden fixtures 保护；发布前有意修改协议时必须同步更新 fixture。v1 发布后，新增 variant、字段或折叠语义必须进入新 `SchemaVersion`，Registry 继续 decode/fold 全部已发布版本；同一 Run 的 writer 不得混写不同版本。Run 版本演进不触发 Session kernel 版本变化。

**RUN-CMP-2** Runtime conformance 只断言 Run 模块自己的语义；组原子性、digest chain、所有权与 Epoch fencing、幂等索引、投影缓存复用由 Session kernel 与 Module Framework 的 conformance 覆盖（SES 第 7 节、EXT 第 7 节），本清单以引用代替重复。conformance 以 `session.Store` 为参数（`agent/session/run/runtimetest`），Memory 与文件 adapter 跑同一套。必须覆盖：

- 建立与寻址：Start 组建立 Run；同一 RunID 第二条 `created` 使投影 fold 失败；未知 RunID 的 Load、Commit、Record 返回 `ErrRunNotFound`；已终结 Run 的 Load 返回终态 snapshot 且与 Record 一致，Commit 返回 `ErrRunTerminal`（RUN-CMT-1）；`CommandEnvelope.SchemaVersion` 与 `created.SchemaVersion` 不一致的 command 被拒绝且不可重试；
- 重放与 Base：同 CommandID 返回 `CommitAlreadyApplied` 与原组且不再 Decide；Run 已终结后对已接受 command 的重放仍返回 AlreadyApplied，新 command 返回 `ErrRunTerminal`；prepare 的 Base 不等于该 Run 的 Position 时返回 `ErrStaleRuntime`；非 Prepare command 接受零值或过期的 Base（call-local rebase）；
- 输入入队：`AcceptInput` 在 Open、Model Prepared、Model Executing、ToolStep 都被接受；Prepared 期间入队后 `Next` 返回 `WithdrawPrepared`，Withdraw 后重规划的 Prepare 包含该输入；Executing 期间入队的输入在无 tool call 的 `SubmitModelResult` 后使 Run 回到 Open 而不结束；
- start 与 claim：同 claim 的 start 重放返回 AlreadyApplied；不同 claim 的 start 在 target 已是 Executing 时返回 `ErrStaleRuntime`；同一 attempt 的 settlement 以其 Claim 派生 CommandID，重放返回 AlreadyApplied；
- 组的组成：一 command 一组，同一 CommitID；组内 run 事实在 companion 与 Attach 之前；companion 中非空 `SourceDigest` 等于同组 fact 记录的 ResultDigest / OutputDigest / ResponseDigest；Attach 携带 `twilight/run/` 事件被拒绝；companion 与 Attach 中的 ReferencePart 经 admission，未注册 Binding 使 Commit 失败且无写入，合法 Binding 在 Append 之前建立 Active claim（EXT-WRT-3）；
- 结算返回值：`CommitResult.Snapshot` 是 Evolve 后状态；终结 Run 的结算其 `Snapshot.Status` 为终态且 `Result` 非空，与 Record 一致；
- Prepare hard CAS 只对该 Run 自己的事件敏感：同一 Session 内 chatlog、turn 或其他 Run 的写入不改变该 Run 的 Position，也不使 Prepare 失效；
- 投影：`SnapshotPolicy` 在 Run 回到 Open 或终结时写入投影缓存；终态 Run 不出现在 `Active`，其 RunID 在 `Ended`；Record 对活动 Run 的 fold 与投影一致；非法 fact 序列使 FoldRun 报错（篡改与缺口的检测属于 SES-REP-1）；
- 隔离：同一 Session 内多 Run 互不影响 Position 与 Record；chatlog 与 turn 事件不影响 Run fold。不同 SchemaVersion 的 Run 共存在第二个 SchemaVersion 发布后启用；
- 接管处置：关闭 Writer 后以新 Writer 打开（Epoch 加一）并调用 `RecoverInterrupted`：Executing model 回到 Prepared 且 `FrozenRequest` 返回同一 RequestDigest 的请求；Executing tool 记 Unknown 且 companion 在同组写入 status=`unknown` 的 `tool_result`，同 step 的 Pending 与 Waiting call 不受影响；Run 保持 Active；同一 Epoch 重复调用返回 0 且无新写入；没有 Executing 目标时返回 0；
- 所有权失效：旧 Writer 上的 Runtime 在被接管后 Commit 返回 `ErrOwnershipLost` 且 stream 无新行（fencing 由 SES-OWN-2 保证，本层观察结果）；
- FrozenValueStore：`Put` 幂等；未知 digest 的 `FrozenRequest` 返回 `ErrFrozenValueMissing`；step 终结后删除本体不影响 Record；
- MachineState codec：每个 Current variant 与终态 round-trip、拒绝 unknown field / 非法判别式 / trailing data（`agent/run` 单元测试）。

Loop conformance 必须覆盖：

- 单模型完成、tool round trip、approval/external response wait/resume；
- known failure 继续、Unknown 继续、tool panic、aliased ToolRef 与 validation；
- parallel/sequential 按冻结 `ToolStep.Scheduling` 调度，不得改用当时 ExecutionPolicy；
- `ModelCatalog.ResolveModel` 失败或 nil 时恢复 ModelStep、Run 保持 active；
- ctx cancellation、model recovery 后重发同一 RequestDigest、explicit malformed-result disposition；
- Cancel 将 Executing tool/model 投影到 `UncertainCalls` / `UncertainModel`；ExternalResponse reject 为 `response_rejected`；
- streaming delta 与 nil result、EventSink committed observation 携带完整组；
- 非 sentinel commit error 的一次重放、prepare no-progress rejection 与无 livelock；
- 模型结算终结 Run 时 Loop 不再 Load，返回 `LoopFinished` 且 `Result` 等于 Record 的终态；
- Writer 返回 `ErrOwnershipLost` 时 Loop 取消 worker、不再提交 settlement、以该错误返回；随后新 owner 的 `RecoverInterrupted` 把该 Executing 目标记为 Unknown 或回到 Prepared。

package 迁移、实施阶段与未完成 adapter 工作记录在 [agent-runtime-refactor.md](agent-runtime-refactor.md)，本协议 authority 以本文为准。
