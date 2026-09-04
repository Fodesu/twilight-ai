# Twilight Agent Run Protocol

状态：设计规范。Machine、command/fact 规则与 Loop 已有实现；第 5 节的 Runtime 与存储层无实现，实施记录见 [agent-runtime-refactor.md](agent-runtime-refactor.md)。

本文定义 `agent/run`、`agent/run/loop` 与 Run 作为 Session Module 的存储形态。文中的"必须""不得""应该"是协议约束；canonical JSON、JCS 与 domain-separated digest 使用 `agent/jsonstable` 和 `agent/es` 的通则。

## 1. 范围与 authority

```text
Session stream               唯一 authority：twilight/run/ 事实与 turn、chatlog 事件同在一条 stream
MachineState                 Run 的语义状态投影（twilight/run/machine）；snapshot 为可丢弃的派生缓存
Runtime                      Run 的 command 入口：在 Session 临界区内 Decide、Evolve，经 Module Framework 追加
loop.Loop                    当前进程的 execution interpreter
FrozenValueStore             内容寻址旁存：模型请求本体（含工具定义），按 digest 存取
extension.Lease              占用与续期：grant 是 Lease Token，ExecutionClaim 是 Holder；建立在 Session 控制面 KV 上，与 commit 同事务
```

`MachineState` 决定 Run 当前可执行动作。每次接受的 command 产生一个 SessionCommit，其中的 `twilight/run/` 事件经该 Run 版本的 `Protocol.Evolve` 从 `twilight/run/created` 重放后必须得到同一 `MachineState`。

Run 的职责分成五个相互独立的层面：

```text
Agent Machine   = Run/Step 状态与合法转移（Decide、Evolve、Next）
Agent Loop      = Machine effect 的进程内解释器
Runtime         = command 到 SessionCommit 的原子提交边界、grant/lease、recovery
Model / Tool    = 一次模型请求或一次工具调用的 effect 执行器
Request Planner = Session context 到 sdk.Request 的投影器
```

Machine 处理已冻结的值和已提交的事实；Loop 解释 `Next` 产生的 transient effect；Runtime 保存并验证 Machine 的推进；Model/Tool 执行一次外部 effect；Request Planner 组装下一次模型请求。

`Step` 是 Run 的持久化恢复边界；`execution attempt` 表示某个 Loop 进程对该 Step 或 ToolCall 的一次易失执行。一个 Step 可以有多个 attempt，Machine 只接受带有效 grant 的 settlement。Attempt 的执行控制信息由 start command 的 `ExecutionClaim` 和 Runtime 返回的 opaque `ExecutionGrant` 表达；它们不进入 stream。

**RUN-SCP-1** `agent/run` 拥有 Run identity、persisted frozen values、Machine、command/fact protocol、fact codec、fold 与 `Runtime`、`Companion` contract；它依赖 `agent/session` 的 identity 与 Store 类型，不依赖 loop、turn 或 extension。`agent/run/loop` 拥有 planner/model/tool ports、streaming、并发执行、EventSink 与 Loop policy。`agent/session/run` 是 Run 的 Session Module 实现：EventDefinition（按 SchemaVersion 的 codec）、`twilight/run/machine` projection、`Runtime` 实现（经 `extension.SemanticAppender.AppendSemanticIn` 写入，经 `extension.Lease` 占用与续期）、FrozenValueStore adapter。

**RUN-SCP-2** Run 是 first-party Session Module（Source `twilight`，ModuleID `run`）。Run 不解释它的上层实体：`OwnerID` 是 opaque 字符串，由 turn 模块以 TurnID 填充。本模块的 `Requires`（EXT-REG-4）为空；`Companion` 是 Runtime 的构造参数，由组装代码注入，为 nil 时构造失败，不作为模块依赖声明。Turn 的创建、attempt 归属与结算、Run 事实到对话内容的 companion 映射由 [agent-turn.md](agent-turn.md) 定义；对话内容 ontology 由 [agent-session-chatlog.md](agent-session-chatlog.md) 定义；stream、commit、projection、snapshot 与控制面 KV 机制由 [agent-session.md](agent-session.md) 与 [agent-session-extension.md](agent-session-extension.md) 定义。Artifact、queue、provider registry、权限与产品 policy 分别由其 package 或 Application 拥有。

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
type ExecutionGrant string
type Digest = es.Digest
```

**RUN-WIR-1** identity 必须非空、稳定且为有效 UTF-8。`ExecutionClaim` 由 Loop 为一次 start command 生成并在该 command 的重试中保持不变，用于绑定 start command 与执行尝试。`ExecutionGrant` 是 Runtime 签发的 opaque capability，交还给签发它的 Runtime 完成 settlement。两者服务于执行授权，不进入 fact。Run 跨 domain causation 记录在 `twilight/run/created` 的 `CausationID`。

Run 持久化协议保存 run-owned frozen values。模型请求、模型结果、消息、工具定义、usage、provider metadata 与所有动态 JSON 在进入 command 前，分别经 `FreezeModelRequest`、`FreezeModelResult`、`FreezeToolDefinition`、`FreezeToolCallInput` 等入口转为纯数据和 immutable `CanonicalJSON`。Runtime 接收 agent-owned value；调用方负责在边界前完成冻结。

**RUN-WIR-2** Run 事实是 Session event：EventType 为 `twilight/run/<name>`，payload 为 canonical JSON object，第一层携带 `runId` 与 payload 版本字段 `v`（SES-VER-1、EXT-REG-2）。`v` 等于该 Run 的 `SchemaVersion`：由 `twilight/run/created` 记录，同一 Run 的全部事实使用同一值，Registry 永久保留每个已发布版本的 codec、Decide 与 Evolve。envelope、revision、index、digest chain 与 idempotency 由 Session kernel 提供，Run 不另设 envelope。fact codec 必须拒绝 unknown type、duplicate key、unknown field、trailing data、非法 UTF-8、非 canonical-equivalent wire。精确 identity 和 digest 使用 JSON string，整数字段使用 Session profile 的整数 wire shape。

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

command 不持久化。`CommandEnvelope.ID` 就是该 command 产生的 SessionCommit 的 `CommitID`；`Digest` 只用于 Runtime 在临界区内比对精确重放。

**RUN-WIR-3** 一个 command 恰产生一个 SessionCommit；其 `twilight/run/` 事件 Index 从 0 连续递增，companion 事件（TRN-CMP）与调用方附加事件（`CommitRequest.Attach`）依次紧随其后。所有事件的 EventID 由 Appender 按 `Digest(EventType, CommitID, index)` 统一赋值（EXT-APP-5）；Runtime 提交的 commit 其 CommitID 等于 CommandID，Coordinator 写入的 Start 与 Retry group 使用该 group 的 CommitID。`RecordedAtUnixMilli` 由写入方的时钟填入，是 metadata，不参与 Run 的任何派生，也不进入 append fingerprint（SES-APP-1）。构造 command 必须使用该 Run 版本的 `Protocol.BuildEnvelope`（Loop 通过 `RuntimeSnapshot.Protocol()` 取得）。`agent/run` 不提供隐式选择版本的包级 `BuildEnvelope`、`Decide`、`Evolve` 或 `Digest*` 函数；新 Run 与测试显式使用 `ProtocolV1()`。

**RUN-WIR-4** 内容与执行状态分离。fact 只保存执行状态与内容 digest，内容本体落在两处：

| 内容 | fact 中的字段 | 本体位置 |
|---|---|---|
| 冻结模型请求 `ModelRequest`（含工具定义） | `ModelStepPrepared.RequestDigest` | `FrozenValueStore`，key 为 RequestDigest |
| 工具定义 `ToolDefinition` | `ToolSpec.DefinitionDigest`，只用于执行前校验 | 请求本体内；不另设存储 |
| 模型输出文本、reasoning、tool call 列表 | `ModelStepCompleted.ResultDigest` | 同 commit 的 `twilight/chatlog/assistant`，其 `SourceDigest` 等于 ResultDigest |
| 工具输出 | `ToolCallCompleted.OutputDigest` / `ToolCallAnswered.ResponseDigest` | 同 commit 的 `twilight/chatlog/tool_result`，其 `SourceDigest` 等于该 digest |
| tool call 参数 | `ToolCallBinding.Arguments` | fact 本身（执行不得依赖 chatlog 解码） |

companion 与 Attach 事件与 Run 事实一起经 Module Framework 的 admission（EXT-APP-1）：它们可以携带 `ReferencePart`，其 Binding 在同一事务建立 claim。`FrozenValueStore` 是内容寻址存储：`Put(digest, bytes)` 幂等，`Get(digest)`。请求本体的有效期是该 ModelStep 从 Prepared 到终结；step 终结后 adapter 可按保留策略删除或归档，Record 校验不依赖本体。

下列 identity 稳定派生并由 Commit 验证：

| identity | preimage |
|---|---|
| PrepareModelRequest CommandID | RunID、loaded `RunPosition`（该 RunID 最后一条事件的 (revision, index)） |
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
| model recovery CommandID | RunID、StepID、Claim |
| tool recovery CommandID（RecoverExpired 的 Unknown） | RunID、StepID、CallID、Claim |

同一派生 identity 的内容变化在 Session kernel 表现为 `CommitConflict`（同 CommitID、不同 event group）。`PlanningToken` 是 Application-owned opaque freshness token，属于 prepare command identity 内容；Run 不校验它的语义（RUN-CMT-4）。

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
    Created session.EventPosition
    Snapshot RuntimeSnapshot
    Events []session.SessionEvent // 该 RunID 的全部 twilight/run/ 事件，按 stream 顺序
}
```

**RUN-NEW-1** `twilight/run/created` 是 Run 的第一个事实。v1 初始状态恰为：相同 RunID、Owner、Attempt、`RunActive`、`Current=Open`、无 pending input、零 model step、零 usage、无 result。初始输入随后以 `twilight/run/input_accepted` 进入同一 commit（TRN-STR-2）。`Protocol.BuildCreateGroup(NewRun, []AgentInput)` 返回 `created` 与 `input_accepted` 的 facts，编码为 Session event 由 `agent/session/run` 完成，Coordinator 不自行编码。同一 RunID 第二条 `created` 为 Evolve 错误。

**RUN-NEW-2** `FoldRun(events)` 按 stream 顺序折叠该 RunID 的完整事件序列，第一条必须是 `created`，并按其 `SchemaVersion` 绑定 `Protocol`。Fold 过程执行纯状态重建。import、诊断与 `Runtime.Record` integrity verification 都经 FoldRun；snapshot 通过 FoldRun 结果校验。

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

`RunEnded.End` 必须恰好是上述三个 variant 之一；`RunStoppedEnd.Reason` 必须非空，`RunFailedEnd.Reason` 必须是失败原因，`RunFailedEnd.Failure.Class` 必须非空。`RunEnded` 是 terminal commit 中最后一个 `twilight/run/` 事实。RunStatus、RunResult 等读取模型从该 union 派生。v1 wire 是 tagged union：`{"completed":{}}`、`{"stopped":{reason, uncertainCalls?, uncertainModel?}}` 或 `{"failed":{reason, failure}}`，恰有一个 variant key；codec 拒绝零个或多个 variant、缺失字段与多余字段。Cancel 时仍 Executing 的 tool call 与 model step 必须写入 `RunStoppedEnd` 并投影到 `RunResult`。

```text
ModelStep: Prepared -> Executing -> Completed
             |           |             |
             |           +-> Recovered-+  (回到同一 frozen request 的 Prepared)
             |           +-> Rejected     (retry 回到 Prepared，或同 commit 失败 Run)
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

**RUN-MCH-2** `ToolCallBinding` 冻结 CallID、ProviderCallID、ToolRef、definition digest、canonical arguments、response policy 与 binding digest。`CallID` 由 Run 派生（`DeriveCallID(source, index)`），是 Run 内的持久化 identity，进入 fact、lease key、派生 CommandID 与 chatlog；`ProviderCallID` 是模型发出的 `tool_call_id`，只用于 Planner 回传工具结果时与模型配对，Run 不以它为键，也不要求它唯一或非空。Decide 校验每个 binding 的 CallID 等于派生值、ProviderCallID 等于模型结果中对应位置的 id。已知工具使用匹配 frozen ToolSpec 的 ref/digest/policy；未知工具保留为同名 unresolved DirectExecution binding，并在执行前收束为已知 lookup failure。approval/external response 的 `ResponseRequest` 由 Decide 稳定派生。Unknown outcome 使用 class `effect_unknown`，只把该 Executing call 记为 `ToolCallFailed(Unknown)`。Run 保持 Active；同 step 其他 call 继续。全部 call 进入 Completed 或 Failed 后 Evolve 关闭 ToolStep。

`AgentCommand` 与 `Fact` 都是 sealed interface。v1 的 command→fact 规则为：

| command | precondition / facts |
|---|---|
| `AcceptInput` | 任意非终态；`InputAccepted`，追加到 `PendingInputs`。同一 InputID 重复接受为错误 |
| `PrepareModelRequest` | `Open`，完整有序消费 PendingInputs，request/tools digests 有效；`ModelStepPrepared`。command 携带请求本体，fact 只留 digest，本体由 Runtime 写入 FrozenValueStore |
| `WithdrawPreparedStep` | Model Prepared 且 `PendingInputs` 非空；`ModelStepWithdrawn`，`Current` 回到 `Open`，该请求本体可释放 |
| `StartModelExecution` | Model Prepared；`ModelStepStarted`。command 必须携带本次 start 的 `ExecutionClaim` |
| `RecoverModelExecution` | Model Executing；`ModelStepRecovered`。恢复 durable attempt 时携带该 attempt 的 `Claim` |
| `SubmitModelResult` | Model Executing；`ModelStepCompleted{Usage, FinishReason, ResultDigest}`。有 calls 时随后 `ToolStepOpened`（携带冻结的 `Scheduling` 与 bindings）；无 calls 且 `PendingInputs` 为空时随后 `RunEnded(completed)`；无 calls 且 `PendingInputs` 非空时 `Current` 回到 `Open`，Run 继续。command 携带冻结 `ModelResult` 本体，companion 写 `twilight/chatlog/assistant` |
| `SubmitModelFailure` | Model Executing；`RunEnded(failed/provider_failure)` |
| `RejectModelResult` | Model Executing；`ModelStepRejected`，由调用方显式选择回到 Prepared 或在同一 commit 追加 `RunEnded(failed/malformed_model_result)` |
| `StartToolCall` | Tool Pending；`ToolCallStarted`。command 必须携带本次 start 的 `ExecutionClaim` |
| `SubmitToolResult` | Tool Executing；`ToolCallCompleted{OutputDigest}`。command 携带输出本体，companion 写 `tool_result`。Evolve 后若全部 call 已 terminal，则关闭 ToolStep |
| `SubmitToolFailure(Known)` | Tool Pending/Executing；`ToolCallFailed(Known)`。Evolve 后若全部 call 已 terminal，则关闭 ToolStep |
| `SubmitToolFailure(Unknown)` | Tool Executing；`ToolCallFailed(Unknown)`。Evolve 后若全部 call 已 terminal，则关闭 ToolStep |
| `ApproveToolCall` | Waiting(Approval)；`ToolCallApproved` |
| `RejectToolCall` | Waiting(Approval) 记 `ToolCallFailed(Known/permission_denied)`；Waiting(ExternalResponse) 记 `ToolCallFailed(Known/response_rejected)`。Evolve 后若全部 call 已 terminal，则关闭 ToolStep |
| `SubmitToolResponse` | Waiting(ExternalResponse)；`ToolCallAnswered{ResponseDigest}`。Evolve 后若全部 call 已 terminal，则关闭 ToolStep |
| `CancelRun` | active；先把仍 Executing 的 tool call 记 `ToolCallFailed(Unknown)`，随后 `RunEnded(stopped/cancelled)`，并在 `RunStoppedEnd` / `RunResult` 上列出 `UncertainCalls` 与 `UncertainModel`。Waiting call 无论有无 Executing sibling 都不记 Failed；`RunEnded` 把 `Current` 置空。若这批 Unknown 使全部 call 进入终态，折叠会走 ToolStep 关闭路径并写入 `LastToolStep`；仍有 Waiting 或 Pending 时不走关闭路径，`LastToolStep` 保持原值。 |

没有独立的 `ToolStepClosed` fact。最后一个 ToolCall 进入 Completed 或 Failed 时，`Evolve` 在折叠该 fact 后若全部 call 已 terminal，则把 Current 设为 `Open` 并写入 `LastToolStep`；下一次 `PlanningHint.SourceStep` 取自 `LastToolStep.RefValue.ID`。Cancel 的 Unknown fact 同样走这条关闭规则；`RunEnded` 再把 Current 置空。

**RUN-MCH-3** `Protocol.Decide(state, command)` 执行全部验证与 derived consequence，一次返回该 commit 的完整 ordered fact group；验证成功后返回完整 facts。`Protocol.Evolve(state, fact)` 机械折叠 fact，依赖 fact 携带的完整执行状态数据。accepted facts 必须 self-contained；若 commit terminalize，`RunEnded` 必须是 Decide 输出的最后一个 fact。

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

一次执行 attempt 的全部 command identity 都从其 `Claim` 派生：start、owner settlement、model recovery、tool recovery 的 CommandID 分别按上表计算，Commit 对 start 强制校验该派生。因此 Loop 只需保留 `Claim` 一个值（`loop.ClaimStore`）：提交响应丢失时，以同一 Claim 重放得到同一 CommandID，Runtime 对精确重放返回原 `ExecutionGrant`；settlement 重放同理。Claim 存在进程内时，恢复只覆盖响应丢失；宿主注入 durable ClaimStore 时，替代进程可以直接重放前一进程已开始 attempt 的 start 并完成 settlement，不必等 lease 过期。Loop 已丢失 Claim 时，由 Runtime 的 recovery authority（lease 过期）处理该 execution。

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

**RUN-MCH-4** Effect 由调用方每次 Load 后重新派生。`AcceptInput` 在任意非终态入队，Decide 不因 Run 正在执行而拒绝它；`PendingInputs` 只在 `Open` 的 Prepare 中被消费。`PrepareModelRequest.InputIDs` 必须与当前 PendingInputs 等长、同顺序、逐项相同；prepare 接受后一次消费全部 pending input。ToolStep 的 Waiting call 禁止 Start，同一 step 中的 Pending call 仍可执行。没有可执行 Start 时 `Next` 返回 `Idle`。Application 从投影读取 `WaitingCalls` 并提交 `ApproveToolCall` / `RejectToolCall` / `SubmitToolResponse`。执行恢复由 Runtime 的 recovery authority 根据 `NeedsRecovery` 提供。

## 5. Runtime、投影与 Commit

```go
type Runtime interface {
    Load(context.Context, session.SessionID, RunID) (RuntimeSnapshot, error)
    Commit(context.Context, session.SessionID, CommitRequest) (CommitResult, error)
    Record(context.Context, session.SessionID, RunID) (RunRecord, error)
    FrozenRequest(context.Context, Digest) (ModelRequest, error)
    RenewLease(ctx context.Context, sessionID session.SessionID, runID RunID, stepID StepID, callID CallID, grant ExecutionGrant) error
    RecoverExpired(context.Context) (int, error)
}
// RunPosition 是该 RunID 最后一条 twilight/run/ 事件在 stream 中的位置；
// 只有这个 Run 自己的事件会移动它。
type RunPosition struct { Revision es.Revision; Index uint16 }
type RuntimeSnapshot struct {
    State MachineState // detached in-process view
    Position RunPosition
    Head session.Head  // 读取时的 Session head
    SchemaVersion uint16 // created.SchemaVersion
}

// ModuleEvent 是其他模块的 typed event，由 agent/session/run 经 Registry 编码；EventID 由 Appender 按位置赋值（EXT-APP-5）。
type ModuleEvent struct {
    Type session.EventType
    Value any
}
// Companion 把一个 commit 的 Run facts 与 command 携带的 transient 内容映射为
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
    Base RunPosition // Load 时的 Position；PrepareModelRequest 为 hard CAS
    Grant ExecutionGrant
    Command CommandEnvelope
    Attach []ModuleEvent // 调用方附加事件，追加在 companion 之后；例如 Coordinator.Stop 的 twilight/turn/failed
}
type CommitResult struct {
    Status CommitStatus // CommitAccepted | CommitAlreadyApplied
    Snapshot RuntimeSnapshot
    Commit session.SessionCommit // 完整 commit：run facts、companion、Attach
    Grant ExecutionGrant
}
```

**RUN-CMT-1** Runtime 按 `(SessionID, RunID)` 寻址。Run 由 Coordinator 的 Start commit 创建（TRN-STR-2），Runtime 没有 `Create`。缺失 Run 的 Load、Commit、Record 返回 `ErrRunNotFound`。

**RUN-CMT-2** 投影 `twilight/run/machine` 消费全部 `twilight/run/` 事件，忽略其他模块事件（EXT-PRJ-2），`RequireComplete` 为 `run`，状态为：

```go
type MachineProjection struct {
    Active map[RunID]MachineState   // 非终态 Run
    Positions map[RunID]RunPosition // 非终态 Run 的最后事件位置
}
```

终态 Run 在 `RunEnded` 折叠后从投影中移除；终态结果由 `Record` 与 turn surface 提供，投影大小与活动 Run 数成正比。snapshot 是可丢弃缓存（SES-SNP-1）：`Load` 经 `extension.ProjectionReader`（EXT-PRJ-4）读取，即 snapshot（若存在且 `Through` 是当前前缀）加其后类型前缀为 `twilight/run/` 的 tail；没有 snapshot 时从 stream 的过滤 replay 全量 fold。写入策略由 `agent/session/run` 的 `SnapshotPolicy` 决定，默认在 Run 的 `Current` 回到 `Open` 或 Run 终结时写入，并可按 commit 计数补充；kernel 不要求每次 commit 都写。`Record` 以 `Types=[twilight/run/]` 过滤 replay 读取该 RunID 的全部事件（SES-REP-2），FoldRun 重建；该 Run 仍在投影中时与投影状态比对，corrupt、gap 或 divergence 必须失败。

**RUN-CMT-3** Commit 经 `extension.SemanticAppender.AppendSemanticIn` 在 Session Store 的一个事务内完成（SES-API-2、EXT-APP-3）。所有 Runtime implementation 在 fn 内调用同一个 pure `EvaluateCommit`，顺序固定为：

```text
AppendSemanticIn(sessionID, func(tx):
  1  validate envelope SessionID/RunID/schema/type/digest（digest 不匹配为不可重试错误）
  2  tx.LookupCommit(CommitID = CommandID)
  3  found -> fn 返回 nil（Appender 记 Noop）；Runtime 以查到的 commit 构造 CommitAlreadyApplied，
     start 精确重放时 LookupLease，Token 仍 live 则返回原 grant
  4  derived CommandID check
  5  state = fold(tx.LoadSnapshot(twilight/run/machine) + tx.Tail(after, [twilight/run/]))
     缺少 created -> ErrRunNotFound；schema 不等于 created.SchemaVersion -> 不可重试错误；terminal check
  6  validate hard CAS（prepare 的 Base == Positions[RunID]）/ target state / execution grant / recovery authority
     grant 经 LookupLease 校验 Token；recovery authority 要求条目 deadline 已过且 command Claim 等于 Holder
  7  facts = Protocol.Decide(state, command) exactly once
  8  Protocol.Evolve in order；facts -> ModuleEvent（Type twilight/run/<name>，v = SchemaVersion）
  9  companion = Companion.Map(...)；校验 SourceDigest（TRN-MAP-3）；追加 request.Attach（不得为 twilight/run/ 事件）
  10 start -> AcquireLease；settlement / recovery -> ReleaseLease；按 SnapshotPolicy tx.SaveSnapshot
  11 return SemanticGroup{CommitID: CommandID, Events: run ++ companion ++ attach}
)
// Appender 在同一事务内完成 codec、Binding admission、claim 写入与 append。
```

FrozenValueStore 的 `Put` 幂等且内容寻址，在进入事务之前完成；事务失败时留下的本体无害，可由保留策略回收。

**RUN-CMT-4** `PrepareModelRequest` 是 hard-CAS command：`Base` 必须等于 section 内投影记录的该 Run 的 `Position`。这是有意选择：同一 Session 内其他模块的写入（用户提交新输入、summary、checkpoint、其他 Turn 的事件）不移动 Position，因此不使 Prepare 失效；Plan 与 Prepare 之间发生的 chatlog 写入不会被本次请求包含，新鲜度由 Application 经 `PlanningToken` 与 Planner 自行负责，Run 不校验 `PlanningToken` 的语义。其他 command 通过当前 target state 和 grant 做 call-local rebase；stale Base 本身不阻止无冲突的 ingress/control/settlement。相同 command 的 replay 判定先于 terminal check，因此 terminal Run 仍能返回原 commit。

**RUN-CMT-5** 幂等键为 Session kernel 的 `(SessionID, CommitID)`，CommitID 等于 CommandID。同 CommandID 的精确重放返回 `CommitAlreadyApplied`、当前 snapshot 与原完整 commit，且不得再次 Decide 或产生外部 effect。对于 `StartModelExecution` 和 `StartToolCall`，Runtime 还必须验证 command 中的 `ExecutionClaim`：相同 CommandID、相同 digest、相同 claim 的精确重放在 grant 仍 live 时返回原 start grant；不同 claim 触发 `ErrCommandConflict`，并保持现有执行授权。非 start command 的 replay 不返回 grant。

**RUN-CMT-6** accepted `StartModelExecution`/`StartToolCall` 为目标签发新 grant；该 start 的 `CommitAccepted` 和在 grant 仍 live 时满足精确 replay 条件的 `CommitAlreadyApplied` 返回同一个 grant。若该 start 已 settlement 或 Run 已 terminal，精确 replay 仍返回 `CommitAlreadyApplied`，并返回空 grant。model result/failure/reject 与 executing tool result/known failure 必须携带 live target grant。settlement 接受后 grant 失效；terminal commit 撤销该 Run 全部 grant。`RecoverModelExecution` 由 live grant holder 提交，或在 Runtime 验证 lease 已过期且 command Claim 等于该 lease 的 Claim 后无 grant 提交。Executing tool 的 recovery 使用同一条 `SubmitToolFailure{Outcome:Unknown}` command：工具 owner 必须携带 live grant；`RecoverExpired` 仅在 lease 已过期且没有已接受 settlement 时无 grant 提交。该 Unknown 只结算这一 call，Run 保持 Active。

**RUN-CMT-7** commit、lease 变更、claim 与（若写入）snapshot 在同一 Session Store 事务内生效：lease 经 `extension.AcquireLease` / `ReleaseLease` 在 `SemanticTx` 内写入，claim 由 Appender 写入，三者与 commit 同时可见或同时不可见。`CommitResult`、Load 与 Record 返回 detached values。预期拒绝映射为 `ErrCommandConflict`、`ErrStaleRuntime`、`ErrRunTerminal`；transport/storage failure 保持可判别且不得伪装为 rejection。

**RUN-CMT-8** 每个 Run 的协议版本是 `created.SchemaVersion`，创建时冻结。`RuntimeSnapshot.SchemaVersion` 等于该值；`ProtocolFor(schemaVersion)` 返回绑定该版本 digest/codec/Decide/Evolve 的 `Protocol`。`EvaluateCommit` 接受 command 当且仅当 `CommandEnvelope.SchemaVersion` 等于该 Run 的版本。新 Run 由 `NewRun.SchemaVersion` 决定版本；同一 Session 内不同 Run 可以使用不同版本；v1 Run 的 replay 必须继续使用 `ProtocolV1()`。Run 的版本与 Session kernel 的 `ProtocolVersion` 无关（SES-VER-1）。

### 5.1 控制面

grant、lease、ExecutionClaim、ClaimStore 与投影 snapshot 都不进入 stream。占用与续期使用 Module Framework 的 `extension.Lease`（EXT-LSE），它建立在 Session 控制面 KV 上，与 commit 同事务；durable claim 直接使用控制面 KV；snapshot 使用 Session snapshot（SES-SNP）。Run 对这些设施的映射为：

| Run 概念 | Lease 字段 |
|---|---|
| `ExecutionGrant` | `Token`，由 Acquire 所在的 start CommitID 派生，因此 start 的精确重放得到同一 grant |
| `ExecutionClaim` | `Holder` |
| target | `Namespace = twilight/run/lease`，`Key = <RunID>/model/<StepID>` 或 `<RunID>/call/<StepID>/<CallID>`；三段都是定长 hex digest，恢复时由 Key 解析 target，`Attrs` 为空 |
| `LeaseTTL` | `TTL`；零表示不超时（进程内占用） |

一个 target 至多一条 live lease。start 在提交 `ModelStepStarted` / `ToolCallStarted` 的事务内 `AcquireLease`；settlement 与 recovery 在提交对应 fact 的事务内 `ReleaseLease`。因此"日志中该 target 为 Executing"与"存在其 lease"同时成立或同时不成立。过期且无 settlement 时 Runtime 允许 grantless Recover：recovery authority 的判定为条目 deadline 已过、且 command 的 Claim 等于条目的 Holder。durable `loop.ClaimStore` 使用控制面 KV namespace `twilight/run/claim`。

`RecoverExpired` 以 `Leases.Expired(twilight/run/lease, now)` 枚举过期条目，由 Key 解析 target：Executing tool call 无 grant 提交 `SubmitToolFailure{Unknown}`，Executing model 提交 `RecoverModelExecution`；command 的 Claim 取自 `Holder`，因此 recovery CommandID 确定，重复扫描幂等。该 Run 保持 Active，同一 RunID 继续。进程内宿主使用 Memory 实现，lease 不超时，grantless recover 被拒绝。生产崩溃恢复使用带 TTL 的 Runtime。

**RUN-CMT-9** lease 续期。`Runtime.RenewLease` 调用 `Leases.Renew`（EXT-LSE-3）：Token 等于该 target 当前 lease 的 Token 时把 deadline 推后一个 `LeaseTTL`；条目不存在、Token 不匹配或条目已被 Release 时返回 `ErrStaleRuntime`。续期在临界区之外，由 kernel 的条件写保证不会把已删除的 lease 写回。持有 grant 的 worker 在效果执行期间必须以远小于 `LeaseTTL` 的间隔续期（Loop 的 `ExecutionPolicy.LeaseRenewInterval`）；续期返回 `ErrStaleRuntime` 表示该 target 已被 recovery 接管，worker 必须停止执行并放弃 settlement。`LeaseTTL` 是恢复延迟上界。`LeaseTTL` 为零时 `RenewLease` 只验证 grant，不改变 deadline。

持久结构与一致性等级的总表见 [agent-runtime-refactor.md](agent-runtime-refactor.md) 第 7 节。

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
    LeaseRenewInterval time.Duration
    Claims ClaimStore // nil 为进程内存储
    OnMalformedModelResult func(run.ModelStep, run.StepFailure) run.ModelRejectDisposition
}
type ClaimStore interface {
    Put(ctx, session.SessionID, run.RunID, run.StepID, run.CallID, run.ExecutionClaim) error
    Get(ctx, session.SessionID, run.RunID, run.StepID, run.CallID) (run.ExecutionClaim, bool, error)
    Delete(ctx, session.SessionID, run.RunID, run.StepID, run.CallID) error
    DeleteRun(ctx, session.SessionID, run.RunID) error
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

**RUN-LOP-1** `ExecutionPolicy` 是 Loop 的本地执行策略。`ToolExecution` 与 `MaxParallel` 在 `SubmitModelResult` 时写入 `ToolStepOpened.Scheduling` 并冻结在该 ToolStep 上；后续 Loop 必须按冻结值调度，不得改用当时进程的 ExecutionPolicy。未指定 `ToolExecution` 时冻结为 `parallel`，`MaxParallel` 零值表示当前 Start 批次全部 Pending call 可并行。空 Mode 按 parallel 解释，不得在 normalize 时填入默认字符串。nil handler 时结构错误的模型结果选择 `ModelRejectFailRun`；重试由 handler 明确返回 `ModelRejectRetry`。`streaming` 表示是否请求可用的流式模型端口；两种模式都产生同一完整 `sdk.ModelResult`。`LeaseRenewInterval` 是 worker 续期间隔（RUN-CMT-9）：模型与工具 worker 在效果执行期间按该间隔调用 `Runtime.RenewLease`，续期被拒时取消该 worker 的 ctx；零值关闭续期，只对 lease 不超时的 Runtime 正确。

**RUN-LOP-7** `ModelRef` 是冻结请求中的执行身份。`ModelCatalog.ResolveModel` 在同一 Run 生命周期内必须把同一 `ModelRef` 解析为等价的执行语义。provider 绑定不进入 frozen request，因此 Catalog 不得把同一 ref 改绑到不同实现。

`LoopResult` 的语义固定为：`LoopWaiting` 时 `Result` 为 nil，表示没有可执行 effect、Run 仍为 active。`ExecutionRecovery` 等于 `NeedsRecovery(state)`。该值为 true 时由 recovery authority 唤醒，`Reason` 为 `execution_recovery`；否则 `Reason` 为空。Waiting call 不进入 `LoopResult`；Application 通过投影的 `WaitingCalls` 读取。`LoopFinished` 时 `Result` 非 nil，并等于 terminal Run 的 `RunResult`。

`RequestPlanner` 从 `PlanningHint` 接收 Run 边界事实；它从 Session 的 chatlog fold 读取对话内容（上一步的 assistant 与 tool_result 已随 Run fact 同 commit 提交），并使用自己注入的 memory、attachments 与 product policy 组装 `sdk.Request`。Runtime 验证并冻结 planner 返回的 request，Planner 管理 application context。

## 7. Loop execution

```text
Loop.Run(ctx, runtime, sessionID, runID, sink):
  repeat:
    snapshot = Runtime.Load(sessionID, runID)
    if terminal: emit observational run_finished; return Finished(snapshot.Result)
    if resumeSettlement: continue   // 重放可能已提交、响应丢失的 settlement
    if resumeCachedStart: continue  // 重放本进程已接受、尚未结算的 start
    effect = run.Next(snapshot.State)
    dispatch effect
```

每个 `Loop` 实例为每个 `(SessionID, RunID)` 分配一个本地 driver slot。同一实例对同一 Run 的并发 `Run` 调用返回 `ErrRunAlreadyRunning`；不同 Run 可以并行驱动。

**RUN-LOP-2** `NeedModelRequest` 调用 Planner，冻结 sdk.Request，验证 model、ordered InputIDs 与 ToolSpecs，计算 request/tools/binding digests 和 derived CommandID/StepID，再提交 Prepare（command 携带本体）。prepare stale 后重新 Load；同 Position 的内容拒绝不得 livelock 重试。业务停止统一使用 `CancelRun`。

**RUN-LOP-8** `WithdrawPrepared` 时 Loop 提交 `WithdrawPreparedStep{StepID}`，随后重新 Load；被放弃请求的本体在 FrozenValueStore 中可立即释放。Loop 不为输入做任何其他事：Executing 与 ToolStep 期间到达的输入留在 `PendingInputs`，由随后 `Open` 的 `NeedModelRequest` 经 `PlanningHint.Inputs` 交给 Planner。

**RUN-LOP-3** `StartModelCall` 先 Commit start barrier；首次 `CommitAccepted` 或使用同一 command ID、同一 `ExecutionClaim` 精确重放得到原 grant 的 Loop，才拥有该 execution。Loop 必须在 `ClaimStore` 中保留该 attempt 的 claim 直到完成 settlement，其余 identity 按需派生；缺少 grant 的 replay 进入 reload 流程。调用使用 `Runtime.FrozenRequest(snapshot.State.Current.RequestDigest)` 取回的本体的 detached SDK materialization；本体缺失为不可重试错误，交由 Application 处理。streaming 与 non-streaming 必须产生同一种完整 `sdk.ModelResult`；delta 只发 EventSink。`ModelCatalog.ResolveModel` 失败或返回 nil 时提交 `RecoverModelExecution` 并返回错误，不得把 Run 记为 `provider_failure`：尚未发生模型调用。provider 调用失败提交 `SubmitModelFailure`；ctx cancellation 提交 `RecoverModelExecution`；结构、binding 或 freeze 失败提交 `RejectModelResult`，并由调用方显式选择 retry 或 fail-run。成功结果只提交一次 `SubmitModelResult`。

**RUN-LOP-4** Tool execution 先按 frozen binding resolve tool，并验证 Ref、definition digest、response policy 和 arguments。lookup/definition/argument failure 在 Pending 状态提交 `SubmitToolFailure(Known)`，不得跨越 start barrier。通过验证后逐 call 提交 `StartToolCall`；只有 Accepted owner 可执行。冻结的 `ToolStep.Scheduling` 决定 `parallel` 或 `sequential` 以及 `MaxParallel`；不得改用 Loop 进程当前的 ExecutionPolicy。每个结果以自己的 grant 提交。同一 ToolStep 中 DirectExecution 的 Pending call，在外层 ctx 未取消时于本次 `Run` 内按冻结 Scheduling 分批 Start 并结算；ctx 已取消时停止再 Start，只结算已持有 grant 的 call。`Next` 返回 `Idle` 时 Loop 返回 `LoopWaiting`，并用 `NeedsRecovery(state)` 设置 `ExecutionRecovery`。Loop 不解释 Waiting call，也不携带 `ResponseRequest`。Application 从投影读取 `WaitingCalls`，提交 `ApproveToolCall` / `RejectToolCall` / `SubmitToolResponse` 之后再次 `Run`。tool panic 或 effect 状态无法确定的错误转为对该 call 的 Unknown，并提交 `SubmitToolFailure(Unknown)`。该 settlement 不取消同批 sibling workers，也不结束 Run。`CancelRun` 先把仍 Executing 的 call 记为 `ToolCallFailed(Unknown)`，再 `RunEnded(stopped/cancelled)`，并把这些 CallID 与仍 Executing 的 ModelStep 写入 `RunStoppedEnd` / `RunResult` 的 `UncertainCalls`、`UncertainModel`。Waiting call 无论有无 Executing sibling 都不记 Failed。已接受 start 的 worker 必须在收到外层取消后返回并尝试 settlement；settlement 使用独立 control context。lookup/definition/argument failure 只允许发生在 Pending；Executing 且本进程持有 start cache 时只重放 start 并结算，不得再提交 grantless Known。

**RUN-LOP-5** model 与 tool worker 都接收外层 ctx；Loop 对已接受 effect 使用独立 control context 完成 known/unknown outcome settlement。Application 的业务停止顺序为先 Commit `CancelRun`，再取消 Loop ctx。非 sentinel Commit error 以同 CommandID/digest 重放一次；仍未知时返回错误，由后续 Load/Record 查询 authority。stale/terminal/conflict 触发 reload/drop，旧 external effect 保持单次执行尝试。工具实现配合 context 返回；永久阻塞由 application/durable recovery 处理。

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
    Committed *session.SessionCommit
}
```

`Sequence` 仅用于同一临时观察流内的顺序（例如 ToolProgress），从 1 开始；committed observation 的权威顺序由 Session `(Revision, Index)` 表达，未提供临时序号时保持 0。

**RUN-LOP-6** EventSink 提供 realtime observation，Loop 通过序列化调用向 sink 发送事件。`EventAgentCommitted` 携带 accepted commit；text/reasoning delta、tool progress、tool lifecycle 与 run-finished observation 可丢失、重复或断流。sink failure 保持 Commit 结果；恢复与审计读取 Session stream，EventSink gap 通过 stream 对账。

`AcceptInput` 在任意非终态提交，`PendingInputs` 就是回合中途输入的队列；Loop 不解释 queue 或 steer：`Open` 时立刻 `NeedModelRequest`，Prepare 一次消费全部 pending input。Application 负责 admission；Turn 创建、attempt、中途投递、结算与 companion 映射由 [agent-turn.md](agent-turn.md) 定义。

## 9. compatibility 与 conformance

**RUN-CMP-1** 当前 pre-release schema v1 的 command/fact discriminator、wire fields、canonical digest、derived ID 和 `ProtocolV1().Evolve` 由 golden fixtures 保护；发布前有意修改协议时必须同步更新 fixture。v1 发布后，新增 variant、字段或折叠语义必须进入新 `SchemaVersion`，Registry 继续 decode/fold 全部已发布版本；同一 Run 的 writer 不得混写不同版本。Run 版本演进不触发 Session kernel 版本变化。

**RUN-CMP-2** Runtime conformance 必须覆盖：

- Start group 建立 Run、重复 `created` 拒绝、missing Run、schema 与 created 不一致的 command 拒绝；
- command exact replay/conflict、prepare hard CAS、call-local rebase、terminal replay；
- 输入入队：`AcceptInput` 在 Open、Model Prepared、Model Executing、ToolStep（含 Waiting）都被接受；Prepared 期间入队后 `Next` 返回 `WithdrawPrepared`，Withdraw 后重规划的 Prepare 包含该输入；Executing 期间入队的输入在该步结算后的 Prepare 中被消费；无 tool call 且有 pending 输入的 `SubmitModelResult` 不结束 Run；
- grant 签发、隔离、精确 start replay、消费、跨 Run 拒绝与 recovery authorization；
- 一 command 一 commit、run facts 在 companion 与 Attach 之前、companion 的 `SourceDigest` 等于 fact 记录的 ResultDigest / OutputDigest、Attach 拒绝 `twilight/run/` 事件、companion 与 Attach 经 admission 并建立 claim；
- Prepare hard CAS 只对该 Run 自己的事件敏感：同一 Session 内其他模块的写入不使 Prepare 失效；
- commit、lease、claim，以及该 commit 若写入的 snapshot 同事务：在任一写入点注入崩溃后它们同时存在或同时缺失；
- 投影 snapshot 加 tail 与全量 fold 等价；删除 snapshot 后 Load 结果不变；终态 Run 不再出现在投影中；Record 单一一致点、FoldRun 等价、gap/tamper/corrupt failure；
- 同一 Session 内多 Run 隔离、不同 SchemaVersion 的 Run 共存；同一 Session 的 chatlog/turn 事件不影响 Run fold；
- lease 过期 recovery：live lease 拒绝 grantless、过期 model 回到 Prepared、过期 tool 记 Unknown 且 sibling 不受影响、RecoverExpired 幂等、恢复事务删除 lease 条目；
- lease 续期：续期后原 deadline 不触发 recovery、错误/空 grant 与 settlement 后续期被拒、续期与结算并发时条件写失败且不写回已删除条目；
- FrozenValueStore：Put 幂等、Recovered 后按 RequestDigest 取回同一请求、本体缺失的错误分类、step 终结后删除本体不影响 Record；
- MachineState codec：每个 Current variant 与终态 round-trip、拒绝 unknown field / 非法判别式 / trailing data。

Loop conformance 必须覆盖：

- 单模型完成、tool round trip、approval/external response wait/resume；
- known failure 继续、Unknown 继续、tool panic、aliased ToolRef 与 validation；
- parallel/sequential 按冻结 `ToolStep.Scheduling` 调度，不得改用当时 ExecutionPolicy；
- `ModelCatalog.ResolveModel` 失败或 nil 时恢复 ModelStep、Run 保持 active；
- ctx cancellation、model recovery 后重发同一 RequestDigest、explicit malformed-result disposition；
- Cancel 将 Executing tool/model 投影到 `UncertainCalls` / `UncertainModel`；ExternalResponse reject 为 `response_rejected`；
- streaming delta 与 nil result、EventSink committed observation；
- stale/unknown commit response、prepare no-progress rejection 与无 livelock；
- 超过 LeaseTTL 的工具调用在续期下不被记为 Unknown，其结果被接受；
- 共享 ClaimStore 的第二个 Loop 实例重放前一实例的 start 并完成 settlement，不等待 lease 过期。

package 迁移、实施阶段与未完成 adapter 工作记录在 [agent-runtime-refactor.md](agent-runtime-refactor.md)，本协议 authority 以本文为准。
