# Twilight Agent Run Protocol

状态：设计规范

本文定义 `agent/run` 与 `agent/run/loop`。文中的“必须”“不得”“应该”是协议约束；canonical JSON、JCS 与 domain-separated digest 使用 `agent/jsonstable` 和 `agent/es` 的通则。

## 1. 范围与 authority

```text
MachineState                    Run 的语义状态投影
RunHeader + TransitionRecord[]  canonical verified Run record
Runtime                         Run authority 与 atomic Commit boundary
loop.Loop                       当前进程的 execution interpreter
```

`MachineState` 决定 Run 当前可执行动作。每次接受的 command 产生一个完整 `TransitionRecord`，其 facts 经该 Run 的 `Protocol.Evolve` 从 `RunHeader.InitialState` 重放后必须得到同 revision 的 `MachineState`。

Run 的职责分成五个相互独立的层面：

```text
Agent Machine   = Run/Step 状态与合法转移（Decide、Evolve、Next）
Agent Loop      = Machine effect 的进程内解释器
Runtime         = 一个 Run 的 authority、并发校验与原子提交
Model / Tool    = 一次模型请求或一次工具调用的 effect 执行器
Request Planner = application context 到 sdk.Request 的投影器
```

Machine 处理已冻结的值和已提交的事实；Loop 解释 `Next` 产生的 transient effect；Runtime 保存并验证 Machine 的推进；Model/Tool 执行一次外部 effect；Request Planner 组装下一次模型请求。`Runtime` 叠在 `Store` 上：`MemoryStore` 使用进程内锁，SQLite 与 Postgres 使用数据库事务，它们保存同一份 authority。

`Step` 是 Run 的持久化恢复边界；`execution attempt` 表示某个 Loop 进程对该 Step 或 ToolCall 的一次易失执行。一个 Step 可以有多个 attempt，Machine 只接受带有效 grant 的 settlement。Attempt 的执行控制信息由 start command 的 `ExecutionClaim` 和 Runtime 返回的 opaque `ExecutionGrant` 表达。

每次 accepted command 产生的 `TransitionRecord.Events` 构成 canonical event plane：它与新的 MachineState 在同一 Runtime commit 中写入，按 revision/index 有序，可用于 replay、materialization 和审计。`EventSink` 转发这些已提交事件及临时 delta；canonical authority 由 Runtime.Record 提供。

**RUN-SCP-1** `agent/run` 拥有 Run identity、persisted frozen values、Machine、command/fact protocol、wire codec、fold、Runtime contract、Store 与 MemoryStore。`agent/run/loop` 拥有 planner/model/tool ports、streaming、并发执行、EventSink 与 Loop policy；它单向依赖 `agent/run`，根 package 不依赖 loop。

**RUN-SCP-2** Session 长期语义、Turn→Run 协调、Run→Session materialization、Artifact、queue/claim、provider registry、权限、durable lease/fence/outbox 与产品 policy 分别由其 package 或 Application 拥有。对应 authority 为 [agent-session.md](agent-session.md)、[agent-turn.md](agent-turn.md) 与 [agent-artifact.md](agent-artifact.md)。

## 2. identity、persisted values 与 wire

```go
type RunID string
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

**RUN-WIR-1** identity 必须非空、稳定且为有效 UTF-8。`ExecutionClaim` 由 Loop 为一次 start command 生成并在该 command 的重试中保持不变，用于绑定 start command 与执行尝试。`ExecutionGrant` 是 Runtime 签发的 opaque capability，交还给签发它的 Runtime 完成 settlement。两者服务于执行授权；Run 跨 domain causation 记录在 immutable `RunHeader.CausationID`。

Run 持久化协议保存 run-owned frozen values。模型请求、模型结果、消息、工具定义、usage、provider metadata 与所有动态 JSON 在进入 command/fact 前，分别经 `FreezeModelRequest`、`FreezeModelResult`、`FreezeToolDefinition`、`FreezeToolCallInput` 等入口转为纯数据和 immutable `CanonicalJSON`。Runtime 接收 agent-owned value；调用方负责在边界前完成冻结。

**RUN-WIR-2** 当前 pre-release schema v1 使用 RFC 8785/JCS canonical bytes。字段、omission、array order、type discriminator、digest preimage 与 `Protocol.Evolve` 语义在发布前继续演进，发布后永久冻结。command、fact 和 transition decoder 必须拒绝 unknown version/type、duplicate key、unknown field、trailing data、非法 UTF-8、非 canonical-equivalent wire 与 digest mismatch。精确 identity 和 digest 使用 JSON string；当前 v1 revision/index/schema fields 使用整数 wire shape。

```go
type CommandEnvelope struct {
    SchemaVersion uint16
    Type string
    RunID RunID
    ID CommandID
    Digest Digest
    Command AgentCommand
}
type AgentEvent struct {
    SchemaVersion uint16
    Type string
    RunID RunID
    Revision uint64
    Index uint16
    CommandID CommandID
    CommandDigest Digest
    Digest Digest
    Fact Fact
}
type TransitionRecord struct {
    SchemaVersion uint16
    RunID RunID
    Revision uint64
    CommandID CommandID
    CommandDigest Digest
    Events []AgentEvent
    TransitionDigest Digest
}
```

`CommandEnvelope.Digest` 覆盖 schema、type 与完整 command；RunID、CommandID、BaseRevision 和 grant 属于 envelope/commit metadata。`AgentEvent.Digest` 覆盖 schema、type 与完整 fact。`TransitionDigest` 覆盖自身以外的完整 transition，包括有序 event group。

**RUN-WIR-3** 一个 transition 的 event 数至少为 1；其 RunID、Revision、CommandID、CommandDigest 相同，Index 从 0 连续递增。revision 从 1 连续递增。构造 command 必须使用该 Run 的 `Protocol.BuildEnvelope`（Loop 通过 `RuntimeSnapshot.Protocol()` 取得）。`agent/run` 不提供隐式选择版本的包级 `BuildEnvelope`、`Decide`、`Evolve` 或 `Digest*` 函数；新 Run 与测试显式使用 `ProtocolV1()`。构造与验证 transition 必须使用 `BuildTransitionRecord`、`ValidateTransitionRecord`。所有公开返回值具有 detached snapshot 语义。

下列 identity 稳定派生并由 Commit 验证：

| identity | preimage |
|---|---|
| PrepareModelRequest CommandID | RunID、loaded BaseRevision |
| ModelStep StepID | RunID、prepare CommandID、model/request/tools binding digest |
| ToolStep StepID | source ModelStepID、ordered binding-set digest |
| ResponseID | RunID、ToolStepID、CallID、ResponseKind |
| response CommandID | RunID、StepID、CallID、ResponseID |
| input CommandID | RunID、InputID |
| start CommandID（StartModelExecution / StartToolCall） | RunID、StepID、CallID（model 为空）、Claim |
| owner settlement CommandID（model result/failure/reject、tool result/failure） | RunID、StepID、CallID、Claim |
| Pending Known failure CommandID | RunID、StepID、CallID、空 Claim |
| model recovery CommandID | RunID、StepID、Claim |
| tool recovery CommandID（RecoverExpired 的 Unknown） | RunID、StepID、CallID、Claim |

同一派生 identity 的内容变化通过 command digest 触发 conflict。`PlanningToken` 是 Application-owned opaque freshness token，属于 prepare command identity 内容；Planner 负责解释它的 freshness，`ModelStepPrepared` 保存冻结请求及其 digest。

## 3. 创建与 canonical record

```go
type NewRun struct {
    SchemaVersion uint16
    RunID RunID
    CausationID es.CausationID
}
type RunHeader struct {
    SchemaVersion uint16
    RunID RunID
    InitialStateVersion uint16
    InitialState MachineState
    InitialStateDigest Digest
    CausationID es.CausationID
    HeaderDigest Digest
}
type CreateResult struct { Header RunHeader; Created bool }
type RunRecord struct {
    Header RunHeader
    Snapshot RuntimeSnapshot
    Transitions []TransitionRecord
}
```

**RUN-NEW-1** `BuildNewRun` 建立当前 version 的 creation value；`BuildRunHeaderFromNewRun` 按 `NewRun.SchemaVersion` 的冻结规则建立 header。v1 Revision-0 state 恰为：相同 RunID、`RunActive`、`Current=Open`、无 pending input、零 model step、零 usage、无 result。初始输入必须随后通过 `AcceptInput` transition 进入 log。

Header 创建后 immutable。`InitialStateDigest` 覆盖 frozen initial state；`HeaderDigest` 覆盖 schema、RunID、initial-state version/digest 与 causation。`ValidateRunHeader` 验证这些约束。

**RUN-NEW-2** `FoldRun(header, transitions)` 先验证 header，再按 revision/index 折叠完整 transition sequence。每条 `TransitionRecord` 按其自身 `SchemaVersion` 绑定 `Protocol` 再 `Evolve`。合法 Run 的每条 record 的 SchemaVersion 必须等于 `header.SchemaVersion`（RUN-CMP-1）。Fold 过程执行纯状态重建。import、diagnostic 与 `Runtime.Record` integrity verification 从 header 开始；外部 snapshot 通过 FoldRun 结果校验。

## 4. Machine

```go
type Current interface{ current() }
type Open struct{}
func (Open) current() {}
func (ModelStep) current() {}
func (ToolStep) current() {}

type MachineState struct {
    RunID RunID
    Status RunStatus
    Current Current
    PendingInputs []AgentInput
    ModelSteps int
    LastToolStep *ToolStep
    Usage Usage
    LastModelResult *ModelResult
    Result *RunResult
}

type RunResult struct {
    Status  RunStatus
    Reason  RunReason
    Failure *RunFailure
    UncertainCalls []CallID
    UncertainModel StepID
    Model   *ModelResult
    Usage   Usage
}

type ModelStepStatus uint8 // Prepared | Executing
type ModelStep struct {
    RefValue StepRef
    Request ModelRequest
    RequestDigest Digest
    Model ModelRef
    Tools []ToolSpec
    ToolsDigest Digest
    Status ModelStepStatus
    Rejects int // 已接受的 ModelStepRejected 次数；不进入 RefValue.Digest
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

`Current` 是 Active 期间的内容。`Open` 是可进入区间：可提交 `AcceptInput` 与 `PrepareModelRequest`，`Next` 返回 `NeedModelRequest`。`ModelStep` 与 `ToolStep` 表示正在进行的步骤。终态的 `Current` 为空；终态由 `Status` 表达，不另设 Current variant。Active 的 `Current` 不得为空。`Step` 仍只有 `ModelStep` 与 `ToolStep`，提供 `Ref()`。

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

`RunEnded.End` 必须恰好是上述三个 variant 之一；`RunStoppedEnd.Reason` 必须非空，`RunFailedEnd.Reason` 必须是失败原因，`RunFailedEnd.Failure.Class` 必须非空。`RunEnded` 是 terminal transition 的最后一个 fact。RunStatus、RunResult 等读取模型从该 union 派生。v1 wire 是 tagged union：`{"completed":{}}`、`{"stopped":{reason, uncertainCalls?, uncertainModel?}}` 或 `{"failed":{reason, failure}}`，恰有一个 variant key；codec 拒绝零个或多个 variant、缺失字段与多余字段。Cancel 时仍 Executing 的 tool call 与 model step 必须写入 `RunStoppedEnd` 并投影到 `RunResult`，不得只留下 `stopped/cancelled`。

```text
ModelStep: Prepared -> Executing -> Completed
                         |             |
                         +-> Recovered-+  (回到同一 frozen request 的 Prepared)
                         +-> Rejected     (retry 回到 Prepared，或同 transition 失败 Run)

ToolCall:
  Pending -> Executing -> Completed
     |          |
     |          +-> Failed(Known|Unknown)
     +-> Failed(Known)
  Waiting(Approval)         -> Pending | Failed(Known)
  Waiting(ExternalResponse) -> Completed | Failed(Known)
```

**RUN-MCH-1** MachineState 保存 Run 的 execution semantics。`LastToolStep` 保存最近一个经 Evolve 关闭路径写下的 ToolStep 只读投影，必须与 transition log 折叠出的最后关闭 step 一致，供下一次 planner 构造模型请求。Cancel 经 `RunEnded` 把 `Current` 置空、不走关闭路径时不改写 `LastToolStep`。terminal state 吸收所有未幂等命令；`RunEnded` 建立唯一 terminal result。RunResult 在该 terminal transition 中建立，并由后续 snapshot/record 读取。

**RUN-MCH-2** `ToolCallBinding` 冻结 CallID、ToolRef、definition digest、canonical arguments、response policy 与 binding digest。已知工具使用匹配 frozen ToolSpec 的 ref/digest/policy；未知工具保留为同名 unresolved DirectExecution binding，并在执行前收束为已知 lookup failure。approval/external response 的 `ResponseRequest` 由 Decide 稳定派生。Unknown outcome 使用 class `effect_unknown`，只把该 Executing call 记为 `ToolCallFailed(Unknown)`。Run 保持 Active；同 step 其他 call 继续。全部 call 进入 Completed 或 Failed 后 Evolve 关闭 ToolStep。

`AgentCommand` 与 `Fact` 都是 sealed interface。v1 的 command→fact 规则为：

| command | precondition / facts |
|---|---|
| `AcceptInput` | `Open`；`InputAccepted` |
| `PrepareModelRequest` | `Open`，完整有序消费 PendingInputs，request/tools digests 有效；`ModelStepPrepared` |
| `StartModelExecution` | Model Prepared；`ModelStepStarted`。command 必须携带本次 start 的 `ExecutionClaim` |
| `RecoverModelExecution` | Model Executing；`ModelStepRecovered`。恢复 durable attempt 时携带该 attempt 的 `Claim` |
| `SubmitModelResult` | Model Executing；`ModelStepCompleted`，随后无 calls 时 `RunEnded(completed)`，有 calls 时 `ToolStepOpened`（携带冻结的 `Scheduling`） |
| `SubmitModelFailure` | Model Executing；`RunEnded(failed/provider_failure)` |
| `RejectModelResult` | Model Executing；`ModelStepRejected`，由调用方显式选择回到 Prepared 或在同一 transition 追加 `RunEnded(failed/malformed_model_result)` |
| `StartToolCall` | Tool Pending；`ToolCallStarted`。command 必须携带本次 start 的 `ExecutionClaim` |
| `SubmitToolResult` | Tool Executing；`ToolCallCompleted`。该 fact 经 Evolve 后若全部 call 已 terminal，则关闭 ToolStep |
| `SubmitToolFailure(Known)` | Tool Pending/Executing；`ToolCallFailed(Known)`。Evolve 后若全部 call 已 terminal，则关闭 ToolStep |
| `SubmitToolFailure(Unknown)` | Tool Executing；`ToolCallFailed(Unknown)`。Evolve 后若全部 call 已 terminal，则关闭 ToolStep |
| `ApproveToolCall` | Waiting(Approval)；`ToolCallApproved` |
| `RejectToolCall` | Waiting(Approval) 记 `ToolCallFailed(Known/permission_denied)`；Waiting(ExternalResponse) 记 `ToolCallFailed(Known/response_rejected)`。Evolve 后若全部 call 已 terminal，则关闭 ToolStep |
| `SubmitToolResponse` | Waiting(ExternalResponse)；`ToolCallAnswered`。Evolve 后若全部 call 已 terminal，则关闭 ToolStep |
| `CancelRun` | active；先把仍 Executing 的 tool call 记 `ToolCallFailed(Unknown)`，随后 `RunEnded(stopped/cancelled)`，并在 `RunStoppedEnd` / `RunResult` 上列出 `UncertainCalls` 与 `UncertainModel`。Waiting call 无论有无 Executing sibling 都不记 Failed；`RunEnded` 把 `Current` 置空。若这批 Unknown 使全部 call 进入终态，折叠会走 ToolStep 关闭路径并写入 `LastToolStep`；仍有 Waiting 或 Pending 时不走关闭路径，`LastToolStep` 保持原值。 |

没有独立的 `ToolStepClosed` fact。最后一个 ToolCall 进入 Completed 或 Failed 时，`Evolve` 在折叠该 fact 后若全部 call 已 terminal，则把 Current 设为 `Open` 并写入 `LastToolStep`；下一次 `PlanningHint.SourceStep` 取自 `LastToolStep.RefValue.ID`。Cancel 的 Unknown fact 同样走这条关闭规则；`RunEnded` 再把 Current 置空。

**RUN-MCH-3** `Protocol.Decide(state, command)` 执行全部验证与 derived consequence，一次返回该 transition 的完整 ordered fact group；验证成功后返回完整 facts。`Protocol.Evolve(state, fact)` 机械折叠 fact，依赖 fact 携带的完整数据。accepted facts 必须 self-contained；若 transition terminalize，`RunEnded` 必须是 Decide 输出的最后一个 fact。

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

一次执行 attempt 的全部 command identity 都从其 `Claim` 派生：start、owner settlement、model recovery、tool recovery 的 CommandID 分别按上表计算，Commit 对 start 强制校验该派生。因此 Loop 只需保留 `Claim` 一个值（`loop.ClaimStore`）：提交响应丢失时，以同一 Claim 重放得到同一 CommandID 与 digest，Runtime 对精确重放返回原 `ExecutionGrant`；settlement 重放同理。Claim 存在进程内时，恢复只覆盖响应丢失；宿主注入 durable ClaimStore 时，替代进程可以直接重放前一进程已开始 attempt 的 start 并完成 settlement，不必等 lease 过期。Loop 已丢失 Claim 时，由 Runtime 的 recovery authority（lease 过期）处理该 execution。

`Next(state)` 最多返回一个 transient `Effect`：

| state | effect |
|---|---|
| terminal | 返回 `ErrRunTerminal`，没有 effect |
| `Open` | `NeedModelRequest{PlanningHint}` |
| Model Prepared | `StartModelCall` |
| Model Executing | `Idle` |
| ToolStep 有 Pending calls | `StartToolCalls` |
| ToolStep 无 Pending、仍有 Waiting 或 Executing | `Idle` |

Waiting call 上的 `ResponseRequest` 由 `WaitingCalls(state)` 读取。Executing call 由 `ExecutingCalls(state)` 读取。`NeedsRecovery(state)` 在 Model Executing 或 ToolStep 无 Pending 且仍有 Executing 时为 true。这些查询不是 Effect。

**RUN-MCH-4** Effect 由调用方每次 Load 后重新派生。`PrepareModelRequest.InputIDs` 必须与当前 PendingInputs 等长、同顺序、逐项相同；prepare 接受后一次消费全部 pending input。ToolStep 的 Waiting call 禁止 Start，同一 step 中的 Pending call 仍可执行。没有可执行 Start 时 `Next` 返回 `Idle`。Application 从 snapshot 读取 `WaitingCalls` 并提交 `ApproveToolCall` / `RejectToolCall` / `SubmitToolResponse`。执行恢复由 Runtime/application 的 recovery authority 根据 `NeedsRecovery` 提供。

## 5. Runtime 与 Commit

```go
type Runtime interface {
    Create(context.Context, NewRun) (CreateResult, error)
    Load(context.Context, RunID) (RuntimeSnapshot, error)
    Commit(context.Context, CommitRequest) (CommitResult, error)
    Record(context.Context, RunID) (RunRecord, error)
    RenewLease(ctx context.Context, runID RunID, stepID StepID, callID CallID, grant ExecutionGrant) error
    RecoverExpired(context.Context) (int, error)
}
type RuntimeSnapshot struct {
    State MachineState // detached in-process view
    Revision uint64
    SchemaVersion uint16 // RunHeader.SchemaVersion
}
type Protocol struct {
    // ProtocolFor 一次绑定该 SchemaVersion 的函数。方法不再接受 version 参数。
    // 零值 Version()==0，不得调用。
}
func ProtocolFor(schemaVersion uint16) (Protocol, error)
func (RuntimeSnapshot) Protocol() (Protocol, error) // ProtocolFor(SchemaVersion)
func (Protocol) Version() uint16
func (Protocol) DigestRequest(ModelRequest) (Digest, error)
func (Protocol) DigestToolDefinition(ToolDefinition) (Digest, error)
func (Protocol) DigestToolSpec(ToolSpec) (Digest, error)
func (Protocol) DigestToolSpecs([]ToolSpec) (Digest, error)
func (Protocol) DigestModelStepBinding(ModelRef, Digest, Digest) (Digest, error)
func (Protocol) DigestToolResponseDecision(ResponseKind, ResponseDecision, string) (Digest, error)
func (Protocol) DigestToolResponsePayload(CanonicalJSON) (Digest, error)
func (Protocol) DigestCommand(typ string, command AgentCommand) (Digest, error)
func (Protocol) DigestFact(typ string, fact Fact) (Digest, error)
func (Protocol) EncodeFact(typ string, fact Fact) ([]byte, error)
func (Protocol) DecodeCommand(typ string, raw []byte) (AgentCommand, error)
func (Protocol) DecodeFact(typ string, raw []byte) (Fact, error)
func (Protocol) Decide(MachineState, AgentCommand) ([]Fact, error)
func (Protocol) Evolve(MachineState, Fact) (MachineState, error)
func (Protocol) BuildEnvelope(run RunID, id CommandID, cmd AgentCommand) (CommandEnvelope, error)
func (Protocol) EncodeMachineState(*MachineState) ([]byte, error) // 持久化 snapshot bytes，含 Current
func (Protocol) DecodeMachineState([]byte) (MachineState, error)
func (Protocol) ValidateHeader(*RunHeader) error
func ProtocolV1() Protocol // SchemaVersion1 绑定；新 Run 的创建版本。没有委托它的包级函数
type CommitRequest struct {
    BaseRevision uint64
    Grant ExecutionGrant
    Command CommandEnvelope
}
type CommitResult struct {
    Status CommitStatus // CommitAccepted | CommitAlreadyApplied
    Snapshot RuntimeSnapshot
    Events []AgentEvent
    Grant ExecutionGrant
}
```

**RUN-CMT-1** Runtime 是 RunID-addressed collection。`Create` 原子保存 immutable header 与 Revision-0 state；相同 canonical header 幂等返回 `Created=false`，同 RunID 的不同 header 返回 `ErrCreateConflict`。缺失 Run 的 Load、Commit、Record 返回 `ErrRunNotFound`。

**RUN-CMT-2** `Load` 返回当前 detached execution snapshot，并验证 `RunID`、revision 与 MachineState 的基本语义不变量；snapshot 与 revision 必须来自同一 authority 版本。`RuntimeSnapshot` 提供 Go 读取视图。`Protocol.EncodeMachineState` / `DecodeMachineState` 定义 MachineState 的持久化 snapshot wire（含 `Current` 判别式与 step body）；Store 以该 wire 保存每个 revision 的 snapshot，`Load` 直接读取 snapshot 而不折叠日志。canonical record 仍是 Header 与 TransitionRecord；snapshot 是派生数据，`Record` 与 `Rebuild` 通过 FoldRun 校验它。`Record` 在一个一致点读取 detached Header、Snapshot 和完整 TransitionRecord sequence，验证 header、每个 transition、连续 fold 与 snapshot 等价后返回；corrupt、gap 或 divergence 必须失败。普通消费者使用 Record 获取一致的完整记录。`FoldRun` 从 Header 和完整 transition sequence 重建状态；任何导入、诊断或 Record 校验先完成 FoldRun，再使用重建状态。

所有 Runtime implementation 在自己的 critical section/transaction 内调用同一个 pure `EvaluateCommit`。顺序固定为：

```text
1 validate envelope RunID/schema/type/digest（digest 不匹配为不可重试错误，不映射为 conflict）
2 lookup prior transition by CommandID
3 exact digest replay -> AlreadyApplied + original complete event group
4 same CommandID/different digest -> conflict
5 derived CommandID check (after replay)
6 terminal check
7 validate hard CAS / target state / execution grant / recovery authority
8 facts = Protocol.Decide(current, command) exactly once
9 snapshot each fact; Protocol.Evolve in order; assign next Revision and Index
10 build and validate one complete TransitionRecord
11 atomically persist transition and new MachineState
```

**RUN-CMT-3** `PrepareModelRequest` 是 hard-CAS command：BaseRevision 必须等于 current revision。其他 command 通过当前 target state 和 grant 做 call-local rebase；stale BaseRevision 本身不阻止无冲突的 ingress/control/settlement。相同 command 的 replay 判定先于 terminal check，因此 terminal Run 仍能返回原 transition。

**RUN-CMT-4** 幂等键为 `(RunID,CommandID)`。相同 digest 返回 `CommitAlreadyApplied`、当前 snapshot 与原完整 event group，且不得再次 Decide、分配 revision 或产生外部 effect。对于 `StartModelExecution` 和 `StartToolCall`，Runtime 还必须验证 command 中的 `ExecutionClaim`：相同 command ID、相同 digest、相同 claim 的精确重放在 grant 仍 live 时返回原 start grant；不同 claim 触发 `ErrCommandConflict`，并保持现有执行授权。非 start command 的 replay 不返回 grant。revision 每个 accepted command 恰加 1。

**RUN-CMT-5** accepted `StartModelExecution`/`StartToolCall` 为目标签发新 grant；该 start 的 `CommitAccepted` 和在 grant 仍 live 时满足精确 replay 条件的 `CommitAlreadyApplied` 返回同一个 grant。若该 start 已 settlement 或 Run 已 terminal，精确 replay 仍返回 `CommitAlreadyApplied`，并返回空 grant。model result/failure/reject 与 executing tool result/known failure 必须携带 live target grant。settlement 接受后 grant 失效；terminal transition 撤销全部 grant。`RecoverModelExecution` 由 live grant holder 提交，或在 Runtime 验证 lease 已过期且 command Claim 等于该 lease 的 Claim 后无 grant 提交。Executing tool 的 recovery 使用同一条 `SubmitToolFailure{Outcome:Unknown}` command：工具 owner 必须携带 live grant；`RecoverExpired` 仅在 lease 已过期且没有已接受 settlement 时无 grant 提交。该 Unknown 只结算这一 call，Run 保持 Active。scanner 使用确定性 recovery CommandID，因而 recovery update 也遵守同一 `(RunID, CommandID)` 幂等规则。attempt、owner、fence、lease 与 recovery record 由 Store adapter 内部管理。

**RUN-CMT-6** Commit 必须原子保存新 MachineState 与完整 TransitionRecord，保证 event group 完整写入。`CommitResult`、Load 与 Record 返回 detached values。预期拒绝映射为 `ErrCommandConflict`、`ErrStaleRuntime`、`ErrRunTerminal`；transport/storage failure 保持可判别且不得伪装为 rejection。

**RUN-CMT-7** 每个 Run 的协议版本是 `RunHeader.SchemaVersion`，在 Create 时冻结。`RuntimeSnapshot.SchemaVersion` 必须等于该 header。`ProtocolFor(header.SchemaVersion)` 在 Run 边界返回绑定了该版本 digest/decode/Decide/Evolve 函数的 `Protocol` 值；随后的方法调用不再接受 version 参数。`EvaluateCommit` 接受 command 当且仅当 `CommandEnvelope.SchemaVersion` 等于该 Protocol 的 Version。`agent/run` 不保存进程全局的当前写入版本，也不提供隐式选择版本的包级函数；新 Run 由 `NewRun.SchemaVersion` 决定版本。v1 Run 的 replay 必须继续使用 `ProtocolV1()`，即使进程已支持更高版本。Loop 通过 `RuntimeSnapshot.Protocol().BuildEnvelope` 构造写入该 Run 的 envelope。

`Runtime` 的唯一实现叠在 `Store` 上。`Store` 是追加式合同：

```go
type RunHead struct { Header RunHeader; State MachineState; Revision uint64; Leases map[string]ExecutionLease }
type LeaseOps struct { Put map[string]ExecutionLease; Delete []string; Clear bool }
type Append struct { ExpectedRevision uint64; Transition TransitionRecord; State MachineState; Leases LeaseOps }
type ExpiredLease struct { RunID RunID; Key string; Lease ExecutionLease }
type Store interface {
    Create(ctx, header RunHeader) (created bool, existing RunHeader, err error)
    LoadHead(ctx, RunID) (RunHead, error)
    LoadLog(ctx, RunID, from uint64) ([]TransitionRecord, error)
    LoadRecord(ctx, RunID) (RunHead, []TransitionRecord, error)
    LookupTransition(ctx, RunID, CommandID) (TransitionRecord, bool, error)
    Commit(ctx, RunID, fn func(RunTx) (*Append, error)) error
    // RunTx: Head() RunHead; LookupTransition(CommandID) (TransitionRecord, bool, error)
    RenewLease(ctx, RunID, key string, grant ExecutionGrant, deadline time.Time) error
    ExpiredLeases(ctx, before time.Time) ([]ExpiredLease, error)
    ReplaceSnapshot(ctx, RunID, revision uint64, *MachineState) error
}
```

`Store` 不调用 Decide/Evolve。`Store.Commit` 是唯一推进 Run 的写路径，也是该 Run 的 critical section：Store 在其中向 Runtime 提供 `RunTx`（当前 head 与按 CommandID 查 transition），Runtime 在同一 section 内调用 `EvaluateCommit`，返回的 `Append` 由 Store 在同一事务内追加 transition、替换 snapshot 并应用 lease 变更。同一 Run 的所有写入者在此串行，head 在读与写之间不会移动，因此没有 compare-and-swap 重试；`ExpectedRevision` 与 head 不一致只可能是实现错误，返回 `ErrAppendConflict`。`MemoryStore` 用 per-Run 锁，SQLite 用 immediate 写事务，Postgres 用行锁事务实现该 section。日志不可改写：没有删除或替换 transition 的 Store 方法；`ReplaceSnapshot` 只覆盖派生 snapshot，且只由 `Rebuild` 调用。`LoadRecord` 在一个一致读取内返回 head 与完整日志，`Runtime.Record` 与 `Rebuild` 只用它。`Load` 只读 head，`Commit` 只在 section 内读 head 与一条 transition，二者的代价都与日志长度无关。`MemoryStore`、SQLite、Postgres 实现同一合同，并通过同一 conformance。

lease 以 `(RunID, key)` 存放，key 为 `model/<StepID>` 或 `call/<StepID>/<CallID>`，一个 target 至多一条 live lease；grant 只存在于该 lease 上，没有独立的 grant 表。lease 的 `Deadline` 为零表示不超时（进程内占用）；过期且无 settlement 时 Runtime 将 `recoveryValid` 置真，允许 grantless Recover。进程崩溃时不写 settlement。`RecoverExpired` 通过 `Store.ExpiredLeases(now)` 只加载有过期 lease 的 Run：对每个过期 Executing tool call 无 grant 提交 `SubmitToolFailure{Unknown}`，对过期 Executing model 提交 `RecoverModelExecution`。该 Run 保持 Active，同一 RunID 继续。`Rebuild` 从 header 与完整 transition log 重折 snapshot，与存储的 snapshot 比较后经 `ReplaceSnapshot` 修复，供诊断使用；日志短于 head revision 返回 `ErrLogTruncated`。进程内宿主使用 `NewRuntime(NewMemoryStore())`，lease 不超时，grantless recover 被拒绝。生产崩溃恢复使用带 TTL 的 Runtime。

**RUN-CMT-8** lease 续期。`Runtime.RenewLease(runID, stepID, callID, grant)` 在 grant 等于该 target 当前 lease 的 grant 时，把 deadline 推后一个 `LeaseTTL`；lease 不存在、grant 不匹配或 target 已 settlement 时返回 `ErrStaleRuntime`。持有 grant 的 worker 在效果执行期间必须以远小于 `LeaseTTL` 的间隔续期（Loop 的 `ExecutionPolicy.LeaseRenewInterval`）；续期返回 `ErrStaleRuntime` 表示该 target 已被 recovery 接管，worker 必须停止执行并放弃 settlement。`LeaseTTL` 是恢复延迟上界，不再是单次工具调用的时长上界。`LeaseTTL` 为零时 `RenewLease` 只验证 grant，不改变 deadline。

## 6. Loop ports 与 policy

```go
// package agent/run
type PlanningHint struct {
    RunID RunID
    SourceStep StepID
    Inputs []AgentInput
    LastToolStep *ToolStep
    LastModelResult *ModelResult
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
    Tools []run.ToolSpec
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
    Put(ctx, run.RunID, run.StepID, run.CallID, run.ExecutionClaim) error
    Get(ctx, run.RunID, run.StepID, run.CallID) (run.ExecutionClaim, bool, error)
    Delete(ctx, run.RunID, run.StepID, run.CallID) error
    DeleteRun(ctx, run.RunID) error
}
type LoopResult struct {
    Disposition LoopDisposition // LoopWaiting | LoopFinished
    Reason WaitReason           // 仅 ExecutionRecovery 时为 execution_recovery；否则为空
    ExecutionRecovery bool
    Result *run.RunResult
}
func New(models ModelCatalog, tools ToolCatalog, planner RequestPlanner, policy ExecutionPolicy, streaming bool) (*Loop, error)
func (*Loop) Run(context.Context, run.Runtime, run.RunID, EventSink) (LoopResult, error)
```

**RUN-LOP-1** `ExecutionPolicy` 是 Loop 的本地执行策略。`ToolExecution` 与 `MaxParallel` 在 `SubmitModelResult` 时写入 `ToolStepOpened.Scheduling` 并冻结在该 ToolStep 上；后续 Loop 必须按冻结值调度，不得改用当时进程的 ExecutionPolicy。未指定 `ToolExecution` 时冻结为 `parallel`，`MaxParallel` 零值表示当前 Start 批次全部 Pending call 可并行。空 Mode 按 parallel 解释，不得在 normalize 时填入默认字符串。nil handler 时结构错误的模型结果选择 `ModelRejectFailRun`；重试由 handler 明确返回 `ModelRejectRetry`。`streaming` 表示是否请求可用的流式模型端口；两种模式都产生同一完整 `sdk.ModelResult`。`LeaseRenewInterval` 是 worker 续期间隔（RUN-CMT-8）：模型与工具 worker 在效果执行期间按该间隔调用 `Runtime.RenewLease`，续期被拒时取消该 worker 的 ctx；零值关闭续期，只对 lease 不超时的 Runtime 正确。

**RUN-LOP-7** `ModelRef` 是冻结请求中的执行身份。`ModelCatalog.ResolveModel` 在同一 Run 生命周期内必须把同一 `ModelRef` 解析为等价的执行语义。provider 绑定不进入 frozen request，因此 Catalog 不得把同一 ref 改绑到不同实现。

`LoopResult` 的语义固定为：`LoopWaiting` 时 `Result` 为 nil，表示没有可执行 effect、Run 仍为 active。`ExecutionRecovery` 等于 `NeedsRecovery(state)`：Model Executing，或 ToolStep 无 Pending 且仍有 Executing。该值为 true 时由 recovery authority 唤醒，`Reason` 为 `execution_recovery`；否则 `Reason` 为空。Waiting call 不进入 `LoopResult`；Application 通过 `Runtime.Load` / `Record` 与 `WaitingCalls` 读取。`LoopFinished` 时 `Result` 非 nil，并等于 terminal RunRecord 派生的 `RunResult`。

`RequestPlanner` 从 `PlanningHint` 接收 Run 边界事实；它使用自己注入的 history、session context、memory、attachments 与 product policy 组装 `sdk.Request`。Runtime 验证并冻结 planner 返回的 request，Planner 管理 application context。

## 7. Loop execution

```text
Loop.Run(ctx, runtime, runID, sink):
  repeat:
    snapshot = Runtime.Load(runID)
    validate snapshot.State.RunID == runID
    if terminal: emit observational run_finished; return Finished(snapshot.Result)
    if resumeSettlement: continue   // 重放可能已提交、响应丢失的 settlement
    if resumeCachedStart: continue  // 重放本进程已接受、尚未结算的 start
    effect = run.Next(snapshot.State)
    dispatch effect
```

每个 `Loop` 实例为每个 `RunID` 分配一个本地 driver slot。同一实例对同一 `RunID` 的并发 `Run` 调用返回 `ErrRunAlreadyRunning`；不同 `RunID` 可以并行驱动。

**RUN-LOP-2** `NeedModelRequest` 调用 Planner，冻结 sdk.Request，验证 model、ordered InputIDs 与 ToolSpecs，计算 request/tools/binding digests 和 derived CommandID/StepID，再提交 Prepare。prepare stale 后重新 Load；同 revision 的内容拒绝不得 livelock 重试。业务停止统一使用 `CancelRun`。

**RUN-LOP-3** `StartModelCall` 先 Commit start barrier；首次 `CommitAccepted` 或使用同一 command ID、同一 `ExecutionClaim` 精确重放得到原 grant 的 Loop，才拥有该 execution。Loop 必须在 `ClaimStore` 中保留该 attempt 的 claim 直到完成 settlement，其余 identity 按需派生；缺少 grant 的 replay 进入 reload 流程。调用只使用 frozen ModelRequest 的 detached SDK materialization。streaming 与 non-streaming 必须产生同一种完整 `sdk.ModelResult`；delta 只发 EventSink。`ModelCatalog.ResolveModel` 失败或返回 nil 时提交 `RecoverModelExecution` 并返回错误，不得把 Run 记为 `provider_failure`：尚未发生模型调用。provider 调用失败提交 `SubmitModelFailure`；ctx cancellation 提交 `RecoverModelExecution`；结构、binding 或 freeze 失败提交 `RejectModelResult`，并由调用方显式选择 retry 或 fail-run。成功结果只提交一次 `SubmitModelResult`。

**RUN-LOP-4** Tool execution 先按 frozen binding resolve tool，并验证 Ref、definition digest、response policy 和 arguments。lookup/definition/argument failure 在 Pending 状态提交 `SubmitToolFailure(Known)`，不得跨越 start barrier。通过验证后逐 call 提交 `StartToolCall`；只有 Accepted owner 可执行。冻结的 `ToolStep.Scheduling` 决定 `parallel` 或 `sequential` 以及 `MaxParallel`；不得改用 Loop 进程当前的 ExecutionPolicy。每个结果以自己的 grant 提交。同一 ToolStep 中 DirectExecution 的 Pending call，在外层 ctx 未取消时于本次 `Run` 内按冻结 Scheduling 分批 Start 并结算；ctx 已取消时停止再 Start，只结算已持有 grant 的 call。`Next` 返回 `Idle` 时 Loop 返回 `LoopWaiting`，并用 `NeedsRecovery(state)` 设置 `ExecutionRecovery`。Loop 不解释 Waiting call，也不携带 `ResponseRequest`。Application 从 snapshot 读取 `WaitingCalls`，提交 `ApproveToolCall` / `RejectToolCall` / `SubmitToolResponse` 之后再次 `Run`。tool panic 或 effect 状态无法确定的错误转为对该 call 的 Unknown，并提交 `SubmitToolFailure(Unknown)`。该 settlement 不取消同批 sibling workers，也不结束 Run。`CancelRun` 先把仍 Executing 的 call 记为 `ToolCallFailed(Unknown)`，再 `RunEnded(stopped/cancelled)`，并把这些 CallID 与仍 Executing 的 ModelStep 写入 `RunStoppedEnd` / `RunResult` 的 `UncertainCalls`、`UncertainModel`。Waiting call 无论有无 Executing sibling 都不记 Failed。已接受 start 的 worker 必须在收到外层取消后返回并尝试 settlement；settlement 使用独立 control context。lookup/definition/argument failure 只允许发生在 Pending；Executing 且本进程持有 start cache 时只重放 start 并结算，不得再提交 grantless Known。

**RUN-LOP-5** model 与 tool worker 都接收外层 ctx；Loop 对已接受 effect 使用独立 control context 完成 known/unknown outcome settlement。Application 的业务停止顺序为先 Commit `CancelRun`，再取消 Loop ctx。非 sentinel Commit error 以同 CommandID/digest 重放一次；仍未知时返回错误，由后续 Load/Record 查询 authority。stale/terminal/conflict 触发 reload/drop，旧 external effect 保持单次执行尝试。工具实现配合 context 返回；永久阻塞由 application/durable recovery 处理。

Waiting call 的批准与外部结果由 Application 提交。Loop 不生成、不返回、不解释 `ResponseRequest`。Application 以 snapshot 中的 stable ResponseID、derived CommandID 与 payload/decision digest 提交 `ApproveToolCall`、`RejectToolCall` 或 `SubmitToolResponse`；随后再次运行 Loop。

## 8. EventSink 与边界

```go
type EventSink interface { Emit(context.Context, Event) error }
type Event struct {
    RunID run.RunID
    StepID run.StepID
    CallID run.CallID
    Sequence uint64
    Kind EventKind
    Durability EventDurability
    Payload json.RawMessage
    Canonical *run.AgentEvent
}
```

`Sequence` 仅用于同一临时观察流内的顺序（例如 ToolProgress），从 1 开始；committed observation 的权威顺序由 `AgentEvent.Revision` 与 `Index` 表达，未提供临时序号时保持 0。

**RUN-LOP-6** EventSink 提供 realtime observation，Loop 通过序列化调用向 sink 发送事件。`EventAgentCommitted` 携带 accepted transition 中的 canonical AgentEvent；text/reasoning delta、tool progress、tool lifecycle 与 run-finished observation 可丢失、重复或断流。sink failure 保持 Commit 结果；恢复、materialization 与审计读取 `Runtime.Record`，EventSink gap 通过 canonical record 对账。

可进入区间是 `Current=Open`。Application 在此提交 `AcceptInput`，并负责 admission 与 planning 之间的线性化。Loop 不解释 queue 或 steer：`Open` 时立刻 `NeedModelRequest`。Run→Session materialization、terminal settlement、stable cross-domain IDs 与 crash recovery 由 [agent-turn.md](agent-turn.md) 定义。

## 9. compatibility 与 conformance

**RUN-CMP-1** 当前 pre-release schema v1 的 command/fact discriminator、wire fields、canonical digest、derived ID 和 `ProtocolV1().Evolve` 由 golden fixtures 保护；发布前有意修改协议时必须同步更新 fixture。v1 发布后，新增 variant、字段或折叠语义必须进入新 schema version，并继续 decode/fold 全部已发布版本。同一 Run 的 writer 不得混写不兼容 schema。

**RUN-CMP-2** Runtime conformance 必须覆盖：

- Create 首次/幂等/conflict、并发 Create、missing Run；
- command exact replay/conflict、prepare hard CAS、call-local rebase、terminal replay；
- grant 签发、隔离、精确 start replay、消费、跨 Run 拒绝与 recovery authorization；
- revision/index、atomic complete TransitionRecord、fact/transition digest；
- Load/Commit/Record alias isolation、多 Run 隔离；
- Record 单一一致点、FoldRun 等价、gap/tamper/corrupt failure；
- lease 过期 recovery：live lease 拒绝 grantless、过期 model 回到 Prepared、过期 tool 记 Unknown 且 sibling 不受影响、RecoverExpired 幂等；
- lease 续期：续期后原 deadline 不触发 recovery、错误/空 grant 与 settlement 后续期被拒；
- snapshot codec：每个 Current variant 与终态 round-trip、拒绝 unknown field / 非法判别式 / trailing data；重开 Store 后 Load 不折叠日志且 Record 校验通过。

Loop conformance 必须覆盖：

- 单模型完成、tool round trip、approval/external response wait/resume；
- known failure 继续、Unknown 继续、tool panic、aliased ToolRef 与 validation；
- parallel/sequential 按冻结 `ToolStep.Scheduling` 调度，不得改用当时 ExecutionPolicy；
- `ModelCatalog.ResolveModel` 失败或 nil 时恢复 ModelStep、Run 保持 active；
- ctx cancellation、model recovery、explicit malformed-result disposition；
- Cancel 将 Executing tool/model 投影到 `UncertainCalls` / `UncertainModel`；ExternalResponse reject 为 `response_rejected`；
- streaming delta 与 nil result、EventSink committed observation；
- stale/unknown commit response、prepare no-progress rejection 与无 livelock；
- 超过 LeaseTTL 的工具调用在续期下不被记为 Unknown，其结果被接受；
- 共享 ClaimStore 的第二个 Loop 实例重放前一实例的 start 并完成 settlement，不等待 lease 过期。

历史 package 迁移、实施阶段与未完成 adapter 工作记录在 [agent-runtime-refactor.md](agent-runtime-refactor.md)，本协议 authority 以本文为准。
