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

`MachineState` 决定 Run 当前可执行动作。每次接受的 command 产生一个完整 `TransitionRecord`，其 facts 经 `EvolveVersion` 从 `RunHeader.InitialState` 重放后必须得到同 revision 的 `MachineState`。

Run 的职责分成五个相互独立的层面：

```text
Agent Machine   = Run/Step 状态与合法转移（Decide、Evolve、Next）
Agent Loop      = Machine effect 的进程内解释器
Runtime         = 一个 Run 的 authority、并发校验与原子提交
Model / Tool    = 一次模型请求或一次工具调用的 effect 执行器
Request Planner = application context 到 sdk.Request 的投影器
```

Machine 处理已冻结的值和已提交的事实；Loop 解释 `Next` 产生的 transient effect；Runtime 保存并验证 Machine 的推进；Model/Tool 执行一次外部 effect；Request Planner 组装下一次模型请求。MemoryRuntime 和 durable adapter 实现同一个 Runtime contract，分别使用内存锁和数据库事务保存 authority。

`Step` 是 Run 的持久化恢复边界；`execution attempt` 表示某个 Loop 进程对该 Step 或 ToolCall 的一次易失执行。一个 Step 可以有多个 attempt，Machine 只接受带有效 grant 的 settlement。Attempt 的执行控制信息由 start command 的 `ExecutionClaim` 和 Runtime 返回的 opaque `ExecutionGrant` 表达。

每次 accepted command 产生的 `TransitionRecord.Events` 构成 canonical event plane：它与新的 MachineState 在同一 Runtime commit 中写入，按 revision/index 有序，可用于 replay、materialization 和审计。`EventSink` 转发这些已提交事件及临时 delta；canonical authority 由 Runtime.Record 提供。

**RUN-SCP-1** `agent/run` 拥有 Run identity、persisted frozen values、Machine、command/fact protocol、wire codec、fold、Runtime contract 与 MemoryRuntime。`agent/run/loop` 拥有 planner/model/tool ports、streaming、并发执行、EventSink 与 Loop policy；它单向依赖 `agent/run`，根 package 不依赖 loop。

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

**RUN-WIR-2** 当前 pre-release schema v1 使用 RFC 8785/JCS canonical bytes。字段、omission、array order、type discriminator、digest preimage 与 `EvolveVersion` 语义在发布前继续演进，发布后永久冻结。command、fact 和 transition decoder 必须拒绝 unknown version/type、duplicate key、unknown field、trailing data、非法 UTF-8、非 canonical-equivalent wire 与 digest mismatch。精确 identity 和 digest 使用 JSON string；当前 v1 revision/index/schema fields 使用整数 wire shape。

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

**RUN-WIR-3** 一个 transition 的 event 数至少为 1；其 RunID、Revision、CommandID、CommandDigest 相同，Index 从 0 连续递增。revision 从 1 连续递增。构造 command 必须使用 `BuildEnvelope`；构造与验证 transition 必须使用 `BuildTransitionRecord`、`ValidateTransitionRecord`。所有公开返回值具有 detached snapshot 语义。

下列 identity 稳定派生并由 Commit 验证：

| identity | preimage |
|---|---|
| PrepareModelRequest CommandID | RunID、loaded BaseRevision |
| ModelStep StepID | RunID、prepare CommandID、model/request/tools binding digest |
| ToolStep StepID | source ModelStepID、ordered binding-set digest |
| ResponseID | RunID、ToolStepID、CallID、ResponseKind |
| response CommandID | RunID、StepID、CallID、ResponseID |
| input CommandID | RunID、InputID |
| recovery CommandID | RunID、StepID、Claim、CallID（如适用）、recovery kind |

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

**RUN-NEW-1** `BuildNewRun` 建立当前 version 的 creation value；`BuildRunHeaderFromNewRun` 按 `NewRun.SchemaVersion` 的冻结规则建立 header。v1 Revision-0 state 恰为：相同 RunID、`RunActive`、无 Current、无 pending input、零 model step、零 usage、无 result。初始输入必须随后通过 `AcceptInput` transition 进入 log。

Header 创建后 immutable。`InitialStateDigest` 覆盖 frozen initial state；`HeaderDigest` 覆盖 schema、RunID、initial-state version/digest 与 causation。`ValidateRunHeader` 验证这些约束。

**RUN-NEW-2** `FoldRun(header, transitions)` 先验证 header，再按 revision/index 使用 `EvolveVersion` 折叠完整 transition sequence。Fold 过程执行纯状态重建。import、diagnostic 与 `Runtime.Record` integrity verification 从 header 开始；外部 snapshot 通过 FoldRun 结果校验。

## 4. Machine

```go
type MachineState struct {
    RunID RunID
    Status RunStatus
    Current Step
    PendingInputs []AgentInput
    ModelSteps int
    LastClosedStep StepID
    LastToolStep *ToolStep
    Usage Usage
    LastModelResult *ModelResult
    Result *RunResult
}

type RunResult struct {
    Status  RunStatus
    Reason  RunReason
    Failure *RunFailure
    Model   *ModelResult
    Usage   Usage
}
```

Run status 为 `RunActive | RunCompleted | RunStopped | RunFailed`。`RunCompleted`、`RunStopped` 与 `RunFailed` 是 terminal status。`RunStatus` 表示当前 MachineState 的投影；终态 fact 使用 RunEnd union 表达具体结果。`Step` 是 sealed interface，只有 `ModelStep` 与 `ToolStep`。

终态 fact 使用 Go 的 sealed-union 形式，终态结构由合法的 RunEnd variant 构成：

```go
type RunEnd interface{ runEnd() }

type RunCompletedEnd struct{}
type RunStoppedEnd struct { Reason RunReason }
type RunFailedEnd struct {
    Reason  RunReason
    Failure RunFailure
}

func (RunCompletedEnd) runEnd() {}
func (RunStoppedEnd) runEnd() {}
func (RunFailedEnd) runEnd() {}

type RunEnded struct { End RunEnd }
```

`RunEnded.End` 必须恰好是上述三个 variant 之一；`RunStoppedEnd.Reason` 必须非空，`RunFailedEnd.Reason` 必须是失败原因，`RunFailedEnd.Failure.Class` 必须非空。`RunEnded` 是 terminal transition 的最后一个 fact。RunStatus、RunResult 等读取模型从该 union 派生。当前 v1 wire body 使用 `status/reason/failure` 字段；codec 负责在 wire 与 union 之间做严格映射，并拒绝 `RunActive`、缺失字段或多余字段。

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

**RUN-MCH-1** MachineState 保存 Run 的 execution semantics。`LastToolStep` 保存最近一个已关闭 ToolStep 的只读投影，必须与 transition log 折叠出的最后关闭 step 一致，供下一次 planner 构造模型请求。terminal state 吸收所有未幂等命令；`RunEnded` 建立唯一 terminal result。RunResult 在该 terminal transition 中建立，并由后续 snapshot/record 读取。

**RUN-MCH-2** `ToolCallBinding` 冻结 CallID、ToolRef、definition digest、canonical arguments、response policy 与 binding digest。已知工具使用匹配 frozen ToolSpec 的 ref/digest/policy；未知工具保留为同名 unresolved DirectExecution binding，并在执行前收束为已知 lookup failure。approval/external response 的 `ResponseRequest` 由 Decide 稳定派生。Unknown outcome 使用 `effect_unknown`，并使 Run 进入 `RunFailed(effect_unknown)`；后续处理由新的 Run 决定。

`AgentCommand` 与 `Fact` 都是 sealed interface。v1 的 command→fact 规则为：

| command | precondition / facts |
|---|---|
| `AcceptInput` | 无 Current；`InputAccepted` |
| `PrepareModelRequest` | 无 Current，完整有序消费 PendingInputs，request/tools digests 有效；`ModelStepPrepared` |
| `StartModelExecution` | Model Prepared；`ModelStepStarted`。command 必须携带本次 start 的 `ExecutionClaim` |
| `RecoverModelExecution` | Model Executing；`ModelStepRecovered`。恢复 durable attempt 时携带该 attempt 的 `Claim` |
| `SubmitModelResult` | Model Executing；`ModelStepCompleted`，随后无 calls 时 `RunEnded(completed)`，有 calls 时 `ToolStepOpened` |
| `SubmitModelFailure` | Model Executing；`RunEnded(failed/provider_failure)` |
| `RejectModelResult` | Model Executing；`ModelStepRejected`，由调用方显式选择回到 Prepared 或在同一 transition 追加 `RunEnded(failed/malformed_model_result)` |
| `StartToolCall` | Tool Pending；`ToolCallStarted`。command 必须携带本次 start 的 `ExecutionClaim` |
| `SubmitToolResult` | Tool Executing；`ToolCallCompleted`，最后一个 call 进入 terminal 时隐式关闭 ToolStep |
| `SubmitToolFailure(Known)` | Tool Pending/Executing；`ToolCallFailed(Known)`，最后一个 call 进入 terminal 时隐式关闭 ToolStep |
| `SubmitToolFailure(Unknown)` | Tool Executing；`ToolCallFailed(Unknown)`、`RunEnded(failed/effect_unknown)` |
| `ApproveToolCall` | Waiting(Approval)；`ToolCallApproved` |
| `RejectToolCall` | Waiting(Approval/ExternalResponse)；`ToolCallFailed(Known/permission_denied)`，最后一个 call 进入 terminal 时隐式关闭 ToolStep |
| `SubmitToolResponse` | Waiting(ExternalResponse)；`ToolCallAnswered`，最后一个 call 进入 terminal 时隐式关闭 ToolStep |
| `CancelRun` | active；`RunEnded(stopped/cancelled)` |

`ToolStepClosed` 作为旧 v1 transition 的兼容 fact 保留；新 command 在最终 ToolCall fact 中完成 ToolStep 的关闭。

**RUN-MCH-3** `Decide(state, command)` 执行全部验证与 derived consequence，一次返回该 transition 的完整 ordered fact group；验证成功后返回完整 facts。`EvolveVersion(version,state,fact)` 机械折叠 fact，依赖 fact 携带的完整数据。accepted facts 必须 self-contained；若 transition terminalize，`RunEnded` 必须是 Decide 输出的最后一个 fact。

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

Loop 在提交 start 前生成并保留 `Claim`、`CommandID` 与 command digest。提交响应丢失时，Loop 以完全相同的三者重放；Runtime 对精确重放返回原 `ExecutionGrant`。恢复同一模型 attempt 时，`RecoverModelExecution` 携带原 `Claim`，其 CommandID 由 `(RunID, StepID, Claim)` 派生。若 Run 已记录 Executing 而 Loop 已丢失这些值，Runtime 的 recovery authority 处理该 execution，Loop 根据 recovery 结果继续。

`Next(state)` 最多返回一个 transient `Effect`：

| state | effect |
|---|---|
| terminal | 无 |
| Current=nil | `NeedModelRequest{PlanningHint}` |
| Model Prepared | `StartModelCall` |
| Model Executing | `WaitForExecutionRecovery` |
| ToolStep 有 Pending calls | `StartToolCalls` |
| ToolStep 无 Pending、含 Executing | `WaitForExecutionRecovery` |
| ToolStep 无 Pending、无 Executing、含 Waiting | `Idle` |

Waiting call 上的 `ResponseRequest` 由 `WaitingCalls(state)` 从 MachineState 读取，不进入 Effect。

**RUN-MCH-4** Effect 由调用方每次 Load 后重新派生。`PrepareModelRequest.InputIDs` 必须与当前 PendingInputs 等长、同顺序、逐项相同；prepare 接受后一次消费全部 pending input。ToolStep 的 Waiting call 禁止 Start，同一 step 中的 Pending call 仍可执行。无 Pending 且仍有 Executing 时返回 `WaitForExecutionRecovery`。无 Pending、无 Executing、仍有 Waiting 时返回 `Idle`：Run 仍为 active，解释器没有可执行 effect。Application 从 snapshot 读取 `WaitingCalls` 并提交 `ApproveToolCall` / `RejectToolCall` / `SubmitToolResponse`。执行恢复由 Runtime/application 的 recovery authority 提供。

## 5. Runtime 与 Commit

```go
type Runtime interface {
    Create(context.Context, NewRun) (CreateResult, error)
    Load(context.Context, RunID) (RuntimeSnapshot, error)
    Commit(context.Context, CommitRequest) (CommitResult, error)
    Record(context.Context, RunID) (RunRecord, error)
}
type RuntimeSnapshot struct {
    State MachineState // detached in-process view
    Revision uint64
    SchemaVersion uint16 // RunHeader.SchemaVersion
}
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

**RUN-CMT-2** `Load` 返回当前 detached execution snapshot，并验证 `RunID`、revision 与 MachineState 的基本语义不变量；snapshot 与 revision 必须来自同一 authority 版本。`RuntimeSnapshot` 提供 Go 读取视图；跨实现持久化使用 Header 与 TransitionRecord，优化 snapshot 由实现管理。`Record` 在一个一致点读取 detached Header、Snapshot 和完整 TransitionRecord sequence，验证 header、每个 transition、连续 fold 与 snapshot 等价后返回；corrupt、gap 或 divergence 必须失败。普通消费者使用 Record 获取一致的完整记录。`FoldRun` 从 Header 和完整 transition sequence 重建状态；任何导入、诊断或 Record 校验先完成 FoldRun，再使用重建状态。

所有 Runtime implementation 在自己的 critical section/transaction 内调用同一个 pure `EvaluateCommit`。顺序固定为：

```text
1 validate envelope RunID/schema/type/digest and derived CommandID
2 lookup prior transition by CommandID
3 exact digest replay -> AlreadyApplied + original complete event group
4 same CommandID/different digest -> conflict
5 terminal check
6 validate hard CAS / target state / execution grant / recovery authority
7 facts = Decide(current, command) exactly once
8 snapshot each fact; EvolveVersion in order; assign next Revision and Index
9 build and validate one complete TransitionRecord
10 atomically persist transition and new MachineState
```

**RUN-CMT-3** `PrepareModelRequest` 是 hard-CAS command：BaseRevision 必须等于 current revision。其他 command 通过当前 target state 和 grant 做 call-local rebase；stale BaseRevision 本身不阻止无冲突的 ingress/control/settlement。相同 command 的 replay 判定先于 terminal check，因此 terminal Run 仍能返回原 transition。

**RUN-CMT-4** 幂等键为 `(RunID,CommandID)`。相同 digest 返回 `CommitAlreadyApplied`、当前 snapshot 与原完整 event group，且不得再次 Decide、分配 revision 或产生外部 effect。对于 `StartModelExecution` 和 `StartToolCall`，Runtime 还必须验证 command 中的 `ExecutionClaim`：相同 command ID、相同 digest、相同 claim 的精确重放在 grant 仍 live 时返回原 start grant；不同 claim 触发 `ErrCommandConflict`，并保持现有执行授权。非 start command 的 replay 不返回 grant。revision 每个 accepted command 恰加 1。

**RUN-CMT-5** accepted `StartModelExecution`/`StartToolCall` 为目标签发新 grant；该 start 的 `CommitAccepted` 和在 grant 仍 live 时满足精确 replay 条件的 `CommitAlreadyApplied` 返回同一个 grant。若该 start 已 settlement 或 Run 已 terminal，精确 replay 仍返回 `CommitAlreadyApplied`，并返回空 grant。model result/failure/reject 与 executing tool result/known failure 必须携带 live target grant。settlement 接受后 grant 失效；terminal transition 撤销全部 grant。公共 Runtime 的 `RecoverModelExecution` 由 live grant holder 提交；durable 实现内部可以在自己验证 execution recovery record 后无 grant 提交。Executing tool 的 recovery 使用同一条 `SubmitToolFailure{Outcome:Unknown}` command：工具 owner 必须携带 live grant；recovery scanner 仅在 Runtime 验证其 lease/claim 已失效且没有已接受 settlement 时无 grant 提交。scanner 使用上表的 deterministic recovery CommandID，因而 recovery update 也遵守同一 `(RunID, CommandID)` 幂等规则。durable adapter 保存 `(command ID, claim) -> grant` 精确 replay 所需的私有 start record；attempt、owner、fence、lease 与 recovery record 由 adapter 内部管理。

**RUN-CMT-6** Commit 必须原子保存新 MachineState 与完整 TransitionRecord，保证 event group 完整写入。`CommitResult`、Load 与 Record 返回 detached values。预期拒绝映射为 `ErrCommandConflict`、`ErrStaleRuntime`、`ErrRunTerminal`；transport/storage failure 保持可判别且不得伪装为 rejection。

**RUN-CMT-7** 每个 Run 的协议版本是 `RunHeader.SchemaVersion`，在 Create 时冻结。`RuntimeSnapshot.SchemaVersion` 必须等于该 header。`EvaluateCommit` 接受 command 当且仅当 `CommandEnvelope.SchemaVersion` 等于该 Run 的 header 版本；不得使用进程全局 `currentSchemaVersion` 作为写入许可。digest、decode、`DecideVersion` 与 `EvolveVersion` 均按该 version 分发到 `ProtocolV1`（`digestRequestV1`、`decodeCommandVariantV1`、`evolveV1` 等）。v1 Run 的 replay 必须继续使用 v1 digest preimage，即使进程已经把 `currentSchemaVersion` 升到 2。`BuildEnvelopeVersion` 是 Loop 写入该 Run 时的构造入口。

`MemoryRuntime` 提供 multi-Run in-process reference implementation：collection lock 保护 Run map，每个 Run 使用独立锁。跨进程 durable recovery 由其他 Runtime adapter 提供。

## 6. Loop ports 与 policy

```go
// package agent/run/loop
type RequestPlanner interface {
    Plan(context.Context, run.PlanningHint) (RequestPlan, error)
}
type PlanningHint struct {
    RunID RunID
    SourceStep StepID
    Inputs []AgentInput
    LastToolStep *ToolStep
    LastModelResult *ModelResult
}
type RequestPlan struct {
    Model run.ModelRef
    Request sdk.Request
    InputIDs []run.InputID
    PlanningToken run.PlanningToken
    Tools []run.ToolSpec
}
type ModelCatalog interface { Resolve(run.ModelRef) (ModelInvoker, error) }
type ModelInvoker interface { Generate(context.Context, sdk.Request) (sdk.ModelResult, error) }
type StreamingModelInvoker interface { Stream(context.Context, sdk.Request) (sdk.ModelStream, error) }
type ToolCatalog interface { Resolve(run.ToolRef) (ExecutableTool, error) }
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
    Reason WaitReason           // execution_recovery；Idle 时为空
    ExecutionRecovery bool
    Result *run.RunResult
}
func New(models ModelCatalog, tools ToolCatalog, planner RequestPlanner, policy ExecutionPolicy, streaming bool) (*Loop, error)
func (*Loop) Run(context.Context, run.Runtime, run.RunID, EventSink) (LoopResult, error)
```

**RUN-LOP-1** `ExecutionPolicy` 是 Loop 的本地执行策略，包含 `ToolExecution`、`MaxParallel` 和可选的 malformed-result handler；未指定 `ToolExecution` 时使用 `parallel`，正数 `MaxParallel` 限制当前 ToolStep 的本地 worker 数量，零值允许当前批次的所有可执行 call 并行。nil handler 时结构错误的模型结果选择 `ModelRejectFailRun`；重试由 handler 明确返回 `ModelRejectRetry`。`streaming` 表示是否请求可用的流式模型端口；两种模式都产生同一完整 `sdk.ModelResult`。

`LoopResult` 的语义固定为：`LoopWaiting` 时 `Result` 为 nil，表示没有可执行 effect、Run 仍为 active。`ExecutionRecovery` 为 true 时至少有一个 call 仍为 Executing，由 recovery authority 唤醒。`Reason` 在该情况下为 `execution_recovery`，Idle 时为空。Waiting call 不进入 `LoopResult`；Application 通过 `Runtime.Load` / `Record` 与 `WaitingCalls` 读取。`LoopFinished` 时 `Result` 非 nil，并等于 terminal RunRecord 派生的 `RunResult`。

`RequestPlanner` 从 `PlanningHint` 接收 Run 边界事实；它使用自己注入的 history、session context、memory、attachments 与 product policy 组装 `sdk.Request`。Runtime 验证并冻结 planner 返回的 request，Planner 管理 application context。

## 7. Loop execution

```text
Loop.Run(ctx, runtime, runID, sink):
  repeat:
    snapshot = Runtime.Load(runID)
    validate snapshot.State.RunID == runID
    if terminal: emit observational run_finished; return Finished(snapshot.Result)
    effect = run.Next(snapshot.State)
    dispatch effect
```

每个 `Loop` 实例为每个 `RunID` 分配一个本地 driver slot。同一实例对同一 `RunID` 的并发 `Run` 调用返回 `ErrRunAlreadyRunning`；不同 `RunID` 可以并行驱动。

**RUN-LOP-2** `NeedModelRequest` 调用 Planner，冻结 sdk.Request，验证 model、ordered InputIDs 与 ToolSpecs，计算 request/tools/binding digests 和 derived CommandID/StepID，再提交 Prepare。prepare stale 后重新 Load；同 revision 的内容拒绝不得 livelock 重试。业务停止统一使用 `CancelRun`。

**RUN-LOP-3** `StartModelCall` 先 Commit start barrier；首次 `CommitAccepted` 或使用同一 command ID、同一 `ExecutionClaim` 精确重放得到原 grant 的 Loop，才拥有该 execution。Loop 必须保留 start command 的 ID、digest 和 claim，直到完成 settlement；缺少 grant 的 replay 进入 reload 流程。调用只使用 frozen ModelRequest 的 detached SDK materialization。streaming 与 non-streaming 必须产生同一种完整 `sdk.ModelResult`；delta 只发 EventSink。provider failure 提交 `SubmitModelFailure`；ctx cancellation 提交 `RecoverModelExecution`；结构、binding 或 freeze 失败提交 `RejectModelResult`，并由调用方显式选择 retry 或 fail-run。成功结果只提交一次 `SubmitModelResult`。

**RUN-LOP-4** Tool execution 先按 frozen binding resolve tool，并验证 Ref、definition digest、response policy 和 arguments。lookup/definition/argument failure 在 Pending 状态提交 `SubmitToolFailure(Known)`，不得跨越 start barrier。通过验证后逐 call 提交 `StartToolCall`；只有 Accepted owner 可执行。`parallel` 模式并发执行同一 ToolStep 中所有可执行的 Pending call，`sequential` 模式按 call 顺序逐个执行；每个结果以自己的 grant 提交。同一 ToolStep 中 DirectExecution 的 Pending call 在本次 `Run` 内 Start 并结算。`Next` 返回 `Idle` 时 Loop 返回 `LoopWaiting`，不解释 Waiting call，也不携带 `ResponseRequest`。Application 从 snapshot 读取 `WaitingCalls`，提交 `ApproveToolCall` / `RejectToolCall` / `SubmitToolResponse` 之后再次 `Run`。`Next` 返回 `WaitForExecutionRecovery` 时 Loop 返回 `LoopWaiting` 且 `ExecutionRecovery` 为 true。tool panic 或 effect 状态无法确定的错误转为 Unknown；一个 Unknown 取消同批 sibling workers，并由 Machine 在同一 terminal transition 中记录目标及仍 Executing sibling 的 `ToolCallFailed(Unknown)`，最后追加 `RunEnded(failed/effect_unknown)`。`CancelRun` 也在 `RunEnded(stopped/cancelled)` 前记录所有仍 Executing call 的 Unknown。已接受 start 的 worker 必须在收到外层取消后返回并尝试 settlement；settlement 使用独立 control context。

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

Queue 只能在 `Current=nil` 的 safe boundary 提交 `AcceptInput`；Application 负责 admission 与 planning 之间的线性化。Run→Session materialization、terminal settlement、stable cross-domain IDs 与 crash recovery 由 [agent-turn.md](agent-turn.md) 定义。

## 9. compatibility 与 conformance

**RUN-CMP-1** 当前 pre-release schema v1 的 command/fact discriminator、wire fields、canonical digest、derived ID 和 `EvolveVersion(1)` 由 golden fixtures 保护；发布前有意修改协议时必须同步更新 fixture。v1 发布后，新增 variant、字段或折叠语义必须进入新 schema version，并继续 decode/fold 全部已发布版本。同一 Run 的 writer 不得混写不兼容 schema。

**RUN-CMP-2** Runtime conformance 必须覆盖：

- Create 首次/幂等/conflict、并发 Create、missing Run；
- command exact replay/conflict、prepare hard CAS、call-local rebase、terminal replay；
- grant 签发、隔离、精确 start replay、消费、跨 Run 拒绝与 recovery authorization；
- revision/index、atomic complete TransitionRecord、fact/transition digest；
- Load/Commit/Record alias isolation、多 Run 隔离；
- Record 单一一致点、FoldRun 等价、gap/tamper/corrupt failure。

Loop conformance 必须覆盖：

- 单模型完成、tool round trip、approval/external response wait/resume；
- known failure 继续、Unknown terminal、tool panic、aliased ToolRef 与 validation；
- parallel/sequential tool execution、ctx cancellation、model recovery、explicit malformed-result disposition；
- streaming delta 与 nil result、EventSink committed observation；
- stale/unknown commit response、prepare no-progress rejection 与无 livelock。

历史 package 迁移、实施阶段与未完成 adapter 工作记录在 [agent-runtime-refactor.md](agent-runtime-refactor.md)，本协议 authority 以本文为准。
