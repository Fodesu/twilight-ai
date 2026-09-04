# Twilight Agent Turn 协议

状态：设计草案。现有 `agent/turn` 代码不符合本文，待重写；Run 事实与 Turn、Chatlog 事件同在一条 Session stream。

本文定义 `agent/turn`：回合生命周期、Run attempt 的创建与结算、Run 事实到对话内容的伴随映射。"必须""应该"为协议约束。Run Machine 与 Runtime 的 authority 是 [agent-run.md](agent-run.md)；对话内容的 authority 是 [agent-session-chatlog.md](agent-session-chatlog.md)；stream、commit 与 projection 机制的 authority 是 [agent-session.md](agent-session.md) 与 [agent-session-extension.md](agent-session-extension.md)。

## 1. 模型与范围

```text
Turn   逻辑回合。由一组 delivered Input 触发，以 completed / failed / superseded 结束。
Run    完成一个 Turn 的一次 attempt。同一 Turn 至多一个非终态 Run；可以有多个已终结的 Run。
```

| Concern | Canonical owner | 写入者 |
|---|---|---|
| 回合存在、attempt 归属与结束 | `twilight/turn/` events | Coordinator |
| Run 执行状态 | `twilight/run/` events（[agent-run.md](agent-run.md)） | `run.Runtime`，由 Loop 与 Coordinator 驱动 |
| 对话内容 | `twilight/chatlog/` events | Start 与 Deliver 时 delivered input；Run commit 内的 companion events |
| Application policy | Application | binding、driver、retry、context 策略、产品策略 |

**TRN-SCP-1** Source 为 `twilight`，ModuleID 为 `turn`。一个 Turn 与它的全部 Run attempt 在同一 Session stream 内。`Coordinator` 创建 Turn、创建 attempt、在回合中途投递输入、驱动 Run、结算 Turn。turn 依赖 run；run 不依赖 turn，Run 事实中的 `OwnerID` 由本模块以 `TurnID` 填充。本模块的 `Requires`（EXT-REG-4）为：`run`，消费 `twilight/run/created` v1、`twilight/run/input_accepted` v1 与 `twilight/run/ended` v1；`chatlog`，只要求存在。

**TRN-SCP-2** Turn 与 Run 的关系为 1:N，不变量为同一 Turn 至多一个非终态 Run：

| 动作 | 语义 | RunID |
|---|---|---|
| resume | 继续一个非终态 Run（进程重启、lease 恢复、Waiting 响应后） | 不变 |
| retry | 前一 Run 已终结且未 completed，同一 Turn 再开一个 attempt | 新 RunID，`Attempt` 加 1 |
| replace | 输入内容被替换，`twilight/turn/superseded` 指向新 Turn | 新 Turn、新 RunID |

subagent 使用独立 Session 与独立 Turn。

**TRN-SCP-3** Coordinator 没有隐藏状态。它从 `twilight/turn/surface` 投影与 `twilight/run/machine` 投影重建。

**TRN-SCP-4** Turn 自己的写入经 `extension.SemanticAppender`；Run 事实的写入经 `run.Runtime`，后者同样经 `SemanticAppender.AppendSemanticIn` 落在同一 `session.Store`（EXT-SCP-1）。Artifact 由其 owner 管理。

**TRN-SCP-5** Application 管理 model、provider、tool、prompt、token、approval、queue、retry 决策与并发。Coordinator 按 persisted binding 解析 driver。参考 Planner 每次 Plan 使用 Binding 的 `ModelRef`。

**TRN-SCP-6** Start 之前建立 immutable execution binding。Session 保存 `ID` 与 `Digest`。密钥与 client 留在进程内。Resolve 失败返回 `binding_unavailable`。公开字段见 [参考组装](agent-reference-assembly.md)。

## 2. identity 与事件

```go
type TurnID string
type TurnRef struct { SessionID session.SessionID; TurnID TurnID }
type ExecutionBindingRef struct { ID ExecutionBindingID; Digest es.Digest }
type CompanionVersion string

type Settlement string
const (
    SettlementCompleted Settlement = "completed"
    SettlementFailed    Settlement = "failed"
    SettlementStopped   Settlement = "stopped"
)

type StartedPayload struct {
    TurnID TurnID
    InputIDs []chatlog.InputID
    ExecutionBinding ExecutionBindingRef
    Companion CompanionVersion
    PlanDigest es.Digest
}
type CompletedPayload struct {
    TurnID TurnID
    RunID run.RunID // 产生 completed 的 attempt
}
type FailedPayload struct {
    TurnID TurnID
    RunID run.RunID // 最后一个 attempt
    Settlement Settlement // failed | stopped
    FailureClass string
}
type SupersededPayload struct {
    TurnID TurnID
    ReplacementTurnID TurnID
}
```

**TRN-ID-1** `TurnRef`、RunID、binding ID、CompanionVersion、InputID 与 digest 非空且稳定。

**TRN-ID-2** `PlanDigest = Digest("twilight/turn/plan", TurnID, ExecutionBinding.Digest, Companion, ordered InputIDs)`。

**TRN-ID-3** `StartOperationDigest = Digest("twilight/turn/start-operation", SessionID, TurnID, PlanDigest)`。用户正文 identity 在对应 `twilight/chatlog/input_submitted` 中。

**TRN-ID-4** attempt 的 RunID 由 Coordinator 派生：`RunID = Digest("twilight/turn/run", SessionID, TurnID, Attempt)`。Attempt 从 1 开始。`twilight/run/created` 的 `Owner` 等于 `OwnerID(TurnID)`，`Attempt` 等于该值（RUN-NEW-1）。

**TRN-EVT-1** EventType：

```text
twilight/turn/started
twilight/turn/completed
twilight/turn/failed
twilight/turn/superseded
```

unsettled Turn 是尚未 completed、failed 或 superseded 的 `started`。

**TRN-EVT-2** 本模块产生的事件（含 companion 与 Attach 产生的）的 EventID 由 Appender 按 `Digest(EventType, CommitID, index)` 统一赋值（EXT-APP-5）。Start 的 CommitID 由 StartOperationDigest 派生；Retry、Settle、Stop 的 CommitID 见各自条目。相同 canonical payload 幂等；差异为 conflict。事件时间戳不参与幂等判定（SES-APP-1）。

**TRN-EVT-3** stream 内每个 TurnID 至多一条 `started`，至多一条 `completed` / `failed` / `superseded`。

**TRN-PRJ-1** ProjectionID 为 `twilight/turn/surface`。消费 `twilight/turn/started|completed|failed|superseded` 与 `twilight/run/created|input_accepted|ended`，`RequireComplete` 为 `turn` 与 `run`（EXT-PRJ-2）：

```go
type TurnStatus string
const (
    TurnActive        TurnStatus = "active"         // 存在非终态 Run
    TurnAttemptFailed TurnStatus = "attempt_failed" // 最后一个 Run 已终结且未 completed，Turn 未结算
    TurnCompleted     TurnStatus = "completed"
    TurnFailed        TurnStatus = "failed"
    TurnStopped       TurnStatus = "stopped"
    TurnSuperseded    TurnStatus = "superseded"
)
type AttemptView struct {
    RunID run.RunID
    Attempt uint32
    End *run.RunEnd // 非终态时为 nil
}
type TurnView struct {
    TurnID TurnID
    Status TurnStatus
    InputIDs []chatlog.InputID // started 的初始输入，加此后经 Deliver 进入任一 attempt 的输入，按 accepted 顺序去重
    ExecutionBinding ExecutionBindingRef
    Attempts []AttemptView // 按 Attempt 递增
    ActiveRun run.RunID    // Status=active 时非空
    ReplacementTurnID TurnID
}
type TurnSurface struct {
    Order []TurnID
    Turns map[TurnID]TurnView
}
```

UI 按 `TurnID` 连接 `twilight/chatlog/surface` 的条目，按 `RunID` 连接 `twilight/run/machine` 的实时视图。终态 attempt 的结果从 `twilight/run/ended` 记录在 `AttemptView.End`，不依赖 Run 投影。

## 3. API

```go
type Coordinator struct {
    Projections extension.ProjectionReader
    Appender extension.SemanticAppender
    Runtime run.Runtime
    Bindings ExecutionBindingRegistry
}
type DriveRequest struct { Ref TurnRef; RunID run.RunID }
type RunDriver interface { Drive(context.Context, DriveRequest) error }
type ExecutionBindingRegistry interface { Resolve(ExecutionBindingRef) (RunDriver, error) }

type Service interface {
    Start(context.Context, StartRequest) (TurnResponse, error)
    Deliver(context.Context, DeliverRequest) (TurnResponse, error)
    Resume(context.Context, TurnRequest) (TurnResponse, error)
    Retry(context.Context, RetryRequest) (TurnResponse, error)
    Stop(context.Context, StopRequest) (TurnResponse, error)
    Settle(context.Context, SettleRequest) (TurnResponse, error)
}
type StartRequest struct {
    Ref TurnRef
    Inputs []run.AgentInput // ID 为已 submitted 的 InputID，Payload 等于其 Content
    ExecutionBinding ExecutionBindingRef
    Companion CompanionVersion
}
type DeliverRequest struct { Ref TurnRef; Inputs []run.AgentInput } // 回合中途追加输入
type TurnRequest struct { Ref TurnRef }
type RetryRequest struct { Ref TurnRef; Reason string }
type StopRequest struct { Ref TurnRef; Reason string }
type SettleRequest struct { Ref TurnRef; FailureClass string }
type TurnResponse struct {
    Ref TurnRef
    RunID run.RunID
    Attempt uint32
    Status TurnStatus
    Disposition ResumeDisposition
    End *run.RunEnd // 该 attempt 已终结时非空，来自 twilight/run/ended
    Waiting []run.ResponseRequest
}
type ResumeDisposition string
const (
    ResumeWaitingForResponse ResumeDisposition = "waiting_for_response"
    ResumeWaitingForRecovery ResumeDisposition = "waiting_for_recovery"
    ResumeFinished           ResumeDisposition = "finished"
)
```

**TRN-API-1** Coordinator 经 `ProjectionReader` 读取 `twilight/turn/surface` 与 `twilight/run/machine` 两个投影接续；每个方法先读投影再决定动作。Coordinator 不持有 `session.Store`。

**TRN-API-2** Registry 用同一 `run.Runtime` 组装 driver。Run 的写入只经 `run.Runtime`。

**TRN-API-3** DTO 为值语义。`Waiting` 为 `twilight/run/machine` 的 `WaitingCalls`。`NeedsRecovery` 为 true 时返回 `ResumeWaitingForRecovery`；Application 调用 `Runtime.RecoverExpired` 后再 Resume。

**TRN-API-4** `twilight/turn/superseded` 由 Application 追加。Coordinator 的方法不写该事件。superseded 的 Turn 若仍有非终态 Run，Application 必须先 Stop。

## 4. Start 与 Retry

**TRN-STR-1** StartRequest：

1. Ref、binding ref、companion version 非空；
2. `Inputs` 无重复 ID；每个 ID 对应 chatlog 中状态为 submitted 的 Input，Payload 等于其 Content（Coordinator 经 chatlog surface 投影核对）。

`started.InputIDs` 与 `input_delivered`、`input_accepted` 的顺序都取 `Inputs` 的顺序。

**TRN-STR-2** Start 是一次原子 commit，顺序为：

```text
twilight/turn/started{TurnID, InputIDs, ExecutionBinding, Companion, PlanDigest}
twilight/chatlog/input_delivered{InputIDs[0], TurnID}
...
twilight/chatlog/input_delivered{InputIDs[n-1], TurnID}
twilight/run/created{RunID, Owner:TurnID, Attempt:1, SchemaVersion, CausationID}
twilight/run/input_accepted{RunID, InputIDs[0], Payload}
...
twilight/run/input_accepted{RunID, InputIDs[n-1], Payload}
```

InputIDs 为空时 group 为 `started` 加 `created`。`created` 与 `input_accepted` 的 facts 由 `run.Protocol.BuildCreateGroup` 构造（RUN-NEW-1），Coordinator 只负责把它们放入 group。

**TRN-STR-3** 派生 PlanDigest、StartOperationDigest、RunID 与 group identity，再 `AppendSemantic`。相同 identity 为 applied / already-applied。head conflict 时 CAS rebase，identity 与 payload 保持不变。

**TRN-STR-4** append 成功后进入 Drive。

**TRN-RTY-1** Retry 要求投影中该 Turn 为 `attempt_failed`。commit 为 `twilight/run/created{Attempt: n+1}` 加该 Turn 已 delivered 的全部 Input 的 `input_accepted`，顺序与 `TurnView.InputIDs` 相同（初始输入在前，中途 Deliver 的输入按 accepted 顺序在后）；payload 与首次 delivered 时相同，仅 RunID 与 Attempt 不同。Turn 为其他状态时 Retry 返回 conflict。

**TRN-RTY-2** Retry 的 CommitID 由 `Digest("twilight/turn/retry", SessionID, TurnID, Attempt)` 派生。

**TRN-RTY-3** 失败 attempt 已提交的 assistant 与 tool_result 保留在 stream 中，协议不删除、不隐藏。它们是否进入后续 attempt 的模型请求是 Application 策略，由 Planner 依据 turn surface 的 attempt 状态决定（REF-PLN-6）；协议只保证内容可用。

## 5. Deliver、Drive、Resume 与 Stop

**TRN-DLV-1** Deliver 在回合中途追加输入，要求 Turn 为 `active`；`attempt_failed`、已结算或不存在的 Turn 返回 conflict，输入保持 `submitted`，由 Application 决定开新 Turn。输入的校验与 TRN-STR-1 第 2 条相同。

**TRN-DLV-2** 对 `Inputs` 中每个输入按顺序提交一个 Run commit：`Runtime.Commit(AcceptInput{Input})`，`Attach` 携带 `twilight/chatlog/input_delivered{InputID, TurnID}`。Run 接受输入与 chatlog 把输入挂到 Turn 在同一 commit 可见。`AcceptInput` 在 Run 的任意非终态都被接受（RUN-MCH-4），Deliver 不关心 Run 当前处于哪一步。CommandID 为 Run 的 input CommandID，重放幂等；多条输入中途失败时，以剩余条目重试。

**TRN-DLV-3** Deliver 不取消正在进行的模型调用或工具调用；要打断用 Stop。提交后，若本进程没有在驱动该 Run，Deliver 进入 Drive；已在驱动时不动，运行中的 Loop 在下一次 Load 看到 `PendingInputs`。Deliver 与该 Run 的最后一步 `SubmitModelResult` 并发时由 Session 临界区定序：输入先提交，Run 回到 `Open` 继续；结果先提交，Run 已终结，Deliver 得到 `ErrRunTerminal` 并返回 `completed`，该输入未被 delivered。

**TRN-DRV-1** Drive 解析 binding 得到 driver，调用 `driver.Drive(ctx, {Ref, RunID})`。driver 内部为 `loop.Run(ctx, runtime, SessionID, RunID, sink)`。Drive 返回后读投影设置 `Disposition` 与 `End`：Run 终态为 `ResumeFinished`，`End` 取 surface 中该 attempt 的 `AttemptView.End`；`NeedsRecovery` 为 true 为 `ResumeWaitingForRecovery`；仅有 WaitingCalls 为 `ResumeWaitingForResponse`。

**TRN-DRV-2** EventSink 的 `text_delta` / `reasoning_delta` 为临时观察。Waiting 由 Application 提交 `ApproveToolCall` / `RejectToolCall` / `SubmitToolResponse` 后再次 Resume。

**TRN-RSM-1** Resume 要求投影中该 Turn 为 `active`，取 `ActiveRun` 进入 Drive。`attempt_failed` 时返回该状态，由 Application 选择 Retry 或 Settle。

**TRN-STP-1** Stop 要求 Turn 为 `active`。Coordinator 提交 `CancelRun{Reason:ReasonCancelled}`，并在 `CommitRequest.Attach` 中附加 `twilight/turn/failed{Settlement:stopped, FailureClass:"cancelled"}`；两者在同一 commit 可见。结算 Turn 是 Turn 层的决定，由发起 Stop 的 Coordinator 声明，Run 事实与 companion 不推断它。Application 直接提交的 `CancelRun` 不附加结算事件，Turn 进入 `attempt_failed`。Stop 时仍在 `PendingInputs` 中、尚未被 Prepare 消费的输入已经 delivered 到该 Turn：随后 Retry 会把它们与其他已 delivered 输入一起重放给新 attempt；Settle 则让它们随该 Turn 一起结束，不再进入任何模型请求。

**TRN-STP-2** Cancel CommandID = `Digest("twilight/turn/cancel-run", SessionID, TurnID, RunID, ReasonCancelled)`。StopRequest.Reason 供审计。

**TRN-STL-1** Settle 要求 Turn 为 `attempt_failed`，追加 `twilight/turn/failed{Settlement:failed, FailureClass}`。CommitID 由 `Digest("twilight/turn/settle", SessionID, TurnID, RunID)` 派生。

## 6. companion：Run 事实到对话内容

Run 事实只保存执行状态与内容 digest（RUN-WIR-4）。模型文本、工具调用与工具输出以 chatlog 事件形式与产生它们的 Run 事实写在同一 SessionCommit。`run.Runtime.Commit` 在 Decide 之后、写入之前调用注入的 `run.Companion`，把本 commit 的 facts 与 command 携带的 transient 内容映射为 `run.ModuleEvent`，追加在 Run facts 之后；随后整个 group 经 `SemanticAppender` 的 codec、Binding admission 与 claim 写入（EXT-APP-3）。接口定义在 `agent/run`（第 5 节）；本模块提供实现 `CompanionV1`，它把 `CompanionRequest.Owner` 解释为 TurnID。

**TRN-CMP-1** `Map` 为确定性纯函数，不做 IO；时间取 `CompanionRequest.RecordedAtUnixMilli`。输出事件不携带 EventID，由 Appender 按所在 commit 的 CommitID 与位置赋值（EXT-APP-5）；条目自身的 identity（AssistantID、ToolResultID）按 TRN-MAP-2 派生。同一 commit 重放得到同一 group。companion 事件可以携带 `ReferencePart`；其 Binding 由 Appender 在同一事务 admission 并建立 claim，Runtime 不另行处理。

**TRN-CMP-2** v1 映射：

| Run fact | companion event |
|---|---|
| `ModelStepCompleted` | `twilight/chatlog/assistant{TurnID, Parts: text, reasoning, tool_call*, SourceDigest}` |
| `ToolCallCompleted` / `ToolCallAnswered` | `twilight/chatlog/tool_result` status=`success` |
| `ToolCallFailed` Outcome=`Known` | `twilight/chatlog/tool_result` status=`error` |
| `ToolCallFailed` Outcome=`Unknown` 或 class=`effect_unknown` | `twilight/chatlog/tool_result` status=`unknown` |
| `RunEnded(completed)` | `twilight/turn/completed{TurnID, RunID}` |

其余 fact 不产生 companion。`RunEnded(failed)` 与 `RunEnded(stopped)` 都不由 companion 结算 Turn：没有附加结算事件时 Turn 进入 `attempt_failed`，由 Retry 或 Settle 决定；Coordinator.Stop 以 `Attach` 声明 stopped 结算（TRN-STP-1）。模型无 tool call 但 Run 有 pending 输入时不产生 `RunEnded`（RUN-MCH 表），companion 只写 assistant，Turn 保持 `active`。

**TRN-MAP-2** `AssistantID = Digest("twilight/chatlog/assistant-id", TurnID, ModelStepID, CompanionVersion)`。`ToolResultID = Digest("twilight/chatlog/tool-result-id", TurnID, CallID, CompanionVersion)`。assistant 的 ToolCall 顺序与模型结果一致；`ToolCallPart` 携带 `CallID` 与 `ProviderCallID`。tool_result 以 CallID 与同 Turn 的 call 配对。CallID 由 Run 从 `(ModelStepID, index)` 派生，同一 Turn 内不跨 ModelStep 复用。

**TRN-MAP-3** assistant 正文与工具输出来自 command 携带的冻结值。`Assistant.SourceDigest` 等于 `ModelStepCompleted.ResultDigest`，`ToolResult.SourceDigest` 等于 `ToolCallCompleted.OutputDigest` 或 `ToolCallAnswered.ResponseDigest`；chatlog 条目自身的 `Digest` 仍按 CHT-COD-3 覆盖 parts。Runtime 在写入前校验这一等式（RUN-CMT-3 第 9 步）。

**TRN-MAP-4** Known 对应 `error`；Unknown 对应 `unknown`。v1 companion 不写 `tool_result_superseded`。

## 7. recovery

**TRN-REC-1** 恢复扫描 `twilight/turn/surface` 中 `active` 与 `attempt_failed` 的 Turn。

**TRN-REC-2**

| 情形 | 动作 |
|---|---|
| `started` 已提交、进程在 Drive 前退出 | Resume |
| Loop 提交响应丢失 | Loop 以 ClaimStore 中的 Claim 重放（RUN-LOP-3）；Runtime 按 `(SessionID, CommitID)` 幂等 |
| 模型 Executing、lease 过期 | `RecoverExpired` 提交 `RecoverModelExecution`；Run 保持 Active，同一 RunID 以同一冻结请求继续 |
| 工具效果未知 | lease 过期后 `RecoverExpired` 提交该 call 的 Unknown，companion 写 status=`unknown`；Run 保持 Active |
| Run 已 `failed`、Turn 未结算 | Turn 为 `attempt_failed`；Application 选择 Retry 或 Settle |
| Stop commit 响应丢失 | 以同一 Cancel CommandID 重放 |
| Deliver 中某条输入的 commit 响应丢失 | 以同一 input CommandID 重放，得到 already-applied 后继续剩余条目 |
| Start 或 Retry 响应丢失 | 以同一 CommitID 重放，得到 already-applied |
| binding 缺失 | 返回 `binding_unavailable`；Turn 状态不变 |

**TRN-REC-3** 没有跨存储的对账：Run 事实、companion 内容、claim 与 Turn 结算在同一事务，要么全部可见要么全部不可见。

## 8. conformance

- **TRN-SCP-1 至 TRN-SCP-6**：一 Turn 至多一个非终态 Run、Source `twilight`、ModuleID `turn`、无隐藏状态、run 不依赖 turn；
- **TRN-ID-1 至 TRN-EVT-3**：所列 EventType、`twilight/turn/plan`、`twilight/turn/run` 派生 RunID、每 Turn 至多一条结算事件、不同时间戳的重试幂等；
- **TRN-PRJ-1**：surface 状态机，`active` 与 `attempt_failed` 的判定，`AttemptView.End` 来自 `run/ended`，`InputIDs` 含 Deliver 追加的输入；
- **TRN-STR-1 至 TRN-RTY-3**：Start group 顺序与原子性、Input 状态与 Content 核对、Retry 前置条件与全部已 delivered 输入的重放、Attempt 递增、幂等 CommitID、失败 attempt 内容保留在 stream；
- **TRN-DLV-1 至 TRN-DLV-3**：Deliver 前置条件、`input_accepted` 与 `input_delivered` 同 commit、Run 在 Executing 与 Waiting 时的输入入队、与最后一步结果并发时的两种定序结果、不打断进行中的调用；
- **TRN-DRV-1 至 TRN-STL-1**：Drive disposition、Stop 以 Attach 单 commit 结算、Application 的 Cancel 进入 `attempt_failed`、Settle 前置条件；
- **TRN-CMP-1 至 TRN-MAP-4**：companion 纯函数、v1 映射表、`SourceDigest` 等于 Run fact 记录值、companion 中的 ReferencePart 经 admission 并建立 claim、同 commit 可见性；
- **TRN-REC-1 至 TRN-REC-3**：上表恢复情形、崩溃后 Resume、无跨存储对账。
