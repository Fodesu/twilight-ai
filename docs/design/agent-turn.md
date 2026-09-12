# Twilight Agent Turn 协议

状态：设计草案。本文是 Turn 协议的目标设计。Coordinator 只做协议提交与状态读取（Start / Deliver / Retry / Stop / Settle / Status），驱动属宿主；写入经 `writer.Writer`、以 `Seq` 定位、恢复走接管处置。Run 事实与 Turn、Chatlog 事件同在一条 Session stream。

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
| Application policy | Application | profile、driver、retry、context 策略、产品策略 |

**TRN-SCP-1** Source 为 `twilight`，ModuleID 为 `turn`。一个 Turn 与它的全部 Run attempt 在同一 Session stream 内。`Coordinator` 创建 Turn、创建 attempt、在回合中途投递输入、驱动 Run、结算 Turn。turn 依赖 run；run 不依赖 turn，Run 事实中的 `OwnerID` 由本模块以 `TurnID` 填充。本模块的 `Requires`（EXT-REG-4）为：`run`，消费 `twilight/run/created` v1、`twilight/run/input_accepted` v1 与 `twilight/run/ended` v1；`chatlog`，只要求存在。

**TRN-SCP-2** Turn 与 Run 的关系为 1:N，不变量为同一 Turn 至多一个非终态 Run：

| 动作 | 语义 | RunID |
|---|---|---|
| resume | 继续一个非终态 Run（进程重启、接管、Waiting 响应后） | 不变 |
| retry | 前一 Run 已终结且未 completed，同一 Turn 再开一个 attempt | 新 RunID，`Attempt` 加 1 |
| replace | 输入内容被替换，`twilight/turn/superseded` 指向新 Turn | 新 Turn、新 RunID |
| regenerate | 已 completed 的回答需要重新生成：新 Turn（可经 `superseded` 关联）或 Session fork；原 Turn 不变 | 新 Turn、新 RunID，或新 stream |

四种动作的持久性语义在第 7 节 TRN-DUR-1 至 4 逐条区分。subagent 使用独立 Session 与独立 Turn。

**TRN-SCP-3** Coordinator 没有隐藏状态。它从 `twilight/turn/surface` 投影与 `twilight/run/machine` 投影重建。

**TRN-SCP-4** Turn 自己的写入经该 Session 的 `writer.Writer.Commit`；Run 事实的写入经 `run.Runtime`，后者经同一个 Writer 落在同一 `session.Store`（EXT-SCP-1）。Coordinator 与 Runtime 经 `writer.Writers` 取得 Writer（EXT-WRT-6）。Artifact 由其 owner 管理。

**TRN-SCP-5** Application 管理 model、provider、tool、prompt、token、approval、queue、retry 决策与并发。宿主按 persisted profile 解析 driver 并驱动（REF-DRV-1）。参考 Planner 每次 Plan 使用 Profile 的 `ModelRef`。

**TRN-SCP-6** Start 之前建立 immutable execution profile。Session 保存 `ProfileRef{ID, Digest}`。密钥与 client 留在进程内。Resolve 失败返回 `profile_unavailable`。公开字段与 digest 边界见 [参考组装](agent-reference-assembly.md)。

## 2. identity 与事件

```go
type TurnID string
type TurnRef struct { SessionID session.SessionID; TurnID TurnID }
type ProfileRef struct { ID ProfileID; Digest es.Digest }
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
    Profile ProfileRef
    Companion CompanionVersion
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

**TRN-ID-1** `TurnRef`、RunID、profile ID、CompanionVersion、InputID 与 digest 非空且稳定。

**TRN-ID-2** `PlanDigest = Digest("twilight/turn/plan", TurnID, Profile.Digest, Companion, ordered InputIDs)`。PlanDigest 只参与 TRN-ID-3 的派生，不落盘：`started` payload 的每个字段都是它的 preimage 成员，落盘该 digest 不提供额外判定。

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

**TRN-EVT-2** 本模块产生的事件（含 companion 与 Attach 产生的）没有独立 EventID，`Seq` 即身份（SES-WIR-1）；同一次写入的事件共用 CommitID。Start 的 CommitID 由 StartOperationDigest 派生；Retry、Settle、Stop 的 CommitID 见各自条目。同 CommitID 相同 canonical payload 为 already-applied；差异为 conflict（EXT-WRT-2）。事件时间戳不参与幂等判定。

**TRN-EVT-3** stream 内每个 TurnID 至多一条 `started`，至多一条 `completed` / `failed` / `superseded`。

**TRN-PRJ-1** ProjectionID 为 `twilight/turn/surface`。消费 `twilight/turn/started|completed|failed|superseded` 与 `twilight/run/created|input_accepted|ended`，其他事件按 EXT-PRJ-2 处理：

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
    SchemaVersion uint16 // created.SchemaVersion；Coordinator 据此构造该 attempt 的 command envelope
    End *run.RunEnd      // 非终态时为 nil
}
type TurnView struct {
    TurnID TurnID
    Status TurnStatus
    InputIDs []chatlog.InputID // started 的初始输入，加此后经 Deliver 进入任一 attempt 的输入，按 accepted 顺序去重
    Profile ProfileRef
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
    Writers writer.Writers // 每个方法按 Ref.SessionID 取 Writer：写入经 Commit，读取经 Projections()
    Runtime run.Runtime
}

// Service 只做协议提交与状态读取；驱动 Run 属宿主（REF-DRV）。
// 每个方法在提交落盘后立即返回，响应反映已提交的状态。
type Service interface {
    Start(context.Context, StartRequest) (TurnResponse, error)
    Deliver(context.Context, DeliverRequest) (TurnResponse, error)
    Retry(context.Context, RetryRequest) (TurnResponse, error)
    Stop(context.Context, StopRequest) (TurnResponse, error)
    Settle(context.Context, SettleRequest) (TurnResponse, error)
    Status(context.Context, TurnRef) (TurnResponse, error)
}
type StartRequest struct {
    Ref TurnRef
    Inputs []run.AgentInput // ID 为已 submitted 的 InputID，Payload 等于其 Content
    Profile ProfileRef
    Companion CompanionVersion
}
type DeliverRequest struct { Ref TurnRef; Inputs []run.AgentInput } // 回合中途追加输入
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

宿主在该词汇表上扩展 `already_driving`（`ref.ResumeAlreadyDriving`，REF-DRV-1）：输入已提交、同 Run 的另一个本地驱动者继续推进。Coordinator 本身不产生该值。

**TRN-API-1** Coordinator 经 Writer 的 `Projections()` 读取 `twilight/turn/surface` 与 `twilight/run/machine` 两个投影（EXT-PRJ-4）；每个方法先读投影再决定动作。Coordinator 不持有 `session.Store`。

**TRN-API-2** Run 的写入只经 `run.Runtime`。driver 的组装与解析在宿主（REF-BND-2）。

**TRN-API-3** DTO 为值语义。`Waiting` 为 `twilight/run/machine` 的 `WaitingCalls`。`NeedsRecovery` 为 true 时返回 `ResumeWaitingForRecovery`；这只出现在接管处置之前，宿主调用 `Runtime.RecoverInterrupted`（RUN-CMT-7）后再驱动（REF-DRV-1）。

**TRN-API-4** `twilight/turn/superseded` 由 Application 追加。Coordinator 的方法不写该事件。superseded 的 Turn 若仍有非终态 Run，Application 必须先 Stop。

## 4. Start 与 Retry

**TRN-STR-1** StartRequest：

1. Ref、profile ref、companion version 非空；
2. `Inputs` 无重复 ID；每个 ID 对应 chatlog 中状态为 submitted 的 Input，Payload 等于其 Content（Coordinator 经 chatlog surface 投影核对）。

`started.InputIDs` 与 `input_delivered`、`input_accepted` 的顺序都取 `Inputs` 的顺序。

**TRN-STR-2** Start 是一次原子 commit，顺序为：

```text
twilight/turn/started{TurnID, InputIDs, Profile, Companion}
twilight/chatlog/input_delivered{InputIDs[0], TurnID}
...
twilight/chatlog/input_delivered{InputIDs[n-1], TurnID}
twilight/run/created{RunID, Owner:TurnID, Attempt:1, SchemaVersion, CausationID}
twilight/run/input_accepted{RunID, InputIDs[0], Payload}
...
twilight/run/input_accepted{RunID, InputIDs[n-1], Payload}
```

InputIDs 为空时 group 为 `started` 加 `created`。`created` 与 `input_accepted` 的 facts 由 `run.Protocol.BuildCreateGroup` 构造（RUN-NEW-1），Coordinator 只负责把它们放入 group。

**TRN-STR-3** 派生 PlanDigest、StartOperationDigest、RunID 与 group identity，再经 `Writer.Commit` 写入一组。相同 identity 为 applied / already-applied；Writer 串行执行全部写入，不存在 head conflict。

**TRN-STR-4** append 成功后 Start 返回已提交状态的响应；驱动新 Run 是宿主的下一步（REF-DRV-1）。

**TRN-RTY-1** Retry 要求投影中该 Turn 为 `attempt_failed`。commit 为 `twilight/run/created{Attempt: n+1}` 加该 Turn 已 delivered 的全部 Input 的 `input_accepted`，顺序与 `TurnView.InputIDs` 相同（初始输入在前，中途 Deliver 的输入按 accepted 顺序在后）；payload 与首次 delivered 时相同，仅 RunID 与 Attempt 不同。Turn 为其他状态时 Retry 返回 conflict。

**TRN-RTY-2** Retry 的 CommitID 由 `Digest("twilight/turn/retry", SessionID, TurnID, Attempt)` 派生。

**TRN-RTY-3** 失败 attempt 已提交的 assistant 与 tool_result 保留在 stream 中，协议不删除、不隐藏。它们是否进入后续 attempt 的模型请求是 Application 策略，由 Planner 依据 turn surface 的 attempt 状态决定（REF-PLN-6）；协议只保证内容可用。

## 5. Deliver、Status 与 Stop

**TRN-DLV-1** Deliver 在回合中途追加输入，要求 Turn 为 `active`；`attempt_failed`、已结算或不存在的 Turn 返回 conflict，输入保持 `submitted`，由 Application 决定开新 Turn。输入的校验与 TRN-STR-1 第 2 条相同：每个输入必须是 chatlog 中状态为 `submitted` 的 Input 且内容一致，任一不满足即 conflict，整批不写入。该校验读 chatlog surface，在 `Runtime.Commit` 之前、Writer 互斥区之外进行；校验与提交之间输入被 withdraw 的竞态由 chatlog 投影的预折叠兜底——`input_delivered` 对非 `submitted` 的输入折叠失败，整组被拒（EXT-PRJ-1），结果仍是全有或全无。

**TRN-DLV-2** `Inputs` 作为一个批次以一次 `Runtime.Commit` 提交：命令为 `AcceptInput{Inputs}`（有序列表，RUN-MCH-4），`Attach` 为每个输入一条 `twilight/chatlog/input_delivered{InputID, TurnID}`。Run 接受全部输入与 chatlog 把全部输入挂到 Turn 在同一组可见；任一输入被拒则一条都不写。envelope 的 SchemaVersion 取自 turn surface 中该 attempt 的 `SchemaVersion`，`Base` 为零值（`AcceptInput` 不做 hard CAS，RUN-CMT-4）；Deliver 不读取 `twilight/run/machine` 投影。`AcceptInput` 在 Run 的任意非终态都被接受，Deliver 不关心 Run 当前处于哪一步。CommandID 由 RunID 与有序 InputID 列表派生（RUN-WIR-4），同一批次重放幂等；不同批次（含子集或另一顺序）是不同命令，其中已接受过的输入使该批次整体被 Decide 以 conflict 拒绝。

**TRN-DLV-3** Deliver 不取消正在进行的模型调用或工具调用；要打断用 Stop。提交后 Deliver 返回；是否驱动由宿主决定（REF-DRV-1），已在驱动时运行中的 Loop 在下一次 Load 看到 `PendingInputs`。Deliver 与该 Run 的最后一步 `SubmitModelResult` 并发时由 Writer 串行定序：输入先提交，Run 回到 `Open` 继续；结果先提交，Run 已终结，Deliver 得到 `ErrRunTerminal` 并返回 `completed`，该输入未被 delivered。

**TRN-STA-1** Status 是纯读取，disposition 判定的单一来源：读投影设置 `Disposition` 与 `End`。Run 终态为 `ResumeFinished`，`End` 取 surface 中该 attempt 的 `AttemptView.End`；`NeedsRecovery` 为 true 为 `ResumeWaitingForRecovery`；仅有 WaitingCalls 为 `ResumeWaitingForResponse`。宿主驱动结束后调用 Status 组装结果（REF-DRV-1）；Start/Deliver/Retry/Stop/Settle 的响应用同一判定。

**TRN-STA-2** EventSink 的 `text_delta` / `reasoning_delta` 为临时观察。Waiting 由 Application 提交 `ApproveToolCall` / `RejectToolCall` / `SubmitToolResponse` 后再次驱动（REF-DRV-1）。

**TRN-STP-1** Stop 要求 Turn 为 `active`。Coordinator 提交 `CancelRun{Reason:ReasonCancelled}`，并在 `CommitRequest.Attach` 中附加 `twilight/turn/failed{Settlement:stopped, FailureClass:"cancelled"}`；两者在同一 commit 可见。envelope 的 SchemaVersion 与 Deliver 同样取自 `AttemptView`，`Base` 为零值。结算 Turn 是 Turn 层的决定，由发起 Stop 的 Coordinator 声明，Run 事实与 companion 不推断它。Application 直接提交的 `CancelRun` 不附加结算事件，Turn 进入 `attempt_failed`。Stop 时仍在 `PendingInputs` 中、尚未被 Prepare 消费的输入已经 delivered 到该 Turn：随后 Retry 会把它们与其他已 delivered 输入一起重放给新 attempt；Settle 则让它们随该 Turn 一起结束，不再进入任何模型请求。

**TRN-STP-2** Cancel CommandID = `Digest("twilight/turn/cancel-run", SessionID, TurnID, RunID, ReasonCancelled)`。StopRequest.Reason 供审计。

**TRN-STL-1** Settle 要求 Turn 为 `attempt_failed`，追加 `twilight/turn/failed{Settlement:failed, FailureClass}`。CommitID 由 `Digest("twilight/turn/settle", SessionID, TurnID, RunID)` 派生。

## 6. companion：Run 事实到对话内容

Run 事实只保存执行状态与内容 digest（RUN-WIR-4）。模型文本、工具调用与工具输出以 chatlog 事件形式与产生它们的 Run 事实写在同一组（一次 `Append`，同一 CommitID）。`run.Runtime.Commit` 在 Decide 之后、写入之前调用注入的 `run.Companion`，把本组的 facts 与 command 携带的 transient 内容映射为 `run.ModuleEvent`，追加在 Run facts 之后；随后整个 group 经 `Writer` 的 codec、Binding admission 与 claim 写入（EXT-WRT-1、EXT-WRT-3）。接口定义在 `agent/run`（第 5 节）；本模块提供实现 `CompanionV1`，它把 `CompanionRequest.Owner` 解释为 TurnID。

**TRN-CMP-1** `Map` 为确定性纯函数，不做 IO；时间取 `CompanionRequest.RecordedAtUnixMilli`。条目自身的 identity（AssistantID、ToolResultID）按 TRN-MAP-2 派生。同一 command 重放得到同一 group。companion 事件可以携带 `ReferencePart`；其 Binding 由 Writer 在 Append 之前 admission 并建立 claim（EXT-WRT-3），Runtime 不另行处理。

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

**TRN-MAP-3** assistant 正文与工具输出来自 command 携带的冻结值。`Assistant.SourceDigest` 等于 `ModelStepCompleted.ResultDigest`，`ToolResult.SourceDigest` 等于 `ToolCallCompleted.OutputDigest` 或 `ToolCallAnswered.ResponseDigest`；`ToolCallFailed` 产生的 `tool_result` 没有 fact 记录的 digest，其 `SourceDigest` 为空。chatlog 条目自身的 `Digest` 仍按 CHT-COD-3 覆盖 parts。Runtime 在写入前校验非空 `SourceDigest` 的这一等式（RUN-CMT-3 第 9 步）。

**TRN-MAP-4** Known 对应 `error`；Unknown 对应 `unknown`。v1 companion 不写 `tool_result_superseded`。

## 7. recovery

### 7.1 Run 的持久性语义

下面四条区分四种表面相似、语义不同的情形。它们的差别只在两个问题上：哪个身份保持不变，谁做出决定。

| 情形 | 保持不变 | 新建 | 决定者 |
|---|---|---|---|
| 进程崩溃 / 所有权丢失 | Turn、Run、attempt | 无 | 无人：接管处置是协议动作 |
| 语义重试 | Turn | Run（attempt 加 1） | Application |
| 重新生成已提交的回答 | 原 Turn 与其回答 | Turn，或 Session 分支 | Application |
| 外部工具执行中且 owner 丢失 | Turn、Run、该 call 的 Unknown 事实 | 无 | 模型或 Application，从不自动 |

**TRN-DUR-1（崩溃恢复同一 Run）** 进程崩溃或所有权丢失不结束 Run，也不创建 attempt。新 owner 的 `RecoverInterrupted`（RUN-CMT-7）对 Executing 的目标做一次性处置——模型步回到 Prepared，以同一 `RequestDigest` 的冻结请求继续；工具 call 记 Unknown——之后同一 RunID 在同一 Turn 下由宿主 Drive 继续。恢复不改变 Run 的身份、attempt 号或已提交的任何事实。

**TRN-DUR-2（语义重试是新 Run、同一 Turn）** 只有 Run 已终结且未 completed、Turn 处于 `attempt_failed` 时才存在 Retry；Retry 创建 attempt n+1、新 RunID，同一 Turn，重新接受该 Turn 已 delivered 的全部输入（TRN-RTY-1）。协议从不自动 Retry：崩溃恢复走 TRN-DUR-1，Retry 是 Application 的显式决定。失败 attempt 的 assistant 与 tool_result 保留在 stream 中，是否进入新 attempt 的模型请求由 Planner 决定（TRN-RTY-3）。

**TRN-DUR-3（重新生成已提交的回答是新 Turn 或分支）** 已 completed 的 Turn 及其回答是不可变事实：不存在"修改回答""重开同一 Turn"或"对 completed Turn 再开 attempt"。`Start` 要求输入处于 `submitted`（TRN-STR-1），已 delivered 的输入不能再次开 Turn，因此重新生成只有两种形态：(a) 同一 stream 内的新 Turn——Application 提交新 Input（内容可与原输入相同）并 Start；若它在语义上替代原 Turn，以 `twilight/turn/superseded` 关联（TRN-API-4），原回答是否进入上下文由 Planner 决定；(b) 分支——在原 Turn 的 `started` 之前的 Seq 处 fork Session（SES 第 8 节），在新 stream 上开 Turn。两种形态都不改写历史。

**TRN-DUR-4（外部效果未知不等于重试）** owner 丢失时处于 Executing 的工具 call 由接管处置记为 Unknown（RUN-CMT-7），companion 写 status=`unknown` 的 `tool_result`。Unknown 是该 call 的终态事实，协议在任何路径上都不重新执行它：接管处置不执行（它只记录）；下一次 Loop 不执行（start barrier 只启动 Pending call，Executing 与终态 call 永不重跑，RUN-LOP-4）；Retry 不执行（新 attempt 从上下文重新规划步骤，Unknown 结果作为对话内容可见）。外部效果是否已经发生、是否需要重做，由模型依据上下文判断，或由 Application 在带外核实后以 `tool_result_superseded` 换成 `success`/`error`（CHT-ENT-2）；两者都是决定，不是协议的自动行为。`CancelRun` 留下的 `UncertainCalls` 同理。

### 7.2 恢复表

**TRN-REC-1** 恢复扫描 `twilight/turn/surface` 中 `active` 与 `attempt_failed` 的 Turn。

**TRN-REC-2**

| 情形 | 动作 |
|---|---|
| `started` 已提交、进程在驱动前退出 | 新 owner 的 `RecoverInterrupted` 无事可做（Run 在 Open）；宿主 Drive |
| Loop 的 Commit 返回非 sentinel 错误 | Loop 以同一 Claim 重放一次（RUN-LOP-5）；Writer 按 CommitID 幂等 |
| 模型 Executing、owner 进程崩溃 | 新 owner 的 `RecoverInterrupted` 提交 `RecoverModelExecution`（RUN-CMT-7）；Run 保持 Active，同一 RunID 以同一冻结请求继续 |
| 工具 Executing、owner 进程崩溃 | 新 owner 的 `RecoverInterrupted` 提交该 call 的 Unknown，companion 写 status=`unknown`；Run 保持 Active |
| Writer 返回 `ErrOwnershipLost` | 本进程放弃该 Session 的全部 Turn 与 Loop（RUN-CMT-6）；由持有新 Epoch 的进程按上两行接管 |
| Run 已 `failed`、Turn 未结算 | Turn 为 `attempt_failed`；Application 选择 Retry 或 Settle |
| Stop 的 Commit 返回非 sentinel 错误 | 以同一 Cancel CommandID 重放 |
| Deliver 的 Commit 返回非 sentinel 错误 | 以同一批次 CommandID 重放整批，得到 already-applied（TRN-DLV-2） |
| Start 或 Retry 的 Commit 返回非 sentinel 错误 | 以同一 CommitID 重放，得到 already-applied |
| profile 缺失 | 宿主 Drive 返回 `profile_unavailable`（REF-BND-2）；Turn 状态不变 |

**TRN-REC-3** 没有跨存储的对账：Run 事实、companion 内容与 Turn 结算在同一组，`Append` 原子，要么全部可见要么全部不可见。claim 在 Append 之前建立，崩溃只可能留下孤儿 claim，由 artifact 的回收前核对释放（EXT-WRT-3、ART-RET-3）。

## 8. conformance

套件以 `session.Store` 为参数（`agent/turn/turntest`），Memory 与每个 durable adapter 跑同一组断言。Coordinator 只做提交与读取，因此套件不含 Loop、driver、模型或工具桩：Run 的推进由 `run.Runtime` 的 command 提交完成，Application 的 `CancelRun` 制造 `attempt_failed`，`SubmitModelResult` 制造 completed 与 approval 等待。

- **TRN-STR-1 至 TRN-STR-4、TRN-ID-2/3/4、TRN-EVT-2**：缺 profile 或 companion、重复 InputID、未 submitted 的输入、Payload 与 Content 不符各自被拒且不写入；Start 的 group 为 `started`、每输入一条 `input_delivered`、`created{Owner:TurnID, Attempt:1}`、每输入一条 `input_accepted`，CommitID 为 StartOperationDigest，RunID 为 `twilight/turn/run` 派生值；响应为 `active`、attempt 1、无 disposition；不同时间戳的重放为 already-applied 且不写入；同 TurnID 的另一 plan 与第二个活跃 Turn 为 conflict，被拒输入保持 `submitted`。
- **TRN-DLV-1、TRN-DLV-2**：一个批次的全部 `input_accepted` 与 `input_delivered` 在以批次 CommandID 为 CommitID 的同一 commit；Run 的 `PendingInputs` 与 surface 的 `InputIDs` 追加全部输入；同一批次重放不写入；不存在或非 `active` 的 Turn 为 conflict 且输入保持 `submitted`；未提交的输入或内容不一致的输入使整批 conflict，批内其他输入也不写入、Run 的 `PendingInputs` 不变。
- **TRN-RTY-1、TRN-RTY-2、TRN-RTY-3**：`active` 或不存在的 Turn 为 conflict；`attempt_failed` 的 Turn 得到 attempt n+1、`twilight/turn/retry` 派生的 CommitID、`created` 加全部已 delivered 输入按 `InputIDs` 顺序的 `input_accepted`（payload 同首次）；surface 的 `InputIDs` 不重复；失败 attempt 的行原样保留，其 Record 仍可读；重试后 Turn 为 `active`，再次 Retry 为 conflict。
- **TRN-STP-1、TRN-STP-2、TRN-STL-1、TRN-EVT-3**：Stop 的 `CancelRun` 与 `failed{stopped, cancelled}` 在以 Cancel CommandID 为 CommitID 的同一 commit，其中含 `run_ended`；Settle 需要 `attempt_failed`，写 `failed{failed, FailureClass}`，CommitID 为 `twilight/turn/settle` 派生值；已结算（stopped、failed、completed）的 Turn 上 Stop、Retry、Settle、Deliver 一律 conflict；completed 由 companion 在 Run 终结的同一组写入。
- **TRN-STA-1、TRN-API-3**：Open 的 Run 无 disposition 与 Waiting；模型 Executing 为 `waiting_for_recovery`；approval 调用为 `waiting_for_response` 且 `Waiting` 含该请求；completed 与 Application 取消的 Run 为 `finished`，`End` 分别为 completed 与 stopped；不存在的 Turn 为 conflict。
- **TRN-PRJ-1、TRN-EVT-3、TRN-SCP-2/3**：Owner 不是本 Session Turn 的 Run 不进入 surface；第二条 `started`、未知 Turn 的结算、第二次结算、结算后的 `completed` 在 fold 阶段被拒且不写入；`run_ended` 写入 `AttemptView.End` 并使未结算 Turn 进入 `attempt_failed`、清空 `ActiveRun`；`Order` 按 started 顺序；结算后的 Session 没有活跃 Turn。
- **TRN-REC-1、TRN-REC-2、TRN-SCP-3**：`started` 提交后接管，`RecoverInterrupted` 处置 0 个目标，Status 仅从投影重建为 `active`；模型 Executing 时接管，处置 1 个目标后 disposition 不再是 `waiting_for_recovery`；被替代的 Coordinator 的 Deliver 得到 `ErrOwnershipLost` 且不改变输入状态，新 owner 的 Deliver 成功。
- **TRN-CMP-1 至 TRN-MAP-4**：companion 纯函数、v1 映射表、`SourceDigest` 等于 Run fact 记录值、companion 中的 ReferencePart 经 admission 并建立 claim、同组可见性，由 RUN-CMP-2 套件经 Runtime 的组构成观察。
- **TRN-DLV-3** 的并发定序（输入与最后一步结果的两种先后）由 Writer 串行保证，单进程套件不构造并发，以 Deliver 对已终结 Run 的 `completed` 响应作为可观察结果。
- **TRN-DUR-1 至 TRN-DUR-4**：接管后同一 RunID 继续、`Attempt` 不变、Turn 保持 `active`；工具 Executing 时接管，该 call 记 Unknown 后，接管处置、随后的 Loop 与 Retry 的新 attempt 都不再出现该 call 的 `StartToolCall` 或 `ToolCallCompleted`；Retry 得到新 RunID 与 attempt 加 1、Turn 不变；对已 `completed` 的 Turn 以其已 delivered 的输入再次 Start 为 conflict。Unknown call 不被重跑的断言与 Loop 一起在 RUN-LOP-4/5 的套件中验证。
