# Twilight Agent Turn 协议

状态：设计草案。

本文定义 `agent/turn`：回合生命周期与 Run linkage。“必须”“应该”为协议约束。Run 的 authority 是 [agent-run.md](agent-run.md)；对话内容的 authority 是 [agent-session-chatlog.md](agent-session-chatlog.md)。

## 1. 模型与范围

| Concern | Canonical owner | Coordinator |
|---|---|---|
| 回合存在与结束 | `twilight/turn/` events | Start / Stop / settlement |
| `MachineState` | Run Runtime | Load / Commit / Record |
| `TransitionRecord` | Run Runtime | fold、materialize 完整前缀 |
| 对话内容 | `twilight/chatlog/` events | Start 时 delivered input；Drive 后写出 assistant / tool_result |
| Application policy | Application | binding、driver、产品策略 |

**TRN-SCP-1** Source 为 `twilight`，ModuleID 为 `turn`。`Coordinator` 协调一个 Turn 与一个 primary Run。retry 复用该 RunID。replacement 经 `twilight/turn/superseded` 指向新 Turn 与新 RunID。同一 Turn 上第二个 primary Run 为 conflict。

**TRN-SCP-2** primary RunID 在该 Turn 内存活期内保持不变。subagent 使用独立 Session 与独立 Turn。

**TRN-SCP-3** Coordinator 从 Session facts 与 `Runtime.Record` 重建。

**TRN-SCP-4** Session 写入经 `extension.SemanticAppender`。Artifact 由其 owner 管理。

**TRN-SCP-5** Application 管理 model、provider、tool、prompt、token、approval、queue 与并发。Coordinator 按 persisted binding 解析 driver。参考 Planner 每次 Plan 使用 Binding 的 `ModelRef`。

**TRN-SCP-6** Start 之前建立 immutable execution binding。Session 保存 `ID` 与 `Digest`。密钥与 client 留在进程内。Resolve 失败返回 `binding_unavailable`。公开字段见 [参考组装](agent-reference-assembly.md)。

## 2. identity、计划与事件

```go
type TurnID string
type TurnRef struct { SessionID session.SessionID; TurnID TurnID }
type ExecutionBindingRef struct { ID ExecutionBindingID; Digest es.Digest }
type MapperVersion string
type SourceFactID string

type Settlement string
const (
    SettlementCompleted Settlement = "completed"
    SettlementFailed Settlement = "failed"
    SettlementStopped Settlement = "stopped"
)

type ResumeDisposition string
const (
    ResumeWaitingForResponse ResumeDisposition = "waiting_for_response"
    ResumeWaitingForRecovery ResumeDisposition = "waiting_for_recovery"
    ResumeFinished            ResumeDisposition = "finished"
)

type RunHeadRef struct { RunID run.RunID; Revision uint64; TransitionDigest es.Digest }
type ResultReference struct {
    RunID run.RunID; Revision uint64; EventIndex uint16
    EventType string; EventDigest es.Digest
}

type RunCreateSpec struct {
    Run run.NewRun
    InitialInputs []run.AgentInput
    ExecutionBinding ExecutionBindingRef
    MapperVersion MapperVersion
}

type StartedPayload struct {
    TurnID TurnID
    InputIDs []chatlog.InputID
    RunID run.RunID
    Create RunCreateSpec
    PlanDigest es.Digest
}
type CompletedPayload struct {
    TurnID TurnID
    RunID run.RunID
    Settlement Settlement
    Head RunHeadRef
    Result ResultReference
}
type FailedPayload struct {
    TurnID TurnID
    RunID run.RunID
    Settlement Settlement
    FailureClass string
    Head RunHeadRef
    Result ResultReference
}
type SupersededPayload struct {
    TurnID TurnID
    ReplacementTurnID TurnID
}
```

**TRN-ID-1** `TurnRef`、RunID、binding ID、MapperVersion、InputID 与 digest 非空且稳定。`ExecutionBindingRef` 编码公开 identity 与 digest。

**TRN-ID-2** `Head` 等于 terminal `RunRecord` 的 head。`Result` 指向该 record 中的 `RunEnded`。

**TRN-ID-3** `RunCreateSpec.Run.RunID` 等于 payload 的 RunID。`NewRun` 经 `run.ValidateNewRun`。InitialInputs 随后以 `AcceptInput` 进入 Run。

**TRN-ID-4** `PlanDigest = Digest("twilight/turn/plan", canonical(RunCreateSpec))`。

```go
type StartOperationDigest es.Digest
func DigestStartOperation(
    sessionID session.SessionID, turnID TurnID, runID run.RunID,
    planDigest es.Digest, inputIDs []chatlog.InputID,
) (StartOperationDigest, error)
```

**TRN-ID-5** `StartOperationDigest = Digest("twilight/turn/start-operation", SessionID, TurnID, RunID, PlanDigest, ordered InputIDs)`。用户正文 identity 在对应 `twilight/chatlog/input_submitted` 中。

**TRN-EVT-1** EventType：

```text
twilight/turn/started
twilight/turn/completed
twilight/turn/failed
twilight/turn/superseded
```

`started` 含 RunCreateSpec。`completed` 与 `failed` 含 Settlement 与 Result。unsettled linkage 是尚未 completed、failed 或 superseded 的 `started`。

**TRN-EVT-2** Start 的 EventID 与 CommitID 由 StartOperationDigest、event kind、group ordinal 派生。retry 复用同一 identity。

**TRN-EVT-3** resolved stream 内每个 TurnID 至多一条 unsettled `started`。相同 canonical payload 幂等；差异为 conflict。

**TRN-PRJ-1** ProjectionID 为 `twilight/turn/surface`。消费 `started`、`completed`、`failed`、`superseded`：

```go
type TurnView struct {
    TurnID TurnID
    Status string // started | completed | failed | stopped | superseded
    InputIDs []chatlog.InputID
    RunID run.RunID
    Settlement Settlement
    ReplacementTurnID TurnID
}
type TurnSurface struct {
    Order []TurnID
    Turns map[TurnID]TurnView
}
```

`Settlement=stopped` 时 `Status` 为 `stopped`。UI 按 `TurnID` 连接 `twilight/chatlog/surface` 的条目。

## 3. API

```go
type Coordinator struct {
    Sessions session.Store
    Appender extension.SemanticAppender
    Runtime run.Runtime
    Bindings ExecutionBindingRegistry
    Mapper FactMapper
}
type DriveRequest struct { Ref TurnRef; RunID run.RunID }
type RunDriver interface { Drive(context.Context, DriveRequest) error }
type ExecutionBindingRegistry interface { Resolve(ExecutionBindingRef) (RunDriver, error) }

type Service interface {
    Start(context.Context, StartRequest) (StartResponse, error)
    Resume(context.Context, ResumeRequest) (ResumeResponse, error)
    Stop(context.Context, StopRequest) (StopResponse, error)
}
type StartRequest struct {
    Ref TurnRef
    Run run.NewRun
    InputIDs []chatlog.InputID
    InitialInputs []run.AgentInput
    ExecutionBinding ExecutionBindingRef
    MapperVersion MapperVersion
}
type StartResponse struct {
    Ref TurnRef; RunID run.RunID; PlanDigest es.Digest; Operation StartOperationDigest
    Created bool; Disposition ResumeDisposition; Result *ResultReference; Waiting []run.ResponseRequest
}
type ResumeRequest struct { Ref TurnRef }
type ResumeResponse struct {
    Ref TurnRef; RunID run.RunID; Disposition ResumeDisposition
    Result *ResultReference; Waiting []run.ResponseRequest
}
type StopRequest struct { Ref TurnRef; Reason string }
type StopResponse struct { Ref TurnRef; RunID run.RunID; Result *ResultReference }
```

**TRN-API-1** Coordinator 从 Session facts 与 `Runtime.Record` 接续。

**TRN-API-2** Registry 用同一 `run.Runtime` 组装 driver。

**TRN-API-3** Run 的写入经 `run.Runtime`。

**TRN-API-4** Create、conflict、missing、corrupt 按 Run contract 处理。创建计划来自 persisted `started`。

**TRN-API-5** DTO 为值语义。`Waiting` 为 snapshot 的 `WaitingCalls`。`Disposition` 为 `ResumeWaitingForResponse`、`ResumeWaitingForRecovery` 或 `ResumeFinished`。`NeedsRecovery` 为 true 时 Coordinator 返回 `ResumeWaitingForRecovery`；Application 调用 `Runtime.RecoverExpired` 后再 Resume。

**TRN-API-6** `twilight/turn/superseded` 由 Application 追加，指向 replacement Turn 与新 RunID。Coordinator 的 Start / Resume / Stop 不写该事件。同一 Turn 上第二个 primary Run 仍为 conflict。

## 4. Start

**TRN-STR-1** Start 验证请求、追加 Session group、调用 `Runtime.Create`。causation 等于 `NewRun.CausationID`。

**TRN-STR-2** StartRequest：

1. Ref、RunID、binding ref、mapper version 非空；
2. InputIDs 与 InitialInputs 等长、无重复、顺序相同；
3. `InitialInputs[i].ID = InputIDs[i]`，Payload 等于对应 `twilight/chatlog/input_submitted` 的 Content；
4. Input 为 submitted，可 delivered。

**TRN-STR-3** 一次原子 commit，顺序为：

```text
twilight/turn/started{TurnID, InputIDs, RunID, Create, PlanDigest}
twilight/chatlog/input_delivered{InputIDs[0], TurnID}
...
twilight/chatlog/input_delivered{InputIDs[n-1], TurnID}
```

InputIDs 为空时 group 仅含 `twilight/turn/started`。

**TRN-STR-4** 派生 RunCreateSpec、PlanDigest、StartOperationDigest 与 group identity，再 `AppendSemantic`。相同 identity 为 applied / already-applied。head conflict 时 CAS rebase，identity 与 payload 保持不变。

**TRN-STR-5** append 成功后 replay 解析 linkage，再 `Runtime.Create`。Created 为 true 或 false 均进入 Resume。

**TRN-STR-6** Create 之后调用 Resume。

## 5. Resume、Drive 与 Stop

**TRN-RSM-1** Resume 查找 Ref 上唯一 unsettled `twilight/turn/started`。

**TRN-RSM-2** 对 persisted `Create.Run` 再 Create，然后 `EnsureInitialInputs`。

**TRN-RSM-3** 按序对 InitialInputs：Load，以 `DeriveInputCommandID` 提交 `AcceptInput`。Accepted 与 AlreadyApplied 均前进。结果未知时先 Record。

**TRN-RSM-4** payload 或顺序与 persisted spec 不一致为 conflict。

**TRN-RSM-5** 投递完成后 Record，`MaterializeAll`，再 `driver.Drive`。Drive 返回后再 Record 与 `MaterializeAll`，并据此设置 Disposition。`NeedsRecovery` 为 true 时为 `ResumeWaitingForRecovery`；仅有 WaitingCalls 时为 `ResumeWaitingForResponse`。

**TRN-RSM-6** EventSink 的 `text_delta` / `reasoning_delta` 为临时观察。Waiting 由 Application 提交 `ApproveToolCall` / `RejectToolCall` / `SubmitToolResponse` 后再次 Resume。

**TRN-STP-1** Stop 解析 linkage。terminal Run 走 Record、materialize、settlement。active Run 提交 `CancelRun{Reason:ReasonCancelled}`。

**TRN-STP-2** Cancel CommandID = `Digest("twilight/turn/cancel-run", SessionID, TurnID, RunID, ReasonCancelled)`。StopRequest.Reason 供审计。

**TRN-STP-3** Commit 结果未知时先 Record。已应用则 MaterializeAll，再按 `RunEnded` 写入 `twilight/turn/completed` 或 `twilight/turn/failed`。

## 6. settlement

**TRN-SET-1** `RunRecord` 为 materialize、recovery、settlement 的一致读取。

**TRN-SET-2** 终态取最后一个 `RunEnded`。

**TRN-SET-3**

| RunEnded | EventType | Settlement |
|---|---|---|
| `RunCompletedEnd` | `twilight/turn/completed` | `completed` |
| `RunFailedEnd` | `twilight/turn/failed` | `failed` |
| `RunStoppedEnd` | `twilight/turn/failed` | `stopped`（`FailureClass:"cancelled"`） |

**TRN-SET-4** settlement 的 CommitID / EventID 由 ResultReference 派生。相同 reference 幂等。

**TRN-SET-5** Turn 尚未 `completed`/`failed` 时，`ToolCallFailed{Unknown}` 按 TRN-MAP-1 写成 `twilight/chatlog/tool_result`（status=`unknown`）。Turn 终态只由 `RunEnded` 按上表写入。

## 7. materialization

```go
type FactMapRequest struct {
    TurnID TurnID; RunID run.RunID; MapperVersion MapperVersion
    Prefix []run.TransitionRecord
    TargetRevision uint64; TargetTransitionDigest es.Digest
}
type FactMap struct { SourceFacts []SourceFactID; Events []extension.TypedEvent }
type FactMapper interface { Map(FactMapRequest) (FactMap, error) }
```

**TRN-MAT-1** `MaterializeAll` 按 revision 1 至 head 递增。每次 Map 的输入是截至 target 的完整 prefix。EventSink delta 留在观察面。

**TRN-MAT-2**

```text
SourceFactID = Digest("twilight/turn/source-fact",
    RunID, Revision, TransitionDigest, Index, Type, EventDigest)
```

**TRN-MAT-3** materialization CommitID 的 domain 为 `twilight/turn/materialize`；输出 EventID 的 domain 为 `twilight/turn/materialized-event`。

**TRN-MAT-4** Map 为确定性纯函数。MapperVersion 为协议输入。

**TRN-MAT-5** 空 Events 时 SourceFacts 为空，该 revision coverage 完成。非空 batch 先 `LookupCommit`。

**TRN-MAT-6** outbox 优化投递；coverage 以 Session facts 与 RunRecord 为准。

### 7.1 v1 映射

**TRN-MAP-1** 下表列出产生 chatlog events 的 AgentEvent。其余 AgentEvent 的 FactMap 为空。

| AgentEvent | chatlog event |
|---|---|
| `ModelStepCompleted` | `twilight/chatlog/assistant` |
| `ToolCallCompleted` / `ToolCallAnswered` | `twilight/chatlog/tool_result` status=`success` |
| `ToolCallFailed` Outcome=`Known` | `twilight/chatlog/tool_result` status=`error` |
| `ToolCallFailed` Outcome=`Unknown` 或 class=`effect_unknown` | `twilight/chatlog/tool_result` status=`unknown` |

`ModelStepPrepared` 期间 EventSink 可发送 `text_delta` / `reasoning_delta`。回合结束由 `twilight/turn/completed` 或 `twilight/turn/failed` 表达。

**TRN-MAP-2** `AssistantID = Digest("twilight/chatlog/assistant-id", TurnID, ModelStepID, MapperVersion)`。`ToolResultID = Digest("twilight/chatlog/tool-result-id", TurnID, CallID, MapperVersion)`。assistant 的 ToolCall 顺序与模型结果一致。tool_result 与同 Turn 的 call 配对。同一 Turn 内 CallID 不得跨 ModelStep 复用，否则 `ToolResultID` 冲突。

**TRN-MAP-3** Known 对应 `error`；Unknown 对应 `unknown`。

**TRN-MAP-4** assistant 正文来自冻结的 model result。

## 8. recovery

**TRN-REC-1** 按 `(SessionID,TurnID,RunID)` 扫描 unsettled `twilight/turn/started` 并 Resume。

**TRN-REC-2**

| 情形 | 动作 |
|---|---|
| `started` 已提交、Run 缺失 | Create persisted NewRun 后 Resume |
| Create 响应丢失 | 同一 NewRun 再 Create |
| InitialInputs 仅前缀 | 按 derived command 继续 AcceptInput |
| transition 已提交、chatlog 落后 | MaterializeAll |
| Map 为空 | 该 revision coverage 完成 |
| materialize 响应丢失 | LookupCommit |
| Run 已终态、settlement 缺失 | 按 RunEnded 写 `twilight/turn/completed` 或 `twilight/turn/failed` |
| 模型 Executing、lease 过期 | `RecoverExpired` 提交 `RecoverModelExecution`；Run 保持 Active，同一 Turn、同一 RunID 继续 |
| 模型结果未知 | 以 Record 为准 |
| 工具效果未知 | 对该 Executing call materialize status=`unknown`；Run 保持 Active，同一 Turn、同一 RunID 继续。lease 过期后 `RecoverExpired` 提交该 call 的 Unknown |
| Record 与并发 Commit | 重读 Record |

**TRN-REC-3** Commit、Create、append 结果未知时先查 Runtime.Record 或 Session LookupCommit。

**TRN-REC-4** binding 缺失返回 `binding_unavailable`。已终态 Run 继续 materialize 与 settle。

**TRN-REC-5** 执行推进遵循 MachineState 与 grant。

## 9. conformance

- **TRN-SCP-1 至 TRN-SCP-6**：一 Turn 一 primary Run、Source `twilight`、ModuleID `turn`；
- **TRN-ID-1 至 TRN-EVT-3**：所列 EventType 与 `twilight/turn/plan`；
- **TRN-API-1 至 TRN-API-5**：StartRequest 含 InputIDs 与 InitialInputs；
- **TRN-STR-1 至 TRN-STR-6**：Start group 为 `twilight/turn/started` 加 `twilight/chatlog/input_delivered*`；
- **TRN-RSM-1 至 TRN-STP-3**：unsettled `started`、Drive、`twilight/turn/cancel-run`；
- **TRN-SET-1 至 TRN-SET-5**：settlement 为 `twilight/turn/completed` 或 `twilight/turn/failed`；
- **TRN-MAT-1 至 TRN-MAP-4**：v1 表；
- **TRN-REC-1 至 TRN-REC-5**：上表恢复情形。
