# Twilight Agent Turn 协议

状态：设计规范。

本文定义 `agent/turn` 对一个 Chatlog Turn 的协调协议；“必须”“应该”均为协议约束。Run Machine、Runtime 与 Loop 的 authority 是 [agent-run.md](agent-run.md)；本文只规定 Turn 如何消费该协议。

## 1. 模型与范围

| Concern | Canonical owner / record | Coordinator action |
|---|---|---|
| `MachineState` | execution authority | 经 shared Runtime 驱动、读取和验证 |
| `TransitionRecord` | canonical Run record | Record、fold、materialize 完整前缀 |
| Session events | verified resolved stream | SemanticAppender 提交、replay linkage 与 settlement |
| Runtime | 唯一 Run access 与 atomic Commit boundary | Create、Load、Commit、Record |
| Application policy | Application | 建立 binding，提供 driver 与业务 policy |

**TRN-SCP-1** `agent/turn.Coordinator` 为 concrete Coordinator，`twilight.turn` 为唯一 first-party `ModuleID`。一个 Turn 恰有一个 primary Run；retry 重用该 RunID。replacement Turn 通过 chatlog replacement graph 建立其 primary Run；同 Turn 新建 primary Run 的请求触发 conflict。

**TRN-SCP-2** primary Run 保持既有 identity。放弃或替换对话时，Application 创建 replacement Turn 与新 RunID，并使用 chatlog replacement graph。实现拒绝旧 Turn 或旧 RunID 的 replacement reuse。subagent 运行于独立 Session、独立 Turn。

**TRN-SCP-3** Coordinator 从 Session facts 和 `Runtime.Record` 重建；`MachineState` 提供 execution authority，`TransitionRecord` 提供 canonical record。Coordinator 编排持久化事实，以 injected capability 执行协调动作；Session、Run、driver、mapper cursor、terminal verdict、队列 lease、provider client、per-Run handle 与额外权限均由各自 canonical owner 提供。

**TRN-SCP-4** Coordinator 通过 `extension.SemanticAppender` 完成所有 Session 写入；Extension 负责 Binding admission、claim 及其恢复。`artifact` 由其 canonical owner 管理。

**TRN-SCP-5** Application 管理 model、provider、tool、prompt、token、approval、queue、channel、并发、重试时机及其他业务 policy。Coordinator 按 persisted binding 解析 driver 并协调协议步骤，历史计划仅按其 persisted choices 执行。

**TRN-SCP-6** Application 在开始前建立 immutable execution binding，并持久化公开 `ID`、`Digest` 与 mapper version。serialized binding 仅含这些公开值；运行时对象、credential、secret、可变 policy 由 Application 管理。binding registry 精确解析持久化 identity；缺失 binding 返回 `binding_unavailable`。

## 2. identity、计划与事件

```go
type TurnRef struct { SessionID session.SessionID; TurnID chatlog.TurnID }
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
type RunRequestedPayload struct {
    TurnID chatlog.TurnID; RunID run.RunID
    Create RunCreateSpec; PlanDigest es.Digest
}
type RunSettledPayload struct {
    TurnID chatlog.TurnID; RunID run.RunID; Settlement Settlement
    Head RunHeadRef
    Result ResultReference
}
```

**TRN-ID-1** `TurnRef`、RunID、binding ID、MapperVersion、chatlog identity 与 digest 必须非空稳定。`ExecutionBindingRef` 仅编码公开 identity 与 digest；secret、token、provider handle、policy 分别由其 owner 管理。

**TRN-ID-2** `RunHeadRef` 指向经验证的 transition head；结果查询使用 `ResultReference`。`RunSettledPayload.Head` 精确等于 terminal `RunRecord` 的 head（RunID、revision、transition digest），该 head transition 包含其 `Result` 所指向的最后一个 terminal `RunEnded`。`ResultReference` 精确指向 terminal `RunEnded` AgentEvent；其 RunID、revision、index、type、digest 与 `RunRecord` 的该 event 逐字段相同。

**TRN-ID-3** `RunCreateSpec.Run.RunID` 等于 requested payload 的 RunID，完整 `NewRun` 通过 `run.ValidateNewRun`。InitialInputs 为有序 immutable admission data，随后以 `AcceptInput` transition 进入 Run；revision-0 header 与 Run collision identity 保持稳定。

**TRN-ID-4** `PlanDigest` 使用 `twilight.turn/plan/v1` domain，覆盖完整 versioned `RunCreateSpec`：完整 NewRun、有序每个 initial input 的 ID/canonical payload、ExecutionBindingRef、MapperVersion。字段、presence、顺序或版本变化均触发 digest 变化。

```go
type StartOperationDigest es.Digest
func DigestStartOperation(
    sessionID session.SessionID, turnID chatlog.TurnID, runID run.RunID,
    planDigest es.Digest, userMessageDigest es.Digest, inputIDs []chatlog.InputID,
) (StartOperationDigest, error)
```

**TRN-ID-5** StartOperationDigest 使用 `twilight.turn/start-operation/v1` domain，按序覆盖 SessionID、TurnID、RunID、PlanDigest、UserMessage.Digest、ordered InputIDs。它排除 head、clock、registry state、进程 identity，并作为 Start commit、event identity、response 的稳定 identity。

**TRN-EVT-1** 最小 committed ontology 为 `twilight.turn/run_requested` 与 `twilight.turn/run_settled`，payload 分别为 `RunRequestedPayload`、`RunSettledPayload`。transient UI delta、driver return、outbox record、观察日志提供辅助信息；ontology 查询仅接受这两个 committed event。

**TRN-EVT-2** Start 的 EventID、CommitID 由 StartOperationDigest、固定 event kind、group ordinal 在版本化 domain 中唯一派生；retry 重用该 identity。materialization、settlement 使用本规范所列 stable identity，恢复性 commit ID 使用确定性派生。

**TRN-EVT-3** resolved stream 内 `(TurnID,RunID)` 的 `run_requested` 唯一；一个 unsettled Turn 持有一个 linkage。相同 canonical payload 幂等；payload、PlanDigest、event identity 或 linkage 差异触发 conflict。

## 3. API 与 authority boundary

```go
// run is github.com/memohai/twilight/agent/run.
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
```

**TRN-API-1** 上列字段为 injected capability。每个 Coordinator 实例依据 Session facts 与 `Runtime.Record` 接续；Session、Run、driver、mapper cursor、terminal verdict 均由其 canonical record 提供，跨调用状态通过这些 records 重建。

**TRN-API-2** binding registry 以同一个 shared `run.Runtime` 组合 driver。driver 接收 `DriveRequest{Ref,RunID}`，并经 shared Runtime 读写；其 Run access 以该 Runtime contract 完成，persisted binding 含义保持稳定。

**TRN-API-3** Coordinator、driver、mapper 与 settlement 只通过 [agent-run.md](agent-run.md) 的 `run.Runtime` contract 协调 Run。input/transition 全部经 Commit 持久化；绕过 Runtime authority 的写入不构成 Turn progress。

**TRN-API-4** Coordinator 按 Run contract 处理 idempotent Create、create conflict、missing Run、corrupt record 与 unavailable，并且只从 persisted linkage 取得创建计划；Turn 不重新定义 Runtime 的 collision、commit、grant 或 record 语义。

```go
type Service interface {
    Start(context.Context, StartRequest) (StartResponse, error)
    Resume(context.Context, ResumeRequest) (ResumeResponse, error)
    Stop(context.Context, StopRequest) (StopResponse, error)
}
type StartRequest struct {
    Ref TurnRef; Run run.NewRun; InputIDs []chatlog.InputID
    UserMessage chatlog.Message; InitialInputs []run.AgentInput
    ExecutionBinding ExecutionBindingRef; MapperVersion MapperVersion
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

**TRN-API-5** DTO 均为值语义。Start 对既有 linkage 的新 RunID 或计划改写返回 conflict；Resume/Stop 按 Ref 解析唯一 unsettled linkage。`Disposition` 取 `ResumeWaitingForResponse`、`ResumeWaitingForRecovery` 或 `ResumeFinished`；前两者表示 Run 仍为 active，后者必须带 `Result`。`Waiting` 只列当前 snapshot 的 response requests，执行恢复等待不伪造 response request；terminal fact 与后续计划授权由 RunRecord 和 persisted linkage 提供。

## 4. Start admission

**TRN-STR-1** Start 依次验证、写入 Session intent/linkage、调用 Runtime.Create。Start 的 Session append causation 等于 persisted `NewRun.CausationID`；恢复沿用该 causation。

**TRN-STR-2** StartRequest 满足：

1. Ref、RunID、binding ref、mapper version 非空；
2. InputIDs 与 InitialInputs 等长、无重复、顺序完全相同；
3. `InitialInputs[i].ID=InputIDs[i]`，其 payload 与对应 `InputSubmitted` admission payload 完全对应；
4. InitialInputs 为 Run ingress payload，UserMessage 按其 chatlog wire 规则持久化；
5. `UserMessage.Role=RoleUser`、`TurnID=Ref.TurnID`，并经 Message registry 重算 digest；
6. UserMessage.InputIDs 精确等于 ordered InputIDs，ItemIDs 为空；
7. input 已 submitted、未终结、可 delivered；message 的其余 role、part、binding、canonical 约束由 chatlog/Extension admission 执行。

**TRN-STR-3** v1 Start 写入一个原子 semantic Session commit，顺序严格为：

```text
TurnOpened{TurnID, InputIDs}
InputDelivered{InputIDs[0], TurnID}
...
InputDelivered{InputIDs[n-1], TurnID}
MessageCommitted{UserMessage}
run_requested{RunRequestedPayload}
```

恰有一个 MessageCommitted，且为该 user message；该 group 的 chatlog/turn event 集合即如上。空 input group 为 `TurnOpened → MessageCommitted → run_requested`。

**TRN-STR-4** Start 构造 RunCreateSpec、PlanDigest、StartOperationDigest、stable CommitID/EventID group，再调用 `SemanticAppender.AppendSemantic`。applied/already-applied 表示同一成功事实；head conflict 进行 CAS rebase，并保留完整 group 的 identity、payload、输入顺序、causation。

**TRN-STR-5** semantic append 确认 intent 后，Start 重新 resolved replay 精确解析 linkage，再调用 `Runtime.Create(ctx, linkage.Create.Run)`。Created true/false 均进入 Resume；Create conflict、linkage 不唯一、payload 不符、header 非法返回失败并保留诊断事实。

**TRN-STR-6** Start 在 Create 后调用 Resume；inputs 投递、model 驱动、Session settlement 均收敛到恢复算法。

## 5. Resume、Drive 与 Stop

**TRN-RSM-1** Resume 在 verified resolved stream 查询 Ref 的唯一 unsettled `run_requested`。无 linkage、多个 linkage、已 settled linkage、payload identity 冲突分别返回 not-found、corrupt 或 conflict；计划取自 persisted linkage。

**TRN-RSM-2** Resume 对 persisted `linkage.Create.Run` 再调 Create，验证 RunID collision identity；成功后执行 `EnsureInitialInputs`。

**TRN-RSM-3** EnsureInitialInputs 对 persisted InitialInputs 按序逐项：

1. `Runtime.Load(RunID)` 取得 revision；
2. 用 `run.DeriveInputCommandID(RunID, Input.ID)` 与 `run.BuildEnvelope(RunID, commandID, run.AcceptInput{Input})` 建 envelope；
3. 以该 revision 提交 CommitRequest；
4. CommitAccepted 或同 command 的 CommitAlreadyApplied 均前进；
5. stale 时 Load 后以同 identity 重试，terminal 时停止投递并读 record；
6. 调用结果 unknown 时先 Record；完整 transition 出现该 command 后前进，否则返回 indeterminate。

**TRN-RSM-4** input payload/顺序差异触发 conflict。partial crash 后，已接受前缀和未接受后缀以同 command identity 收敛；进度由 snapshot 与 Record 共同验证，本地 cursor 不构成进度证据。

**TRN-RSM-5** inputs 后先 Record，并调用 `MaterializeAll(record)` 按 revision 递增补齐 revision 1 至 record head 的全部 transition；terminal record 随后进入 settlement。active Run 解析 persisted binding 并调用：

```go
driver.Drive(ctx, DriveRequest{Ref: ref, RunID: linkage.RunID})
```

无论 driver 返回完成、等待、error，Coordinator 均再次 Record、调用 `MaterializeAll(record)` 补齐完整新前缀，并据 record snapshot/terminal fact 返回 `ResumeWaitingForResponse`、`ResumeWaitingForRecovery` 或 `ResumeFinished`。前两者不返回 terminal Result；等待用户响应时携带 snapshot 中的 response requests，同时存在 execution recovery 时保留这些 requests 并由 `ExecutionRecovery` 标记；只有没有 response request 时才返回空 `Waiting`，表示由 recovery authority 负责后续唤醒。

**TRN-RSM-6** driver 的 provisional stream、返回错误、网络响应丢失、context cancel 作为观察结果处理。driver 以 shared Runtime 的 Load/Commit 推进 MachineState；并发 Resume 依 Runtime command idempotency、revision、grant、record 一致性收敛。具体执行器可以使用 `agent/run/loop.Loop`；其 `LoopResult` 映射为本节的 `ResumeDisposition`，并由 Coordinator 始终以随后读取的 `RunRecord` 判断 waiting、execution recovery、terminal 与 settlement。

**TRN-STP-1** Stop 先解析 linkage、Load。terminal Run 进入 record/materialize/settlement；active Run 以稳定 domain-separated CancelRun CommandID 构造 `CancelRun{Reason:ReasonCancelled}`，并经 shared Runtime Commit。

**TRN-STP-2** Cancel command identity 使用 `twilight.turn/cancel-run/v1` domain，至少覆盖 SessionID、TurnID、RunID、固定 cancel reason；Stop retry 重用它。StopRequest.Reason 供 Application audit/presentation，Run 采用固定 cancellation reason。

**TRN-STP-3** Stop Commit outcome unknown 时先 Record 查询完整 transition 是否含该 command；证明已应用后继续，未能证明时返回 indeterminate。CommitAccepted、CommitAlreadyApplied 或 Record 已证明应用时，Stop 重新 Record，调用 `MaterializeAll(record)` 补齐全部 transition，再从 exact terminal `RunEnded` 构造 ResultReference 并提交 settlement。StopResponse 在 settlement 成功或 already-applied 后返回 Result；indeterminate 路径保留 Session Turn 当前状态。

## 6. record、结果与 settlement

**TRN-SET-1** `RunRecord` 为 materialization、unknown resolution、recovery、settlement 的唯一一致 verified read。它含 detached 且相互一致的 Header、Snapshot、完整 TransitionRecord sequence；消费者验证 header、records、fold、snapshot，并拒绝拼接独立 reads。

**TRN-SET-2** terminal 判断读取最后一个 `RunEnded` AgentEvent，并验证其 `End` 是唯一合法 terminal variant；Coordinator 从该 variant 派生 `RunStatus` 后构建 ResultReference。RunResult、snapshot revision、driver return 与其他 event 不参与该构建。terminal fact 缺失、多个、variant 非法或未处于末尾时触发 corruption。

**TRN-SET-3** settlement mapping 固定如下：

| RunEnded variant（派生 status） | run_settled settlement | 同一 Session commit 的 chatlog event |
|---|---|---|
| `RunCompletedEnd` | `completed` | `TurnCompleted{TurnID}` |
| `RunFailedEnd` | `failed` | `TurnFailed{TurnID, FailureClass}` |
| `RunStoppedEnd` | `stopped` | `TurnFailed{TurnID, FailureClass:"stopped"}` |

RunFailed 的 FailureClass 取 terminal failure class；没有 failure 时采用稳定非空 class。`RunStopped` 映射 `TurnFailed{..., FailureClass:"stopped"}`。

**TRN-SET-4** run_settled 与对应 TurnCompleted/TurnFailed 位于同一 semantic Session commit，CommitID/EventID 由 stable terminal ResultReference 派生。相同 reference retry 幂等；settlement/result reference 差异触发 conflict。

**TRN-SET-5** unknown external effect 的 persisted MachineState/terminal record 已包含 `ToolCallFailed{Outcome:Unknown, FailureEffectUnknown}` 加 terminal `RunEnded{End: RunFailedEnd{...}}`。Coordinator materialize unknown tool result 并 failed settle。

## 7. materialization

```go
type FactMapRequest struct {
    TurnID chatlog.TurnID; RunID run.RunID; MapperVersion MapperVersion
    Prefix []run.TransitionRecord // complete through target
    TargetRevision uint64; TargetTransitionDigest es.Digest
}
type FactMap struct { SourceFacts []SourceFactID; Events []extension.TypedEvent }
type FactMapper interface { Map(FactMapRequest) (FactMap, error) }
```

**TRN-MAT-1** `MaterializeAll(record)` 按 revision 1 至 `record.Snapshot.Revision` 递增枚举每条 transition，并为 revision `r` 构造一个 FactMapRequest。Prefix 从 revision 1 连续至 TargetRevision，且含 target 的完整 transition group；每条 record/event 均经验证。TargetRevision/TargetTransitionDigest 等于 prefix 尾。每次 Map 处理 TargetRevision 的完整 target transition；`Prefix[1..target]` 提供 pure context。FactMapRequest 覆盖完整 prefix 与 target；mapper 输入排除 partial transition、provisional delta、current binding、Session projection cache、hidden state。

**TRN-MAT-2** SourceFactID 唯一规则为：

```text
Digest("twilight.turn/source-fact/v1",
       RunID, Transition.Revision, Transition.TransitionDigest,
       AgentEvent.Index, AgentEvent.Type, AgentEvent.Digest)
```

同一 event 的 source ID 随 retry、Session head、mapper process 保持稳定。`FactMap.SourceFacts` 按 target transition event 顺序，精确列出实际产生本 batch `Events` 的 AgentEvents；每个输出 event 可追溯到其中 source fact。`Events` 为空时 `SourceFacts` 为空。后续 revision 仅处理其 target transition 的输出与 source fact。

**TRN-MAT-3** materialization CommitID 使用 `twilight.turn/materialize/v1` domain，覆盖 SessionID、TurnID、RunID、MapperVersion、TargetRevision、TargetTransitionDigest、ordered target-source SourceFactIDs。输出 EventID 使用 `twilight.turn/materialized-event/v1` domain，覆盖 CommitID、ordinal、event type、schema version、canonical payload digest；CAS rebase 后保持字节稳定。

**TRN-MAT-4** mapper 为 pure deterministic total-prefix function：相同 FactMapRequest 产生同一 ordered FactMap，输入为请求中的持久化 facts。IO、clock、random、Application defaults、前次调用状态不参与 Map。MapperVersion 是协议输入；升级使用显式新 version，既有事实保持原 version。

**TRN-MAT-5** Map 空 Events 时 SourceFacts 为空，该 target transition 以可重试 deterministic no-op 完成 coverage。非空 batch 先以 stable CommitID `LookupCommit`；找到逐字段相同 commit 即完成该 target transition，未找到则按当前 head append，head conflict 后重新 replay/fold，以同一完整 target transition Map，直至 append 或 conflict。`MaterializeAll` 仅在每个 revision 分别达到 matched commit、applied commit 或 empty-map coverage 后前进；空 map 保持无 commit。

**TRN-MAT-6** transactional outbox 服务于 Session adapter 投递优化。source coverage、EventID、CommitID、materialized state、settlement 由 Session facts 和 RunRecord 重建。

### 7.1 v1 映射表

**TRN-MAP-1** v1 映射下表所列 AgentEvent；未列 AgentEvent 产生零个 chatlog event，未来扩展使用新 MapperVersion。`ItemID=DeriveItemID(TurnID,StepID,MapperVersion)`；UI delta 用于呈现。

| AgentEvent | 必须产生的 chatlog facts |
|---|---|
| `ModelStepPrepared` | `ItemOpened`；sequence 依完整 Run prefix 的稳定 model-step 顺序 |
| `ModelStepCompleted` | 该 item 的 `ItemUpdated`、`ItemCompleted`，一个 RoleAssistant MessageCommitted，含 text/reasoning/reference 与全部 tool calls |
| `ToolCallCompleted` / `ToolCallAnswered` | 对应 CallID 的 RoleToolResult MessageCommitted，status=`success` |
| `ToolCallFailed`，Outcome=`Known` | 对应 CallID 的 RoleToolResult MessageCommitted，status=`error` |
| `ToolCallFailed`，Outcome=`Unknown` 或 failure class=`effect_unknown` | 对应 CallID 的 RoleToolResult MessageCommitted，status=`unknown` |
| terminal failed/stopped `RunEnded` | 该 Turn 每个尚未 completed item 的 `ItemFailed` |

**TRN-MAP-2** assistant Message.ItemIDs 仅指向同 Turn 已 completed 且 final digest 匹配 item；tool-call part 的 CallID/顺序与 ModelStepCompleted result 一致。tool-result message 遵守 chatlog 的既有 call、同 Turn、单 active result/replacement 规则。

**TRN-MAP-3** `ToolCallFailed.Outcome=Known` 映射 error；`Outcome=Unknown`、`FailureEffectUnknown` 或持久事实无法区分的结果映射 unknown。mapper 以持久事实完成该判定。

**TRN-MAP-4** ItemUpdated/ItemCompleted 的 content、version、final digest 从完整 persisted model result canonical projection 导出；流式 text delta 可丢弃且不持久化为映射事实，replay 使用 canonical projection。

## 8. recovery

**TRN-REC-1** recovery 扫描 resolved stream 的 unsettled linkages，按稳定 `(SessionID,TurnID,RunID)` 顺序 Resume。实现验证 linkage、RunRecord、mapper input；header、transition、digest、fold、source ID、settlement 差异触发 fail loudly，并保留历史事实，不修补记录。

**TRN-REC-2** 最少覆盖如下 crash/concurrency matrix：

| 情形 | 必须恢复动作 |
|---|---|
| Start 已提交、Run 缺失 | 以 persisted NewRun Create 后 Resume |
| Create retry/响应丢失 | 同 NewRun Create，Created=false 为同一 Run |
| initial inputs 仅前缀 | 逐项 Load、相同 derived command Commit，已应用跳过 |
| transition 已提交、materialization 缺失 | Record，按 revision 递增执行 MaterializeAll，以每个完整 target prefix Map/append |
| mapper 空 batch | 保持无 append；后续 Record 可再次 Map |
| materialization 响应丢失 | stable CommitID LookupCommit，匹配即成功 |
| Run terminal、settlement 缺失 | Record/materialize 后从 exact RunEnded 同 commit settle |
| model result unknown | 查询 `Runtime.Record` 的 MachineState/transition record；accepted result 未获证明时执行恢复，不派生猜测结果 |
| tool effect unknown | 查询 authority；已有 unknown terminal facts 时 materialize/failed settle，不重执行 effect |
| Record 与并发 Commit | 丢弃过时 record、重读；每次 Map 使用自洽完整 prefix |

**TRN-REC-3** Commit、Create、append、query 的 unknown outcome 一律先查 authority：Run 使用 Record，Session 使用 LookupCommit 与 verified replay。terminal abort evidence 集合为 verified terminal RunRecord、匹配 ResultReference 的 `RunEnded` 与同 commit settlement；查询失败、瞬时 NotFound、本地超时保持 indeterminate。indeterminate 保留 effect、replacement、Turn terminalization 的后续决策。

**TRN-REC-4** binding 缺失返回可恢复 `binding_unavailable` 并保留 linkage；registry 仅解析 persisted binding，已 terminal Run 无需 binding，继续 materialize/settle。

**TRN-REC-5** recovery 在 start barrier 后遵循 MachineState、grant/recovery 规则、完整 transition record 所允许的 command 推进 Run。Coordinator 重驱动、读取、投影，并以 record 重建执行历史；重复越过 start barrier 的 model/tool effect 触发协议拒绝。

## 9. conformance

实现按下列条款映射验证：

- **TRN-SCP-1 至 TRN-SCP-6**：一 Turn/primary Run、retry/replacement/subagent 边界、Coordinator 重建与 MachineState authority、SemanticAppender、Application policy ownership、immutable binding/no defaults；
- **TRN-ID-1 至 TRN-ID-5、TRN-EVT-1 至 TRN-EVT-3**：完整 types、PlanDigest、start-operation domain、stable EventID/CommitID、最小 events、unique linkage；
- **TRN-API-1 至 TRN-API-5**：Session/Runtime 重建、shared Runtime driver、四方法 contract、Create/missing errors、Service/Start/Resume/Stop DTO；
- **TRN-STR-1 至 TRN-STR-6**：intent-before-Create、causation、InputIDs/InitialInputs exact group、合法 user message、原子严格顺序、retry；
- **TRN-RSM-1 至 TRN-RSM-6、TRN-STP-1 至 TRN-STP-3**：unique linkage、Create retry、partial inputs、DriveRequest、record-after-drive、stable CancelRun、unknown query-first、Stop 的 MaterializeAll 与 settlement、Turn settlement 时机；
- **TRN-SET-1 至 TRN-SET-5**：verified RunRecord、terminal RunEnded ResultReference、completed/failed/stopped、同 commit settlement、effect unknown failed history；
- **TRN-MAT-1 至 TRN-MAT-6、TRN-MAP-1 至 TRN-MAP-4**：MaterializeAll 的 revision 1..head 完整 coverage、complete prefix 与 target-only FactMap source coverage、SourceFactID domain、materialization identity、pure versioned mapper、empty Events/SourceFacts no-op、LookupCommit/CAS rebase、outbox、v1 table；
- **TRN-REC-1 至 TRN-REC-5**：crash matrix、unknown query-first、missing binding、concurrent Record、effect 与执行历史恢复。

第一版 conformance 对 memory 与 durable Runtime adapter 使用同一断言；Application、provider、transport 行为以本协议为准。
