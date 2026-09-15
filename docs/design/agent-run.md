# Twilight Agent Run Protocol

状态：v1 设计规范。本文定义 Run Machine、Runtime 与 Loop。Runtime 经 `writer.Writer` 写入；Session/Run 语义不使用 per-effect lease，Executor Worker 的 ownership 由 control plane 与 Execution Store 管理。`RecoverInterrupted` 负责无法关联或明确放弃的语义接管处置。本文依据 [agent-session.md](agent-session.md)（Session 级单写者、一行一个 event）与 [agent-session-extension.md](agent-session-extension.md)（`writer.Writer`）。

本文定义 `agent/run`、`agent/run/loop` 与 Run 作为 Session Module 的存储形态。文中的"必须""不得""应该"是协议约束；canonical JSON、JCS 与 domain-separated digest 使用 `agent/jsonstable` 和 `agent/es` 的通则。

## 1. 范围与 authority

```text
Session stream               唯一 authority：twilight/run/ 事实与 turn、chatlog 事件同在一条 stream，一行一个 event
MachineState                 Run 的语义状态投影（twilight/run/machine）；投影缓存为可丢弃的派生缓存
Runtime                      Run 的 command 入口：在 Session 的 writer.Writer 内 Decide、Evolve、companion，一次 Append
loop.Loop                    当前进程的 execution interpreter
FrozenValueStore             Authority 侧的不可变请求存储；Dispatch 时形成 executor-owned payload，按 digest 校验
```

`MachineState` 决定 Run 当前可执行动作。每次接受的 command 产生一组同 CommitID 的 Session event，其中的 `twilight/run/` 事件经该 Run 版本的 `Protocol.Evolve` 从 `twilight/run/run_created` 重放后必须得到同一 `MachineState`。

Run 的职责分成五个相互独立的层面：

```text
Agent Machine   = Run/Step 状态与合法转移（Decide、Evolve、Next）
Agent Loop      = 决策解释器：把 Machine effect 记录为事实、交给 Executor 为 Assignment、把 Outcome 结算为事实
Runtime         = command 到 Session event 组的原子提交边界、接管处置
Executor        = 效果层端口：执行 Assignment（一次模型请求或一次工具调用）并交回 Outcome
Prompt Builder  = 从 Session context 构造下一条 prompt（决策层，agent-decision.md）
```

Machine 处理已冻结的值和已提交的事实；Loop 解释 `Next` 产生的 transient effect，只做决策与记录，从不在一次推进里等待效果；Runtime 保存并验证 Machine 的推进；Executor 执行外部 effect 并以数据形式交回结果，它与 Loop 是否同进程是部署选择（第 6 节 RUN-EXE）；Prompt Builder 构造下一条 prompt。

`Step` 是 Run 的持久化恢复边界；`execution attempt` 表示某个 Loop 进程对该 Step 或 ToolCall 的一次逻辑执行尝试。一个 Step 可以有多个 attempt。Session Writer 负责 Run 语义事实的 ownership；Executor Worker 可以在另一个 control-plane ownership 下执行同一个 Assignment。attempt 的 identity 由 start command 的 `ExecutionClaim` 表达；start 事实记录它（`ModelStepStarted.Claim`、`ToolCallStarted.Claim`），接管者据此判断一个仍在执行的 attempt 能否继续（RUN-CMT-7）。

**RUN-SCP-1** `agent/run` 拥有 Run identity、persisted frozen values、Machine、command/fact protocol、fact codec、fold 与 `Runtime`、`Companion` contract；它依赖 `agent/session` 的 identity 与 wire 类型，不依赖 loop、turn 或 extension。`agent/run/loop` 拥有 prompt builder/model/tool ports、streaming、并发执行、EventSink 与 Loop policy。`agent/session/run` 是 Run 的 Session Module 实现：EventDefinition（按 SchemaVersion 的 codec）、`twilight/run/machine` projection、`Runtime` 实现（经 `writer.Writer.Commit` 写入）、接管处置、FrozenValueStore adapter。

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
type PromptToken string
type ExecutionClaim string
type Digest = es.Digest
```

**RUN-WIR-1** identity 必须非空、稳定且为有效 UTF-8。`ExecutionClaim` 由 Loop 为一次 start command 生成并在该 command 的重试中保持不变，用于把同一执行尝试的 start 与 settlement 派生为确定的 CommandID；接管处置使用 `TakeoverClaim = Digest("twilight/run/takeover", SessionID, Epoch)`（RUN-CMT-7）。start 事实记录 Claim（`ModelStepStarted`、`ToolCallStarted`），Executing 的 step 与 call 在 MachineState 中携带它：这是接管者向 Executor 询问"这个 attempt 是否仍在执行"并接受其迟到 Outcome 所需的唯一身份；start command 缺少 Claim 被 Decide 拒绝。Run 跨 domain causation 记录在 `twilight/run/run_created` 的 `CausationID`。

Run 持久化协议保存 run-owned frozen values。模型请求、模型结果、消息、工具定义、usage、provider metadata 与所有动态 JSON 在进入 command 前，分别经 `FreezeModelRequest`、`FreezeModelResult`、`FreezeToolDefinition`、`FreezeToolCallInput` 等入口转为纯数据和 immutable `CanonicalJSON`。Runtime 接收 agent-owned value；调用方负责在边界前完成冻结。

**RUN-WIR-2** Run 事实是 Session event：EventType 为 `twilight/run/<name>`，payload 为 canonical JSON object，第一层携带 `runId` 与 payload 版本字段 `v`（SES-VER-1、EXT-REG-2）。`v` 等于该 Run 的 `SchemaVersion`：由 `twilight/run/run_created` 记录，同一 Run 的全部事实使用同一值，Registry 永久保留每个已发布版本的 codec、Decide 与 Evolve。行字段（Seq、CommitID、Index、Last、digest）由 Session kernel 提供，Run 不另设 envelope。fact codec 必须拒绝 unknown type、duplicate key、unknown field、trailing data、非法 UTF-8、非 canonical-equivalent wire。精确 identity 和 digest 使用 JSON string，整数字段使用 Session preset 的整数 wire shape。

```go
type CommandEnvelope struct {
    SchemaVersion uint16 // 必须等于该 Run 的 created.SchemaVersion
    Type string
    SessionID session.SessionID
    RunID RunID
    ID CommandID
    Command AgentCommand
}
```

command 不持久化。`CommandEnvelope.ID` 就是该 command 产生的 event 组的 `CommitID`；重放与冲突由 Writer 的行 fingerprint 判定（EXT-WRT-2）。envelope 只经 `Protocol.BuildEnvelope` 构造（RUN-WIR-3），不携带自校验 digest。

**RUN-WIR-3** 一个 command 恰产生一组事件（一次 `Append`，同一 CommitID）；其 `twilight/run/` 事件在组内 Index 从 0 连续递增，companion 事件（TRN-CMP）与调用方附加事件（`CommitRequest.Attach`）依次紧随其后。事件没有独立 EventID，`Seq` 即身份（SES-WIR-1）。Runtime 提交的组其 CommitID 等于 CommandID，Coordinator 写入的 Start 与 Retry 组使用该组自己的 CommitID。`RecordedAtUnixMilli` 由写入方的时钟填入，是 metadata，不参与 Run 的任何派生，也不进入 Writer 的幂等 fingerprint（EXT-WRT-2）。构造 command 必须使用该 Run 版本的 `Protocol.BuildEnvelope`（Loop 通过 `RuntimeSnapshot.Protocol()` 取得）。`agent/run` 不提供隐式选择版本的包级 `BuildEnvelope`、`Decide`、`Evolve` 或 `Digest*` 函数；新 Run 与测试显式使用 `ProtocolV1()`。

**RUN-WIR-4** 内容与执行状态分离。fact 只保存执行状态与内容 digest，内容本体落在两处：

| 内容 | fact 中的字段 | 本体位置 |
|---|---|---|
| 冻结模型请求 `ModelRequest`（含工具定义） | `ModelStepPrepared.RequestDigest` | Authority 的 `FrozenValueStore`；Dispatch 后由 Execution Record 持有 payload 或 payload ref |
| 工具定义 `ToolDefinition` | `ToolSpec.DefinitionDigest`，只用于执行前校验 | 请求本体内；不另设存储 |
| 模型输出文本、reasoning、tool call 列表 | `ModelStepCompleted.ResultDigest` | 同组的 `twilight/chatlog/assistant`，其 `SourceDigest` 等于 ResultDigest |
| 工具输出 | `ToolCallCompleted.OutputDigest` / `ToolCallAnswered.ResponseDigest` | 同组的 `twilight/chatlog/tool_result`，其 `SourceDigest` 等于该 digest |
| tool call 参数 | `ToolCallBinding.Arguments` | fact 本身（执行不得依赖 chatlog 解码） |

companion 与 Attach 事件与 Run 事实一起经 Module Framework 的 admission（EXT-REF-2）：它们可以携带 `ReferencePart`，其 Binding 的 claim 由 Writer 在 `Append` 之前建立（EXT-WRT-3）。`FrozenValueStore` 是 run 层对内容寻址存储的端口：`Put(digest, bytes)` 幂等，`Get(digest)`。它不是第二个内容寻址存储，而是 artifact `cas` ContentStore 的一个 Authority（`twilight/run/frozen`，`agent/session/run.FrozenValues` 适配）。它保存 Authority 生成的 frozen request；Dispatch 时必须把请求复制到 executor-owned 的 durable Execution Record，或复制到 Worker 可访问的 payload store。Worker 不应在执行时反查 Authority 的 Session 或依赖某个 Worker 的本地文件。接管时继续使用相同 AssignmentKey；是否重试由 control plane 和 effect recovery policy 决定。内存与文件两种 ContentStore（`artifact.NewMemoryContentStore`、`filestore.NewContentStore`）经同一适配器服务。工具列表摘要（`DigestToolSpecs`）的预映像不区分 nil 与空列表：fact wire 省略空列表，重算方拿到的是 nil。

下列 identity 稳定派生并由 Commit 验证：

| identity | preimage |
|---|---|
| PrepareModelRequest CommandID | RunID、loaded `RunPosition`（该 RunID 最后一条事件的 Seq） |
| ModelStep StepID | RunID、prepare CommandID、model/request/tools binding digest |
| ToolStep StepID | source ModelStepID、ordered binding-set digest |
| CallID | source ModelStepID、该 call 在模型结果 `ToolCalls` 中的位置 |
| ResponseID | RunID、ToolStepID、CallID、ResponseKind |
| response CommandID | RunID、StepID、CallID、ResponseID |
| input CommandID | RunID、有序 InputID 列表（批次） |
| withdraw CommandID（WithdrawPreparedStep） | RunID、StepID |
| start CommandID（StartModelExecution / StartToolCall） | RunID、StepID、CallID（model 为空）、Claim |
| owner settlement CommandID（model result/failure/reject、tool result/failure） | RunID、StepID、CallID、Claim |
| Pending Known failure CommandID | RunID、StepID、CallID、空 Claim |
| model recovery CommandID（RecoverModelExecution） | RunID、StepID、Claim |
| tool recovery CommandID（接管处置的 Unknown） | RunID、StepID、CallID、TakeoverClaim |

派生 identity 使同 CommandID 即同一 command：内容差异只可能出现在 identity 有意不覆盖内容的两族（同一 ResponseID 的 approve 与 reject、同一 attempt 的两次结算），Runtime 对它们按精确重放处理，调用方从投影读取实际生效的结果。`PromptToken` 是 Application-owned opaque freshness token，属于 prepare command identity 内容；Run 不校验它的语义（RUN-CMT-4）。

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

**RUN-NEW-1** `twilight/run/run_created` 是 Run 的第一个事实。v1 初始状态恰为：相同 RunID、Owner、Attempt、`RunActive`、`Current=Open`、无 pending input、零 model step、零 usage、无 result。初始输入随后以 `twilight/run/input_accepted` 进入同一组（TRN-STR-2）。`Protocol.BuildCreateGroup(NewRun, []AgentInput)` 返回 `created` 与 `input_accepted` 的 facts，编码为 Session event 由 `agent/session/run` 完成，Coordinator 不自行编码。同一 RunID 第二条 `created` 为 Evolve 错误。

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
    RequestDigest Digest // Authority frozen payload；Dispatch 后由 Execution Record 持有
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

MachineState 不保存模型输出与工具输出本体。上一步的内容由 PromptBuilder 从 chatlog fold 读取（DEC-PMT），MachineState 只提供 `LastToolStep` 作为 Run 边界事实。

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
             |           +-> Recovered -> Open  (attempt 丢失且不可重连：撤回该请求，下一次 Prepare 重新规划)
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

同一 Assignment 的恢复不重新生成 ModelRequest；Recovered 与 Withdrawn 都使 Run 回到 `Open`、不计入 `ModelSteps`，下一次 `Prepare` 按当时的投影与 `PendingInputs` 重新规划，产生新的 StepID 与 RequestDigest：恢复记录为一次新的 Prepared 事实，StepID 与 RequestDigest 均为新值（TRN-DUR-1）。Prepared 期间到达的输入使该请求不再完整，`Next` 改为返回 `WithdrawPrepared`，Loop 提交 `WithdrawPreparedStep` 后回到 `Open` 重规划；Executing 期间到达的输入等待该步结算或撤回，在随后的 `Open` 被消费。

**RUN-MCH-1** MachineState 保存 Run 的 execution semantics。`LastToolStep` 保存最近一个经 Evolve 关闭路径写下的 ToolStep 只读投影，必须与事件序列折叠出的最后关闭 step 一致，供下一次 prompt builder 定位 `SourceStep`。Cancel 经 `RunEnded` 把 `Current` 置空、不走关闭路径时不改写 `LastToolStep`。terminal state 吸收所有未幂等命令；`RunEnded` 建立唯一 terminal result。

**RUN-MCH-2** `ToolCallBinding` 冻结 CallID、ProviderCallID、ToolRef、definition digest、canonical arguments、response policy 与 binding digest。`CallID` 由 Run 派生（`DeriveCallID(source, index)`），是 Run 内的持久化 identity，进入 fact、派生 CommandID 与 chatlog；`ProviderCallID` 是模型发出的 `tool_call_id`，只用于 PromptBuilder 回传工具结果时与模型配对，Run 不以它为键，也不要求它唯一或非空。Decide 校验每个 binding 的 CallID 等于派生值、ProviderCallID 等于模型结果中对应位置的 id。已知工具使用匹配 frozen ToolSpec 的 ref/digest/policy；未知工具保留为同名 unresolved DirectExecution binding，并在执行前收束为已知 lookup failure。approval/external response 的 `ResponseRequest` 由 Decide 稳定派生。Unknown outcome 使用 class `effect_unknown`，只把该 Executing call 记为 `ToolCallFailed(Unknown)`。Run 保持 Active；同 step 其他 call 继续。全部 call 进入 Completed 或 Failed 后 Evolve 关闭 ToolStep。

`AgentCommand` 与 `Fact` 都是 sealed interface。v1 的 command→fact 规则为：

| command | precondition / facts |
|---|---|
| `AcceptInput` | 任意非终态；批内每个输入一条 `InputAccepted`，按顺序追加到 `PendingInputs`，全有或全无。空批次为拒绝；同一 InputID 已在 pending 或在批内重复为 conflict，整批无事实 |
| `PrepareModelRequest` | `Open`，完整有序消费 PendingInputs，request/tools digests 有效；`ModelStepPrepared`。command 携带请求本体，fact 只留 digest，本体由 Runtime 写入 FrozenValueStore；Dispatch 时复制到 executor-owned payload |
| `WithdrawPreparedStep` | Model Prepared 且 `PendingInputs` 非空；`ModelStepWithdrawn`，`Current` 回到 `Open`，该请求本体可释放 |
| `StartModelExecution` | Model Prepared；`ModelStepStarted`。command 必须携带本次 start 的 `ExecutionClaim` |
| `RecoverModelExecution` | Model Executing；`ModelStepRecovered`，`Current` 回到 `Open`、不计入 `ModelSteps`、PendingInputs 保留。携带该 attempt 的 `Claim`，接管处置时为 `TakeoverClaim` |
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
| `CancelRun` | active；Pending / Waiting tool call 记 `ToolCallFailed(Known/cancelled)`，Executing call 记 `ToolCallFailed(Unknown/effect_unknown)`，保留已有终态结果。全部 call 进入终态后关闭 ToolStep 并写入 `LastToolStep`，随后 `RunEnded(stopped/cancelled)` 把 `Current` 置空。`RunStoppedEnd` / `RunResult` 的 `UncertainCalls` 只列本次产生 Unknown 的 CallID，`UncertainModel` 标识仍 Executing 的模型步骤。companion 为每个新 failure 写入配对的 tool result。 |

最后一个 ToolCall 进入 Completed 或 Failed 时，`Evolve` 在折叠该 fact 后若全部 call 已 terminal，则把 Current 设为 `Open` 并写入 `LastToolStep`；下一次 `PromptInput.SourceStep` 取自 `LastToolStep.RefValue.ID`。Cancel 产生的 failure facts 同样走这条关闭规则；`RunEnded` 再把 Current 置空。

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

一次执行 attempt 的全部 command identity 都从其 `Claim` 派生：start、owner settlement、model recovery 的 CommandID 分别按上表计算，Commit 对 start 强制校验该派生。start 事实持久化 Claim；重连沿用该 Claim 定位 Assignment 与提交结果。提交返回非 sentinel 错误时，以同一 Claim 重放得到同一 CommandID，Writer 对精确重放返回 AlreadyApplied（RUN-LOP-5）。进程崩溃后由接管者按 RUN-CMT-7 重连或处置 Executing 目标。

`Next(state)` 最多返回一个 transient `Effect`：

| state | effect |
|---|---|
| terminal | 返回 `ErrRunTerminal`，没有 effect |
| `Open` | `NeedModelRequest{PromptInput}` |
| Model Prepared 且 `PendingInputs` 非空 | `WithdrawPrepared` |
| Model Prepared | `StartModelCall` |
| Model Executing | `Idle` |
| ToolStep 有 Pending calls | `StartToolCalls` |
| ToolStep 无 Pending、仍有 Waiting 或 Executing | `Idle` |

Waiting call 上的 `ResponseRequest` 由 `WaitingCalls(state)` 读取。Executing call 由 `ExecutingCalls(state)` 读取。`NeedsRecovery(state)` 在 Model Executing 或 ToolStep 无 Pending 且仍有 Executing 时为 true。这些查询不是 Effect。

**RUN-MCH-4** Effect 由调用方每次 Load 后重新派生。`AcceptInput{Inputs}` 携带一个有序、非空的输入批次，在任意非终态入队，Decide 不因 Run 正在执行而拒绝它；批次全有或全无，任一输入非法则不产生任何事实；`PendingInputs` 只在 `Open` 的 Prepare 中被消费。`PrepareModelRequest.InputIDs` 必须与当前 PendingInputs 等长、同顺序、逐项相同；prepare 接受后一次消费全部 pending input。ToolStep 的 Waiting call 禁止 Start，同一 step 中的 Pending call 仍可执行。没有可执行 Start 时 `Next` 返回 `Idle`。Application 从投影读取 `WaitingCalls` 并提交 `ApproveToolCall` / `RejectToolCall` / `SubmitToolResponse`。Executing 目标在当前 owner 进程内由其 worker 结算；owner 崩溃后由接管者按 `NeedsRecovery` 一次性处置（RUN-CMT-7）。

## 5. Runtime、投影与 Commit

```go
type Runtime interface {
    Load(context.Context, session.SessionID, RunID) (RuntimeSnapshot, error)
    Commit(context.Context, session.SessionID, CommitRequest) (CommitResult, error)
    Record(context.Context, session.SessionID, RunID) (RunRecord, error)
    FrozenRequest(context.Context, Digest) (ModelRequest, error)
    // RecoverInterrupted 对 Executing 目标重连或提交接管处置，返回处置数。
    // 宿主在 OpenWriter 之后、驱动任何 Run 之前调用（RUN-CMT-7）。
    RecoverInterrupted(context.Context, session.SessionID, Reattacher) (int, error)
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

Runtime 由组装代码以 `writer.Writers`（EXT-WRT-6）、`FrozenValueStore`、`Companion` 与 `SnapshotPolicy` 构造；它按 SessionID 取得该 Session 的 `Writer`，全部语义事实读写经该 Writer。FrozenValueStore 只保存 Authority 生成的 immutable payload；Executor 接受 Assignment 后必须转存为 executor-owned payload。

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

**RUN-CMT-3** Commit 经 `writer.Writer.Commit` 在该 Session 的 Writer 互斥区内完成（EXT-WRT-1）。所有 Runtime implementation 在 fn 内调用同一个 pure `EvaluateCommit`，顺序固定为：

```text
writer.Commit(func(view):
  1  validate envelope SessionID/RunID/schema/type
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

FrozenValueStore 的 `Put` 幂等且内容寻址，在进入 Writer 之前完成；Commit 失败时留下的本体无害，可由保留策略回收。Assignment 被接受后，执行所需 payload 的生命周期由 Execution Store 管理，不能依赖 Authority FrozenValueStore 的短期可用性。

**RUN-CMT-4** `PrepareModelRequest` 是 hard-CAS command：`Base` 必须等于投影记录的该 Run 的 `Position`。这是有意选择：同一 Session 内其他模块的写入（用户提交新输入、summary、checkpoint、其他 Turn 的事件）不移动 Position，因此不使 Prepare 失效；Plan 与 Prepare 之间发生的 chatlog 写入不会被本次请求包含，新鲜度由 Application 经 `PromptToken` 与 PromptBuilder 自行负责，Run 不校验 `PromptToken` 的语义。其他 command 通过当前 target state 做 call-local rebase，`Base` 可为零值或过期值；stale Base 本身不阻止无冲突的 ingress/control/settlement。相同 command 的 replay 判定先于 terminal check，因此 terminal Run 仍能返回原组。

**RUN-CMT-5** 幂等键为 Session 的 `(SessionID, CommitID)` 提交索引（SES-REP-3/4、EXT-WRT-2），CommitID 等于 CommandID，Runtime 不另设幂等索引。同 CommandID 的重放返回 `CommitAlreadyApplied`、当前 snapshot 与原完整组，且不得再次 Decide 或产生外部 effect；command 不持久化，Runtime 不比对重放 command 的内容，同 CommandID 视为同一 command。对于 `StartModelExecution` 和 `StartToolCall`，claim 是 CommandID 的 preimage，不同 claim 即不同 command：其 start 按当前 target state 评估，target 已是 Executing 时返回 `ErrStaleRuntime`。

**RUN-CMT-6** Run 语义提交的 ownership fencing。Runtime 不签发 per-effect grant，也不校验 Worker 的 operational lease；同一进程内同一 Run 至多一个 Loop 在驱动（第 7 节的 driver slot）。Executing 目标的 settlement 必须通过 Session Writer；跨进程的迟到语义写入由 kernel 的 Epoch fencing 拒绝（SES-OWN-2）。Writer 返回 `ErrOwnershipLost` 时 Runtime 原样返回该错误，Loop 必须取消全部 worker、放弃 settlement 并以该错误返回（RUN-LOP-5）。Executor Worker 的 owner/epoch 由 Execution Store 独立校验。

**RUN-CMT-7** 接管处置。处置只恢复同一 Run：不创建 attempt、不结束 Run；对工具 call 的 Unknown 结算是该 call 的终态事实，协议在任何路径上都不据此自动重新执行（TRN-DUR-1、TRN-DUR-4）。新 owner 取得 Writer 后，在驱动任何 Run 之前调用一次 `RecoverInterrupted(sid, reattach)`。对投影中每个 Executing 目标（`RecoveryTargets`：模型步或 tool call，连同其 start 事实记录的 Claim），Executor/control plane 可以先使用同一 AssignmentKey 对 durable Execution Record 做 Attach、Reconcile 或显式 Takeover；Attach 返回 `orphaned` 时表示记录存在但当前没有可关联 backend，不能当作 `missing`，Run 必须保持 Executing，直到 control plane 显式 takeover/reconcile/dispose。若同一 attempt 被接管，Run 仍保持 Executing，结果以原 Claim 结算——这是重连同一次执行，不是新的 Run attempt。只有执行记录不存在或 control plane 明确放弃时，才由 `RecoverInterrupted` 处置：Executing ModelStep 提交 `RecoverModelExecution{Claim: TakeoverClaim}`，回到 `Open` 并按恢复时刻重新规划；Executing tool call 提交 `SubmitToolFailure{Outcome: Unknown}`。Pending call 不处置，Waiting call 不处置。每个处置是一次普通 Commit，Run 保持 Active，同一 RunID 继续。`TakeoverClaim` 由 Writer 的 Epoch 派生，因此同一 owner 重复调用幂等（同 CommandID 得到 AlreadyApplied）。

**RUN-CMT-8** 每个 Run 的协议版本是 `created.SchemaVersion`，创建时冻结。`RuntimeSnapshot.SchemaVersion` 等于该值；`ProtocolFor(schemaVersion)` 返回绑定该版本 digest/codec/Decide/Evolve 的 `Protocol`。`EvaluateCommit` 接受 command 当且仅当 `CommandEnvelope.SchemaVersion` 等于该 Run 的版本。新 Run 由 `NewRun.SchemaVersion` 决定版本；同一 Session 内不同 Run 可以使用不同版本；v1 Run 的 replay 必须继续使用 `ProtocolV1()`。pre-release 期间 v1 的 Evolve 语义可以修订，早期二进制写下的流不保证在修订后的 v1 下可折叠；发布冻结后，任何 Evolve 变化必须以新的 SchemaVersion 发布，已发布版本的 Decide、Evolve 与 codec 永久保留。Run 的版本与 Session kernel 的 `ProtocolVersion` 无关（SES-VER-1）。

### 5.1 不进入 stream 的数据

Executor 的 in-flight 表可以只是进程内缓存；跨 Worker 恢复所需的是 durable Execution Record。投影缓存是可丢弃的派生数据（EXT-PRJ-3）；`FrozenValueStore` 是 Authority 侧的内容寻址旁存。Worker crash 后，接管者从共享 Execution Store 获取同一 AssignmentKey 的 payload，并由 control plane 决定 Attach、Reconcile、Retry 或 Unknown。对于不能仅凭 AssignmentKey 重新发现的 provider job，Execution Record 还持久化 opaque `ExecutionBinding{Provider, ExecutionRef}`；目标到 Workspace/Runtime 的解析由外部 target resolver/provider adapter 负责，实现 `BindingPort` 的 backend 必须按同一 binding 执行 `DispatchBound`、`AttachBound`、`GetStatusBound`、`GetOutcomeBound` 与 `CancelBound`，并使 `PrepareBinding` 按 AssignmentKey 幂等。Session 所有权与 Worker execution ownership 是两层不同的 ownership。

## 6. Loop ports 与 policy

```go
// package agent/run
type PromptInput struct {
    Session session.SessionID
    Owner OwnerID
    RunID RunID
    SourceStep StepID
    Inputs []AgentInput
}
// package agent/run/loop
type PromptBuilder interface {
    Build(context.Context, run.PromptInput) (Prompt, error)
}
type Prompt struct {
    Model run.ModelRef
    Request sdk.Request
    InputIDs []run.InputID
    Token run.PromptToken // 构造时刻 context 的新鲜度标记
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

`ToolExecutionOutcome` 是 sealed interface：`ToolExecutionSucceeded`、`ToolExecutionFailed`（明确未完成）或 `ToolExecutionUnknown`（可能已发生）。`ValidateArguments` 在 start barrier 前运行，并保持无外部 effect。`ToolExecutionRequest` 携带 RunID、StepID、CallID、attempt 的 `Claim`、冻结 binding 与可选 opaque `TargetRef`（Loop 不解释）。

```go
// 效果层端口（RUN-EXE）
type AssignmentKind string // model | tool
type AssignmentKey struct { Session session.SessionID; RunID run.RunID; StepID run.StepID; CallID run.CallID; Claim run.ExecutionClaim }
type ModelAssignment struct { Model run.ModelRef; Request *run.ModelRequest; RequestDigest run.Digest } // Dispatch payload；digest 仍绑定 frozen request
type ToolAssignment struct { ToolRef run.ToolRef; DefinitionDigest run.Digest; Arguments run.CanonicalJSON; Policy run.ResponsePolicy }
type Assignment struct {
    Session session.SessionID; RunID run.RunID; StepID run.StepID; CallID run.CallID; Claim run.ExecutionClaim
    Target *run.TargetRef
    Schema uint16 // Run 的协议版本
    Kind AssignmentKind; Model *ModelAssignment; Tool *ToolAssignment
}
type Outcome struct { Key AssignmentKey; Model *sdk.ModelResult; Tool ToolExecutionOutcome; Err error; Cancelled bool; Unknown bool }
type Attachment struct {
    State AttachmentState // missing | active | orphaned | terminal
    Execution ExecutionStatus
    Owner string; FencingEpoch uint64; LeaseUntilUnixMilli int64
    BackendAttached bool
}
type ExecutionBinding struct {
    Provider string; ExecutionRef string
}
type BindingPort interface {
    PrepareBinding(context.Context, Assignment) (ExecutionBinding, error) // idempotent by AssignmentKey
    DispatchBound(context.Context, Assignment, ExecutionBinding) error
    AttachBound(context.Context, AssignmentKey, ExecutionBinding) (Attachment, error)
    GetStatusBound(context.Context, AssignmentKey, ExecutionBinding) (ExecutionStatus, error)
    GetOutcomeBound(context.Context, AssignmentKey, ExecutionBinding) (Outcome, error)
    CancelBound(context.Context, AssignmentKey, ExecutionBinding) error
}
type Executor interface {
    Validate(context.Context, Assignment) (*run.ToolFailure, error) // start barrier 前的无副作用校验
    Dispatch(context.Context, Assignment) error                    // 接受后通过 GetOutcome 读取结果
    Attach(context.Context, AssignmentKey) (Attachment, error)     // 区分 active、orphaned、terminal、missing
    GetStatus(context.Context, AssignmentKey) (ExecutionStatus, error)
    GetOutcome(context.Context, AssignmentKey) (Outcome, error)
    Cancel(context.Context, AssignmentKey) error
}
type WorkerOptions struct { ID string; LeaseDuration time.Duration; ReconcileInterval time.Duration }
func (*Worker) Takeover(context.Context, AssignmentKey) error // control plane 在确认可接管后调用
func (*Worker) Reconcile(context.Context) (int, error)        // control plane 采用全部租约过期记录，返回采用数
func (*Worker) Dispose(context.Context, AssignmentKey) error   // control plane 放弃一条记录：结算为 Unknown 终态
func NewLocalExecutor(models ModelCatalog, tools ToolCatalog, sink EventSink, streaming bool) (*LocalExecutor, error)
type RecoveryDisposition string // missing | active | deferred | terminal
func Reattach(lifetime context.Context, exec Executor, sid session.SessionID, deliver Deliver) run.Reattacher
```

这里有三个不同的状态域，不能用同一个枚举替代：`ExecutionStatus` 描述 provider execution 的生命周期；`AttachmentState` 描述 Executor 对 Assignment 的观察（`missing | active | orphaned | terminal`）；`RecoveryDisposition` 描述 authority 的下一步权限（`missing | active | deferred | terminal`）。因此 `AttachmentState=orphaned` 明确映射为 `RecoveryDisposition=deferred`：前者是资源观察，后者表示暂时不得处置 Run。`RunStatus` 与 `TurnStatus` 的 `active` 也只表示各自聚合仍未终态，不等价于 Executor 的 `active`。

**RUN-EXE-1（Assignment）** Assignment 是 authority 交给 Executor 的工作单元：目标（RunID、StepID、CallID）、attempt 身份（Claim）、Run 的协议版本，以及执行所需的冻结输入。模型 Assignment 携带 `ModelRequest` 与 `RequestDigest`；Executor 接受后必须将该 payload 写入自己的 durable Execution Record，不能依赖 Authority Session 或某个 Worker 的本地存储。Assignment 可携带 opaque `TargetRef`，但 Agent Core 不解释其 Kind 或生命周期。`Key()` 是 attempt 的身份，也是其结算 CommandID 的 preimage（第 2 节 identity 表）。

**RUN-EXE-2（Outcome）** Outcome 是 Executor 对一个 Assignment 的唯一回答：模型 Assignment 得到 `Model` 或 `Err`，工具 Assignment 得到 sealed 的 `Tool`；`Cancelled` 表示 Executor 按要求停止了该效果；`Unknown` 表示 Executor 已明确结束该执行的恢复，外部结果仍无法确定。Outcome 通过 `GetOutcome` 按 key 读取，也可以由 deployment 层通过通知唤醒读取方；每个被接受的 Assignment 最终至多提交一个 authoritative Outcome。`GetOutcome` 返回的 error 表示读取操作失败，执行状态保持原值。Worker 对读取失败退避重试并保持 lease；失去 ownership 后停止处理。Loop.Run 将读取错误返回给调用方，保留 Executing，后续可重新关联结果。Reattach 的 Attach 请求由调用 context 控制，后台结果读取由构造时传入的 Session ownership `lifetime` 控制。

**RUN-EXE-3（Dispatch 与 Attach）** `Dispatch` 接受 Assignment 后立即返回。确定的 acceptance 失败返回普通 error；请求发出后的超时、取消或响应丢失返回 `ErrDispatchUnknown`，Authority 保留 Executing。接受时 Executor 先持久化完整 Assignment payload，再进入 `Dispatching`，然后调用 backend；`Accepted` 表示确定尚未开始，`Dispatching` 表示可能已经开始，`Running` 表示 backend 已接受。backend 返回 `ErrDispatchUnknown` 时，Worker 保持 Dispatching、续租并读取最终 Outcome；HTTP Server 可确认 Worker 已持久化的 acceptance。相同 Assignment 的 Dispatch 重放确认已有 acceptance，并保留该记录的 owner、epoch 与状态；已有记录的恢复通过显式 `Takeover` 触发，包括首次接受后尚未开始的记录。`Attach` 按 AssignmentKey 返回 `active`、`orphaned`、`terminal` 或 `missing`：只有 `missing` 才允许 Authority 自动 dispose；`orphaned` 必须由 control plane reconcile、takeover 或明确处置。`GetStatus` 与 `GetOutcome` 不读取 Session。对已失效 owner 的 takeover 由 control plane 决定，Worker 以新的 fencing epoch 获取同一个 AssignmentKey；接管 `Running`/`Dispatching` 时先尝试 backend attach：可 attach 则继续观察原执行；不可 attach 时模型 Assignment 允许显式 retry dispatch（同一冻结请求重放），已绑定 backend job 的工具经 BindingPort 按 binding 重连或重派；未绑定 ExecutionBinding 的工具执行可能已越过效果边界、重派可能重复外部效果，采用后直接以 Unknown 结算（TRN-DUR-4），工具定义声明可重放之前不允许重派。已有 ExecutionBinding 的记录使用 BindingPort 完成全部 backend 操作；backend 缺少该能力时返回 `ErrBindingUnsupported`，保留原 binding。`Cancel` 针对一个 Assignment；Run 级批量取消由上层枚举 targets。`LocalExecutor` 使用进程内记录；durable Worker 使用共享 Execution Store。Worker 可配置定时 reconcile（`ReconcileInterval`）：每个 tick 对租约过期的记录显式执行 `Takeover`，由同一进程内嵌的控制面接管 orphaned 执行；带外部控制面的部署保持该循环关闭，直接调用 `Takeover`/`Reconcile`。

**RUN-EXE-4（Outcome 的结算）** `Loop.Deliver` 以 `Outcome.Key` 在投影中定位 Executing 的目标：同一 step 或 call、同一 Claim。找到则以该 Claim 派生的结算 CommandID 提交 Submit*（模型：结果、provider 失败、畸形结果的 Reject、取消或本体缺失的 Recover；工具：按 sealed outcome 映射，Executor 返回的缺失或未知执行结果记 Unknown）；找不到——attempt 已被结算或处置、Run 已终结、Claim 不符——则丢弃，不写任何事实（`LoopDropped`）。GetOutcome 的读取错误保留 Executing，只有成功读取的 Outcome 进入 Deliver。结算使用独立 control context（RUN-LOP-5）。

**RUN-EXE-5（在 start barrier 前校验）** 工具 Assignment 在 `StartToolCall` 之前经 `Executor.Validate` 校验 lookup、definition digest、response policy 与 arguments；非 nil 的失败以空 Claim 的 CommandID 提交 `SubmitToolFailure(Known)`，不跨越 start barrier（RUN-LOP-4）。模型 Assignment 在 `StartModelExecution` 之前经同一入口校验 Executor 能否服务该 `ModelRef`；非 nil 的失败使 Loop 以 `ErrModelUnavailable` 返回，step 保持 Prepared，不写入 start 或 recovery 事实——目录缺失不应在每次驱动上留下三条事实。校验不产生外部效果。

**RUN-EXE-6（控制面）** failure 检测与重试决策不属于数据面：`effect.Port` 的六个方法按单个 Assignment 收发消息，而 Takeover/Reconcile/Dispose 作用于 durable Execution Record，因此控制面是 application 层职责，不进 `effect.Port`。控制面只有三个操作：`Reconcile` 采用全部租约过期记录；`Takeover` 采用一条；`Dispose` 将一条非终态记录无条件结算为 Unknown 终态（OutcomeEnvelope 携带 `WireError{Code:"disposed"}`，best-effort cancel backend，不要求 backend 可达），authority 经下一次 GetOutcome 读取后按 RUN-CMT-7 处置。authority 永不采用。两种存在形态：Worker 内嵌循环（`ReconcileInterval`，每个 tick 对所有过期记录执行 Takeover）或外部控制面经 HTTP 控制端点（`/takeover`、`/reconcile`、`/dispose`）驱动同一组方法；两者取一，带内嵌循环的部署不从外部驱动。会产生孤儿记录的部署（worker 进程可能死亡或与其 store 断连）必须提供其中一种；控制面缺席是部署缺陷，协议本身检测不到控制面是否存在。`RecoveryDisposition=deferred` 无界是设计使然（反重复执行，TRN-DUR-4），Application 必须提供 stuck-Run 的可观测性（事件时间戳）并把 Dispose 暴露为运维入口。Execution Store 是 fencing authority：所有权终止条件是记录缺失、进入终态、或 owner/fencing epoch 被新 owner 改变；租约过期不终止所有权，heartbeat 与 watch 在短暂 store 故障下继续工作（Renew 不检查过期，恢复后续租；PutOwned 要求活租约，结算随续租恢复）。单条记录损坏或读取失败不中断 Reconcile（FileStore.List 跳过无法解码的记录）。

**RUN-EXE-7（Assignment payload）** Dispatch 必须携带内联 payload：模型 Assignment 的 `ModelRequest` 随 Assignment 内联，Executor 派生 request digest 并校验 `RequestDigest` 与 ModelRef 一致后才接受，接受时把完整 Assignment 持久化进 Execution Record；`Attach`/`GetStatus`/`GetOutcome`/`Cancel` 只按 AssignmentKey 定位记录，从不读取 payload。digest-only 的模型 Assignment 只允许出现在 authority 内部的 `AssignmentFromTarget` 重建（RUN-CMT-7 的 Attach 询问），不得进入 Dispatch：缺少内联请求体的 Dispatch 是确定的 acceptance 拒绝，效果未开始。FrozenValueStore 仍是 Authority 侧的持久旁存（RUN-WIR-4）；执行 payload 的生命周期由 Execution Record 管理，Executor 在执行时不反查 Authority 的 FrozenValueStore。

**RUN-EXE-8（部署说明）** v1 假设 worker 池同构：池内全部节点服务同一 Catalog（同一 ModelRef、ToolRef 集合与定义 digest），Assignment 的 definition digest 校验在同构池上恒通过，异构池上转为确定性拒绝；跨异构池的放置由 application 路由，协议不规定。数据面与控制面只面向 loopback 同机信任域：协议层的线上身份只有 ExecutionClaim（RUN-EXE-1）与 Execution Store 的 owner/fencing epoch，无认证机制；跨机器部署由 application 在信任域边界提供传输保护，协议不规定。v1 不规定推送通道：authority 侧统一经 GetOutcome 长轮询读取结果，deployment 层的通知只作为唤醒读取方的优化（RUN-EXE-2），不改变读取语义。

```go
type Settings struct {
    Scheduling       run.ToolScheduling // 来自 AgentPreset：工具调用并行/串行与并发上限
    MalformedRetries uint8              // 来自 AgentPreset：畸形模型结果的重试上限
    TargetResolver   TargetResolver     // application 提供的 opaque target 解析器
}
type LoopResult struct {
    Disposition LoopDisposition // LoopWaiting | LoopFinished | LoopDispatched | LoopDelivered | LoopDropped
    Reason WaitReason           // 仅 ExecutionRecovery 时为 execution_recovery；否则为空
    ExecutionRecovery bool
    Result *run.RunResult
    Dispatched []AssignmentKey  // Advance 交给 Executor 的 Assignment
}
func New(exec Executor, builder PromptBuilder, settings Settings) (*Loop, error)
func (*Loop) Advance(context.Context, run.Runtime, session.SessionID, run.RunID, EventSink) (LoopResult, error)
func (*Loop) Deliver(context.Context, run.Runtime, session.SessionID, Outcome, EventSink) (LoopResult, error)
func (*Loop) Run(context.Context, run.Runtime, session.SessionID, run.RunID, EventSink) (LoopResult, error) // 阻塞封装
```

**RUN-LOP-1** `Settings` 是 Loop 从 AgentPreset 取得的执行参数（TRN-PST-1），不是独立的可插拔组件。`Scheduling` 在 `SubmitModelResult` 时写入 `ToolStepOpened.Scheduling` 并冻结在该 ToolStep 上；后续 Loop 必须按冻结值调度，不得改用当时进程的 Settings。未指定 Mode 时冻结为 `parallel`，`MaxParallel` 零值表示当前 Start 批次全部 Pending call 可并行。空 Mode 按 parallel 解释，不得在 normalize 时填入默认字符串。畸形模型结果的处置由 `MalformedRetries` 决定：该 ModelStep 已记录的 `Rejects` 少于该值时选择 `ModelRejectRetry`，否则 `ModelRejectFailRun`；零即首次失败。`streaming` 表示是否请求可用的流式模型端口；两种模式都产生同一完整 `sdk.ModelResult`。Loop 不管理 Executor lease 或 Worker heartbeat；这些属于 Executor/control plane。Loop 只验证 Run Claim，并通过 Session Writer 完成语义 settlement（RUN-CMT-6）。

**RUN-LOP-7** `ModelRef` 是冻结请求中的执行身份。`ModelCatalog.ResolveModel` 在同一 Run 生命周期内必须把同一 `ModelRef` 解析为等价的执行语义。provider 绑定不进入 frozen request，因此 Catalog 不得把同一 ref 改绑到不同实现。

`LoopResult` 的语义固定为：`LoopWaiting` 时 `Result` 为 nil，表示没有可执行 effect、Run 仍为 active。`ExecutionRecovery` 等于 `NeedsRecovery(state)`。该值为 true 表示存在本进程未持有 Claim 的 Executing 目标（只在崩溃后、接管处置之前出现），`Reason` 为 `execution_recovery`；否则 `Reason` 为空。Waiting call 不进入 `LoopResult`；Application 通过投影的 `WaitingCalls` 读取。`LoopFinished` 时 `Result` 非 nil，并等于 terminal Run 的 `RunResult`。

`PromptBuilder` 从 `PromptInput` 接收 Run 边界事实；它从 Session 的 chatlog fold 读取对话内容（上一步的 assistant 与 tool_result 已随 Run fact 同组提交），并使用自己注入的 memory、attachments 与 product policy 组装 `sdk.Request`。Runtime 验证并冻结 prompt builder 返回的 request，PromptBuilder 管理 application context。

## 7. Loop execution

```text
Loop.Advance(ctx, runtime, sessionID, runID, sink):        // 不等待任何效果
  repeat:
    snapshot = Runtime.Load(sessionID, runID)
    if terminal: emit observational run_finished; return Finished(snapshot.Result)
    effect = run.Next(snapshot.State)
    NeedModelRequest  → Plan、Freeze、Commit Prepare；continue
    WithdrawPrepared  → Commit Withdraw；continue
    StartModelCall    → Commit StartModelExecution{Claim}；Executor.Dispatch(Assignment{model})；return Dispatched
    StartToolCalls    → 对冻结 Scheduling 允许的每个 Pending call：Validate → Known 失败直接结算；
                        否则 Commit StartToolCall{Claim}，Executor.Dispatch(Assignment{tool})；return Dispatched
    Idle              → return Waiting（NeedsRecovery 设 ExecutionRecovery）

Loop.Deliver(ctx, runtime, sessionID, outcome, sink):      // Outcome 到达时，来自任何地方
  snapshot = Runtime.Load
  目标不再 Executing 或 Claim 不符 → return Dropped（不写）
  Commit Submit*（以 outcome.Key.Claim 派生结算 CommandID）
  终态 → emit run_finished；return Finished   否则 return Delivered（宿主接着 Advance）

Loop.Run(...):  // 阻塞封装：Advance → 等待本次 dispatch 的 Outcome → Deliver → Advance，直到 Waiting 或 Finished
```

模型结算（无 tool call 的 SubmitModelResult、SubmitModelFailure、FailRun 的 RejectModelResult）可能终结 Run；此时 CommitResult.Snapshot 已是终态，Loop 直接 emit run_finished 并返回 Finished，不再 Load。工具结算不会终结 Run。

每个 `Loop` 实例为每个 `(SessionID, RunID)` 分配一个本地 slot：同一 Run 的 `Advance` 与 `Deliver` 串行；`Run` 进行期间对同一 Run 的 `Advance` 或第二个 `Run` 返回 `ErrRunAlreadyRunning`；不同 Run 可以并行驱动。宿主必须保证一个 Session 在一个进程内只有一个 Loop 实例驱动它的 Run（与 `Writer` 一一对应）。

**RUN-LOP-2** `NeedModelRequest` 调用 PromptBuilder，冻结 sdk.Request，验证 model、ordered InputIDs 与 ToolSpecs，计算 request/tools/binding digests 和 derived CommandID/StepID，再提交 Prepare（command 携带本体）。prepare stale 后重新 Load；同 Position 的内容拒绝不得 livelock 重试。业务停止统一使用 `CancelRun`。

**RUN-LOP-8** `WithdrawPrepared` 时 Loop 提交 `WithdrawPreparedStep{StepID}`，随后重新 Load；被放弃请求的本体在 FrozenValueStore 中可立即释放。Loop 不为输入做任何其他事：Executing 与 ToolStep 期间到达的输入留在 `PendingInputs`，由随后 `Open` 的 `NeedModelRequest` 经 `PromptInput.Inputs` 交给 PromptBuilder。

**RUN-LOP-3** `StartModelCall` 在 Validate 成功后 Commit start barrier；`CommitAccepted` 或同 Claim 重放的 `CommitAlreadyApplied` 授予该 attempt execution。Loop 将 Assignment 交给 Executor，冻结的 ModelRequest 在 Dispatch 时成为 Executor-owned payload。start fact 保存 Claim，Outcome 据此定位目标（RUN-EXE-4）。streaming 与 non-streaming 均产生完整 `sdk.ModelResult`，delta 发往 EventSink。

Validate 发现模型不可用时返回 `ErrModelUnavailable`，step 保持 Prepared。确定的 Dispatch 拒绝使 Loop 提交 `RecoverModelExecution` 并返回错误；`ErrDispatchUnknown` 保留 Executing、等待实际 Outcome（RUN-EXE-3）。Outcome 中的 provider 失败提交 `SubmitModelFailure`；Cancelled 提交 `RecoverModelExecution` 回到 Open；结构、binding 或 freeze 失败提交 `RejectModelResult`，按调用方 disposition 重试或结束；成功结果提交 `SubmitModelResult`。

**RUN-LOP-4** Tool execution 先经 `Executor.Validate` 按 frozen binding 验证 Ref、definition digest、response policy 与 arguments（RUN-EXE-5）。校验失败在 Pending 状态提交 `SubmitToolFailure(Known)`；通过后逐 call 提交 `StartToolCall{Claim}`，再 Dispatch Assignment。确定拒绝派发以 Known 失败结算；`ErrDispatchUnknown` 保留 Executing 并等待实际 Outcome（RUN-EXE-3）。Executing 由当前执行者或接管流程结算，已终态 call 的结果保持稳定（TRN-DUR-4）。

一次 Advance 按冻结的 `ToolStep.Scheduling`（parallel / sequential、MaxParallel）分批 Start 与 Dispatch Pending call，每个 Outcome 经 Deliver、以原 Claim 派生的 CommandID 独立提交。外层 ctx 取消后停止新增 Start，已派发的执行经 Outcome 结算。`Next=Idle` 对应 `LoopWaiting`，`ExecutionRecovery` 取自 `NeedsRecovery(state)`。Application 从 `WaitingCalls` 读取请求，批准、拒绝或提交外部响应后再次驱动。

tool panic 或 effect 结果无法确定时提交该 call 的 `SubmitToolFailure(Unknown)`，同批 sibling 继续、Run 保持 Active。业务取消按 RUN-MCH-2 的 `CancelRun` 规则结算所有未完成调用。已接受 start 的 worker 响应取消并返回 Outcome；结算使用独立 control context。Outcome 读取失败经协调错误通道返回，Executing 保留至实际结果或接管处置。

**RUN-LOP-5** 效果在 Executor 的上下文里运行；Loop 对已接受 effect 的结算使用独立 control context（`Deliver` 内），调用方请求的取消不能丢弃一个已发生效果的 Outcome。Application 的业务停止顺序为先 Commit `CancelRun`，再取消驱动 ctx；阻塞式 `Run` 的 ctx 取消使它调用 `Executor.Cancel`，已 dispatch 的效果各自交回 Cancelled 的 Outcome 并被结算（模型步撤回到 Open、工具按其 outcome），之后 `Run` 以 `ctx.Err()` 返回。非 sentinel Commit error 以同 CommandID 重放一次；仍未知时返回错误，由后续 Load/Record 查询 authority。stale/terminal/conflict 触发 reload/drop，旧 external effect 保持单次执行尝试。`ErrOwnershipLost` 是终止性错误：第一个被 kernel 围栏的结算使 Loop 调用 `Executor.Cancel(runID)`，其后到达的 Outcome 不再提交（提交也会被 kernel 拒绝），Loop 以该错误返回，且该错误优先于同一步内其他错误；模型步的结算遇该错误同样不做一次重放。已发生的外部 effect 由接管者按 RUN-CMT-7 询问后处置。工具实现配合 context 返回；永久阻塞由 application 处理。

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

**RUN-CMP-1** command/fact discriminator、wire fields、canonical digest、derived ID 与 `ProtocolV1().Evolve` 的任何修改必须进入新 `SchemaVersion`；Registry 继续 decode/fold 全部已发布版本，同一 Run 的 writer 不得混写不同版本。Run 版本演进不触发 Session kernel 版本变化。

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
- 接管处置：关闭 Writer 后以新 Writer 打开（Epoch 加一）并调用 `RecoverInterrupted`：Executing model 被撤回，Run 回到 `Open`、`ModelSteps` 不计入该步、Executing 期间投递的输入仍在 `PendingInputs`；随后的 Prepare 产生新的 StepID 并消费这些输入，不重发原 RequestDigest；Executing tool 记 Unknown 且 companion 在同组写入 status=`unknown` 的 `tool_result`，同 step 的 Pending 与 Waiting call 不受影响；Run 保持 Active；同一 Epoch 重复调用返回 0 且无新写入；没有 Executing 目标时返回 0；
- 接管重连：`RecoverInterrupted` 对每个 Executing 目标先经 `Reattacher.Attach` 询问，携带 start 事实记录的 Claim 与 Run 的协议版本；`active`、`terminal` 保持 Executing 与 Claim，随后以原 Claim 结算；`deferred` 保持原状态，等待 control plane 处置；`missing` 按接管处置；`Reattacher` 为 nil 时全部处置。
- 效果层：`Advance` 提交 start barrier 后把 Assignment 交给 Executor 并返回 `LoopDispatched`，不等待效果；`Deliver` 以 Key 定位 Executing 目标并结算，Run 终结时返回 `LoopFinished`；attempt 已处置或 Claim 不符的迟到 Outcome 返回 `LoopDropped` 且不写入；`Cancelled` 的模型 Outcome 使 step 撤回到 Open；`LocalExecutor.Attach` 对执行中 attempt 返回 `active`，完成后返回 `terminal`，记录不存在时返回 `missing`；GetOutcome 读取失败保持执行状态，真实结果稍后仍可结算；Dispatch 重放保持已有记录，显式 Takeover 处理恢复；已有 binding 缺少 BindingPort 时返回错误。
- 所有权失效：旧 Writer 上的 Runtime 在被接管后 Commit 返回 `ErrOwnershipLost` 且 stream 无新行（fencing 由 SES-OWN-2 保证，本层观察结果）；
- FrozenValueStore：`Put` 幂等；未知 digest 的 `FrozenRequest` 返回 `ErrFrozenValueMissing`；Authority 侧本体可按策略回收，Assignment 被接受后执行 payload 由 Execution Store 管理；Worker takeover 不读取 Session；
- MachineState codec：每个 Current variant 与终态 round-trip、拒绝 unknown field / 非法判别式 / trailing data（`agent/run` 单元测试）。

Loop conformance 必须覆盖：

- 单模型完成、tool round trip、approval/external response wait/resume；
- known failure 继续、Unknown 继续、tool panic、aliased ToolRef 与 validation；
- parallel/sequential 按冻结 `ToolStep.Scheduling` 调度，不得改用当时的 Settings；
- `ModelCatalog.ResolveModel` 失败或 nil 时 ModelStep 保持 Prepared，Run 保持 active，Loop 返回 `ErrModelUnavailable`；
- ctx cancellation 撤回 Executing 的模型步、随后的 Run 重新规划且只调用模型一次、`ModelSteps` 只计重规划的那一步；executor 取不到冻结本体时撤回并返回错误，后续驱动重新规划；explicit malformed-result disposition；
- Cancel 将 Executing tool/model 投影到 `UncertainCalls` / `UncertainModel`；ExternalResponse reject 为 `response_rejected`；
- streaming delta 与 nil result、EventSink committed observation 携带完整组；
- 非 sentinel commit error 的一次重放、prepare no-progress rejection 与无 livelock；
- 模型结算终结 Run 时 Loop 不再 Load，返回 `LoopFinished` 且 `Result` 等于 Record 的终态；
- Writer 返回 `ErrOwnershipLost` 时 Loop 取消 worker、不再提交 settlement、以该错误返回；随后新 owner 的 `RecoverInterrupted` 把该 Executing 目标记为 Unknown 或撤回到 Open。
