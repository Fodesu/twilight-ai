# Twilight Agent 参考组装

状态：设计草案。与 [Run](agent-run.md)、[Turn](agent-turn.md)、[Chatlog](agent-session-chatlog.md) 冲突时以各正式规范为准。

本文规定 Memory 参考 agent 的四处组装：ExecutionBinding 公开字段、Planner、用户正文在 Chatlog Input 与 Run AgentInput 上的同一份 payload、session 作用域的输入路由（SessionDriver）。

## 1. ExecutionBinding

Session 保存 `ExecutionBindingRef{ID, Digest}`。Digest 覆盖公开配置：

```go
type PublicTool struct {
    Ref run.ToolRef
    Definition run.ToolDefinition
    Policy run.ResponsePolicy
}
type BindingPublic struct {
    SchemaVersion uint16 // 1
    Model run.ModelRef
    Tools []PublicTool   // ToolSpec 与 Request.Tools 都由此派生
    Streaming bool
    PlannerID string // "twilight/turn/planner/context-v1"
    SystemPrompt string
}
```

**REF-BND-1** `Digest = Digest("twilight/turn/binding", canonical(BindingPublic))`。

**REF-BND-2** `Resolve(ref)` 在 Digest 匹配时返回 RunDriver：公开配置、ModelCatalog、ToolCatalog、Planner。同一 Run 内同一 ModelRef 的解析语义保持等价（RUN-LOP-7）。

**REF-BND-3** 参考 Planner 的 `RequestPlan.Model` 等于 `BindingPublic.Model`。

## 2. Planner

Planner ID：`twilight/turn/planner/context-v1`。

```go
func Plan(ctx context.Context, hint run.PlanningHint, fold []chatlog.Entry, pub BindingPublic) (loop.RequestPlan, error)
```

**REF-PLN-1** `fold` 为 `ContextFold` 对该 Session chatlog 事件的输出（含已应用的 checkpoint）。Planner 在每次 Plan 时经 `extension.ProjectionReader` 读取 `twilight/chatlog/context` 投影（snapshot 加 tail）。

**REF-PLN-2** `sdk.Messages` 顺序：

1. `pub.SystemPrompt` 非空时一条 system message；
2. 按 `fold`：`input` → user；`assistant` → assistant（ToolCallPart 的 `ProviderCallID` 写入 `sdk.ToolCallPart.ToolCallID`）；`tool_result` → tool（以同 Turn assistant 中同 CallID 的 `ProviderCallID` 配对）；`summary` → assistant text。

上一步的 assistant 与 tool_result 已随对应 Run 事实同 commit 提交，Planner 消费时的 fold 总是包含它们；`PlanningHint` 不携带模型结果或工具结果。

**REF-PLN-3** `hint.Inputs` 与本 Turn 已 delivered、且属于本次 Prepare 的 Input 按 ID 对齐，包括回合中途经 Deliver 进入的输入。这些 Input 的 `input_delivered` 与 `input_accepted` 同 commit，Plan 时一定已在 fold 中，只使用 fold。

**REF-PLN-4** `RequestPlan.Model = pub.Model`；`Request.Tools` 与 `Tools`（ToolSpec：Ref、DefinitionDigest、Policy）都由 `pub.Tools` 派生，顺序一致；`InputIDs` 为本次消费的 PendingInput IDs。`PlanningToken` 随 fold 的 Entry digest 序列或 Binding Digest 变化。

**REF-PLN-5** 无附件时 TextPart 直接写入 sdk.Message。ReferencePart 经 ContextMaterializer 转换。

**REF-PLN-6** 同一 Turn 有多个 Run attempt 时，参考 Planner 把全部 attempt 的 assistant 与 tool_result 按 commit 顺序纳入请求，包括失败 attempt 的部分输出与 status=`unknown` 的工具结果。这与用户中断后继续的语义一致。Application 可以替换为其他策略（例如排除 `AttemptView.End` 为 failed 的 attempt 的条目），策略只影响请求组装，不影响 stream 与 ContextFold。

## 3. 用户正文

同一份 canonical JSON：

```text
twilight/chatlog/input_submitted.Content
run.AgentInput.Payload
```

**REF-INP-1** v1 形状为 `{"text":"<用户字符串>"}`。

**REF-INP-2** `StartRequest.Inputs[i].ID` 等于已 submitted 的 InputID，`Payload` 等于该 Input 的 Content。`input_delivered` 把 InputID 挂到 TurnID；`twilight/run/input_accepted` 在同一 commit 把同一 payload 交给 Run。

**REF-INP-3** Planner 把 `{"text":...}` 投影为 sdk user text。

## 4. SessionDriver

Coordinator 是 Turn 作用域的：Turn 结束即返回。参考组装提供一个 session 作用域的 `SessionDriver`，把用户输入按当前状态路由到 Deliver 或 Start，并在 Turn 结算后自动开启下一个 Turn。它只组合 Coordinator 与两个投影，没有自己的持久状态，不进入 turn 或 run 协议。

```go
type SessionDriver struct {
    Coordinator turn.Service
    Projections extension.ProjectionReader
    Binding turn.ExecutionBindingRef  // 新 Turn 使用的执行绑定
    Companion turn.CompanionVersion
    NewTurnID func() turn.TurnID
}
func (d *SessionDriver) Send(ctx, sid session.SessionID, inputs []run.AgentInput) (turn.TurnResponse, error)
func (d *SessionDriver) OnTurnSettled(ctx, sid session.SessionID) (turn.TurnResponse, bool, error)
```

**REF-DRV-1** `Send` 先读 `twilight/turn/surface`：存在 `active` 的 Turn 时调用 `Deliver`，输入进入该 Run 的下一步；否则以 `NewTurnID()`、`Binding`、`Companion` 调用 `Start`。这对应 inbox 模型中"steer 在运行中注入下一步、在空闲时开启新 turn"的行为。输入在两种情形下都已由 Application 先写入 `input_submitted`。

**REF-DRV-2** `OnTurnSettled` 在 Turn 进入 `completed`、`failed`、`stopped` 或 `superseded` 后调用：读 `twilight/chatlog/surface`，若存在 `submitted` 且未 delivered 的输入，按 `input_submitted` 的 stream 顺序取全部，`Start` 新 Turn 并返回；否则返回 `false`。这对应 inbox 模型的 `next-turn` 列表：已提交而未投递的输入就是该列表，不需要另一份持久结构。

**REF-DRV-3** Turn 为 `attempt_failed` 时 `Send` 返回 conflict，不自动 Retry 或 Settle；这两者是 Application 的决定。`Deliver` 与最后一步结果并发失败（TRN-DLV-3）时，`Send` 得到 `completed`，输入仍为 `submitted`，随后的 `OnTurnSettled` 会把它带入下一个 Turn。

**REF-DRV-4** 崩溃恢复：`SessionDriver` 从两个投影重建。对每个 session，先按 TRN-REC-1 处理 `active` 与 `attempt_failed` 的 Turn；没有未结算 Turn 时调用 `OnTurnSettled` 消费积压的输入。

## 5. Memory 组成

```text
sessionStore = session.NewMemoryStore()                       // commit、CommitIn、snapshot、控制面 KV
registry     = extension.BuildRegistry(profile, chatlog.Module, turn.Module, runmod.Module)
projections  = extension.NewProjectionReader(sessionStore, registry)
bindingStore = artifact.NewMemoryBindingStore()
ledger       = extension.NewKVLedger(sessionStore, bindingStore)  // claim 存于控制面 KV twilight/artifact/claim
appender     = extension.NewSemanticAppender(sessionStore, registry, bindingStore, ledger)
runtime      = runmod.NewRuntime(appender, projections, extension.Leases{Store: sessionStore}, runmod.NewMemoryFrozenValues(), turn.CompanionV1(registry), runmod.DefaultSnapshotPolicy)
drivers      = Resolve(ExecutionBindingRef) -> loop.New(models, tools, contextPlanner, policy, pub.Streaming)
coordinator  = turn.Coordinator{Projections: projections, Appender: appender, Runtime: runtime, Bindings: drivers}
session      = SessionDriver{Coordinator: coordinator, Projections: projections, Binding: bindingRef, Companion: turn.CompanionV1Version, NewTurnID: ...}

input_submitted
session.Send                                   // 无 active Turn → coordinator.Start
  commit 1: twilight/turn/started + twilight/chatlog/input_delivered* + twilight/run/created + twilight/run/input_accepted*
  Loop.Run
    commit: twilight/run/model_step_prepared            （请求本体 → FrozenValueStore）
    commit: twilight/run/model_step_started             （lease → 控制面 KV，同事务）
    commit: twilight/run/model_step_completed + twilight/run/tool_step_opened + twilight/chatlog/assistant
    commit: twilight/run/tool_call_started
      input_submitted; session.Send               // 有 active Turn → coordinator.Deliver
      commit: twilight/run/input_accepted + twilight/chatlog/input_delivered
    commit: twilight/run/tool_call_completed + twilight/chatlog/tool_result
    commit: twilight/run/model_step_prepared            （PlanningHint.Inputs 含中途输入）
    ...
    commit: twilight/run/model_step_completed + twilight/run/ended + twilight/chatlog/assistant + twilight/turn/completed
session.OnTurnSettled                          // 有积压的 submitted 输入 → 开下一个 Turn
```

参考 agent 的工具 ResponsePolicy 为 `DirectExecution`。ContextFold 在无 checkpoint 时输出全部有效条目。
