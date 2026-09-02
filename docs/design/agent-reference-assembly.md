# Twilight Agent 参考组装

状态：设计草案。与 [Run](agent-run.md)、[Turn](agent-turn.md)、[Chatlog](agent-session-chatlog.md) 冲突时以各正式规范为准。

本文规定 Memory 参考 agent 的三处组装：ExecutionBinding 公开字段、Planner、用户正文在 Chatlog Input 与 Run AgentInput 上的同一份 payload。

## 1. ExecutionBinding

Session 保存 `ExecutionBindingRef{ID, Digest}`。Digest 覆盖公开配置：

```go
type BindingPublic struct {
    SchemaVersion uint16 // 1
    Model run.ModelRef
    Tools []run.ToolSpec
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

**REF-PLN-1** `fold` 为 `ContextFold` 的输出（含已应用的 checkpoint）。

**REF-PLN-2** `sdk.Messages` 顺序：

1. `pub.SystemPrompt` 非空时一条 system message；
2. 按 `fold`：`input` → user；`assistant` → assistant；`tool_result` → tool；`summary` → assistant text。

Coordinator 在 Drive 前执行 `MaterializeAll`。Planner 消费当时的 fold。`hint.LastToolStep` 对应的 `tool_result` 已在 fold 中。

**REF-PLN-3** `hint.Inputs` 与本 Turn 已 delivered、且属于本次 Prepare 的 Input 按 ID 对齐。这些 Input 已在 fold 中时，只使用 fold。

**REF-PLN-4** `RequestPlan.Model = pub.Model`，`Tools = pub.Tools`，`InputIDs` 为本次消费的 PendingInput IDs。`PlanningToken` 随 fold 的 Entry digest 序列或 Binding Digest 变化。

**REF-PLN-5** 无附件时 TextPart 直接写入 sdk.Message。ReferencePart 经 ContextMaterializer 转换。

## 3. 用户正文

同一份 canonical JSON：

```text
twilight/chatlog/input_submitted.Content
run.AgentInput.Payload
```

**REF-INP-1** v1 形状为 `{"text":"<用户字符串>"}`。

**REF-INP-2** `StartRequest.InputIDs[i] == InitialInputs[i].ID`，且等于已 submitted 的 InputID；`InitialInputs[i].Payload` 等于该 Input 的 Content。`input_delivered` 把 InputID 挂到 TurnID。

**REF-INP-3** Planner 把 `{"text":...}` 投影为 sdk user text。

## 4. Memory 组成

```text
sessionStore = session.NewMemoryStore()
runRuntime   = run.NewRuntime(run.NewMemoryStore())
catalog      = extension.BuildCatalog(protocol, []Module{chatlog.Module, turn.Module})
appender     = extension.NewSemanticAppender(sessionStore, catalog, artifacts)
bindings     = Resolve(ExecutionBindingRef) -> loop.New(models, tools, contextPlanner, policy, pub.Streaming)
coordinator  = turn.Coordinator{Sessions, Appender, Runtime: runRuntime, Bindings, Mapper: turn.MapperV1}

input_submitted
coordinator.Start
  twilight/turn/started + twilight/chatlog/input_delivered*
  Runtime.Create + AcceptInput* + Loop.Run
  MaterializeAll → twilight/chatlog/assistant, twilight/chatlog/tool_result
  twilight/turn/completed | twilight/turn/failed
```

参考 agent 的工具 ResponsePolicy 为 `DirectExecution`。ContextFold 在无 checkpoint 时输出全部有效条目。
