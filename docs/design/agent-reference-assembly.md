# Twilight Agent 参考组装

状态：设计草案。`agent/ref` 已实现 Agent 配置面（Profile）、ContextPlanner、Memory 组装、宿主驱动（Memory.Drive）、SessionDriver 与 Session 宿主。与 [Run](agent-run.md)、[Turn](agent-turn.md)、[Chatlog](agent-session-chatlog.md) 冲突时以各正式规范为准。

补充说明：ContextPlanner 把回合中途投递的输入排在其之前尚未结算的工具结果之后。原因是这类输入的 `input_delivered` 先于 `tool_result` 进入 stream，而 provider 要求工具结果紧随发出调用的 assistant 消息。fold 顺序不变，只影响请求组装。

本文规定 Memory 参考 agent 的六处组装：Agent 配置面（Profile 公开字段与 digest 边界）、Planner、用户正文在 Chatlog Input 与 Run AgentInput 上的同一份 payload、驱动（Memory.Drive——Coordinator 只做协议提交与状态读取，驱动的生命周期属宿主）、session 作用域的输入路由（SessionDriver）、宿主对象（Session）。

## 1. Agent 与 Profile

Agent 是一个可注册的执行配置：持久的公开配置（Profile）加上解析它的进程内能力。Session 只保存 `turn.ProfileRef{ID, Digest}`；密钥、client 与工具实现留在进程内，重启后以同一公开配置重新注册即可继续解析。

```go
type Agent interface {
    Profile() Profile
    ResolveModel(run.ModelRef) (loop.ModelInvoker, error)
    ResolveTool(run.ToolRef) (loop.ExecutableTool, error)
}
// 常见形态（一个模型 + 一组工具）由构造器组装：
// NewAgent(model run.ModelRef, invoker loop.ModelInvoker, opts ...AgentOption) (Agent, error)
// 选项：WithTool、WithSystemPrompt、WithStreaming、WithPolicy。
// 自定义 catalog 直接实现 Agent 接口。可选接口 PolicyProvider 提供 loop.ExecutionPolicy。

type PublicTool struct {
    Ref run.ToolRef
    Definition run.ToolDefinition
    Policy run.ResponsePolicy
}
type Profile struct {
    SchemaVersion uint16 // 1
    Model run.ModelRef
    Tools []PublicTool   // ToolSpec 与 Request.Tools 都由此派生
    Streaming bool
    SystemPrompt string  // 在 digest 之外
}
```

**REF-BND-1** `Digest = Digest("twilight/ref/profile", canonical(Profile 去除 SystemPrompt))`。digest 只覆盖影响重放正确性的字段（SchemaVersion、Model、Tools、Streaming）；SystemPrompt 是调优文本，修改它不得使可恢复的 Turn 无法 Resolve。

**REF-BND-2** `Agents.Register(id, agent)` 在注册时构建 driver 并返回 `ProfileRef`；`Resolve(ref)` 在 Digest 与注册 agent 的当前 Profile 匹配时返回该注册的 driver，未注册或 digest 不匹配为 `ErrProfileUnavailable`。`RunDriver`、`ProfileRegistry` 与 `ErrAlreadyDriving` 都是宿主层（ref）的合同，turn 协议不感知它们。同一注册的所有 drive 共享一个 Loop 实例，因此同一 Run 的第二个本地驱动者确定地得到 `ErrAlreadyDriving`（REF-DRV-1），而非与首个驱动者并发驱动。同一 Run 内同一 ModelRef 的解析语义保持等价（RUN-LOP-7）。

**REF-BND-3** 参考 Planner 的 `RequestPlan.Model` 等于 `Profile.Model`。

## 2. Planner

参考 Planner 为 context-v1；装配只有这一个 Planner，Profile 不记录 Planner 标识（第二个 Planner 出现时随 Planner 注册表重新引入）。

```go
func Plan(ctx context.Context, hint run.PlanningHint, fold []chatlog.Entry, profile Profile) (loop.RequestPlan, error)
```

**REF-PLN-1** `fold` 为 `ContextFold` 对该 Session chatlog 事件的输出（含已应用的 checkpoint）。Planner 在每次 Plan 时经该 Session Writer 的 `Projections()` 读取 `twilight/chatlog/context` 投影（EXT-PRJ-4）。

**REF-PLN-2** `sdk.Messages` 顺序：

1. `profile.SystemPrompt` 非空时一条 system message；
2. 按 `fold`：`input` → user；`assistant` → assistant（ToolCallPart 的 `ProviderCallID` 写入 `sdk.ToolCallPart.ToolCallID`）；`tool_result` → tool（以同 Turn assistant 中同 CallID 的 `ProviderCallID` 配对）；`summary` → assistant text。

上一步的 assistant 与 tool_result 已随对应 Run 事实同 commit 提交，Planner 消费时的 fold 总是包含它们；`PlanningHint` 不携带模型结果或工具结果。

**REF-PLN-3** `hint.Inputs` 与本 Turn 已 delivered、且属于本次 Prepare 的 Input 按 ID 对齐，包括回合中途经 Deliver 进入的输入。这些 Input 的 `input_delivered` 与 `input_accepted` 同 commit，Plan 时一定已在 fold 中，只使用 fold。

**REF-PLN-4** `RequestPlan.Model = profile.Model`；`Request.Tools` 与 `Tools`（ToolSpec：Ref、DefinitionDigest、Policy）都由 `profile.Tools` 派生，顺序一致；`InputIDs` 为本次消费的 PendingInput IDs。`PlanningToken` 随 fold 的 Entry digest 序列或 Profile Digest 变化。

**REF-PLN-5** 无附件时 TextPart 直接写入 sdk.Message。ReferencePart 经 ContextMaterializer 转换。

**REF-PLN-6** 同一 Turn 有多个 Run attempt 时，参考 Planner 把全部 attempt 的 assistant 与 tool_result 按 commit 顺序纳入请求，包括失败 attempt 的部分输出与 status=`unknown` 的工具结果。这与用户中断后继续的语义一致。Application 可以替换为其他策略（例如排除 `AttemptView.End` 为 failed 的 attempt 的条目），策略只影响请求组装，不影响 stream 与 ContextFold。

## 3. 用户正文

同一份 canonical JSON：

```text
twilight/chatlog/input_submitted.Content
run.AgentInput.Payload
```

**REF-INP-1** v1 形状为 `{"text":"<用户字符串>"}`。

**REF-INP-2** `StartRequest.Inputs[i].ID` 等于已 submitted 的 InputID，`Payload` 等于该 Input 的 Content。`input_delivered` 把 InputID 挂到 TurnID；`twilight/run/input_accepted` 在同一 commit 把同一 payload 交给 Run。`Memory.SubmitText` 以 `NewInputID()`（随机、跨重启无碰撞）提交；需要外部幂等键的调用方使用 `SubmitInput`。

**REF-INP-3** Planner 把 `{"text":...}` 投影为 sdk user text。

## 4. 驱动与 SessionDriver

Coordinator 只做协议提交与状态读取（TRN 3）；**驱动的生命周期整体属于宿主**：何时驱动、驱动 goroutine 的归属、取消与超时、结果组装都是宿主决定。参考组装提供两层：`Memory.Drive`（单次驱动到静止点）与 session 作用域的 `SessionDriver`（把用户输入路由到 Deliver 或 Start、提交后驱动、结算后开启下一个 Turn）。两者都没有自己的持久状态，不进入 turn 或 run 协议。

**REF-DRV-1** `Memory.Drive(ctx, ref)`：读 `twilight/turn/surface`，Turn 为 `active` 时以 `Resolve(view.Profile)` 取 driver（REF-BND-2），调用 `driver.Drive(ctx, {Ref, RunID: ActiveRun})`——driver 内部为 `loop.Run(ctx, runtime, SessionID, RunID, sink)`；随后（或 Turn 非 active 时直接）调用 `Coordinator.Status` 组装响应（TRN-STA-1）。driver 返回 `ErrAlreadyDriving` 时转为成功响应并置 `ResumeAlreadyDriving`：提交的输入由运行中的驱动者继续推进，调用方不经错误通道分辨这一情形。驱动受调用方 ctx 约束：取消是宿主决定，被取消的驱动使 Turn 保持 `active`，下次 Open 后再驱动即恢复。

```go
type SessionDriver struct {
    Coordinator turn.Service
    Writers extension.Writers   // 读投影经 Writer.Projections()
    Profile turn.ProfileRef      // 新 Turn 使用的 Agent Profile
    Companion turn.CompanionVersion
    NewTurnID func() turn.TurnID // nil 时使用随机默认
}
func (d *SessionDriver) Send(ctx, sid session.SessionID, inputs []run.AgentInput) (turn.TurnResponse, error)
func (d *SessionDriver) OnTurnSettled(ctx, sid session.SessionID) (turn.TurnResponse, bool, error)
```

**REF-DRV-2** `Send` 先读 `twilight/turn/surface`：存在 `active` 的 Turn 时调用 `Deliver`，输入进入该 Run 的下一步；否则以 `NewTurnID()`、`Profile`、`Companion` 调用 `Start`。提交成功后进入 `Memory.Drive`（REF-DRV-1）。这对应 inbox 模型中"steer 在运行中注入下一步、在空闲时开启新 turn"的行为。输入在两种情形下都已由 Application 先写入 `input_submitted`。

**REF-DRV-3** `OnTurnSettled` 在 Turn 进入 `completed`、`failed`、`stopped` 或 `superseded` 后调用：读 `twilight/chatlog/surface`，若存在 `submitted` 且未 delivered 的输入，按 `input_submitted` 的 stream 顺序取全部，`Start` 新 Turn 并返回；否则返回 `false`。这对应 inbox 模型的 `next-turn` 列表：已提交而未投递的输入就是该列表，不需要另一份持久结构。

**REF-DRV-4** Turn 为 `attempt_failed` 时 `Send` 返回 conflict，不自动 Retry 或 Settle；这两者是 Application 的决定。`Deliver` 与最后一步结果并发失败（TRN-DLV-3）时，`Send` 得到 `completed`，输入仍为 `submitted`，随后的 `OnTurnSettled` 会把它带入下一个 Turn。

**REF-DRV-5** 崩溃恢复：`SessionDriver` 从两个投影重建。对每个 session，先经 `Writers` 取得 Writer（新 Epoch），调用 `Runtime.RecoverInterrupted` 处置全部 Executing 目标（RUN-CMT-7），再按 TRN-REC-1 对 `active` 的 Turn 调用 `Memory.Drive`、对 `attempt_failed` 的 Turn 由 Application 选择 Retry 或 Settle；没有未结算 Turn 时调用 `OnTurnSettled` 消费积压的输入。

## 5. Session 宿主

宿主面对的单一对象：`ref.Session` 把 EnsureSession、所有权打开、接管处置、输入提交、路由、结算后排空积压与回复读取收拢为一个 API。turn 层只报协议结果（Status/Disposition/Attempt）；回复文本是对话层概念，由宿主从 chatlog 读出。

```go
func (m *Memory) OpenSession(ctx, sid, SessionOptions{Profile, Companion, ResumeActive}) (*Session, error)
type Result struct { TurnID; Status; Disposition; Reply string }
func (s *Session) Send(ctx, text string) ([]Result, error)
func (s *Session) Resume(ctx) ([]Result, bool, error)
func (s *Session) Retry(ctx) ([]Result, bool, error)
func (s *Session) Status(ctx) (SessionStatus, error) // Active 与待 Retry/Settle 的 Turn
func (s *Session) Close(ctx) error                   // 只释放本 Session 的 Writer
```

**REF-SES-1** `OpenSession` 依次：确保 stream 存在（先 `Header` 探测再 `Create`——Create 的幂等要求字段全同，重启后 `CreatedAtUnixMilli` 必然不同）、按装配的 Ownership 打开 Writer、`RecoverInterrupted`；接管处置数暴露为 `Session.Recovered`。`ResumeActive` 为真时同步 Resume 仍在 `active` 的 Turn；交互式宿主保持 false、自行在后台调用 `Resume`。

**REF-SES-2** `Send` 提交文本（`SubmitText`）、路由并驱动（REF-DRV-2）并阻塞到结算：首个 `Result` 是输入落入的 Turn，其后是本次调用在结算后从积压开启并结算的 Turn（REF-DRV-3 的循环，内化在宿主里）。`Disposition` 为 `already_driving` 时该输入由运行中的驱动者推进，本次调用不再排空。`Reply` 为该 Turn 最后一条 assistant 的 TextPart 拼接，仅在 `finished` 时读取。

**REF-SES-3** 并发 `Send` 安全：写入由该 Session 的 Writer 串行化。路由竞态（两个 Send 同时判定 Start，或投递瞬间结算）表现为 `turn.ErrConflict`，宿主重试路由；重试前发现输入已被其他驱动者投递时，返回 `already_driving` 的 `Result`（该 Turn 在取走它的调用里结算与报告）。

**REF-CKP-1**（compaction：机制在 chatlog，策略在宿主）`Memory.Checkpoint(ctx, sid, summaryText, retain)` 在该 Session Writer 的 Commit 临界区内读 `twilight/turn/surface`（存在 active Turn 则拒绝——compaction 是回合之间的操作）与 `twilight/chatlog/context`，以 `View.Head().Next - 1` 为 `CoveredThrough`，把 summary 与 `checkpoint_created` 同组提交（CHT-EVT-3）。`Session.Compact` 用 profile 的模型生成摘要：这是宿主级模型调用，不属于任何 Run，生成中崩溃不写任何事件；随后以 `RetainLast` 的配对封闭后缀提交 checkpoint。自动策略由 `SessionOptions.CompactAfterEntries` 启用：结算且积压排空后、Context 条目数超阈值时触发；失败经 `CompactWarn` 上报，不改变已结算的 `Result`。

**REF-CKP-2**（retained 配对封闭）retained 集必须封闭：保留的 tool_result 连同签发该 call 的 assistant，保留的带 tool_call 的 assistant 连同其在 Context 中的 result——否则压缩后的 Context 组装不出合法的 provider 消息序列。`RetainLast(entries, n)` 返回满足封闭的最短后缀（孤儿 result 向前扩窗到其 assistant）；`Memory.Checkpoint` 校验封闭并拒绝违反者。子集与顺序由 fold 校验（CHT-EVT-3），封闭由宿主校验，两者各管一层。

## 6. Memory 组成

```text
sessionStore = session.NewMemoryStore()                       // Create、Header、Open、Read（SES 第 4 至 6 节）
registry     = extension.BuildRegistry(protocolVersion, chatlog.Module, turn.Module, runmod.Module, opts.Modules...)   // app module 经 Options.Modules 注册（EXT 第 8 节）
bindingStore = artifact.NewMemoryBindingStore()
ledger       = artifact.NewMemoryLedger(bindingStore)          // 自持久化；claim 先于 Append 建立
writers      = extension.NewWriters(sessionStore, registry, ledger, openOptions)   // 每 Session 一个 Writer（EXT-WRT-6）
runtime      = runmod.NewRuntime(writers, runmod.NewMemoryFrozenValues(), turn.CompanionV1(registry))   // ProjectionCache 可选，参考装配不注入
agents       = Agents.Register(id, agent) -> driver = loop.New(agent, agent, contextPlanner, policy, profile.Streaming)   // 每注册一个 Loop
coordinator  = turn.Coordinator{Writers: writers, Runtime: runtime}   // 纯协议：提交 + Status；不驱动
driver       = SessionDriver{Coordinator: coordinator, Writers: writers, Profile: profileRef, Companion: turn.CompanionV1Version}   // 提交后经 Memory.Drive 驱动
host         = Memory.OpenSession(sid, {Profile: profileRef})   // ref.Session

input_submitted
driver.Send                                    // 无 active Turn → coordinator.Start（提交）→ Memory.Drive
  组 1: twilight/turn/started + twilight/chatlog/input_delivered* + twilight/run/created + twilight/run/input_accepted*
  Loop.Run
    组: twilight/run/model_step_prepared            （请求本体 → FrozenValueStore）
    组: twilight/run/model_step_started
    组: twilight/run/model_step_completed + twilight/run/tool_step_opened + twilight/chatlog/assistant
    组: twilight/run/tool_call_started
      input_submitted; driver.Send               // 有 active Turn → coordinator.Deliver
      组: twilight/run/input_accepted + twilight/chatlog/input_delivered
    组: twilight/run/tool_call_completed + twilight/chatlog/tool_result
    组: twilight/run/model_step_prepared            （PlanningHint.Inputs 含中途输入）
    ...
    组: twilight/run/model_step_completed + twilight/run/ended + twilight/chatlog/assistant + twilight/turn/completed
driver.OnTurnSettled                           // 有积压的 submitted 输入 → 开下一个 Turn（Session.Send 内化了这一步）
```

进程重启：writers.Writer(sid) 以新 Epoch 打开 → runtime.RecoverInterrupted(sid) → 对 active 的 Turn 调用 Memory.Drive（REF-DRV-5）

每一行"组"是一次 `Writer.Commit`，落为 stream 中 CommitID 相同、Index 连续的若干行（SES-APP-1）。

参考 agent 的工具 ResponsePolicy 为 `DirectExecution`。ContextFold 在无 checkpoint 时输出全部有效条目。

**REF-MEM-1（app module 开口）** `Options.Modules` 把 application module（EXT 第 8 节）追加进 Registry，须使用自有 Source。app module 的读写走既有入口，装配不另设通道：写事件经 `Memory.Writers` 取该 Session 的 Writer 后 `Commit`（与 `SubmitInput` 同路径）；读自己的投影经 `Memory.Projection(ctx, sid, id, version)`（`ChatlogSurface`/`TurnSurface` 是它对 first-party 投影的封装）。
