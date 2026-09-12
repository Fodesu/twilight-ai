# Twilight Agent 参考组装

状态：设计草案。本文是参考组装的目标设计。与 [Run](agent-run.md)、[Turn](agent-turn.md)、[Chatlog](agent-session-chatlog.md) 冲突时以各正式规范为准。

本文规定 Memory 参考 agent 的组装：Agent 注册单元（Profile 见 Turn，决策组件见 Decision）、用户正文的提交、驱动（Memory.Drive——Coordinator 只做协议提交与状态读取，驱动的生命周期属宿主）、session 作用域的输入路由（SessionDriver）、宿主对象（Session）。

## 1. Agent 与 Profile

Profile 是 Turn 记录的决策身份，定义在 [Turn](agent-turn.md) 第 2 节（TRN-PRF-1/2）：SchemaVersion、Model、Tools、Streaming、PlannerRef、PolicyRef、WorkspaceRef 进摘要，SystemPrompt 不进。Agent 是宿主层把 Profile 与解析它的进程内效果能力放在一起的注册单元：

```go
type Agent interface {
    Profile() turn.Profile
    ResolveModel(run.ModelRef) (loop.ModelInvoker, error)
    ResolveTool(run.ToolRef) (loop.ExecutableTool, error)
}
// 常见形态（一个模型 + 一组工具）由构造器组装：
// NewAgent(model run.ModelRef, invoker loop.ModelInvoker, opts ...AgentOption) (Agent, error)
// 选项：WithTool、WithSystemPrompt、WithStreaming、WithPlanner、WithPolicy、WithWorkspace；
// Planner 与 Policy 默认为 decision.PlannerContextV1 与 decision.PolicyDefaultV1。
```

**REF-BND-1** 宿主不定义 Profile 的摘要边界；它按 TRN-PRF-1 计算并校验（TRN-PRF-2）。

**REF-BND-2** `Agents.Register(id, agent)` 校验 Profile、经 `decision.Catalogs` 解析 PlannerRef 与 PolicyRef（DEC-CAT-2）、构建 driver 并返回 `ProfileRef`；未注册的决策 ref 使注册失败。`Resolve(ref)` 在 Digest 与注册 agent 的当前 Profile 匹配时返回该注册的 driver，未注册或 digest 不匹配为 `ErrProfileUnavailable`。`RunDriver`、`ProfileRegistry` 与 `ErrAlreadyDriving` 都是宿主层的合同，turn 协议不感知它们。同一注册的所有 drive 共享一个 Loop 实例，因此同一 Run 的第二个本地驱动者确定地得到 `ErrAlreadyDriving`（REF-DRV-1）。同一 Run 内同一 ModelRef 的解析语义保持等价（RUN-LOP-7）。

## 2. Planner 与用户正文

Planner、Policy 与用户正文的 v1 形状属于决策层，见 [agent-decision.md](agent-decision.md)（DEC-PLN、DEC-POL、DEC-INP）。宿主只做两件事：`Memory.Options.Decisions` 传入目录（零值取 `decision.DefaultCatalogs()`）；`Memory.SubmitInput`/`SubmitText` 以 `decision.InputContent` 提交用户正文，`SubmitText` 以 `NewInputID()`（随机、跨重启无碰撞）生成 InputID，需要外部幂等键的调用方使用 `SubmitInput`。

**REF-INP-2** `StartRequest.Inputs[i].ID` 等于已 submitted 的 InputID，`Payload` 等于该 Input 的 Content。`input_delivered` 把 InputID 挂到 TurnID；`twilight/run/input_accepted` 在同一 commit 把同一 payload 交给 Run。

## 4. 驱动与 SessionDriver

Coordinator 只做协议提交与状态读取（TRN 3）；**驱动的生命周期整体属于宿主**：何时驱动、驱动 goroutine 的归属、取消与超时、结果组装都是宿主决定。参考组装提供两层：`Memory.Drive`（单次驱动到静止点）与 session 作用域的 `SessionDriver`（把用户输入路由到 Deliver 或 Start、提交后驱动、结算后开启下一个 Turn）。两者都没有自己的持久状态，不进入 turn 或 run 协议。

**REF-DRV-1** `Memory.Drive(ctx, ref)`：读 `twilight/turn/surface`，Turn 为 `active` 时以 `Resolve(view.Profile)` 取 driver（REF-BND-2），调用 `driver.Drive(ctx, {Ref, RunID: ActiveRun})`——driver 内部为 `loop.Run(ctx, runtime, SessionID, RunID, sink)`；随后（或 Turn 非 active 时直接）调用 `Coordinator.Status` 组装响应（TRN-STA-1）。driver 返回 `ErrAlreadyDriving` 时转为成功响应并置 `ResumeAlreadyDriving`：提交的输入由运行中的驱动者继续推进，调用方不经错误通道分辨这一情形。驱动受调用方 ctx 约束：取消是宿主决定，被取消的驱动使 Turn 保持 `active`，下次 Open 后再驱动即恢复。

```go
type SessionDriver struct {
    Coordinator turn.Service
    Writers writer.Writers   // 读投影经 Writer.Projections()
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
cache        = sessionStore 若实现 extension.ProjectionCacheProvider 则取 store.ProjectionCache()，否则 NewMemoryProjectionCache()   // 参考装配用 filestore 时快照落盘
writers      = writer.NewWriters(sessionStore, registry, ledger, openOptions, {Cache: cache, CachePolicy: runmod.WriterCachePolicy()})   // 每 Session 一个 Writer（EXT-WRT-6、EXT-PRJ-6）
runtime      = runmod.NewRuntime(writers, runmod.NewMemoryFrozenValues(), turn.CompanionV1(registry), {Cache: cache})   // machine projection 由 Runtime 自己刷（RUN-CMT-2）
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

**REF-MEM-2（投影缓存归属）** 参考装配解析一次缓存并同时交给两处，各自只写自己有权写的投影（EXT-PRJ-6）：Store 实现 `extension.ProjectionCacheProvider` 时取 `store.ProjectionCache()`，于是 `filestore` 的快照落在 Session 目录内、跨进程存活；否则退回进程内 `NewMemoryProjectionCache()`。`Writers` 用 `runmod.WriterCachePolicy(Options.CacheEvery)`（即 `CacheEvery(every).Exclude(MachineProjectionID)`）刷新全部投影，唯独不碰 machine projection；machine projection 由 `Runtime` 经 `SnapshotPolicy` 在自己的检查点写入（RUN-CMT-2）。`Options.CacheEvery` 是部署可调的区间（0 取 `extension.DefaultCacheEvery`）：调小换更短的重复折叠，调大换更少的写盘。两处的写入互不冲突，读取则不区分作者：重开的 Writer 从找到的任何合法条目续折。
