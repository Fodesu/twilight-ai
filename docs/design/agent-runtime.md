# Twilight Agent Runtime

状态：v1 设计规范。本文定义 core 之上的运行时分层：`agent/authority`（组装与 Session 所有权）、`agent/driver`（Turn 驱动与恢复）、`agent/spawn`（子代理效果）、`agent/preset`、`agent/observe` 与 `agent/app`（产品策略）。协议细节分别见 [Run](agent-run.md)、[Turn](agent-turn.md)、[Decision](agent-decision.md)、[Chatlog](agent-session-chatlog.md) 与 [Session](agent-session.md)。

运行时把 core 的三层——事实层（Store、Writer、Runtime、Coordinator）、决策层（AgentPreset 与 PromptBuilder 目录）、效果层（Executor 端口）——按角色端口组合成一个 authority 进程；应用层在其上实现对话策略。两者都是部署中立的：Store 是内存、文件还是数据库，Executor 在本进程执行效果还是转发给远端 worker，驱动阻塞还是异步，都由端口的实现决定，运行时本身不含任何模型客户端、工具实现或执行环境。

## 1. 定位

**AUTH-SCP-1** 运行时拥有的只有组合、所有权与策略：authority 把端口接成 core 服务并发放 Session 所有权；driver 按 AgentPreset 组合 Loop 并驱动；app 拥有驱动的生命周期（何时驱动、驱动 goroutine 的归属、取消、结果组装）、输入路由、积压排空、compaction 时机、回复读取。协议语义（Turn、Run、事件、投影、恢复）全部在 core，运行时不新增任何事实类型。

**AUTH-SCP-2** 下列都是部署选择，运行时只提供接口位置，不做决定：Store adapter；Executor 是进程内还是远端；驱动阻塞还是异步；何时对一个 Session 声明 `Takeover`；AgentPreset 注册表是进程内还是共享服务；观察者是进程内 sink 还是网络推送。

**AUTH-SCP-3** 分离成立的判据：authority 进程在没有任何模型客户端与工具实现的情况下能组装、注册 AgentPreset、Start Turn、把 Assignment 交给 Executor 并在 Outcome 到达时提交事实；effect 实现只存在于 Executor 一侧。

**AUTH-SCP-4** Loop 运行在 authority 进程里，executor 进程里没有 Loop。Loop 的决策内容——Machine 的 `Next`、PromptBuilder——是纯函数，属于决策层，身份进 AgentPreset 摘要；Loop 本身是把决策翻译成事实层提交与效果层 Assignment 的驱动器，读投影、经 Runtime 写事实、经 Executor 端口派发，因此不纯。换 PromptBuilder 不动 Loop，换驱动方式不动决策层。

角色对照：

| 角色 | 对应 |
|---|---|
| Session Store | `session.Store` 端口及其 adapter |
| Authority | `authority.Authority`：事实层 + 决策层 + driver，发放 `Handle` |
| Executor | `effect.Port`；实现是进程内 `LocalExecutor` 或远端客户端，效果实现只在这一侧 |
| Read Models / 观察者 | `extension.ProjectionReader` 按 SessionID 读取；`observe.Bus` 是 owner 侧的实时流，两者对同一 head 一致（EXT-PRJ-4） |
| Workspace | 运行时只接收 opaque `TargetRef`/resolver；Workspace 与 Runtime 服务在 core 之外 |
| 存活判定 / 何时 Takeover | 不在 core 也不在运行时；由部署决定 |
| Agent Server（API、Auth、路由） | core 之外 |

## 2. Authority 与端口

```go
type Ports struct {
    Store          session.Store               // nil → 内存
    Content        artifact.ContentStore       // nil → 内存；冻结正文的 cas 存储（RUN-WIR-4）
    Artifacts      Artifacts                   // 可为零值
    Presets        preset.Registry             // nil → 内存注册表；只存决策身份
    Decisions      *decision.PromptBuilders    // nil → decision.DefaultPromptBuilders()
    Executor       effect.Port                 // 必填：效果层端口（RUN-EXE-3）
    TargetResolver loop.TargetResolver
    Observers      []writer.CommitObserver     // 提交观察（EXT-WRT-7）
    Modules        []extension.ModuleDescriptor
    Clock          func() time.Time
    Cache          extension.ProjectionCache   // nil → Store 能力或内存
    CacheEvery     session.CommitSeq
    Ownership      session.OpenOptions
    Fail           func(session.SessionID, error) // 调用之外的工作失败时
}
type Authority struct {
    Store; Writers; Registry; Admission; Runtime
    Turns   *turn.Coordinator     // turn.Commands + turn.Reader
    Driver  *driver.Driver
    Presets preset.Registry; Executor effect.Port; Frozen run.FrozenValueStore
    Projections extension.ProjectionReader // 经 Writer 读投影
    Content     chatlog.ContentResolver    // materializer（CHT-MAT-1）
    Chatlog     *chatlog.Commands
    History     turn.History
}
func New(Ports) (*Authority, error)
func (a *Authority) Open(ctx, sid) (*Handle, error)
```

**AUTH-PRT-1** 端口按角色分组，每个字段是接口或 core 值类型；Authority 不知道拿到的是哪个实现，导出的字段是 core 服务与端口。缺省实现只在 nil 时选用，且都是进程内的。

**AUTH-PRT-2** Executor 是唯一必填端口：没有效果层的 authority 无法完成任何 Turn，而效果层的实现从不属于运行时。AUTH-SCP-3 的判据以一个只记录 Assignment 并按脚本回送 Outcome 的 Executor 验收。

**AUTH-PRT-3** 内容寻址只有一个端口：`Content` 是 artifact `cas` ContentStore，冻结正文是它在 `runmod.FrozenAuthority` 下的内容。Runtime 写入；authority 以 `runmod.NewContent` 建立 materializer（`Authority.Content`），供 prompt 构造、回复与 compaction transcript 读取（CHT-MAT-1）。Executor 不读它：Dispatch 携带内联请求（RUN-EXE-7）。重启或接管的进程必须拿到同一个 store，ledger 只有 digest。

## 3. 所有权与命令边界

```go
type Handle struct{ Recovered int }
func (h *Handle) ID() session.SessionID
func (h *Handle) Writer() writer.Writer
func (h *Handle) Close(ctx) error
```

**AUTH-OWN-1** `Authority.Open(sid)` 发放对一个 Session 的执行能力，不是读取能力。Open 取得该 Session 的 Writer（本进程 epoch 下）、运行接管处置（DRV-3）并安装恢复监听；返回的 `Handle` 只承载这份能力与其生命周期：`ID`、`Writer`、`Close`。SessionID 是持久身份；Handle 表示"本进程当前拥有它"。Handle 不带任何业务操作：Send、排空、fork、compaction、spawn 分别属于 app、domain 命令或效果层。

**AUTH-OWN-2** 写侧要求 Handle，读侧只要 SessionID。core 的每个命令——`turn.Commands`（Start/Deliver/Retry/Stop/Settle）、`chatlog.Commands`（Submit/Withdraw/Checkpoint）、`driver.Drive`——以 `Handle.Writer()` 为参数，不再按 SessionID 重新取 Writer；命令校验请求所指 Session 与 Writer 的 Session 一致。投影读取——`turn.ReadSurface`、`chatlog.ReadSurface`、`chatlog.ReadContext`、`Coordinator.Status`、`Authority.Reply`、`Authority.Projection`——按 SessionID 经 `ProjectionReader` 进行，不要求所有权。

**AUTH-OWN-3** Writer 是同步点。跨域不变量由同一个 Session Writer 的原子 Commit 保证，不由 service 之间协调：`Start` 在一个 `SemanticGroup` 里同时提交 `turn/started`、`turn/attempt_started`、`chatlog/input_delivered` 与 Run 创建事实；`Deliver` 以 Runtime command 加 attached module events 一次提交 `AcceptInput` 与 `input_delivered`（TRN-DLV-2）。不存在"chatlog 成功、turn 失败、run 未创建"的中间状态。所有命令经同一 Writer 落盘，因此共享同一 epoch 与同一投影视图，过期 owner 由 Writer 围栏（SES-OWN）。

**AUTH-OWN-4** driver 不拥有事实。它读已提交状态、决定执行、经 Loop/Runtime 提交、再读已提交状态；"提交持久状态"与"继续执行"之间不构成事务，崩溃边界允许落在两者之间：Start 已提交而未驱动时，持久事实已是 `TurnActive + ActiveRun`，新 owner Open 后 Drive 即继续同一 Run。需要原子的是 chatlog/turn/run 之间的事实转换；不需要原子的是提交之后的外部效果与驱动推进。

## 4. Session 生命周期

**AUTH-FRK-1** `Fork` 以 `writer.Fork` 建立子 Session（SES-FRK-1、EXT-WRT-8），不打开它；调用方随后以 `OpenSession` 打开，其接管处置对前缀遗留的 Executing 目标得到 `missing` 并按 RUN-CMT-7 处置（SES-FRK-4）。父不受影响，可以继续被驱动。

**AUTH-FRK-2** `ForkBeforeTurn(parent, turnID, child)` 以 `turn.History.StartCommit` 找到携带该 Turn `twilight/turn/started` 的 Commit `k`，在 `k-1` 处 fork：子的对话止于该 Turn 的输入仍为 `submitted` 的状态。`Drain` 或 `Route` 把这些输入投递给新 Turn 即重新生成；`chatlog.Commands.Withdraw`（经子的 Handle）写 `input_withdrawn`（CHT-EVT-2，要求输入为 `submitted`）后再 `Send` 即编辑。`k = 0` 时没有可 fork 的前缀，返回 `ErrInvalid`；未知 Turn 返回 conflict。edit / retry / regenerate 三种动作因此都归到同一个 fork 原语加输入投递上（TRN 第 1 节）。

**AUTH-FRK-3** `DeleteSession` 先停止该 Session 的恢复监听并关闭其 Writer，再以 `writer.Delete` 撤根并释放 claim（EXT-WRT-9）；被另一进程持有的 Session 为 `ErrOwned`。以它为前缀的 fork 不受影响。`Collect` 调 `writer.Collect` 回收无根可达的段（SES-GC-2）。

## 5. AgentPreset 注册（preset）

```go
// agent/preset
type Registry interface {
    Register(turn.PresetID, turn.AgentPreset) (turn.PresetRef, error)
    Resolve(turn.PresetRef) (turn.AgentPreset, error)
}
func NewMemory() *Memory
// agent/app
func NewPreset(model run.ModelRef, tools []loop.ExecutableTool, opts ...PresetOption) (turn.AgentPreset, error)
```

**PST-1** 注册表只存决策身份。`Register` 按 TRN-PST-2 校验、按 TRN-PST-1 计算摘要并返回 `PresetRef`；`Resolve` 在摘要匹配时返回 AgentPreset。它不持有模型客户端或工具实现：AgentPreset 里的工具只是 `PublicTool{Ref, Definition, Policy}`，实现由 Executor 一侧的目录提供。`app.NewPreset` 是本地便捷构造：从工具实现取冻结定义与响应策略进 AgentPreset，实现本身不进。

**PST-2** 注册表按完整 `PresetRef{ID, Digest}` 保存 immutable 版本。同一 ID 注册新摘要时保留旧版本，已有 Turn 继续解析其记录的版本。注册与 Resolve 均隔离可变字段；SystemPrompt 参与摘要。缺少指定版本时返回 `preset.ErrUnavailable`，Turn 保持原状态，等待宿主提供该版本（TRN-REC-2）。

## 6. 驱动（driver）

**DRV-1** `driver.Drive(ctx, w, turnID)`：读 `twilight/turn/surface`，Turn 为 `active` 时解析其 AgentPreset、取该 AgentPreset 的 Loop、驱动 `ActiveRun` 到下一个静止点（阻塞式 `Loop.Run`，即 Advance/Deliver 之上的封装，RUN-LOP），随后（或 Turn 非 active 时直接）调用 `Coordinator.Status` 组装响应（TRN-STA-1）。Loop 报告同一 Run 已有本地驱动者时，Drive 转为成功响应并置 `ResumeAlreadyDriving`：提交的输入由运行中的驱动者继续推进，调用方不经错误通道分辨这一情形。驱动受调用方 ctx 约束：取消是调用方的决定，被取消的驱动使 Turn 保持 `active`，下次 Open 后再驱动即恢复。

**DRV-2** Loop 按 PresetRef 组合并缓存在 Driver 内：`Decisions.Resolve(preset)` 得到 prompt builder，与 preset 上的 Scheduling、MalformedRetries 及共享的 Executor 一起构成 `loop.New(executor, builder, loop.Settings{Scheduling, MalformedRetries})`。一个 Run 属于一个 Turn、一个 Turn 只有一个 AgentPreset，因此同一 Run 的全部驱动落在同一个 Loop 上，Loop 的 already-driving 守卫成立（RUN-CMT-6）。

**DRV-3** `Authority.Open(sid)` 经 `Writers` 取得 Writer，随后 `driver.Open` 调用 `Runtime.RecoverInterrupted(sid, reattach)`（RUN-CMT-7），其中 `reattach = loop.Reattach(lifetime, executor, sid, deliver)`。Attach 握手受 Open 请求的 context 约束；后台 Outcome 读取与交付使用该 Session 的 recovery lifetime。Open 返回后请求取消仍允许恢复继续；再次 Open 会替换旧监听，`Handle.Close` 与 `Authority.Close` 取消各自拥有的监听。

Attach 的 `active` / `terminal` 保留 Executing 并等待实际 Outcome；`orphaned` 表示 Executor 找到 durable record 但无法关联 live backend，映射为 recovery 层的 `deferred`，必须保留 Executing 供 control plane reconcile/takeover；`missing` 才进入接管处置。进程内 Executor 重启后旧记录为 `missing`，持久 Executor 按其 Execution Store 返回状态。`deliver` 按 Outcome 的 RunID 查找 Turn，使用其 preset 的 Loop 结算并继续驱动；后台失败经 `Ports.Fail` 上报。

| Executor observation (`AttachmentState`) | Recovery disposition | authority 行为 | API 观察 |
|---|---|---|---|
| `missing` | `missing` | 允许协议自动处置 | `recovery_required`，直到处置完成 |
| `active` | `active` | 保留 Executing，等待 Outcome | `observed` |
| `terminal` | `terminal` | 读取并结算 Outcome | `observed` |
| `orphaned` | `deferred` | 保留 Executing，等待显式 reconcile/takeover | `recovery_required` |

`RunStatus=active`、`TurnStatus=active` 与 `AttachmentState=active` 分属三个状态域；API 不直接暴露它们的内部枚举，而通过明确的 view 映射输出。

## 7. 应用层（app）

```go
// agent/app
func Build(Config) (*Application, error)
func (app *Application) OpenSession(ctx, sid, SessionOptions{Preset, NewTurnID, ResumeActive, Compact*}) (*Session, error)
func (app *Application) Fork(ctx, ForkRequest{Parent, At, Child}) (session.SegmentHeader, error)      // AUTH-FRK-1
func (app *Application) ForkBeforeTurn(ctx, parent, turnID, child) (session.SegmentHeader, error)      // AUTH-FRK-2
func (app *Application) DeleteSession(ctx, sid) error                                                 // AUTH-FRK-3
func (app *Application) Collect(ctx) (session.CollectReport, error)
func (app *Application) Events(ctx, sid) <-chan Event                                                 // OBS-1
type Result struct { TurnID; Status; Disposition; Reply string }
func (s *Session) Send(ctx, text string) ([]Result, error)                  // 提交 + 路由 + 同步驱动 + 结算后排空
func (s *Session) Submit(ctx, text string) (turn.TurnRef, error)            // 提交 + 路由，后台驱动，立即返回
func (s *Session) Route(ctx, inputs []run.AgentInput) (turn.TurnResponse, error)
func (s *Session) Drain(ctx) (turn.TurnResponse, bool, error)
func (s *Session) Resume(ctx) ([]Result, bool, error)
func (s *Session) Retry(ctx) ([]Result, bool, error)
func (s *Session) Status(ctx) (SessionStatus, error)
func (s *Session) Compact(ctx) (chatlog.CheckpointID, bool, error)
func (s *Session) Handle() *authority.Handle
func (s *Session) Close(ctx) error
```

**APP-SES-1** `OpenSession` 依次：解析 Preset（PST-2）、确保 stream 存在（先 `Header` 探测再 `Create`——Create 的幂等要求字段全同，重启后 `CreatedAtUnixMilli` 必然不同）、`Authority.Open`（取得 Handle：Writer 与接管处置，AUTH-OWN-1、DRV-3）；处置数暴露为 `Session.Recovered`。`ResumeActive` 为真时同步 Resume 仍在 `active` 的 Turn。此后 Session 的每个命令都经 `Handle.Writer()` 提交。

**APP-SES-2** `Send` 提交文本（`chatlog.Commands.Submit`）、Route 并阻塞到结算：首个 `Result` 是输入落入的 Turn，其后是本次调用在结算后从积压开启并结算的 Turn（Drain 的循环内化在 Session 里）。`Disposition` 为 `already_driving` 时该输入由运行中的驱动者推进，本次调用不再排空。`Reply` 为该 Turn 最后一条 assistant 的文本（`chatlog.LastAssistantText`），仅在 `finished` 时读取——回复是对话层概念，turn 层只报协议结果。

**APP-SES-3** 并发 `Send` 安全：写入由该 Session 的 Writer 串行化。路由竞态（两个 Send 同时判定 Start，或投递瞬间结算）表现为 `turn.ErrConflict`，Session 重试路由；重试前发现输入已被其他驱动者投递时，返回 `already_driving` 的 `Result`。

**APP-SES-4** `Submit` 提交文本并提交其路由（Deliver 或 Start，同 APP-RTE-1 的提交半段），返回输入落入的 `TurnRef` 后立即返回；驱动、结算后排空与自动 compaction 在 Session 拥有的后台 goroutine 里进行，其 ctx 由 `Session` 持有、`Close` 取消并等待。进展与回复经 Events 观察；驱动失败经 `Config.Warn` 与事件流上的一条 `Event{Err}` 报告，不进 stream。输入被运行中的驱动者接走（already_driving）时 Submit 直接返回该 Turn，不起驱动。`Send` 与 `Submit` 共用路由与结算逻辑，差别只在驱动是同步还是后台。`Wait` 阻塞到已启动的后台驱动全部结束而不取消它们。

**APP-RTE-1** 路由是 app 的策略：`app.Session.Route(ctx, inputs)` 先读 turn surface——存在 `active` 的 Turn 时 `Deliver`（输入进入该 Run 的下一步）；否则以新 TurnID 与 Session 的 AgentPreset `Start`；`attempt_failed` 的 Turn 使 Route 返回 conflict，不自动 Retry 或 Settle，那是调用方的决定。提交成功后进入 `driver.Drive`。输入在两种情形下都已先写入 `input_submitted`。

**APP-RTE-2** 排空是 app 的策略：`app.Session.Drain(ctx)` 读 chatlog surface，若存在 `submitted` 且未 delivered 的输入，按 stream 顺序取全部，经 Route 开新 Turn；否则返回 false。已提交而未投递的输入就是 inbox 的 next-turn 列表，不需要另一份持久结构。

**APP-INP-1** `chatlog.Commands.Submit(ctx, w, id, text)` 提交用户正文（构造器为 `chatlog.TextContent`，DEC-INP-1）；`app.Session` 以 `chatlog.NewInputID` 铸造随机、跨重启无碰撞的 InputID，需要外部幂等键的调用方自带 ID。`StartRequest.Inputs[i].ID` 等于已 submitted 的 InputID，`Payload` 等于其 Content。

### 7.1 compaction

**APP-CKP-1** 机制与命令在 chatlog，策略在 `agent/context/compaction`。`chatlog.Commands.Checkpoint(ctx, w, summaryText, retain, guard)` 在该 Session Writer 的 Commit 临界区内读 `twilight/chatlog/context`，以 `View.Head().Next - 1` 为 `CoveredThrough`，把 summary 与 `checkpoint_created` 同组提交；active Turn 的拒绝规则以 `guard` 注入同一临界区，turn 域提供 `turn.RequireNoActiveTurn`（chatlog 不能反向依赖 turn）。CheckpointID 与 SummaryID 由 SessionID、base context digest 与摘要文本派生（同一后缀，前缀分别为 `ckpt-` 与 `sum-`），CommitID 为 `checkpoint/<CheckpointID>`：对同一 base 重试的 Checkpoint 重放同一 CommitID，Writer 按 EXT-WRT-2 回答 `AlreadyApplied`，不会写入第二个 checkpoint。`app.Session.Compact` 经 `compaction.Summarizer` 用 AgentPreset 的模型生成摘要，**这次模型调用与其他效果一样经 Executor 端口**：请求经 `FreezeModelRequest` 冻结并 `Put` 进 Frozen，以一个不属于任何 Run 的临时 key 构成模型 Assignment 交给 `Executor.Dispatch`，等待 Outcome；authority 因此不需要模型客户端，远端 Executor 以同一方式服务它。生成中崩溃不写任何事件。自动策略由 `SessionOptions.CompactAfterEntries` 启用：结算且积压排空后、Context 条目数超阈值时触发；失败经 `CompactWarn` 上报，不改变已结算的 `Result`。

**APP-CKP-2** retained 集必须封闭：保留的 tool_result 连同签发该 call 的 assistant，保留的带 tool_call 的 assistant 连同其在 Context 中的 result。`compaction.RetainLast(entries, n)` 返回满足封闭的最短后缀；`chatlog.CheckRetainClosure` 在命令内校验封闭并拒绝违反者。子集与顺序由 fold 校验（CHT-EVT-3）。

### 7.2 模块与缓存

**APP-MEM-1（app module 开口）** `Ports.Modules` 把 application module（EXT 第 8 节）追加进 Registry。app module 的读写走既有入口：写事件经该 Session Handle 的 `Writer().Commit`（与 `chatlog.Commands.Submit` 同一 Writer）；读自己的投影经 `Authority.Projection(ctx, sid, id, version)`。

**APP-MEM-2（投影缓存归属）** 缓存解析一次并同时交给两处，各自只写自己有权写的投影（EXT-PRJ-6）：Store 实现 `extension.ProjectionCacheProvider` 时取它，否则进程内缓存。`Writers` 用 `runmod.WriterCachePolicy(CacheEvery)` 刷新全部投影、唯独不碰 machine projection；machine projection 由 Runtime 经 `SnapshotPolicy` 写入（RUN-CMT-2）。`CacheEvery` 是部署可调的区间。

## 8. 子代理（spawn）

**SPN-1** 子代理是一个由 ToolCall 启动的普通 Session。模型调用 spawn 工具（默认 `agent_spawn`，经 `Config.Spawn` 配置 Tool、命名 Preset 解析与最大深度）；`spawn.Intercept` 在部署 Executor 之前拦截该工具的 Assignment 交给 `spawn.Executor`。工具定义、参数与结果形状、派生身份、定义摘要核对、深度与重放冲突判定在 `agent/spawn` 协议部分；`spawn.Executor` 只做子 Session 的创建与结算状态机：`Authority.Open(child)` 取得子的 Handle，经 `turn.Commands.Start`、`driver.Drive` 与 `chatlog.Commands.Submit` 推进，不经 app 门面。Run 事实本体不新增子代理生命周期：父只看到一个以子代理回复完成的工具调用。

**SPN-2** 调用到子 Session 的绑定是派生的，不单独存储：`ChildSessionID = spawn.ChildID(parent, runID, callID)`（preimage `twilight/spawn/child`）；子段创建元数据在 `twilight/spawn` 键下记录完整 provenance（父 Session、父 Run、CallID、深度、全量参数），接管方据此在崩溃后重建同一调用。同一 CallID 以不同参数重放在 Validate 与 drive 两侧都被拒绝（RUN-EXE-3）。经持久 Worker 部署时 `PrepareBinding` 返回 `ExecutionBinding{Provider: twilight/session, ExecutionRef: child}`，由 execution store 随调用记录持久化。

**SPN-3** 嵌套深度从 provenance 链得出：未由 spawn 创建的 Session 深度为 0，子的深度为父深度加一。深度达到 `Options.MaxDepth`（默认 3）的 Session 发起 spawn 调用在开始前被拒（FailureExecution），不创建子 Session。

**SPN-4** 崩溃接管沿既有 RUN-CMT-7 Attach 路径：新进程对 Executing 的 spawn 调用执行 Attach 时，本地无记录则以派生 ChildID 查 `SessionStore.Record`——子存在即收养（参数取子 provenance）继续同一调用，不存在交回内层 Executor。`Application.Close` 取消本进程的全部 drive，子的 Turn 保持 active 等待收养。

**SPN-5** 模式 `spawn`（默认）从空 Session 起；`fork` 以 `turn.History.PrefixCommit` 为根，即父在调用 Turn 及其输入之前的全部历史，子拿到的是当前 Turn 开始之前的对话。结算依子的持久状态推进：有 active Turn 则驱动至结算；有 submitted 输入则以其开新 Turn 并驱动；否则比较最新输入与 task——相同且已有 Turn 则读取已结算结果，不同则提交 task 开新 Turn（fork 子的前缀只含已交付对话的情形）。一个子每个 task 只运行一个 Turn，不排空积压。子 Turn 非 `completed` 时调用失败。

## 9. 事件流（observe）

**OBS-1** `Application.Events(ctx, sid)` 是该 Session 从订阅时刻起的事件流：`observe.Bus` 以 `writer.CommitObserver` 接在 Writers 上（EXT-WRT-7），每个已应用组的每一行经 Registry 解码为 `Event{Row, Module, Version, Value, Unknown}`，按提交顺序交付；无 codec 的类型或版本以 `Unknown` 交付原行。订阅者之间互不阻塞，慢读者只延迟自己的交付，从不阻塞 Commit。历史不在此流上：从 Store 或投影读取。UI、SSE 与 CLI 的观察都从这一个源头派生，Loop 的 `EventSink` 只保留给 executor 侧的流式增量。

## 10. 组成

```text
// authority.New
registry    = extension.BuildRegistry(v1, chatlog.Module, runmod.Module, turn.Module, Ports.Modules...)
frozen      = runmod.FrozenValues(Content)
content     = runmod.NewContent(frozen)                          // materializer：prompt、Reply、transcript
writers     = writer.NewWriters(Store, registry, Admission{Artifacts}, Ownership, {Cache, CachePolicy: runmod.WriterCachePolicy(CacheEvery), Observers})
runtime     = runmod.NewRuntime{Writers, registry, Store, Frozen: frozen, Bindings: Artifacts.Bindings, Cache, Clock}
projections = writer.Projections(writers)
turns       = turn.Coordinator{Writers, runtime}                 // 纯协议：命令经传入的 Writer 提交 + Status 读取
chatlog     = chatlog.Commands{Clock}
driver      = driver.New{runtime, turns, Executor, Presets, Decisions, Sources{projections, content}, Targets, projections, Fail}
                                                                  // Loop 按 PresetRef 在 driver 内组合并缓存
// app.Build
executor    = Config.Spawn != nil → spawn.Intercept(spawn.Executor, port) else port
authority   = authority.New(Ports{..., Executor: executor, Observers: [observe.Bus, Config.Observers...], Fail: warn+bus.Failed})
spawn.Bind(authority)                                            // 子经 Authority.Open + turn/chatlog/driver 命令驱动
```

## 11. 部署形态

```text
本地（colocated）          Store: filestore    Content: filestore    Executor: NewLocalExecutor(Catalog)  一个进程
云端（Session Service）    Store: 共享/数据库   Content: 共享 cas     Executor: 远端 worker 的客户端        authority 进程无模型客户端、无工具实现
                           Executor 进程：Catalog + Assignment/Outcome 传输，无 Store、无 Content
```

两种形态用同一个 `authority.New` 与 `app.Build`，差别只在端口实现。core 与运行时在两者之间没有一行分叉代码。

## 12. 未决

- **effect.Port 的可组合性**：`spawn.Intercept` 需要为 Port 与 BindingPort 的每个生命周期方法各转发一次，按 Assignment 内容或 key 归属判定路由。路由应只在选择执行后端时发生一次，之后的生命周期操作沿同一后端进行；这需要从 execution lifecycle 本身重新建模 effect.Port，本文不定义，见 RUN-EXE 的后续修订。
- **Runtime 命令路径的 Writer 传递**：`Coordinator.Deliver`/`Stop` 经 `Runtime.Commit(ctx, sid, ...)` 提交，Loop 经 Runtime 提交，Runtime 仍按 SessionID 经 `Writers` 取 Writer。`Writers` 按 Session 缓存同一个 Writer，因此与 Handle 的 Writer 为同一对象，但该同一性尚未由 API 表达；把 Writer 沿 Runtime 命令路径传递是 AUTH-OWN-2 的后续。

## 13. conformance

- **AUTH-SCP-3 / AUTH-PRT-2**：以只记录 Assignment 的 Executor 组装 authority，注册 AgentPreset、Send 一条输入：模型 Assignment 被 Dispatch 且携带冻结请求的 digest，该 digest 在 Frozen 中可取回，Outcome 回送后 Turn `completed`、`Reply` 等于 Outcome 文本。
- **PST-1/2**：同 ID 注册不同 SystemPrompt 得到不同摘要，两版均可解析；修改注册入参或 Resolve 返回值中的嵌套字段保持注册版本不变；未知 ID 或摘要返回 `ErrUnavailable`；未注册的 PromptBuilderRef 使 Loop 组合失败。
- **AUTH-OWN-2/3**：命令以另一 Session 的 Writer 调用返回 conflict；接管后被替代进程的 Writer 上的 Deliver 得到 ownership lost 且不改变输入状态（turntest recovery）；app module 经同一 Writer 提交的事件与 chatlog 输入出现在同一 ledger。
- **DRV-1/2**：同一 Run 的第二个本地驱动者得到 `already_driving` 的成功响应；ctx 取消后 Turn 保持 active、重开后驱动完成。
- **APP-RTE-1/2**：active Turn 时 Route 走 Deliver，输入在下一次模型请求里紧随工具结果之后；无 active Turn 时 Route 开新 Turn；Drain 取全部积压开一个 Turn；`attempt_failed` 时 Route 为 conflict。
- **DRV-3**：`missing` execution record 的工具记 Unknown 且同一 RunID 继续；缺失记录的模型步被撤回，Resume 时重新规划（`ModelSteps` 只计重规划的那一步）；`active`/`terminal` attempt 以实际 Outcome 完成原步骤，`orphaned` 映射为 `deferred` 并保持 Executing；Open 请求取消后恢复监听继续，Session/Authority 关闭后监听退出；旧进程的迟到结算被围栏。
- **APP-SES-1/2/3**：OpenSession 顺序；Send 的首个 Result 与排空 Result；并发 Send 的 `already_driving` 收敛。
- **APP-SES-4、OBS-1**：Submit 在模型仍阻塞时已返回且 Turn 为 active；事件流按 Seq 顺序交付该 Turn 的 `started`、`attempt_started` 与其 Run 的 `run_ended`；后台驱动失败以 `Event{Err}` 与 `Config.Warn` 报告；同一 Session 上 Send 仍阻塞到 Result。
- **APP-CKP-1/2**：Compact 的模型请求经 Executor 到达模型；压缩后下一请求以 summary 开头且只含 retained 后缀；重启进程组装同一上下文；active Turn 时 Compact 为 conflict；封闭校验的四类边界。
- **SPN-1..5**：spawn 调用以派生身份建子 Session 并以子回复完成父的工具调用；fork 模式拿到当前 Turn 之前的对话且收到 task；参数错误、未知命名 Preset 与深度超限在开始前被拒且不建子；所有者进程在子模型调用中途退出后，新进程收养同一调用并完成父 Turn；收养后子的 Turn 数与输入数不变。
- **APP-MEM-2**：`CacheEvery` 到达 Writer；machine projection 从不被 Writer 写入。
- **AUTH-FRK-1/2**：在某 Turn 之前 fork 得到的子 Session 只含该 Turn 之前的回答且其输入仍待投递；`Drain` 以同一输入重新生成，`Withdraw` 后 `Send` 以新输入替代；两个子都读到共享前缀的冻结正文；父的 head 不变；每个子的首个自身 Commit 从 anchor 续链；未知 Turn 与自身为父被拒。
