# Twilight Agent 宿主层

状态：设计草案。本文是宿主层（`agent/host`）的目标设计。与 [Run](agent-run.md)、[Turn](agent-turn.md)、[Decision](agent-decision.md)、[Chatlog](agent-session-chatlog.md)、[Session](agent-session.md) 冲突时以各正式规范为准。

宿主层把 core 的三层——事实层（Store、Writer、Runtime、Coordinator）、决策层（Profile、Planner 与 Policy 目录）、效果层（Executor 端口）——按角色端口组合成一个 authority 进程，并在其上提供 Session 门面。它是部署中立的：Store 是内存、文件还是数据库，Executor 在本进程执行效果还是转发给远端 worker，驱动阻塞还是异步，都由端口的实现决定，宿主层本身不含任何模型客户端、工具实现或执行环境。

## 1. 定位

**HST-SCP-1** 宿主层拥有的只有组合与门面：把端口接成 Host、按 Profile 组合 Loop、驱动的生命周期（何时驱动、驱动 goroutine 的归属、取消、结果组装）、输入路由、积压排空、compaction 时机、回复读取。协议语义（Turn、Run、事件、投影、恢复）全部在 core，宿主层不新增任何事实类型。

**HST-SCP-2** 下列都是部署选择，宿主层只提供接口位置，不做决定：Store adapter；Executor 是进程内还是远端；驱动阻塞还是异步；何时对一个 Session 声明 `Takeover`；Profile 注册表是进程内还是共享服务；观察者是进程内 sink 还是网络推送。

**HST-SCP-3** 分离成立的判据：authority 进程在没有任何模型客户端与工具实现的情况下能构建 Host、注册 Profile、Start Turn、把 Assignment 交给 Executor 并在 Outcome 到达时提交事实；effect 实现只存在于 Executor 一侧。

**HST-SCP-4** Loop 运行在 authority 里，与 Host 同进程，executor 进程里没有 Loop。Loop 的层次要分开看：它的决策内容——Machine 的 `Next`、Planner、Policy——是纯函数，属于决策层，身份进 Profile 摘要；Loop 本身是把决策翻译成事实层提交与效果层 Assignment 的驱动器，读投影、经 Runtime 写事实、经 Executor 端口派发，因此不纯。这个区分决定了两条独立的替换轴：换 Planner 或 Policy 不动 Loop，换驱动方式（阻塞、后台、事件驱动）不动决策层。

角色对照：

| 角色 | 对应 |
|---|---|
| Session Store | `session.Store` 端口及其 adapter |
| Session Service（authority） | Host：事实层 + 决策层 + Loop 驱动器，加部署提供的薄控制面 |
| Executor | `loop.Executor` 端口；实现是进程内 `LocalExecutor` 或远端客户端，效果实现只在这一侧 |
| Read Models / 观察者 | `extension.NewProjectionReader` 直接挂在 Store 上，不经 Host；`Host.Events` 是 owner 侧的实时流，两者对同一 head 一致（EXT-PRJ-4） |
| Workspace | 只有 Profile 里的 `WorkspaceRef` 插槽；服务在 core 之外 |
| 存活判定 / 何时 Takeover | 不在 core 也不在 Host；由部署（Agent Server 或运维）决定 |
| Agent Server（API、Auth、路由） | core 之外 |

本地部署把 authority 与 executor 折叠进一个进程；云端部署把它们展开。两种部署使用同一个 Host 与同一组端口。

## 2. 端口与 Host

```go
type Artifacts struct { Bindings artifact.BindingStore; Ledger artifact.RetentionLedger }
type Ports struct {
    Store      session.Store              // nil → 内存
    Content    artifact.ContentStore      // nil → 内存；冻结请求本体的 cas 存储（RUN-WIR-4），Runtime 写、Executor 读
    Artifacts  Artifacts                  // 可为零值
    Profiles   ProfileRegistry            // nil → 内存注册表；只存决策身份
    Decisions  decision.Catalogs          // 零值 → decision.DefaultCatalogs()
    Executor   loop.Executor              // 必填：效果层端口（RUN-EXE-3）
    Observers  []writer.CommitObserver   // 提交观察（EXT-WRT-7）；Host 自己的事件流是其一
    Modules    []extension.ModuleDescriptor
    Clock      func() time.Time
    Cache      extension.ProjectionCache  // nil → Store 能力或内存
    CacheEvery session.Seq
    Ownership  session.OpenOptions
    Warn       func(error)                // 宿主在调用之外做的工作失败时
}
type Host struct {
    Store session.Store; Writers writer.Writers; Runtime run.Runtime; Coordinator turn.Service
    Profiles ProfileRegistry; Executor loop.Executor; Decisions decision.Catalogs
}
func New(Ports) (*Host, error)
```

**HST-PRT-1** 端口按角色分组，每个字段是接口或 core 值类型；Host 不知道拿到的是哪个实现，导出的字段也只有接口与 core 类型。缺省实现只在 nil 时选用，且都是进程内的。

**HST-PRT-3** 内容寻址只有一个端口：`Content` 是 artifact `cas` ContentStore，冻结请求本体是它在 `runmod.FrozenAuthority` 下的内容，Host 与 `NewLocalExecutor` 各以 `runmod.FrozenValues` 适配同一个 store，authority 侧写、executor 侧读。

**HST-PRT-2** Executor 是唯一必填端口：没有效果层的 Host 无法完成任何 Turn，而效果层的实现从不属于宿主层。HST-SCP-3 的判据以一个只记录 Assignment 并按脚本回送 Outcome 的 Executor 验收。

## 3. Profile 注册

```go
type ProfileRegistry interface {
    Register(turn.ProfileID, turn.Profile) (turn.ProfileRef, error)
    Resolve(turn.ProfileRef) (turn.Profile, error)
}
func NewProfiles() *Profiles                      // 内存实现
func NewProfile(model run.ModelRef, tools []loop.ExecutableTool, opts ...ProfileOption) (turn.Profile, error)
```

**HST-PRF-1** 注册表只存决策身份。`Register` 按 TRN-PRF-2 校验、按 TRN-PRF-1 计算摘要并返回 `ProfileRef`；`Resolve` 在摘要匹配时返回 Profile。它不持有模型客户端或工具实现：Profile 里的工具只是 `PublicTool{Ref, Definition, Policy}`，实现由 Executor 一侧的目录提供。`NewProfile` 是本地便捷构造：从工具实现取冻结定义与响应策略进 Profile，实现本身不进。

**HST-PRF-2** 未注册的 ID 或摘要不匹配的 ref 解析为 `ErrProfileUnavailable`；宿主对这样的 Turn 不驱动（Turn 状态不变，TRN-REC-2）。同一 ID 重新注册替换 Profile，旧摘要下记录的 Turn 随之不可解析——修改进摘要的字段是有意的决策变更，不得被静默沿用。

## 4. 驱动

**HST-DRV-1** `Host.Drive(ctx, ref)`：读 `twilight/turn/surface`，Turn 为 `active` 时解析其 Profile、取该 Profile 的 Loop、驱动 `ActiveRun` 到下一个静止点（阻塞式 `Loop.Run`，即 Advance/Deliver 之上的封装，RUN-LOP），随后（或 Turn 非 active 时直接）调用 `Coordinator.Status` 组装响应（TRN-STA-1）。Loop 报告同一 Run 已有本地驱动者时，Drive 转为成功响应并置 `ResumeAlreadyDriving`：提交的输入由运行中的驱动者继续推进，调用方不经错误通道分辨这一情形。驱动受调用方 ctx 约束：取消是宿主决定，被取消的驱动使 Turn 保持 `active`，下次 Open 后再驱动即恢复。

**HST-DRV-2** Loop 按 ProfileRef 组合并缓存：`Decisions.Resolve(profile)` 得到 planner 与 policy，与共享的 Executor 一起构成 `loop.New(executor, planner, policy)`。一个 Run 属于一个 Turn、一个 Turn 只有一个 Profile，因此同一 Run 的全部驱动落在同一个 Loop 上，Loop 的 already-driving 守卫成立（RUN-CMT-6）。

**HST-DRV-3** `Session.Route(ctx, inputs)`：先读 turn surface——存在 `active` 的 Turn 时调用 `Deliver`（输入进入该 Run 的下一步）；否则以新 TurnID、Session 的 Profile 与 Companion 调用 `Start`；`attempt_failed` 的 Turn 使 Route 返回 conflict，不自动 Retry 或 Settle，那是宿主的决定。提交成功后进入 `Host.Drive`。输入在两种情形下都已先写入 `input_submitted`。

**HST-DRV-4** `Session.Drain(ctx)`：读 chatlog surface，若存在 `submitted` 且未 delivered 的输入，按 stream 顺序取全部，经 Route 开新 Turn；否则返回 false。已提交而未投递的输入就是 inbox 的 next-turn 列表，不需要另一份持久结构。

**HST-DRV-5** 崩溃恢复：`Host.Open(sid)` 经 `Writers` 取得 Writer（新 Epoch），随后调用 `Runtime.RecoverInterrupted(sid, reattach)`（RUN-CMT-7），其中 `reattach` 是 `loop.Reattach(executor, sid, deliver)`：每个 Executing 目标先被问 Executor 是否仍在执行，能重连的保持 Executing，其 Outcome 稍后经 `deliver` 到达。宿主提供的 `deliver` 按 Outcome 的 RunID 从 turn surface 找到拥有它的 Turn，取该 Profile 的 Loop 调用 `Loop.Deliver` 结算，再驱动该 Run 到静止点；这段工作发生在任何调用之外，失败经 `Ports.Warn` 上报。进程内 Executor 在新进程里对一切 Attach 回答 false，因此本地部署的接管等价于全部处置；远端 Executor 的重连由它的实现决定。

## 5. Session 门面

```go
func (h *Host) OpenSession(ctx, sid, SessionOptions{Profile, Companion, NewTurnID, ResumeActive, Compact*}) (*Session, error)
type Result struct { TurnID; Status; Disposition; Reply string }
func (s *Session) Send(ctx, text string) ([]Result, error)                  // 提交 + 路由 + 同步驱动 + 结算后排空
func (s *Session) Submit(ctx, text string) (turn.TurnRef, error)            // 提交 + 路由，后台驱动，立即返回
func (s *Session) Events(ctx) <-chan Event                                   // 该 Session 的事件流（HST-EVT-1）
func (s *Session) Route(ctx, inputs []run.AgentInput) (turn.TurnResponse, error)
func (s *Session) Drain(ctx) (turn.TurnResponse, bool, error)
func (s *Session) Resume(ctx) ([]Result, bool, error)
func (s *Session) Retry(ctx) ([]Result, bool, error)
func (s *Session) Status(ctx) (SessionStatus, error)
func (s *Session) Compact(ctx) (chatlog.CheckpointID, bool, error)
func (s *Session) Close(ctx) error
```

**HST-SES-1** `OpenSession` 依次：解析 Profile（HST-PRF-2）、确保 stream 存在（先 `Header` 探测再 `Create`——Create 的幂等要求字段全同，重启后 `CreatedAtUnixMilli` 必然不同）、`Host.Open`（打开 Writer 并接管处置，HST-DRV-5）；处置数暴露为 `Session.Recovered`。`ResumeActive` 为真时同步 Resume 仍在 `active` 的 Turn。

**HST-SES-2** `Send` 提交文本（`SubmitText`）、Route 并阻塞到结算：首个 `Result` 是输入落入的 Turn，其后是本次调用在结算后从积压开启并结算的 Turn（Drain 的循环内化在门面里）。`Disposition` 为 `already_driving` 时该输入由运行中的驱动者推进，本次调用不再排空。`Reply` 为该 Turn 最后一条 assistant 的 TextPart 拼接，仅在 `finished` 时读取——回复是对话层概念，turn 层只报协议结果。阻塞式 `Send` 是门面的第一种驱动形态；异步形态加在同一门面上，不另建宿主。

**HST-SES-3** 并发 `Send` 安全：写入由该 Session 的 Writer 串行化。路由竞态（两个 Send 同时判定 Start，或投递瞬间结算）表现为 `turn.ErrConflict`，门面重试路由；重试前发现输入已被其他驱动者投递时，返回 `already_driving` 的 `Result`。

**HST-SES-4** `Submit` 提交文本并提交其路由（Deliver 或 Start，同 HST-DRV-3 的提交半段），返回输入落入的 `TurnRef` 后立即返回；驱动、结算后排空与自动 compaction 在门面拥有的后台 goroutine 里进行，其 ctx 由 `Session` 持有、`Close` 取消并等待。进展与回复经 Events 观察；驱动失败经 `Ports.Warn` 与事件流上的一条 host 级 `Event{Err}` 报告，不进 stream。输入被运行中的驱动者接走（already_driving）时 Submit 直接返回该 Turn，不起驱动。`Send` 与 `Submit` 共用路由与结算逻辑，差别只在驱动是同步还是后台。`Wait` 阻塞到已启动的后台驱动全部结束而不取消它们；后台驱动在结算后会排空积压，因此紧随 Submit 的 Send 若不先 Wait，可能被该排空接走而得到 already_driving（HST-SES-3）。

**HST-EVT-1** `Host.Events(ctx, sid)` 是该 Session 从订阅时刻起的事件流：Host 以 `writer.CommitObserver` 接在自己的 `Writers` 上（EXT-WRT-7），每个已应用组的每一行经 Registry 解码为 `Event{Row, Module, Version, Value, Unknown}`，按提交顺序交付；无 codec 的类型或版本以 `Unknown` 交付原行。订阅者之间互不阻塞，慢读者只延迟自己的交付，从不阻塞 Commit。历史不在此流上：从 Store 或投影读取。UI、SSE 与 CLI 的观察都从这一个源头派生，Loop 的 `EventSink` 只保留给 executor 侧的流式增量。

**HST-INP-1** `SubmitInput(ctx, sid, id, text)` 以 `decision.InputContent` 提交用户正文（DEC-INP-1），`SubmitText` 以随机、跨重启无碰撞的 InputID 提交；需要外部幂等键的调用方使用前者。`StartRequest.Inputs[i].ID` 等于已 submitted 的 InputID，`Payload` 等于其 Content。

## 6. compaction

**HST-CKP-1** 机制在 chatlog（CHT-EVT-3），策略在宿主。`Host.Checkpoint(ctx, sid, summaryText, retain)` 在该 Session Writer 的 Commit 临界区内读 turn surface（存在 active Turn 则拒绝——compaction 是回合之间的操作）与 `twilight/chatlog/context`，以 `View.Head().Next - 1` 为 `CoveredThrough`，把 summary 与 `checkpoint_created` 同组提交。`Session.Compact` 用 Profile 的模型生成摘要，**这次模型调用与其他效果一样经 Executor 端口**：请求经 `FreezeModelRequest` 冻结并 `Put` 进 Frozen，以一个不属于任何 Run 的临时 key 构成模型 Assignment 交给 `Executor.Dispatch`，等待 Outcome；authority 因此不需要模型客户端，远端 Executor 以同一方式服务它。生成中崩溃不写任何事件。自动策略由 `SessionOptions.CompactAfterEntries` 启用：结算且积压排空后、Context 条目数超阈值时触发；失败经 `CompactWarn` 上报，不改变已结算的 `Result`。

**HST-CKP-2** retained 集必须封闭：保留的 tool_result 连同签发该 call 的 assistant，保留的带 tool_call 的 assistant 连同其在 Context 中的 result。`RetainLast(entries, n)` 返回满足封闭的最短后缀；`Host.Checkpoint` 校验封闭并拒绝违反者。子集与顺序由 fold 校验（CHT-EVT-3），封闭由宿主校验。

## 7. 组成

```text
registry    = extension.BuildRegistry(v1, chatlog.Module, runmod.Module, turn.Module, Ports.Modules...)
writers     = writer.NewWriters(Store, registry, Admission{Artifacts}, Ownership, {Cache, CachePolicy: runmod.WriterCachePolicy(CacheEvery)})
frozen      = runmod.FrozenValues(Content)                       // 同一 store 也交给 NewLocalExecutor
writers     = writer.NewWriters(..., {Cache, CachePolicy, Observers: [eventBus, Ports.Observers...]})
runtime     = runmod.NewRuntime{Writers, registry, Store, Frozen: frozen, Companion: turn.CompanionV1, Cache, Clock}
coordinator = turn.Coordinator{Writers, runtime}                 // 纯协议：提交 + Status
loops       = ProfileRef → loop.New(Executor, planner, policy)   // 首次 Drive 时组合
```

**HST-MEM-1（app module 开口）** `Ports.Modules` 把 application module（EXT 第 8 节）追加进 Registry。app module 的读写走既有入口：写事件经 `Host.Writers` 取该 Session 的 Writer 后 `Commit`（与 `SubmitInput` 同路径）；读自己的投影经 `Host.Projection(ctx, sid, id, version)`。

**HST-MEM-2（投影缓存归属）** 缓存解析一次并同时交给两处，各自只写自己有权写的投影（EXT-PRJ-6）：Store 实现 `extension.ProjectionCacheProvider` 时取它，否则进程内缓存。`Writers` 用 `runmod.WriterCachePolicy(CacheEvery)` 刷新全部投影、唯独不碰 machine projection；machine projection 由 Runtime 经 `SnapshotPolicy` 写入（RUN-CMT-2）。`CacheEvery` 是部署可调的区间。

## 8. 部署形态

```text
本地（colocated）          Store: filestore    Executor: NewLocalExecutor(Catalog, Content)  一个进程
云端（Session Service）    Store: 共享/数据库   Executor: 远端 worker 的客户端                 authority 进程无模型客户端、无工具实现
                           Executor 进程：Catalog + Content 只读 + Assignment/Outcome 传输，无 Store
```

两种形态用同一个 `host.New`，差别只在端口实现。core 与宿主层在两者之间没有一行分叉代码。

## 9. conformance

- **HST-SCP-3 / HST-PRT-2**：以只记录 Assignment 的 Executor 构建 Host，注册 Profile、Send 一条输入：模型 Assignment 被 Dispatch 且携带冻结请求的 digest，该 digest 在 Frozen 中可取回，Outcome 回送后 Turn `completed`、`Reply` 等于 Outcome 文本。
- **HST-PRF-1/2**：注册后同 ID 改进摘要字段的字段再注册，旧 ref 解析为 `ErrProfileUnavailable`；未知 ID 同样；未注册的 PlannerRef/PolicyRef 使 Loop 组合失败。
- **HST-DRV-1/2**：同一 Run 的第二个本地驱动者得到 `already_driving` 的成功响应；ctx 取消后 Turn 保持 active、重开后驱动完成。
- **HST-DRV-3/4**：active Turn 时 Route 走 Deliver，输入在下一次模型请求里紧随工具结果之后；无 active Turn 时 Route 开新 Turn；Drain 取全部积压开一个 Turn；`attempt_failed` 时 Route 为 conflict。
- **HST-DRV-5**：接管后 Executing 工具记 Unknown 且同一 RunID 继续；不可重连的 Executing 模型步被撤回，Resume 时按恢复时刻的状态重新规划并只调用模型一次（`ModelSteps` 只计重规划的那一步）；可重连的模型 attempt 不被处置，其 Outcome 经宿主的 deliver 完成同一步；旧进程的迟到结算被围栏。
- **HST-SES-1/2/3**：OpenSession 顺序；Send 的首个 Result 与排空 Result；并发 Send 的 `already_driving` 收敛。
- **HST-SES-4、HST-EVT-1**：Submit 在模型仍阻塞时已返回且 Turn 为 active；事件流按 Seq 顺序交付该 Turn 的 `started` 与 `completed`；后台驱动失败以 `Event{Err}` 与 `Ports.Warn` 报告；同一 Session 上 Send 仍阻塞到 Result。
- **HST-CKP-1/2**：Compact 的模型请求经 Executor 到达模型；压缩后下一请求以 summary 开头且只含 retained 后缀；重启进程组装同一上下文；active Turn 时 Compact 为 conflict；封闭校验的四类边界。
- **HST-MEM-2**：`CacheEvery` 到达 Writer；machine projection 从不被 Writer 写入。
