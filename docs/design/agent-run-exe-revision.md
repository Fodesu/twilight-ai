# RUN-EXE 修订稿：execution identity 与 Backend 契约

状态：修订稿（未实现）。本文给出 [agent-run.md](agent-run.md) §6 RUN-EXE-1..8 的替换文本与新增条款，覆盖 `agent/run/effect`、`agent/executor`、`agent/executor/store`、`agent/executor/http`、`agent/executor/local`、`agent/spawn` 与 `agent/app.Build` 的执行路径。实现完成后本文并入 agent-run.md §6 并删除。条款 ID 沿用 RUN-EXE-N，改动条款标注"修订"，新增条款从 RUN-EXE-9 起。

## 1. 问题

当前实现里 backend 的选择发生在每个生命周期方法上：

- `spawn.Intercept` 为 `effect.Port` 的 6 个方法与 `effect.BindingPort` 的 6 个方法各做一次"是否归我"判定（按 Assignment 内容或按 key 查子 Session 是否存在），其余转发给内层 Port；
- `executor.Worker` 对同一个 backend 维护两条路径：无 binding 走 `Port`，有 binding 走 `BindingPort`，`checkBinding` 在 Dispatch、Attach、Status、Outcome、Cancel、Takeover、Dispose、recover 八处重复；
- colocated 部署的 `LocalExecutor` 直接充当 `effect.Port`，没有 execution record，其进程内表与 Worker 的 record store 是两套生命周期实现；
- `ExecutionBinding` 既是 Worker 的持久字段，又经 `BindingPort` 出现在 backend 接口签名上，Agent Core 一侧的 `effect` 包因此携带了 executor 内部概念。

根因：backend 路由被建模为每次调用的判定，而不是 execution 创建时的一次决定。

## 2. 身份模型

| 身份 | 拥有者 | 含义 | 出现位置 |
|---|---|---|---|
| `AssignmentKey` | Agent Core | 一次 attempt 的语义身份：Session、Run、Step、Call、Claim；结算 CommandID 的 preimage | Session 事实、`effect.Port` 全部方法、Execution Record 的主键、wire |
| `ExecutionRef{Provider, Ref}` | Executor | 该 attempt 绑定到的物理执行：哪个 backend（Provider）、backend 内的不透明句柄（Ref） | 仅 Execution Record 与 Backend 契约；不进 Session 事实，不进 `effect.Port`，不进 Agent Core 任何 API |

`AssignmentKey → ExecutionRef` 的映射只由 Execution Record 持有。Agent Core 始终以 AssignmentKey 说话；Executor 在 record 内把它翻译成 backend 的物理引用。`effect.ExecutionBinding` 更名为 `executor.ExecutionRef`，从 `effect` 包移除。

## 3. 分层

```text
Agent Core（run/loop、driver）
   effect.Port：Validate / Dispatch / Attach / GetStatus / GetOutcome / Cancel，全部按 AssignmentKey
        │
        ▼
Worker（agent/executor）
   Dispatch：选择 backend 一次 → Prepare 得到 Ref → 持久化 record{key, assignmentDigest, executionRef, state} → Start
   其余方法：record → executionRef.Provider → backend，按 Ref 操作
   Takeover / Reconcile / Dispose：控制面，作用于 record
        │
        ▼
Backend（executor.Backend 契约，每个 provider 一个实现）
   local（进程内 goroutine）、spawn（子 Session）、future：k8s job、远端 provider request
```

`effect.Port` 对 Agent Core 的形状不变。变化全部在 Worker 之下：一次选择、一个 record、一条生命周期。colocated 与远端部署使用同一个 Worker，差别只在 record store（内存 / 文件 / 共享）与 backend 表。

## 4. Backend 契约

```go
// agent/executor
type ExecutionRef struct {
    Provider string `json:"provider"`
    Ref      string `json:"ref"`
}

// Backend 执行一个 provider 的效果。它只认 Ref：Worker 在 Prepare 之后持久化 Ref，
// 此后每个生命周期操作都以 Ref 寻址。实现不持有跨调用的 AssignmentKey 表。
type Backend interface {
    // Validate 与 RUN-EXE-5 相同：不产生外部效果。
    Validate(ctx context.Context, a effect.Assignment) (*run.ToolFailure, error)
    // Prepare 为 Assignment 分配或派生 Ref，不启动效果。按 AssignmentKey 幂等：
    // 同一 key 重复 Prepare 返回同一 Ref（派生式 backend 直接计算；分配式 backend
    // 以 key 为幂等键记录分配结果）。Worker 在 Start 之前持久化 Ref，进程在两者之间
    // 死亡时重放 Prepare 得到同一 Ref。
    Prepare(ctx context.Context, a effect.Assignment) (ref string, err error)
    // Start 启动 Ref 对应的执行。确定的拒绝返回普通 error；请求发出后的超时或
    // 响应丢失返回 effect.ErrDispatchUnknown。对已启动的 Ref 重放 Start 为无操作。
    Start(ctx context.Context, ref string, a effect.Assignment) error
    // Attach 报告 Ref 的观察状态：missing / active / orphaned / terminal。
    Attach(ctx context.Context, ref string) (effect.Attachment, error)
    Status(ctx context.Context, ref string) (effect.ExecutionStatus, error)
    // Outcome 阻塞到 Ref 有终态 Outcome 或 ctx 结束；返回的 error 只描述读取失败。
    Outcome(ctx context.Context, ref string) (effect.Outcome, error)
    Cancel(ctx context.Context, ref string) error
}

// Route 把 Assignment 分给一个 provider；Worker 只在 Dispatch 时评估一次。
type Route struct {
    Provider string
    Match    func(effect.Assignment) bool // nil 匹配全部：默认 provider
    Backend  Backend
}
```

契约要点：

- Backend 方法签名里没有 AssignmentKey，除 Validate/Prepare/Start 需要 Assignment 内容之外；Attach/Status/Outcome/Cancel 只有 Ref。因此一个 backend 不需要、也不能按 key 维护自己的记录表。
- Prepare 与 Start 分开，保留现在 `PrepareBinding` 的崩溃语义：Ref 先于效果被持久化，Start 与持久化之间崩溃不会丢失物理引用。
- 派生式 Ref（local：`AssignmentKey` 摘要；spawn：`ChildID(parent, run, call)`）与分配式 Ref（远端 provider 返回的 job id）统一为 `string`，Worker 不解释。

## 5. Worker 生命周期（新模型）

| 操作 | 步骤 |
|---|---|
| Dispatch(a) | 校验内联 payload（RUN-EXE-7）→ 查 record：存在且 digest 相同 → 确认 acceptance 返回；存在且 digest 不同 → `ErrAssignmentConflict`；不存在 → 选择 Route（第一个 Match 为真者，否则默认 provider）→ `backend.Prepare` → 创建 record{Accepted, executionRef} → 取租约 → `Dispatching` → `backend.Start(ref)` → `Running`（或 ErrDispatchUnknown 保持 Dispatching） |
| Attach / GetStatus / GetOutcome / Cancel(key) | 查 record → 缺失 → `missing` / `ErrExecutionNotFound`；存在 → 以 `executionRef` 找 backend → 以 Ref 调用。GetOutcome 优先返回 record 内已持久化的 Outcome |
| Takeover(key) | 查 record → 非终态 → 取租约（新 fencing epoch）→ `backend.Attach(ref)`：active/terminal → 继续观察原执行；orphaned → 保持 Executing，等待控制面；missing → 模型 Assignment 重放 `Prepare`+`Start`（同一冻结请求；Prepare 幂等则 Ref 不变，分配式 backend 可得新 Ref，record 更新为新 Ref），工具 Assignment 以 Unknown 结算（TRN-DUR-4），工具定义声明可重放者除外 |
| Reconcile / Dispose | 与 RUN-EXE-6 相同，操作对象为 record；Dispose 的 best-effort cancel 经 `executionRef` 找 backend |

record 是 execution identity 的唯一来源：**没有 record 就没有 execution**。record 缺失时 Attach 返回 `missing`，authority 按 RUN-CMT-7 处置；Worker 不向任何 backend 询问"你是否认识这个 key"。这条规则取代当前 `spawn.Intercept` 在 Attach 时按派生 ChildID 查 Session store 的收养路径（见 §7 对 SPN-4 的影响）。

## 6. 条款修订

**RUN-EXE-1（Assignment，修订）** 段末追加：AssignmentKey 是 Agent Core 的 attempt 身份；它到物理执行的映射（`ExecutionRef{Provider, Ref}`）只由 Executor 的 Execution Record 持有，不进入 Session 事实，也不出现在 `effect.Port` 或 Agent Core 的任何接口上。

**RUN-EXE-2（Outcome）** 不变。

**RUN-EXE-3（Dispatch 与 Attach，修订）** 替换为：`Dispatch` 接受 Assignment 后立即返回。确定的 acceptance 失败返回普通 error；请求发出后的超时、取消或响应丢失返回 `ErrDispatchUnknown`，Authority 保留 Executing。接受时 Worker 先选择 backend（RUN-EXE-10）并经 `Backend.Prepare` 取得 Ref，把完整 Assignment payload 与 `ExecutionRef` 一起持久化为 record，再进入 `Dispatching`，然后 `Backend.Start(ref)`；`Accepted` 表示确定尚未开始，`Dispatching` 表示可能已经开始，`Running` 表示 backend 已接受。backend 返回 `ErrDispatchUnknown` 时 Worker 保持 Dispatching、续租并读取最终 Outcome。相同 Assignment 的 Dispatch 重放确认已有 acceptance，并保留该 record 的 owner、epoch、状态与 `ExecutionRef`；已有 record 的恢复通过显式 `Takeover` 触发。`Attach` 按 AssignmentKey 查 record：record 缺失即 `missing`；record 存在则经 `ExecutionRef` 向所属 backend 查询，返回 `active`、`orphaned`、`terminal` 或 `missing`；只有 `missing` 允许 Authority 自动处置，`orphaned` 必须由控制面 reconcile、takeover 或明确处置。`GetStatus` 与 `GetOutcome` 不读取 Session。接管 `Running`/`Dispatching` 的 record 时先 `Backend.Attach(ref)`：可 attach 则继续观察原执行；backend 报 `missing` 时模型 Assignment 重放 Prepare 与 Start（同一冻结请求），工具 Assignment 以 Unknown 结算（TRN-DUR-4），工具定义声明可重放之前不允许重派。`Cancel` 针对一个 Assignment；Run 级批量取消由上层枚举 targets。终态 record 的保留由 record store 决定：内存 store 按上限保留（默认 1024 条，执行中的记录不受上限影响），被淘汰的 record 对 `Attach` 返回 `missing`、对 `GetStatus`/`GetOutcome` 返回 `ErrExecutionNotFound`，同一 key 的 Dispatch 重放视为新执行；durable store 不淘汰。HTTP Server 与 Worker 内嵌 reconcile 的规定不变。

删除的内容：`BindingPort`、`ErrBindingUnsupported`、"已有 ExecutionBinding 的记录使用 BindingPort 完成全部 backend 操作"、"未绑定 ExecutionBinding 的工具执行……"——在新模型下每个 record 在 Start 之前都有 `ExecutionRef`，不存在"未绑定"的执行。

**RUN-EXE-4、RUN-EXE-5** 不变（RUN-EXE-5 的 `Executor.Validate` 由 Worker 按 Route 转给所选 backend 的 Validate；选择在 Validate 时评估一次但不持久化，Dispatch 时重新评估——两次评估针对同一 Assignment 内容，结果相同）。

**RUN-EXE-6（控制面，修订）** "Dispose……best-effort cancel backend"改为"经 record 的 `ExecutionRef` 找到 backend 后 best-effort `Cancel(ref)`"；其余不变。

**RUN-EXE-7（Assignment payload）** 不变。

**RUN-EXE-8（部署说明，修订）** 追加：colocated 部署同样经 Worker 与 record store 运行效果，`app.Build` 的本地模式组装 `executor.NewWorker(ctx, records, routes...)`，默认 record store 为内存实现，`Config.Executions` 可换为文件或共享实现；`LocalExecutor` 是 local provider 的 Backend 实现，不再单独实现 `effect.Port`。本地与远端部署之间没有分叉的生命周期代码，差别只在 record store 与 backend 表。

**RUN-EXE-9（ExecutionRef，新增）** `ExecutionRef{Provider, Ref}` 是 Executor 对一个 attempt 的物理绑定。Provider 命名 backend（`local`、`twilight/session`，部署自定的 provider 名），Ref 是该 backend 内的不透明句柄。它在 `Backend.Prepare` 时确定，在 Start 之前随 record 持久化，之后不变；模型 Assignment 在接管发现 backend `missing` 时重放 Prepare，分配式 backend 可返回新 Ref，record 更新为新 Ref，旧 Ref 被确认不存在。`ExecutionRef` 不进入 Session 事实、`effect.Port`、Loop 或 Driver：Agent Core 只认 AssignmentKey。经 HTTP 传输时它是 Worker 侧 record 的字段，不出现在 Assignment 与 Outcome 的 wire 形状上。

**RUN-EXE-10（Backend 选择与 record 的权威性，新增）** backend 选择是 execution 创建的一部分：Worker 在 Dispatch 时按 `Route` 表评估一次（第一个 `Match` 为真的 provider，否则默认 provider），结果作为 `ExecutionRef.Provider` 持久化；此后 Attach、GetStatus、GetOutcome、Cancel、Takeover、Dispose 只查 record 并按 Provider 找 backend，不再评估 Assignment 内容，也不询问任何 backend 是否认识某个 key。record 是 execution identity 的唯一来源：record 缺失即 execution 不存在（`missing`）。record 的持久性由部署决定——内存 store 随进程消失，此时崩溃后的接管处置按 `missing` 进行（RUN-CMT-7）；需要跨进程收养执行的部署使用文件或共享 record store。一个 backend 的 Prepare 按 AssignmentKey 幂等，因此 record 丢失后同一 Assignment 的重新 Dispatch 落到同一物理执行（派生式 Ref）或由 backend 的幂等分配处理。

## 7. 对现有包的影响

| 包 | 变化 |
|---|---|
| `run/effect` | 删除 `BindingPort`、`ExecutionBinding`、`ErrBindingUnsupported`；`Port` 不变 |
| `executor` | 新增 `ExecutionRef`、`Backend`、`Route`；`Worker` 持 routes 而非单个 Port；`checkBinding`、`*Backend` 双路径删除，改为 record → provider → backend 的单路径；`NewWorker(ctx, records, routes ...Route)` |
| `executor/store` | `Record.ExecutionBinding` → `Record.ExecutionRef executor.ExecutionRef`（非指针；Start 之前必有）；删除 legacy JSON 别名（PR 未发布） |
| `executor/local` | `LocalExecutor` 改为实现 `executor.Backend`：Ref 为 AssignmentKey 摘要，进程内表按 Ref 键控，保留 `SetRetainedOutcomes`；不再实现 `effect.Port`。当前代码位于 `run/loop/executor.go`，随本次迁到 `executor/local`；`loop` 内部测试改经 `effect.Port` 的测试替身或内存 Worker |
| `executor/http` | `Client` 仍是 `effect.Port`；`Server` 包装 Worker；wire 不变（`ExecutionRef` 不上线） |
| `spawn` | `Executor` 改为实现 `executor.Backend`（Provider `twilight/session`，Prepare 派生 ChildID，Start 创建/打开子 Session 并驱动，Attach/Status/Outcome/Cancel 按 ChildID 查进程内 drive 表与 Session store）；删除 `Intercept` 与全部转发方法；`Bind(authority)` 保留 |
| `app` | `Build` 本地模式组装 `executor.NewWorker(ctx, Config.Executions or executionstore.NewMemoryStore(), executor.Route{Provider: "local", Backend: local}, spawn route...)`；远端模式不变；`Config.Executions executionstore.Store` 新增 |
| `driver`、`loop`、`turn`、`authority` | 不变：它们只见 `effect.Port` 与 AssignmentKey |
| `context/compaction` | 不变：Summarizer 经 `effect.Port` 派发模型 Assignment，落到 local provider |

**对 SPN-4（收养）的影响**：当前接管靠 `spawn.Intercept` 在 Attach 时按派生 ChildID 查 Session store 收养子 Session，绕过了 record。新模型下收养要求 record 存在：colocated 部署若要在进程崩溃后收养子代理，需要配置持久 record store（`Config.Executions` 为文件 store，与 filestore Session store 同一根目录）；内存 record store 下崩溃后的 spawn 调用按 `missing` 处置，子 Session 保留在 Session store 中，不再被自动继续。`TestSpawnSurvivesOwnerRestart` 改为在两次 `open()` 中使用同一根目录的文件 record store。SPN-4 文本相应改为"接管沿 RUN-CMT-7 Attach 路径查 record；record 存在即经 `twilight/session` backend 以 ChildID 继续同一子 Session"。

## 8. 迁移顺序

每步编译、`go test ./...` 全绿后提交：

1. `executor`：引入 `ExecutionRef`、`Backend`、`Route`；Worker 改为 routes + 单路径；store 字段更名；现有 executor 测试改写（binding 测试 → ref 测试）。此时 `LocalExecutor` 经一个适配器临时实现 Backend。
2. `executor/local`：`LocalExecutor` 迁出 `loop`，原生实现 Backend；`loop` 测试改用替身。
3. `spawn`：改为 Backend，删 Intercept；app 组装 routes；SPN-4 测试改用文件 record store。
4. `effect`：删除 `BindingPort`/`ExecutionBinding`；`app.Build` 本地模式经 Worker；`Config.Executions`。
5. 文档：本文并入 agent-run.md §6，agent-runtime.md SPN-1/4 与 §12 未决项更新，test-cloud-agent.md 的 binding 描述更新，删除本文。

## 9. conformance 变更

- **RUN-EXE-3/9/10**：同一 Assignment 两次 Dispatch 得到同一 record 与同一 `ExecutionRef`；Prepare 之后、Start 之前进程重启，重放 Dispatch 得到同一 Ref 且不重复启动；接管时 backend `missing` 的模型 Assignment 重放 Prepare+Start，工具 Assignment 记 Unknown；record 缺失的 key 对 Attach 为 `missing`，任何 backend 不被询问。
- **Route**：匹配 spawn 工具的 Assignment 落到 `twilight/session` provider，其余落到默认 provider；record 的 Provider 字段决定后续每个方法到达的 backend（以计数 backend 验证只到达一个）。
- **SPN-4**：文件 record store 下所有者进程在子模型调用中途退出后，新进程收养同一调用并完成父 Turn；内存 record store 下同一场景按 `missing` 处置，父的工具结果为 Unknown，子 Session 仍存在。
- **colocated = Worker**：`app.Build` 本地模式产生的 Port 是 `*executor.Worker`；`LocalExecutor` 不再满足 `effect.Port`（编译期断言删除）。

## 10. 未决

- **分配式 backend 的 Prepare 幂等**：远端 provider（如创建 job 才得到 id）需要以 AssignmentKey 为幂等键记录分配结果，这份记录属于 backend 自己的持久状态；本文只要求幂等，不规定其存储。
- **模型接管重放的 Ref 更新**：record 从旧 Ref 换到新 Ref 时旧 Ref 已被确认 `missing`；若 backend 的 `missing` 判断有误（网络分区），可能出现两个物理执行。这是 RUN-EXE-3 既有的重派风险，与本次修订无关，记录于此。
- **Route 的表达力**：v1 只有按 Assignment 内容的 `Match`；按 TargetRef 或 preset 路由属于 application 放置策略（RUN-EXE-8），不进协议。
