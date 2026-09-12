# Twilight Agent Decision Layer

状态：设计草案。本文是决策层的目标设计。与 [Turn](agent-turn.md)、[Run](agent-run.md)、[Chatlog](agent-session-chatlog.md) 冲突时以各正式规范为准。

本文定义 `agent/decision`：把已提交状态与 Profile 变成下一个效果请求的组件。agent core 的三层按纯性划分——事实层（Session stream、Writer、投影）、决策层（本文）、效果层（模型调用、工具执行）；决策层是三层里给定输入即确定的一层，它读投影、不做 IO，全部运行在 Session authority 一侧。文中的"必须""不得""应该"是协议约束。

## 1. 范围与身份

```text
Facts → Decision → Assignment → Effect → Outcome → Facts
              ↑
        决策层：Machine.Next（run 模块）、Planner、Policy
```

**DEC-SCP-1** 决策层的每个组件是投影状态与 Profile 的确定性函数，不做外部 IO：同一 Profile、同一投影状态，任何进程得到同一结果。这是接管后新进程续跑同一 Turn 的前提。

**DEC-SCP-2** 决策组件有持久身份：`turn.PlannerRef`、`turn.PolicyRef`。身份进 Profile 摘要（TRN-PRF-1），因此 Turn 记录的 `ProfileRef` 唯一决定了它的决策函数。效果组件（模型、工具）也有身份（`ModelRef`、`ToolRef` 与 definition digest），但它们的实现不在本层，可以位于另一个进程。

**DEC-SCP-3** 本层不 import 宿主装配包。宿主经目录（第 3 节）解析 Profile，把得到的 Planner 与 Policy 交给 Loop。

## 2. Planner

```go
type ProjectionSource interface {
    Load(ctx, sid session.SessionID, id extension.ProjectionID, v extension.ProjectionVersion) (any, session.Head, error)
}
type PlannerFactory func(turn.Profile, ProjectionSource) loop.RequestPlanner
const PlannerContextV1 turn.PlannerRef = "twilight/decision/planner/context-v1"
```

`ContextPlanner` 是 `PlannerContextV1` 的实现：读 `twilight/chatlog/context` 投影，组装 `sdk.Request`。

**DEC-PLN-1** Planner 在每次 Plan 时经 `ProjectionSource` 读取 `twilight/chatlog/context` 投影（含已应用的 checkpoint，CHT-EVT-3）。owner 进程从 Session Writer 读，观察者从 Store 读，两者对同一 head 给出同一状态（EXT-PRJ-4）。

**DEC-PLN-2** `sdk.Messages` 顺序：

1. `Profile.SystemPrompt` 非空时一条 system message；
2. 按 fold 顺序：`input` → user；`assistant` → assistant（ToolCallPart 的 `ProviderCallID` 写入 `sdk.ToolCallPart.ToolCallID`）；`tool_result` → tool（以同 Turn assistant 中同 CallID 的 `ProviderCallID` 配对；`unknown` 状态渲染为标记 error 的说明文本）；`summary` → assistant text。

回合中途投递的输入在其之前尚未结算的工具结果之后排列：这类输入的 `input_delivered` 先于 `tool_result` 进入 stream，而 provider 要求工具结果紧随发出调用的 assistant 消息。fold 顺序不变，只影响请求组装。已停止 attempt 留下的永不结算的调用不阻塞其后输入。

**DEC-PLN-3** `hint.Inputs` 与本 Turn 已 delivered 且属于本次 Prepare 的 Input 按 ID 对齐，包括回合中途经 Deliver 进入的输入。这些 Input 的 `input_delivered` 与 `input_accepted` 同 commit，Plan 时一定已在 fold 中；Planner 只使用 fold。

**DEC-PLN-4** `RequestPlan.Model = Profile.Model`；`Request.Tools` 与 `RequestPlan.Tools`（ToolSpec：Ref、DefinitionDigest、Policy）都由 `Profile.Tools` 派生，顺序一致；`InputIDs` 为本次消费的 PendingInput IDs；`PlanningToken` 为投影 head 的 `Next:Digest`，随 fold 或 Profile 变化。

**DEC-PLN-5** TextPart 直接写入 sdk.Message；ReferencePart 在 context-v1 中以名字呈现，不物化。

**DEC-PLN-6** 同一 Turn 有多个 Run attempt 时，context-v1 把全部 attempt 的 assistant 与 tool_result 按 commit 顺序纳入请求，包括失败 attempt 的部分输出与 status=`unknown` 的工具结果。其他策略以另一个 `PlannerRef` 注册，不修改本实现。

## 3. Policy 与目录

```go
const PolicyDefaultV1 turn.PolicyRef = "twilight/decision/policy/default-v1"
var DefaultPolicy = loop.ExecutionPolicy{}

type PlannerCatalog struct{ /* PlannerRef → PlannerFactory */ }
type PolicyCatalog  struct{ /* PolicyRef → loop.ExecutionPolicy */ }
type Catalogs struct{ Planners *PlannerCatalog; Policies *PolicyCatalog }
func DefaultCatalogs() Catalogs
func (Catalogs) Resolve(turn.Profile, ProjectionSource) (loop.RequestPlanner, loop.ExecutionPolicy, error)
```

**DEC-POL-1** `loop.ExecutionPolicy`（工具执行模式、并行上限、畸形模型结果处置）以 `PolicyRef` 命名。`PolicyDefaultV1` 的值是零值策略：并行执行、不限本地 worker 数、畸形模型结果使 Run 失败。策略值本身不落盘：`ToolStep.Scheduling` 在 `SubmitModelResult` 时快照进事实（RUN-MCH），其余由 ref 在接管进程重建。

**DEC-CAT-1** 目录在装配期构建、运行期只读：空 ref、nil factory、重复 ref 被拒绝。目录是 authority 侧的组件，不需要效果实现即可构建。

**DEC-CAT-2** `Catalogs.Resolve(profile, source)` 按 `profile.Planner` 与 `profile.Policy` 解析；任一未注册返回 `ErrUnknownPlanner`/`ErrUnknownPolicy`，该 Profile 不得被注册或驱动（REF-BND-2 的 `profile_unavailable`）。接管进程以同一 Profile 解析得到同一决策函数。

## 4. 用户正文

同一份 canonical JSON 同时是 `twilight/chatlog/input_submitted.Content` 与 `run.AgentInput.Payload`。

**DEC-INP-1** v1 形状为 `{"text":"<用户字符串>"}`：`InputContent(text)` 构造，`InputText(content)` 还原，Planner 把它投影为 sdk user text。

## 5. conformance

- **DEC-SCP-1、DEC-CAT-2**：两个独立构建的目录对同一 Profile 与同一投影状态解析出的 Planner 给出逐字段相同的 `RequestPlan`；未注册的 PlannerRef/PolicyRef 解析失败。
- **DEC-CAT-1**：空 ref、nil factory、重复注册被拒绝；不完整的 Catalogs 无法解析。
- **DEC-PLN-2**：中途输入排在未结算工具结果之后；`unknown` 工具结果标记 error；多 attempt 的条目全部进入请求（由 turn 与 ref 的集成测试覆盖）。
- **DEC-INP-1**：`InputText(InputContent(s)) == s`。
- **TRN-PRF-1**：Planner、Policy、Workspace 任一变化改变 Profile 摘要，SystemPrompt 不改变（turn 的 golden）。
