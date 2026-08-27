# Twilight AI Agent Runtime 重构设计规范

状态：重构方案

本文定义 Twilight AI 的 `sdk/`、`agent/` 和 Memoh native runtime 边界，并规定从当前 SDK 多步 loop 迁移到唯一 `agent.Loop` 的方式。

本文只定义 Agent Machine 的语义、Loop 的执行协议、Runtime 的 authority/commit 边界，以及已提交事实的 canonical event 语义。Memoh 的 queue、session、R0/R1、owner、fencing、lease 和数据库投影仍由 Memoh 负责；本文规定它们如何接入，而不把它们提升为 agent core 概念。

正文中的 Go 片段用于说明协议；附录 A 是 public API 草案。正文各节为规范文本，附录是汇总视图；两者不一致时以正文为准，并修订附录。

## 1. 目标

### 1.1 核心目标

Twilight AI 同时支持两种运行形态：

```text
Memory agent
  MachineState 在当前进程内，适合本地会话和测试

Durable agent
  MachineState 在数据库中，进程可以退出，新的进程可以继续
```

两种形态使用同一套 Machine 规则和同一个 Loop。差别只在 Runtime 如何保存权威状态、控制并发和提交状态变化。

本版本固定五个层级：

```text
1. Agent Machine
   定义 Run、Step、ToolCall 的语义状态和合法状态变化

2. Agent Loop
   解释 Machine 产生的 effect，执行模型/工具，再提交结果事件

3. Runtime
   加载权威 MachineState，并原子提交 AgentCommand，产生 AgentEvent

4. Model / Tool
   执行一次模型请求或一次工具调用

5. Request Planner
   把 application context 投影为下一次模型所需的 sdk.Request
```

这里的 “Agent Machine” 是语义层名称，不引入名为 `Agent` 的核心对象；公共执行入口仍是 `Loop`。

整体关系：

```text
                  Agent Machine (pure rules)
              MachineState + Next/Decide/Evolve
                       ^              |
                       | state        | Effect
                       |              v
Request Planner --> sdk.Request --> Agent Loop ------> Model / Tool
 application       (frozen)          |
 context                              | AgentCommand
                                      v
                                   Runtime
                             Load + atomic Commit
                            /                  \
                   MemoryRuntime          MemohRuntime
                    memory + mutex    PostgreSQL + private lease/fence
                            \                  /
                             \                /
                         MachineState + AgentEvents
                              (same commit boundary)
                                      |
                                      v
                    EventSink / replay / projection / OTel
```

Loop 与 Runtime 的职责相互独立：Loop 决定如何执行，Runtime 决定权威状态在哪里以及如何安全提交。Machine 决定什么状态变化合法；Planner 决定模型看到的 application context；Model/Tool 只执行一次外部 effect。

目标目录边界：

```text
twilight-ai/
  sdk/       一次 LLM request/response
  agent/     Machine、Loop、Step、工具 contract、Runtime contract

Memoh native runtime
  Request Planner、产品 context、durable history、queue、session 和数据库事务
```

依赖方向固定为：

```text
agent  ---> sdk
Memoh  ---> agent + sdk
sdk    不依赖 agent 或 Memoh
```

### 1.2 核心执行模型

```text
Run
 └── Step                  durable resume boundary
      ├── ModelStep        一次冻结的模型请求
      └── ToolStep         一组 ToolCall 及其可恢复进度
           ├── Call A Completed
           ├── Call B Waiting
           └── Call C Pending

MachineState + AgentCommand
             |
        Machine.Decide -> 事实序列（一次 transition 的全部决策）
             |
        Machine.Evolve 逐个折叠（机械，无决策）
             |
             v
        new MachineState + AgentEvents（同一 Revision）
             |
        Machine.Next -> Effect -> Loop
```

Step 不是最小的外部操作。ToolStep 保存每个 ToolCall 中会影响恢复决策的状态。durable 实现可以为当前执行建立内部 Attempt；Attempt 可能因进程退出、超时或 lease 失效而消失，但新的 Loop 仍恢复同一个 Step。

最重要的不变量是：

> 一个 Step 可以有多个执行尝试，但只有被 Runtime 接受的一次状态变化能够推进权威 Run；外部工具 effect 仍可能是 at-least-once。

### 1.3 术语

| 名称 | 含义 |
| --- | --- |
| Run | 一个有身份、有权威状态和最终结果的业务执行。 |
| Loop | 当前进程执行 Run 的算法；它不保存权威状态。 |
| MachineState | Machine 的完整语义状态。 |
| Step | Run 中的 durable resume boundary。 |
| ModelStep | 一次冻结的 `sdk.Request`，直到接受模型结果。 |
| ToolStep | 一个模型结果产生的一组 ToolCall 及其 progress。 |
| ToolCall | ToolStep 内的一个结构化工具调用。 |
| AgentCommand | Loop 或外部入口希望 Machine 接受的意图；接受后构成一次 transition。 |
| AgentEvent | Runtime 已接受并持久化的事实；一次 transition 产出一个或多个，带 (Revision, Index) 身份。 |
| Revision | 每 Run 单调递增的 transition 计数；第 N 次接受的 transition 产出 Revision=N 的事件组和 Revision=N 的状态。 |
| Effect | Machine 根据当前状态返回的至多一个待执行动作；不表示一定有外部副作用。 |
| Attempt | Runtime 为一次进程执行建立的内部执行租约。 |
| Runtime | MachineState 的 execution authority、AgentCommand 的原子提交和 AgentEvent 的产生者。 |
| Request Planner | application context 到 `sdk.Request` 的投影器。 |

### 1.4 成功标准

| 场景 | 期望结果 |
| --- | --- |
| 单次模型调用 | 直接使用 `sdk`，不自动执行工具。 |
| 本地会话 | `agent.Loop` 配合 `MemoryRuntime`。 |
| Memoh durable 会话 | 同一个 `agent.Loop` 配合 `MemohRuntime`。 |
| 模型返回 tools | ModelStep 完成后创建一个 ToolStep；ToolStep 完成前不创建下一 ModelStep。 |
| 多个 approval/用户响应 | 每个等待请求有稳定身份；一个响应只推进对应 Call。 |
| ToolStep 中途崩溃 | Completed Call 不再执行；Pending Call 可恢复；结果未知的 Executing Call 终止 Run。 |
| steer/follow-up | 只在 Memoh 的 queue-safe boundary 仲裁。 |
| commit 响应丢失 | 用同一 command identity 和 digest 重放，不重复业务副作用。 |
| worker 取消 | 只结束当前 Loop attempt；不等同于业务取消 Run。 |

## 2. Package 与职责

### 2.1 `sdk/`

sdk 只负责 LLM API：

| 能力 | 内容 |
| --- | --- |
| provider client | 认证、provider dispatch、transport。 |
| 一次调用 | `Generate` 或 `Stream`，每次对应一个 provider request。 |
| 协议类型 | message、provider-neutral tool definition、tool call、finish reason、usage 和 metadata。 |
| stream 归一化 | 将 parts 组装成一次完整 `ModelResult`。 |
| provider 错误 | transport、rate limit、malformed stream 和 provider response error。 |
| request snapshot | provider-neutral、可冻结的请求表示。 |

`sdk.Request` 是一次模型调用的完整输入，不是 session、history 或 queue。`sdk.ModelResult` 是一次完整模型响应，保留旧 `GenerateResult` 的单次调用字段（文本、reasoning parts、tool calls、finish reason、usage、sources/files 和 provider metadata），但不包含自动 tool loop、approval 或多次调用累加。多步执行的 steps 和 messages 由 agent/application 另行保存。

`sdk.Request` 的冻结形态遵守以下规则；`DigestRequest`、StepID 派生和提交幂等都建立在这套规则上：

1. Request 是纯数据。它不包含 provider client、接口值、回调或 `Execute` 句柄；模型以 provider 作用域内的字符串 ID 表示，provider 绑定发生在 `ModelCatalog`/`ModelInvoker` 解析时。
2. 工具以 provider-neutral 的 `ToolDefinition{Name, Description, Parameters}` 表示；`Parameters` 是解析完成的 JSON Schema 文档。由 Go struct 推导 schema 的工作在冻结前完成，冻结后的 Request 不依赖推导或反射。
3. `ToolChoice` 是封闭类型 `{Mode: auto|none|required|tool, Tool string}`，不使用 `any`。
4. 消息中的二进制内容有两种形式：inline bytes（canonical 编码为 base64），或稳定的内容寻址引用 `BlobRef{Digest, MediaType, ByteSize}`。`BlobRef` 的字节解析由组装 `ModelInvoker` 的一方负责；带时效的 URL 等不稳定引用不能进入冻结请求。两种形式产生不同的 digest，Planner 对同一内容必须确定性地选择一种形式。
5. provider metadata 等扩展字段的值必须是 JSON 值。canonical 编码采用 RFC 8785（JCS）：对象键按 UTF-16 码元排序，数字使用最短表示，`json.RawMessage` 先按 JCS 重新序列化。
6. `DigestRequest` 覆盖 Request 的全部字段，不设排除项。cache 配置等只影响成本的字段同样参与摘要；排除任何字段都会把不同请求判成同一事件，产生错误的 `CommitAlreadyApplied`。

```go
package sdk

type Request struct {
    Model            string // provider 作用域内的模型 ID
    System           string
    Messages         []Message // parts 为纯数据；二进制为 inline bytes 或 BlobRef
    Tools            []ToolDefinition
    ToolChoice       ToolChoice
    ResponseFormat   *ResponseFormat
    Temperature      *float64
    TopP             *float64
    MaxTokens        *int
    StopSequences    []string
    FrequencyPenalty *float64
    PresencePenalty  *float64
    Seed             *int
    ReasoningEffort  *string
    ReasoningSummary *string
    PromptCacheKey   *string
    ProviderOptions  map[string]json.RawMessage // 按 provider namespace 存放
}

type ToolDefinition struct {
    Name         string
    Description  string
    Parameters   json.RawMessage // 解析完成的 JSON Schema
    CacheControl *CacheControl
}

type ToolChoiceMode string // "auto" | "none" | "required" | "tool"

type ToolChoice struct {
    Mode ToolChoiceMode
    Tool string // Mode == "tool" 时的目标工具名
}

type BlobRef struct {
    Digest    string // sha256:<hex>，内容寻址
    MediaType string
    ByteSize  int64
}
```

当前 `GenerateParams` 的 `Model *Model`、`Tool.Execute`、`Tool.Parameters any` 和 `ToolChoice any` 都不满足这些规则；阶段 A 实现上述新类型，legacy wrapper 在边界处完成新旧转换。

`MachineState.LastModelResult` 保留最近一次已接受的模型响应；终态时复制到 `RunResult.Model`，其中的 `Text` 对应旧 SDK 调用者看到的 final message。完整的 assistant/tool history 由 application 的 history projection 保存，不重复塞入 `RunResult`。

provider transport 的短暂失败可以由 sdk/provider client 在一次调用内部重试；agent 只看到最终的 `ModelResult` 或 provider error。重试次数和退避属于 sdk/provider 配置。

旧的 `GenerateText`、`StreamText` 和自动 tool loop 在迁移期只能作为显式 legacy wrapper；新 Loop 不依赖它们。

### 2.2 `agent/`

agent 提供通用 core：

| 能力 | 内容 |
| --- | --- |
| Machine | `MachineState`、`Step`、ToolCall 状态、AgentCommand、AgentEvent 和共享的 Decide/Evolve/Next 规则。 |
| Loop | 唯一的多步执行算法。 |
| Tool contract | ToolRef、ExecutableTool、参数校验、结果分类和 response policy。 |
| Runtime contract | `Load`/`Commit` authority 接口。 |
| Planner port | 供 Loop 注入 application Request Planner 的最小接口；规划实现不在 agent。 |
| Effect contract | 模型、工具、等待和观察动作。 |
| EventSink | canonical event 的实时观察出口，也承载不进入权威状态的 provisional delta；它不保存 canonical event。 |
| MCP adapter | MCP schema/call 到 agent tool contract 的适配。 |
| MemoryRuntime | 进程内参考实现；它不是 Machine 的一部分，生产应用也可以在 application 包中实现同一 Runtime contract。 |

agent 不拥有 Memoh 的 session、queue schema、admission、owner、fencing、R0/R1 或数据库类型。它也不组装产品 prompt，不执行 scheduler，不保存产品 memory。

### 2.3 Memoh native runtime

Memoh 负责产品和 durable 层：

| 能力 | 内容 |
| --- | --- |
| context | system prompt、memory、compaction、attachments、workspace 和产品 metadata。 |
| Request Planner | 把 context、history 和 queue-safe 输入投影成冻结的 `sdk.Request`。 |
| durable history | messages、ModelStep record、ToolStep progress 和 tool result record。 |
| queue | steer/follow-up 的入队、accepted order、重排、claim、apply、取消和 admission。 |
| session/run | admission、R0/R1、settled 和后续 Run。 |
| ownership | owner、fencing、lease、liveness 和 takeover。 |
| MemohRuntime | agent.Runtime 的 PostgreSQL adapter。 |
| durable events | AgentEvent、outbox、审计事实和恢复所需的 projection。 |

### 2.4 类型归属

| 类型或能力 | 所属层 |
| --- | --- |
| `sdk.Request`、`sdk.ModelResult`、`sdk.ToolDefinition`（provider-neutral） | sdk |
| `Step`、`ToolCallState`、`AgentCommand`、`RunSeed`、`AgentEvent`、`Effect` | agent |
| `ExecutableTool`、参数校验、response policy | agent |
| `RequestPlanner` port、`PlanningHint`、`RequestPlan` | agent；只用于依赖注入 |
| Request Planner 实现和 context transformer | Memoh/application |
| queue、history、session、R0/R1、owner、fencing、outbox | Memoh |
| MCP server 连接和生命周期 | Memoh/application；schema/call adapter 在 agent |
| provider transport retry | sdk/provider client |

## 3. Agent Machine

### 3.1 Machine 的边界

Machine 是 agent package 中的纯语义规则。它只读取完整 `MachineState`、待决策的 `AgentCommand` 和待折叠的 `AgentEvent`，不访问 IO：

```text
MachineState + AgentCommand
          |
Machine.Decide -> 事实序列（决策，只在提交时运行一次）
          |
Machine.Evolve 逐个折叠（机械） -> new MachineState
Machine.Next(state) -> Effect
```

`Next(state)` 根据当前事实产生至多一个待执行的 `Effect`。`Decide(state, command)` 校验一个意图并产出这次 transition 的完整事实序列——所有决策（是否接受、派生哪些后果、是否进入终态）都发生在这里，且只在提交时运行一次，输出即冻结。`Evolve(state, event)` 把单个事实机械地折叠进状态：它不读 RunConfig，不含任何 policy 分支，对 Decide 产出的每种事实全定义；replay 只依赖 Evolve。Runtime 接受一个 command 后，把 Decide 的事实序列包装为同一 Revision 的 AgentEvent 组，与折叠后的新状态放在同一提交边界。两种 Runtime 不能各自复制规则；Loop 重新 `Load` 后再次调用 `Next`，不会依赖一次提交响应中的 effect。

决策与折叠的分工是协议的兼容边界：Machine 的决策规则（limit 判断、自动关闭、终态转换）可以随版本演进，因为历史事件已把这些决策的结果记录在案；Evolve 与事件编码一起构成永久兼容契约，已发布 SchemaVersion 的折叠语义不再修改。

Run terminal、model-step limit、Step successor、ToolStep 自动关闭和等待条件都由 Machine 的 Decide 决定，并显式产出对应事实（`ToolStepClosed`、`RunEnded`）。Runtime 不再维护另一套 Run 终态和等待判断；Memoh 只把已经接受的 AgentEvent 投影到自己的 history、queue 和 outbox，按事件驱动，不做提交前后的状态差分。

Machine 不知道 PostgreSQL、mutex、lease、fencing、provider client、queue 或产品 history。Runtime 可以在自己的临界区内调用这套规则，但不改变规则。

### 3.2 输入语义

Queue 不属于 Machine，但 Machine 需要知道“一个输入何时已经被接受，以及它对后续执行的影响”。因此 core 只定义两种输入边界：

```text
NextStep(input)
  当前 Run 仍 active 且处于可接收边界；输入进入下一次 ModelStep 的规划上下文

NextRun(input)
  当前 Run 已到 terminal；输入作为 continuation 的初始上下文，不修改旧 Run
```

`AgentInput` 只有稳定的 `InputID` 和不可变 payload，不包含 queue item、priority、order、claim 或 lease。`NextStep` 构造 `AcceptInput` command；被 Runtime 接受后产出 `InputAccepted` 事实，与新的 MachineState 一起提交，Memoh 可以在同一事务中把 queue claim 标记为 applied。

`NextRun` 产生的是 admission seed（`RunSeed`），不是任何 Run 的 command：它不创建 RunID，也不负责 queue claim 或 session admission。Memoh 先完成 queue claim、session admission 和新 Run 的身份分配，再用该 seed 通过 `Initialize` 建立新 Run 的 MachineState。admission 可以复用 `AgentEvent` 的 identity/digest/canonical 编码规则，但其排序和记录归属由 Memoh admission 决定，不调用旧 Run 的 `Runtime.Commit`。

Machine 不在 ToolStep 执行中接受 `NextStep`，也不把 `NextRun` 解释成旧 Run 的状态变化。输入的具体文本如何进入 `sdk.Request` 仍由 Request Planner 决定；Machine 只保证输入边界和一次性接受语义。

### 3.3 MachineState

MachineState 至少包括：

```text
RunID 和冻结的 RunConfig.Model
Run status
其他冻结的 RunConfig
current Step
ToolStep 中每个 ToolCall 的 progress/result
等待中的 ResponseRequest
已接受但尚未用于冻结下一请求的 AgentInput
model-step counter
累计 usage（对已接受 ModelStepCompleted 和 ModelStepRejected 的 sdk.Usage 逐字段求和）
最近一次已接受的 sdk.ModelResult
terminal RunResult（如果已结束）
```

权威语义状态不包括数据库 row、transaction、owner、fence、lease、Attempt 或 queue claim。Runtime 可以保存这些控制元数据，但它们不进入 MachineState。

### 3.4 Run 状态

```text
RunActive
RunCompleted     已得到正常终态模型结果
RunStopped       application 明确停止
RunFailed        provider、step 或未知外部 effect 导致失败
```

等待是 Loop 的当前返回结果，不是单独的持久化 Run 状态。业务取消通过 Memoh/application 的控制事件提交为 `RunStopped`；取消 Loop 的 context 只结束当前执行尝试，不自动修改 Run 状态。
所有 terminal 状态都必须带 `MachineState.Result`；`RunActive` 的 Result 为空。`RunFailed` 的
Failure 至少包含稳定的 Class，未知外部 effect 还包含对应 CallID。

### 3.5 Step 层级

```text
Run R
 └── Step S1: ModelStep
      request frozen
      result pending/completed

ModelResult contains tool calls
          |
          v
      Step S2: ToolStep
        ├── Call A: Completed
        ├── Call B: Waiting(response=101)
        └── Call C: Pending

S2 closes automatically after all Calls are terminal
          |
          v
      Step S3: ModelStep
```

Step 只有 `ModelStep` 和 `ToolStep` 两种语义。ToolCall 是 ToolStep 内的 progress 项，不拥有独立的公共 Step identity。一个 ModelStep 最多产生一个后继 ToolStep；其 Call 集合、顺序和 provider-neutral tool definition digest 一次冻结。模型结果绑定到可执行工具时，`ToolCallBinding` 再冻结 policy 和 binding digest。ModelStep 的 ID 由 `DeriveModelStepID` 生成，并覆盖 ModelRef、request 与 tool-spec digest；ToolStep 的 ID 由原始 Call 顺序的完整 binding-set digest 通过 `DeriveToolStepID(source StepID, binding digest)` 生成，两个 Runtime 必须得到相同结果。

### 3.6 AgentCommand、AgentEvent 与 Effect

协议使用两个 sealed 词表。`AgentCommand` 是 Loop 或受信任的外部入口针对**已有 Run**提出的意图；`AgentEvent` 承载 Runtime 接受一个 command 后产出的事实。一个被接受的 command 构成一次 transition，产出一个或多个事件；命令描述"请求发生什么"，事件描述"已经发生什么"，两个词表不共用类型。

AgentCommand（14 种）：

```text
PrepareModelRequest     冻结下一次模型请求
StartModelExecution     取得 ModelStep 执行权
SubmitModelResult       提交完整模型结果与 tool-call bindings
SubmitModelFailure      提交模型调用的最终失败
RejectModelResult       提交结构性 malformed 的模型结果
RecoverModelExecution   释放或回收 ModelStep 执行权
StartToolCall           取得单个 ToolCall 执行权
SubmitToolResult        提交工具执行成功结果
SubmitToolFailure       提交工具已知/未知失败
ApproveToolCall         批准 Waiting(Approval) Call
RejectToolCall          拒绝 Waiting(Approval) Call
SubmitToolResponse      提交 ask-user 答案
CancelRun               业务取消
AcceptInput             接受 queue-safe 输入（由 NextStep 构造）
```

事实词表 `Fact`（14 种），由 `Machine.Decide` 产出、包装为 AgentEvent：

```text
ModelStepPrepared       冻结的 ModelStep 建立，消费 pending inputs
ModelStepStarted        Prepared -> Executing
ModelStepRecovered      Executing -> Prepared（无已接受结果）
ModelStepRejected       记录一次 malformed 结果：usage 累计，Rejects 加一，回到 Prepared
ModelStepCompleted      模型结果被接受：usage 累计，写 LastModelResult，清空当前 Step
ToolStepOpened          按 bindings 建立 ToolStep 的完整 Call 集合
ToolCallStarted         Pending -> Executing
ToolCallApproved        Waiting(Approval) -> Pending
ToolCallCompleted       Executing -> Completed
ToolCallAnswered        Waiting(ExternalResponse) -> Completed（答案）
ToolCallFailed          Pending/Executing/Waiting -> Failed(Known/Unknown)
ToolStepClosed          全部 Call 到达可关闭终态，清空当前 Step
InputAccepted           输入进入 PendingInputs
RunEnded                终态：Status 为 RunCompleted、RunStopped 或 RunFailed
```

`AgentEvent` 与新的 `MachineState` 在同一个原子提交中写入，具有 authority 分配的 (Revision, Index) 身份、canonical digest 和产生它的 CommandID。`AgentEvent` 可供 replay、projection、审计和 OpenTelemetry 使用。

`RunSeed` 属于新 Run 的 admission，不是 command，也不进入任何 Run 的事件流。如果 Memoh 需要审计这次 admission，可以复用相同的 identity/digest 编码规则，但该记录由 admission 事务保存，不经过 `Runtime.Commit`。

二者的关系固定为：

```text
Loop / response ingress
        -> AgentCommand (intent)
Runtime EvaluateCommit: 幂等/类别校验 + Machine.Decide + Machine.Evolve
        -> MachineState + AgentEvent 组 (committed facts, one Revision)
```

意图与事实由 Decide 显式转换，日志永远记录结果而非请求：`RejectToolCall` 记录为 `ToolCallFailed{permission_denied}`；`CancelRun` 记录为 `RunEnded{RunStopped, cancelled}`；触发 step limit 的最后一次 Call 完成记录为 `[ToolCallCompleted, ToolStepClosed, RunEnded{RunStopped, step_limit}]`。

`PrepareModelRequest.RequestDigest` 和 `SubmitToolResponse.ResponseDigest` 分别是请求/响应 payload 的内容摘要，不是提交身份。一个 command 被重试时复用同一 CommandID 和 digest；Runtime 不会为重试生成第二组 AgentEvent。

Effect 是 Loop 动作：

```text
NeedModelRequest(PlanningHint)
StartModelCall(StepID)
StartToolCalls(StepID, CallIDs)
WaitForResponse(ResponseRequests)
WaitForExecutionRecovery
```

终态由 `MachineState.Status` 表示；Effect 只是待执行动作，既可以是模型调用，也可以是工具调用或等待，该名称不表示一定产生外部副作用。Effect 不写入权威状态；Loop 在每次 `Load` 后由 MachineState 重新得到它们。

### 3.7 共享规则

#### 3.7.1 Decide 规则（命令 -> 事实序列）

`Decide(state, command)` 按下表校验前置条件并产出事实序列。任何前置条件不满足即拒绝整个 command，不产出部分事实：

1. `PrepareModelRequest` 只能在 Run active 且没有当前 Step 时接受；其 `InputIDs` 必须按 `PendingInputs` 的当前顺序完整匹配。`Tools` 必须与 `sdk.Request` 中的 provider tool definitions 按 Ref、顺序和 definition digest 一一对应，`ToolsDigest` 覆盖 Ref、schema、顺序与 policy。产出 `[ModelStepPrepared]`，事实中携带冻结的请求、ToolSpec 与被消费的 InputIDs。
2. `StartModelExecution` 只能作用于 Prepared ModelStep，产出 `[ModelStepStarted]`。`RecoverModelExecution` 只能作用于没有已接受结果的 Executing ModelStep，产出 `[ModelStepRecovered]`；它只能由持有该 Model grant 的当前 Loop，或由 Runtime 自己确认 lease 失效后的 recovery 逻辑提交，普通 response ingress 不能提交。
3. `SubmitModelResult` 只能作用于对应的 Executing ModelStep。结果没有 tool calls 时产出 `[ModelStepCompleted, RunEnded{RunCompleted}]`；有 tool calls 时按冻结 `ToolSpec` 绑定 policy 和 binding digest，产出 `[ModelStepCompleted, ToolStepOpened]`——`ToolStepOpened` 携带完整 Call 集合：DirectExecution 的 Call 为 Pending，ApprovalRequired/ExternalResponse 的 Call 为带稳定 request 的 Waiting，每个 Waiting request 都包含目标 RunID、StepID、CallID、ResponseID、Kind 和 RequestDigest。
4. `SubmitModelFailure` 只能作用于对应的 Executing ModelStep，产出 `[RunEnded{RunFailed, provider_failure}]`，保留稳定失败原因。
5. `RejectModelResult` 只能作用于对应的 Executing ModelStep，必须携带该 start 的 Grant。`Rejects+1` 不超过冻结的 `RunConfig.ModelRejectLimit` 时产出 `[ModelStepRejected]`（Step 回到 Prepared，同一冻结 request 可再次 start）；超过时产出 `[ModelStepRejected, RunEnded{RunFailed, malformed_model_result}]`。被拒绝的结果不写入 `LastModelResult`。
6. `StartToolCall` 只能作用于 Pending Call，产出 `[ToolCallStarted]`。
7. `SubmitToolResult` 只能作用于 Executing Call，产出 `[ToolCallCompleted]`。`SubmitToolResponse` 只能作用于 Waiting(ExternalResponse) Call，产出 `[ToolCallAnswered]`。
8. `SubmitToolFailure`（known）可以作用于 Pending 或 Executing Call：Pending 的已知失败使用空 Grant，Executing 的必须使用对应 Grant，产出 `[ToolCallFailed{Known}]`。`SubmitToolFailure`（unknown）只能作用于 Executing Call，产出 `[ToolCallFailed{Unknown}, RunEnded{RunFailed, effect_unknown}]`，`RunResult.Failure` 记录 `effect_unknown` 和对应 CallID；scanner 提交它时必须先有实现内部的失效执行记录。
9. `ApproveToolCall` 必须匹配目标 Waiting(Approval) Call 保存的 ResponseID 和 kind，产出 `[ToolCallApproved]`（Call 变为 Pending，Loop 随后执行）。`RejectToolCall` 同样必须匹配，产出 `[ToolCallFailed{Known, permission_denied}]`。日志记录的是结果事实，不是请求本身。
10. 一次响应只推进对应 Call，不能修改其他 Call；ResponseID 和 kind 必须匹配该 Call 保存的请求。响应 payload 的 digest 用于内容冲突检测，不需要等于请求 payload 的 digest。
11. 使 ToolStep 内最后一个 Call 到达可关闭终态的 command，其事实序列追加 `ToolStepClosed`；若此时 `ModelSteps` 已达冻结的 model-step limit，再追加 `RunEnded{RunStopped, step_limit}`。例如最后一个 Call 完成且触发 limit：`[ToolCallCompleted, ToolStepClosed, RunEnded{RunStopped, step_limit}]`。ToolStep 关闭前不能创建下一 ModelStep。
12. `CancelRun` 只能作用于非 terminal Run，产出 `[RunEnded{RunStopped, cancelled}]`。
13. `AcceptInput` 只能作用于 active 且没有当前 Step 的 Run，产出 `[InputAccepted]`。Planner 由 `PlanningHint.Inputs` 收到这些输入并在 `RequestPlan.InputIDs` 中明确消费它们；遗漏或伪造 ID 的 `PrepareModelRequest` 被拒绝。
14. `Initialize(RunID, RunConfig, RunSeed)` 只在 application admission 创建新 Run 时使用；它建立初始 `MachineState` 并把 seed 输入放入 `PendingInputs`。`RunSeed` 不能传给已有 Run 的 `Decide` 或 `Runtime.Commit`。

`InputID` 在一个 Run 内唯一。相同 `InputID` 和相同 payload 的重复 `AcceptInput` 是语义 no-op，Runtime 返回原已接受的事件组；相同 ID 携带不同 payload 返回冲突。Memoh 的 queue claim 仍负责防止同一个 queue item 被多个输入入口同时消费。

#### 3.7.2 Evolve 折叠表（事实 -> 状态）

`Evolve(state, event)` 对每种事实执行固定的机械折叠，不读 RunConfig，不含 policy 分支：

| 事实 | 折叠 |
| --- | --- |
| ModelStepPrepared | 设置 Current 为 Prepared ModelStep；`ModelSteps+1`；按事实中的 InputIDs 从 `PendingInputs` 移除 |
| ModelStepStarted | Current.Status = Executing |
| ModelStepRecovered | Current.Status = Prepared |
| ModelStepRejected | `Rejects+1`；Current.Status = Prepared；Usage 逐字段累加事实携带的 usage |
| ModelStepCompleted | 写 `LastModelResult`；Usage 逐字段累加；清空 Current |
| ToolStepOpened | 设置 Current 为携带完整 Call 集合的 ToolStep |
| ToolCallStarted | 目标 Call: Pending -> Executing |
| ToolCallApproved | 目标 Call: Waiting -> Pending |
| ToolCallCompleted | 目标 Call: Executing -> Completed(result) |
| ToolCallAnswered | 目标 Call: Waiting -> Completed(answer) |
| ToolCallFailed | 目标 Call -> Failed(outcome, failure) |
| ToolStepClosed | 清空 Current |
| InputAccepted | 按 InputID 幂等追加到 `PendingInputs` |
| RunEnded | 设置 Status 与 `Result`（含 Usage 副本与最近已接受的模型结果）；清空 Current |

事实序列内的折叠按 Index 顺序进行；`RunEnded` 若出现必须是序列的最后一个事实。`ModelStepRecovered` 不改变 Usage 与 `Rejects`。

#### 3.7.3 终态与竞态

终态按 Runtime 的线性化顺序确定：Cancel 先提交则 RunStopped；Unknown 先提交则 RunFailed，之后的 Cancel 不改变终态。Run 进入 terminal 后，其他并行 Call 的 grant 立即失效，Loop 取消仍在运行的 worker；迟到的完成提交返回 terminal/stale 错误，只能写实现级审计，不能再改变 MachineState；Memoh 必须把这类审计投影到产品可见的 history/审计视图——该工具的外部 effect 已经发生，只落内部日志会让会话记录与外部世界不一致。并行 Call 不引入额外的 settling 状态。

`RunStopped` 或 `RunFailed` 的 `RunResult` 保留最近一次已接受的模型结果（如有）；`RunCompleted` 的 `RunResult.Model` 是产生正常终态的那次模型结果。取消或未知 effect 不会伪造新的模型结果。

`Next` 的主要映射是：无当前 Step -> `NeedModelRequest(PlanningHint{Inputs: PendingInputs})`（model-step limit 在前一个 Step 的提交中已经转换为 terminal）；Prepared ModelStep -> `StartModelCall`；ModelStep 正在 Executing -> `WaitForExecutionRecovery`；有 Pending Call 的 ToolStep -> `StartToolCalls`；没有 Pending 且存在 Waiting Call -> `WaitForResponse`（即使另有 Executing Call，也等待 response 或 execution wake）；没有 Pending/Waiting 但存在 Executing Call -> `WaitForExecutionRecovery`。Runtime 接受 `StartModelExecution` 或 `StartToolCall` 后，在 CommitResult 中返回一次性 `ExecutionGrant`，Loop 使用该授权调用对应的 ModelInvoker 或 ExecutableTool。Model execution lease 失效后，Runtime 通过仅限 Runtime/recovery 使用的 `RecoverModelExecution` 把 ModelStep 恢复为 Prepared，Loop 才能再次 start。

这里的 `WaitForExecutionRecovery` 是一个统一等待结果：它既表示已有 execution 仍可能由原 Loop 持有，也表示该 execution 已失效、等待 Runtime recovery。公共 `LoopResult.Reason` 不暴露 owner、lease 或 Attempt 的细节。

## 4. Step 与 ToolCall progress

### 4.1 ModelStep

ModelStep 冻结：

```text
StepID
sdk.Request 及其 digest
ModelRef
本次请求使用的 provider-neutral tool definitions 及 digest
与这些 definition 对应的 agent `ToolSpec`（包含 response policy）
`ToolsDigest`（按 provider definition 顺序覆盖 schema、Ref 和 policy）
执行状态：Prepared / Executing
reject counter（progress，不参与冻结 digest）
```

`SubmitModelResult` 被接受后，当前 ModelStep 立即被 ToolStep 替换（`ToolStepOpened`），或因没有 tool calls 而关闭 Run（`RunEnded`）；因此模型完成状态不作为当前 Step 状态保存。接受的结果保存在 `LastModelResult`、RunResult 和 application history 中。

一个 ModelStep 代表一次模型调用。Loop 默认调用 `ModelInvoker.Generate`；如果实现提供可选的 `StreamingModelInvoker`，Loop 可以用 `Stream` 发送实时 delta，但两条路径必须得到同一种 `sdk.ModelResult`，且 transport retry 不创建新的 Step。

Request Planner 生成完整 `sdk.Request` 后，Loop 提交 `PrepareModelRequest`。Runtime 以 revision/CAS 或事务保证只冻结一份请求；新 ModelStep 的 StepID 由 RunID、command identity 和 request digest 稳定派生。已经冻结的请求不受后来 queue 或 history 输入影响。

Loop 在 `SubmitModelResult` 时只校验 tool-call ID 与顺序，并从匹配的冻结 `ToolSpec` 生成 `ToolCallBinding`。这里不调用 ExecutableTool；未知工具保留为 `DirectExecution`、空 definition digest 的 unresolved binding，应用级参数错误留到 `StartToolCalls`，作为 Pending Call 的已知失败处理。Runtime 只校验 binding 与冻结请求、模型结果和 Step 身份的一致性，不重复解析工具目录。

模型响应的结构性 malformed（重复/错序 CallID、违反 provider 协议）使 Call 集合无法建立：Loop 不提交 `SubmitModelResult`，而以 start grant 提交 `RejectModelResult{Failure.Class: malformed_model_result}`。Decide 产出 `ModelStepRejected`（usage 累计、`Rejects` 加一、Step 回到 Prepared），同一冻结 request 由后续 start 重试；`Rejects` 超过冻结的 `RunConfig.ModelRejectLimit` 时追加 `RunEnded{RunFailed, malformed_model_result}`。被拒绝的结果不写入 `LastModelResult`，不创建 ToolStep。

单个 Call 的参数无法解析不属于结构性 malformed：该 Call 以原始参数字节绑定为 Pending，`StartToolCalls` 的参数校验把它关闭为已知 `invalid_arguments`，失败结果进入下一次模型请求，由模型自行修正。未知 ToolRef 同样保留为待处理 Call，start 前记录 `tool_lookup_failed`。

### 4.2 ToolStep

ToolStep 保存：

```text
source ModelStep ID
原始 ToolCall 顺序
CallID、ToolRef、canonical arguments
response policy
工具定义 digest
每个 Call 的 binding digest（definition、policy 和 arguments 的摘要）
每个 Call 的 durable progress
```

`BindingDigest` 必须覆盖匹配的 `ToolSpec`、canonical arguments 和 CallID；`ToolStepOpened` 创建 ToolStep 时同时保存到 `ToolCallState`，执行前再次校验。

每个 Call 的状态为：

```text
Pending
Executing
Waiting(request)
Completed(result)
Failed(Known, failure)
Failed(Unknown, failure)
```

这些状态只保存会影响恢复决策的事实。工具内部 stdout、下载百分比、HTTP 字节数等观察信息只走 EventSink，不写 MachineState。

状态字段必须保持互斥且可校验：Pending/Executing 的 Result、Failure、Waiting 都为空；Waiting 必须带 ResponseRequest 且 policy 为 Approval 或 ExternalResponse；Completed 必须带 Result；Failed 必须带 Failure，Unknown outcome 必须使用 `effect_unknown` 且 Run 必须同时为 RunFailed。`Machine.Evolve` 和 Runtime.Load 都必须拒绝非法组合；approval accepted 清除 Waiting 并变为 Pending，answer 清除 Waiting 并变为 Completed，reject 清除 Waiting 并变为已知 Failed。

### 4.3 ToolCall 执行规则

`StartToolCall` 是外部调用前的 durable barrier。它把 Pending 固定为某个 Loop attempt 正在负责；提交成功后才允许调用工具。这个顺序不能消除“提交后、调用前崩溃”的窗口，因此失去执行权且没有结果时仍按 Unknown 终止 Run，但它能阻止多个 Loop 同时执行同一 Call。

1. ModelStep 完成时一次性保存完整 Call 集合（`ToolStepOpened`）。
2. Completed 和 Failed Call 永远跳过。
3. Pending Call 必须先提交 `StartToolCall`；Runtime 接受后才允许调用外部工具。
4. Waiting Call 不阻止其他 Pending Call。Pending Call 是否并行由 Loop 的 ExecutionPolicy 决定。
5. 每个 Call 完成后立即提交自己的 `SubmitToolResult` 或 `SubmitToolFailure`。
6. 没有 Pending 或 Executing、仍有 Waiting 时，Loop 返回等待。
7. 所有 Call 到达 Completed 或已知 Failed 时，Decide 在该次提交的事实序列中追加 `ToolStepClosed`。
8. Executing Call 在执行权失效且结果未提交时不能自动重做；Runtime 记录 Unknown 并终止 Run。

并行是 Loop 的执行策略，不是 ToolStep 固有语义。Machine 只返回可执行的 Pending Call，不推断工具之间是否存在 effect ordering；application 只有在确认一组工具允许并行时才配置大于 1 的并行度。一个 Loop 执行组使用同一份策略：

```text
Sequential
Parallel
BoundedParallel(n)
```

在 API 中统一表示为 `ExecutionPolicy{MaxParallel}`：`1` 表示 Sequential，`n>1` 表示 BoundedParallel(n)，`0` 在 Loop 创建时归一化为 `1`，负值在 Loop 创建时拒绝。`Parallel`（不设上限）不作为默认行为；若未来需要，必须另行规定资源上限。

`MaxParallel` 只限制一个 Loop 本次实际启动的 worker 数量，不是跨进程的全局并发计数。Runtime 仍以每个 Call 的 start grant 防止重复执行；Memoh 如果需要全局资源限额，必须在自己的调度层另行实现。

同一 ToolStep 的结果投影仍按原始 Call 顺序；本版本不定义 Call 之间的依赖边，需要前置结果的调用由后续 ModelStep 产生。

### 4.4 approval、ask-user 和外部响应

approval 和 ask-user 都是 ToolCall 的 response policy：

```text
ModelStep
  -> ToolStep
       approval: Waiting(Approval)
       ask-user: Waiting(ExternalResponse)
```

外部入口按 response policy 转换成对应的 AgentCommand，日志记录 Decide 产出的结果事实：

```text
approve -> ApproveToolCall    -> ToolCallApproved -> Pending -> 工具执行
reject  -> RejectToolCall     -> ToolCallFailed(Known, permission_denied)
answer  -> SubmitToolResponse -> ToolCallAnswered -> Completed(answer)
```

每个 response 有稳定的 `ResponseID`，由 Machine 在创建 Waiting request 时从 RunID、StepID、CallID 和 response kind 稳定派生；一个 Call 至多有一个未决请求，两个 Runtime 派生结果一致。一次响应只作用于对应 `RunID/StepID/CallID`，不需要旧 Loop 的执行租约，也不消费其他 Call 的执行权。

### 4.5 工具结果和失败

工具执行返回封闭的三种结果：

```text
ToolExecutionSucceeded{Result}
    ToolExecutionFailed{Failure}       // 已知没有完成外部 effect
    ToolExecutionUnknown{Failure}      // 无法判断外部 effect
```

`ToolFailure` 使用稳定的 `Class` 和可选的 `Message`：

```text
permission_denied
tool_lookup_failed
invalid_arguments
tool_definition_mismatch
execution_failed
effect_unknown
```

工具只有在能够确定外部 effect 没有完成时才能返回 `ToolExecutionFailed`；只要结果可能已经产生 effect，就必须返回 `ToolExecutionUnknown`。

已知失败作为下一次模型请求中的 `sdk.ToolResultPart{IsError:true}`，保留原始 CallID、工具名、Class 和 Message。模型可以在新的 ModelStep 中再次发起工具调用。Unknown 不投影给模型，不自动重试，Run 直接进入 RunFailed；通用 core 不假设外部系统支持查询或撤销。

## 5. Runtime contract

### 5.1 Runtime 的职责

Runtime 只回答两个问题：

```text
当前权威 MachineState 是什么？
一个 AgentCommand 如何安全地提交并生成 AgentEvent？
```

它不组装 prompt，不调用模型或工具，不定义 Machine 规则，不实现 queue policy。Runtime 可以在同一事务中更新 Memoh 的 history、queue projection、response record 和 outbox，但这些是 adapter 的原子投影，不是 Runtime 的语义职责。

`Commit` 必须在 authority 的临界区内调用共享的 `EvaluateCommit`（内部执行幂等/类别校验、`Machine.Decide` 和 `Machine.Evolve`），因为“读取状态、验证 command、计算事实与新状态、保存并写入 AgentEvent”不能在 durable 实现中拆成几个由 Loop 拼接的公开操作。这个必要的原子边界不等于 Runtime 拥有 Machine 规则；规则仍只有 agent 一份，也不等于 Runtime 拥有 Memoh 的产品数据。

因此 Runtime 的接口很小，但一次 `Commit` 的事务范围可以很大：它必须让一个 AgentCommand、产出的 AgentEvent 组及其必要的产品投影一起成功或一起失败；这不意味着 Runtime 获得了 history、queue 或 prompt 的所有权。

MachineState 与 AgentEvent log 是同一个 transition 序列的两个 materialization：MachineState 是 execution authority（提交验证与 Loop 执行的依据），AgentEvent log 是 historical authority（replay、审计与投影的依据）。两者由同一原子提交产生、共享同一 Revision；对任意 Revision N，状态必须等于初始状态按事件流 fold `Evolve` 到 N 的结果。两者出现分歧是 halt 级一致性违规：停止该 Run 的执行并报告，不定义任何一方自动覆盖另一方。

一个 Runtime 实例服务一个 Run；多个 Run 由上层创建多个 Runtime 实例。Run 的创建、身份分配和初始
`MachineState` 由 application/Memoh admission 完成，Runtime 从一个已经有效的初始状态开始。
Loop 是对这个 Run 的一次进程执行。

### 5.2 最小接口

```go
type Runtime interface {
    Load(context.Context) (RuntimeSnapshot, error)
    Commit(context.Context, CommitRequest) (CommitResult, error)
}
```

`Load` 是纯读取，只返回权威状态和 Revision；它不创建 Attempt、不取得 lease，也不因为另一个 execution 正在运行而返回 busy。`Commit` 接受一个 AgentCommand；Runtime 在自己的同步/事务边界内调用共享 `EvaluateCommit`、保存结果并写入 AgentEvent 组。只有 start command 的 `Commit` 可以建立执行占用并返回授权，不增加第三个“执行”方法。

因此 Executing 不是 Runtime error。Loop 从 snapshot 调用 `Machine.Next` 后得到 `WaitForExecutionRecovery`；ToolStep 同时存在 Executing 与 Pending Call 时，Machine 仍返回可执行的 Pending Call；同时存在 Executing 与 Waiting Call 时，Machine 仍返回等待请求。不能用一个 run-level busy 锁住整个 ToolStep。

请求构造不在 Runtime 接口中。Loop 使用 `NeedModelRequest` 提示调用 Request Planner，再把完整请求作为 `PrepareModelRequest` 提交。

Runtime 的语义范围仍然只有 authority 和 commit。MemohRuntime 为了保持 AgentEvent、MachineState、history projection 和 outbox 的一致性，可以在自己的数据库事务内一起写入这些投影；这不是 Runtime 对外暴露的通用业务 API，也不让 Runtime 获得 prompt、queue 或 history 的所有权。

### 5.3 Snapshot、Commit 和执行授权

```go
type RuntimeSnapshot struct {
    State    MachineState
    Revision uint64 // 已接受的 transition 数；初始状态为 0
}

// The representation is implementation-defined. Callers only pass it back;
// it is not a Step identity, a durable domain value, or a user credential.
type ExecutionGrant string

type CommitRequest struct {
    BaseRevision uint64
    Grant        ExecutionGrant
    Command      CommandEnvelope
}

type CommandEnvelope struct {
    SchemaVersion uint16
    Type          string
    RunID         RunID
    ID            CommandID
    Digest        Digest
    Command       AgentCommand
}

// AgentEvent carries one fact produced by an accepted command. All events of
// one transition share the same Revision, CommandID and CommandDigest; Index
// orders them within the transition. Identity and ordering are assigned by the
// authority and are not supplied by callers. Run admission has a separate
// record because it initializes a new Run rather than advancing an existing one.
type AgentEvent struct {
    SchemaVersion uint16
    Type          string
    RunID         RunID
    Revision      uint64
    Index         uint16
    CommandID     CommandID
    CommandDigest Digest // digest of the accepted command; idempotent replay compares against it
    Digest        Digest // canonical digest of this fact
    Fact          Fact
}

type CommitResult struct {
    Status   CommitStatus
    Snapshot RuntimeSnapshot
    Events   []AgentEvent   // Accepted 或 AlreadyApplied 时为该 transition 的完整事件组
    Grant    ExecutionGrant // 仅 Accepted 的 start command 会返回；AlreadyApplied 为空
}

func EncodeCommand(CommandEnvelope) ([]byte, error) // 不包含 Digest 字段
func DigestCommand(schemaVersion uint16, typ string, command AgentCommand) (Digest, error)
func EncodeFact(schemaVersion uint16, typ string, fact Fact) ([]byte, error)
func DigestFact(schemaVersion uint16, typ string, fact Fact) (Digest, error)
func EncodeRunSeed(RunSeed) ([]byte, error)
func DigestRunSeed(schemaVersion uint16, seed RunSeed) (Digest, error)
func DigestRequest(sdk.Request) (Digest, error)
func DigestToolDefinition(sdk.ToolDefinition) (Digest, error)
func DigestToolSpec(ToolSpec) (Digest, error)
func DigestToolSpecs([]ToolSpec) (Digest, error)
func DigestModelStepBinding(model ModelRef, requestDigest, toolsDigest Digest) (Digest, error)
func DeriveModelRequestCommandID(RunID, uint64) CommandID
func DeriveModelStepID(RunID, CommandID, Digest) StepID
func DeriveToolStepID(StepID, Digest) StepID
func DeriveResponseID(RunID, StepID, CallID, ResponseKind) ResponseID
func DeriveResponseCommandID(RunID, StepID, CallID, ResponseID) CommandID
func DeriveInputCommandID(RunID, InputID) CommandID
```

`ExecutionGrant` 是 Runtime 返回的 opaque capability，只用于证明当前 Loop 获得了执行许可；调用方只保存并原样传回，不依赖其内容。它不是 Step 的业务字段，不暴露 AttemptID、FenceToken、lease 或数据库类型，也不是用户认证凭证。Runtime 必须生成不可预测且绑定到单个 Step/Call 和当前执行所有者的值，并在完成 command 中校验它。MemoryRuntime 可以用 mutex 加随机 generation 实现它；MemohRuntime 可以用 owner/fence/lease 实现它。

`Attempt` 只表示一次进程对当前 ModelStep 或 ToolCall 的执行占用，不是 MachineState，也不是恢复边界。一个 Step 可以先后有多个 Attempt；旧 Attempt 失效后，新的 Loop 重新读取同一个 Step。MemoryRuntime 的 Attempt 是内存中的占用记录，MemohRuntime 的 Attempt 是私有 lease/fence 记录。

grant 规则固定为：

```text
PrepareModelRequest、StartModelExecution、StartToolCall
  使用空 Grant；start command 只有 CommitAccepted 才返回新的 Grant。

SubmitModelResult、SubmitModelFailure、RejectModelResult、
Executing Call 的 SubmitToolResult/SubmitToolFailure
  必须带回对应 start command 返回的非空 Grant；Pending Call 的已知失败使用空 Grant。

ApproveToolCall、RejectToolCall、SubmitToolResponse、CancelRun、AcceptInput
  使用空 Grant；Runtime 依据状态、Revision 和 command 身份校验。scanner 的 Unknown 和
  无 grant 的 RecoverModelExecution 还必须匹配 Runtime 自己记录的 lease-expired/recovery
  事实，不能仅凭调用者构造同名 command。当前 Loop 主动释放自己持有的 Model grant 时，
  RecoverModelExecution 必须带对应非空 Grant。
```

Grant 只绑定一个 ModelStep 或一个 ToolCall，不能转用于另一个 Call。外部 response 入口不需要持有 Loop attempt 的执行凭据。

### 5.4 Commit 语义

提交决策是一个由 agent 导出的纯函数；两个 Runtime 在各自的临界区/事务内调用同一份实现，不各自复刻规则：

```go
type CommitDecision struct {
    Kind     DecisionKind // Apply | AlreadyApplied | Conflict | Stale | Terminal
    NewState MachineState
    Events   []AgentEvent
}

// grantValid/recoveryValid 由 Runtime 依据自己的 lease/occupancy 记录判定后传入；
// prior 是相同 (RunID, CommandID) 的已有事件组（若有）。
func EvaluateCommit(
    cur MachineState, curRevision uint64,
    prior []AgentEvent,
    req CommitRequest,
    grantValid bool,
    recoveryValid bool,
) (CommitDecision, error)
```

`EvaluateCommit` 的固定顺序：

1. 校验 CommandEnvelope 的 canonical digest 和 command identity。
2. 已有相同 `(RunID, CommandID)` 且 digest 相同，返回 `AlreadyApplied` 与原事件组，不重复写入任何 projection 或 outbox，不重新运行 Decide（决策在首次接受时冻结）。
3. 相同 identity 携带不同 digest，返回 `ErrCommandConflict`。
   对 `AcceptInput`，还按 RunID/InputID 检查已接受索引：相同 payload 返回原事件组和 `CommitAlreadyApplied`，不同 payload 返回冲突，不产生第二条输入事实。
4. 校验 BaseRevision、当前 Step、CallID 和 Grant。BaseRevision 只对 `PrepareModelRequest` 是硬校验——其 CommandID 由 Revision 派生，Revision 即它的并发控制，过期即返回 `ErrStaleRuntime`。其余 command 在 BaseRevision 过期时按类别前置条件基于当前状态重新评估：start（`StartModelExecution` 目标仍须为同一 Prepared ModelStep；`StartToolCall` 与 Pending 的已知失败目标 Call 仍须为 Pending，均空 Grant）；owner 完成（Executing Call 的完成/失败、`SubmitModelResult`/`SubmitModelFailure`/`RejectModelResult`，以及持有效 Model grant 的 `RecoverModelExecution`，以提交者仍持有对应有效 Grant 为条件）；system recovery（无 grant 的 `RecoverModelExecution` 和 scanner 的 Unknown，以 Runtime 自己的 recovery record 为条件）；ingress（approval/external response 目标 Call 仍须为对应 Waiting 且 ResponseID/kind 匹配；`AcceptInput` 要求 Run 仍 active 且没有当前 Step，连续多条输入互不拒绝；均空 Grant）；run-control（`CancelRun` 只要求 Run 非 terminal）。前置条件不满足时按具体原因返回 stale/terminal/冲突。start command 建立 grant/lease 的动作与状态提交属于同一个原子操作。
5. 调用 `Machine.Decide` 产出事实序列，逐个 `Machine.Evolve` 折叠出新状态；为这次 transition 分配 `Revision = curRevision + 1`，事实按序获得 `Index = 0..k-1`，全部携带产生它们的 CommandID。
6. Runtime 在自己的原子边界内保存 MachineState、AgentEvent 组及需要一致的 Memoh projection。
7. 非重复 command 若目标 Run 已经 terminal，返回 `ErrRunTerminal`；迟到的 worker 结果不会重新打开 Run。其他提交成功后返回新 snapshot。

`EvaluateCommit` 用 `DecisionKind` 表达结果；`Runtime.Commit` 把非成功结果映射为对外错误：`DecisionConflict -> ErrCommandConflict`、`DecisionStale -> ErrStaleRuntime`、`DecisionTerminal -> ErrRunTerminal`。Loop 与 ingress 只依赖这三个错误值和 `CommitStatus`，不接触 DecisionKind。

`RunSeed` 是新 Run admission 的初始化输入，不属于任何 Run 的 `AgentCommand`，也不通过旧 Run 的 `Runtime.Commit`。`Initialize` 使用与 Machine 相同的不变量以及统一的 identity/digest 编码规则建立初始 `MachineState`（Revision=0）；普通 Runtime 只实现已有 Run 的 `Load/Commit`，admission 路径负责应用 `RunSeed`。

Commit 的 effectively-once 只针对状态提交。事务已提交但响应丢失时，Loop 使用相同 CommandID 和 digest 重放；`CommitAlreadyApplied` 不能重新授予工具执行权，也不能重复 history、queue action、计数或 outbox。对于 start command，AlreadyApplied 的 `Grant` 必须为空；新 Loop 要等待原执行或 recovery，不能把重放当成新的 start。外部工具 effect 不因此变成 exactly-once。

这意味着 start 提交成功但响应丢失时，无法证明重试者仍是原 execution owner；Runtime 不安全地重新发放旧 grant。重试者只能等待原 owner 完成，或等待 lease recovery 将该 Call 收束为 Unknown。这是为了避免两个进程同时执行同一个外部工具的保守可用性取舍。

这条 call-local rebase 是并行执行成立的条件：A 的 start 使 Run 的 Revision 前移后，仍基于旧 Revision 提交的 B start 可以在 B 仍为 Pending 时被接受；A 的变化不能使 B 的 start 条件失效。B 的完成 command 也可以在 A 的无关 transition 之后提交，但必须带 B 自己的有效 Grant。重新评估不能跨越使前置条件失效的变化：同一 Call 已被推进、当前 Step 已关闭、Run 已因 Cancel 或 Unknown 进入 terminal 时，迟到的 command 按具体原因返回 stale/terminal，不静默改写。一个 Call 进入 Unknown 并使 Run terminal 后，其他 Call 的完成 command 不再改变 MachineState。

### 5.5 Command identity 和幂等

幂等是为了处理“提交已经成功，但 Loop 没收到响应”的情况：

```text
Loop -> Commit(command C)
Runtime 已提交 C（事实组落地）
进程在收到响应前崩溃
新 Loop -> 重放同一个 C
Runtime -> CommitAlreadyApplied + 原事件组
```

command 身份规则：

```text
RunID 和 CommandID 始终非空，且是区分大小写的 UTF-8 字符串。包含 Step、Call、Response 或 Input 的 command 必须提供相应的非空 ID；CancelRun 等 run-level command 不伪造 StepID/CallID。

Digest
  是 agent canonical bytes 的 SHA-256，wire 形式为 sha256:<64 位小写十六进制>。
  CommandEnvelope.Digest 摘要 command 内容；AgentEvent.Digest 摘要事实内容。

普通 CommandID
  由产生意图的执行者生成，重试时必须复用。

每个 AgentCommand 都有自己的 CommandID；同一个 execution attempt 的 start、completion 和 recovery 是不同 command，因此各自使用不同 ID。某个 command 因响应丢失而重放时必须复用原 ID。Runtime 以当前 Step/Call 状态和 CAS 防止第二个 attempt 获得执行权；ModelStep 经 recovery 回到 Prepared 后，下一次 start 使用新的 ID。

`RecoverModelExecution` 有两条合法来源：当前 Loop 持有效 Model grant 且模型调用未产生可接受结果时，使用该 grant 主动释放 execution；或者 Runtime 在 lease 失效后依据自己的 recovery record 提交。后者的 CommandID 由 Runtime 根据 recovery record 生成；同一个 recovery record 重放使用同一个 ID，不同的失效执行使用不同 ID。新的 Loop 随后重新提交同一冻结 request 的 start command。

response CommandID
  只由 RunID、StepID、CallID 和 ResponseID 稳定派生；payload/decision digest
  只放在 CommandEnvelope.Digest，用于检测同一响应身份的内容冲突。

input CommandID
  由 RunID 和 InputID 稳定派生；queue claim 的私有 item reference
  不进入 agent command/event payload。

AgentEvent 身份
  由 authority 分配：(RunID, Revision, Index) 全序唯一，CommandID/CommandDigest 关联到
  产生它的 transition——重放判定即比较传入 command 的 digest 与已存事件组的
  CommandDigest，Runtime 不需要独立的 command 索引表。事件身份不由调用方提供，
  也不参与 command 幂等判定。
```
```

canonical 编码和 digest 函数由 agent 提供；Memoh 只保存和比较结果，不重新实现排序或编码。编码必须包含 sealed command/fact discriminator、按声明顺序编码有序 slice、对 map key 排序，并对 `json.RawMessage` 使用 canonical JSON；不把 `Digest`、BaseRevision、Revision、Index 或 ExecutionGrant 编入 digest。

已发布 `SchemaVersion` 的 canonical 编码和 digest 规则永久冻结；字段增删只能进入新的 SchemaVersion，旧事件按其自带版本校验。同一个 Run 不允许由写入不同 SchemaVersion 的进程混跑：升级窗口内先全量部署可读写新版本的代码，再开始写入新版本；否则同一 command 的重放会因编码不同被误判为 `ErrCommandConflict`。

CommandEnvelope 和 AgentEvent 的 `SchemaVersion` 和 `Type` 是持久化协议字段；Type 必须与 sealed AgentCommand/Fact 的具体变体一致，未知版本或类型直接拒绝。`DigestCommand`/`DigestFact` 对 `SchemaVersion`、`Type` 和内容做 canonical digest，但不把 `Digest` 字段自身纳入摘要，保证 Memoh scanner、MemoryRuntime 和不同进程使用同一身份规则。`Revision` 只用于 authority 的 CAS，不进入任何 digest。

Evolve 的折叠语义与事件编码同属永久兼容契约：已发布 SchemaVersion 的事件必须永远能被折叠出与写入当时相同的状态。conformance kit 为每个已发布 SchemaVersion 冻结 golden event stream 与对应的状态字节，任何 Evolve 实现变更都必须通过全部历史版本的 golden 校验。

### 5.6 Runtime 不拥有 Planner

agent 只声明供 Loop 依赖注入的 `RequestPlanner` port；Planner 的实现和语义属于
application/Memoh。Runtime 不调用这个 port，也不读取 Planner 的 context：

```go
type RequestPlanner interface {
    Plan(context.Context, PlanningHint) (RequestPlan, error)
}

type RequestPlan struct {
    Model          ModelRef
    Request        sdk.Request
    InputIDs       []InputID
    Tools          []ToolSpec
    PlanningToken  PlanningToken // application-owned freshness token
}

type PlanningHint struct {
    RunID       RunID
    Model       ModelRef
    SourceStep  StepID
    Inputs      []AgentInput
}
```

Planner 可以读取 application 自己的 history、memory、workspace 和 queue-safe 输入，但不直接修改 Runtime。它返回 `RequestPlan`（完整 `sdk.Request`、ModelRef、已消费的 InputID 集合和 application-owned `PlanningToken`）；Loop 随后提交 `PrepareModelRequest`。Memoh 在事务外构造请求，在提交时用自己的 context revision/CAS 和 create-if-absent 确保并发 Planner 只冻结一份结果。`PlanningToken` 只是供 adapter 验证 planner 输入是否新鲜的 opaque token；agent 不解释其内容，也不把它当成 authority revision。

Planner 所需的 history 必须由宿主提供：Memoh 从 durable history projection 读取；in-process
调用者可以用一个简单的内存 history projection 或 planner 自己持有的会话上下文。Runtime
不负责把 AgentEvent 推送给 Planner，也不因此新增 history/store 方法。

`PrepareModelRequest` 必须使用 Planner 开始前 Load 得到的 BaseRevision，并按当前顺序携带完整的 `PendingInputs` `InputIDs` 集合。相同 RunID 和 Revision 的并发 Planner 使用同一个 `DeriveModelRequestCommandID`：相同请求得到 `CommitAlreadyApplied`，不同请求得到 `ErrCommandConflict`；如果 Planner 在新的 Revision 上重试，则生成新的 CommandID。Memoh adapter 同时检查 `PlanningToken` 的 context revision，后到者不能覆盖已经冻结的请求。

Loop 校验 `RequestPlan.Model` 与 `PlanningHint.Model`、RunConfig.Model 一致；Runtime 通过共享 Decide 规则再次校验 `PrepareModelRequest.Model` 与冻结的 RunConfig 一致。Planner 的 context 一致性由 Memoh 在自己的 queue-safe admission/planning 边界保证，不由 agent 解释。

### 5.7 外部 response

agent 不提供第三个 Loop 操作。Memoh/application 的 response ingress：

1. 验证用户权限、Run/Step/Call 身份和 payload。
2. 读取 authority snapshot，确认目标 Call 仍为 Waiting；这个 ingress 读取不取得 Loop 的执行租约。
3. 用 `ResponseRequest.RunID/StepID/CallID/ID` 路由响应。将 approval 转成
   `ApproveToolCall`/`RejectToolCall`，将外部结果转成 `SubmitToolResponse`；
   `ResponseRequest.RequestDigest` 是原请求的摘要，用户决定或答案的摘要单独放在
   command 的 `ResponseDigest`，并用 `DeriveResponseCommandID` 生成稳定 CommandID。
4. 通过同一个 `Runtime.Commit` 提交；重复响应按 AlreadyApplied/Conflict 处理。

响应提交后，若有 Pending Call 或 ToolStep 已可自动关闭，Memoh 写入幂等 wake/outbox；其他 Waiting Call 保持原状态。响应入口不执行工具，也不调用模型。

如果同一 ToolStep 还有 Executing Call，响应提交仍写入自己的 wake；执行 worker 的完成提交或 recovery 也必须写 wake。下一次 Loop 从 `Load` 重新判断两类事实，不要求把它们合并成一个等待状态。

### 5.8 queue 边界

steer/follow-up 的数据结构和仲裁属于 Memoh：

```text
ModelStep 完成且没有 tool calls
ToolStep 自动关闭
```

只有在这些 boundary，Memoh 才能把 queue 输入绑定到下一次 Planner 请求或创建后续 Run。ToolStep 中间的新输入不能修改已经冻结的 ModelStep，也不能跳过 Pending/Waiting Call。

steer 必须进入下一个 ModelStep 的规划上下文，而不是延迟到更晚的边界。Loop 在 boundary 处的 Plan/Prepare 提交与 Memoh 的 queue 仲裁存在竞态；MemohRuntime adapter 用以下 gate 消除它：处理 `PrepareModelRequest` 的同一事务内检查是否存在 eligible 的 steer item，存在时不接受该次 Prepare，先在事务内应用对应的 `AcceptInput`（Revision 递增，Prepare 按 `ErrStaleRuntime` 返回）。Loop 重新 Load 后由 `PlanningHint.Inputs` 携带该输入重新规划。这条 gate 是 adapter 行为，不进入 agent Machine 规则；in-process 宿主没有 queue，输入由宿主在 Loop 空闲边界提交。

## 6. Loop 算法

### 6.1 Loop 结构

```go
type Loop struct {
    Models    ModelCatalog
    Tools     ToolCatalog
    Planner   RequestPlanner
    Execution ExecutionPolicy
    Streaming bool
}

func (l *Loop) Run(context.Context, Runtime, EventSink) (LoopResult, error)
```

Loop 是当前进程的解释器。它不复制权威状态，不直接访问数据库或 Memoh queue。

Loop 使用一个用于读取/提交的 `controlCtx`，并为每个模型或工具 worker 派生独立的执行 context。`controlCtx` 由宿主提供，或由 `context.WithoutCancel` 再加一个有限 deadline 得到；它不能因为单个 worker 取消而失效。worker 被取消后，Loop 仍必须用未被 worker 取消的 control context 提交已知结果、Unknown 或 model recovery。整个进程退出时，MemoryRuntime 的内存状态当然不会保留。

两类 worker 的执行 context 派生规则不同。模型 worker 的执行 context 从外层 ctx 派生：取消模型调用是安全的，未接受结果时以 `RecoverModelExecution` 释放，同一冻结 request 之后重试。工具 worker 的执行 context 不随外层 ctx 取消（由 `context.WithoutCancel(ctx)` 加每次执行的 deadline 派生）：工具一旦 start 就运行到自身结束，外层 ctx 取消不会把本可正常完成的工具打断成 Unknown。工具 worker 只在两种情况下被主动取消：Run 已进入 terminal（其迟到结果按 terminal/stale 审计），或同组另一个 worker 报告 Unknown。

### 6.2 主算法

```text
Loop.Run(ctx, runtime, events):
  outer:
  for:
    if ctx is cancelled:
      return LoopResult{}, ctx.Err()   // 已 start 的本地 worker 先按 6.1 的派生规则收束
    snapshot, err := runtime.Load(controlCtx)
    if err != nil:
      return err
    if snapshot.State.Status is terminal:
      return LoopFinished(snapshot.State.Result)

    effect, err := Machine.Next(snapshot.State)
    if err != nil:
      return err
    switch effect:
      case NeedModelRequest(hint):
        plan, err := l.Planner.Plan(ctx, hint)
        if err != nil:
          return err
        requestDigest, err := DigestRequest(plan.Request)
        if err != nil:
          return err
        toolsDigest, err := DigestToolSpecs(plan.Tools)
        if err != nil:
          return err
        bindingDigest, err := DigestModelStepBinding(plan.Model, requestDigest, toolsDigest)
        if err != nil:
          return err
        commandID := DeriveModelRequestCommandID(snapshot.State.RunID, snapshot.Revision)
        stepID := DeriveModelStepID(snapshot.State.RunID, commandID, bindingDigest)
        prepared, err := commit(
          PrepareModelRequest{
            StepID: stepID, Model: plan.Model, Request: plan.Request,
            RequestDigest: requestDigest, InputIDs: plan.InputIDs,
            PlanningToken: plan.PlanningToken, Tools: plan.Tools,
            ToolsDigest: toolsDigest},
          commandID=commandID, baseRevision=snapshot.Revision, grant=zero)
        if err == nil and (prepared is Accepted or AlreadyApplied):
          continue outer loop
        if err == ErrCommandConflict or err == ErrStaleRuntime:
          continue outer loop
        if err != nil:
          return err

      case StartModelCall(stepID):
        startID := freshCommandIDForThisCommand()
        start, err := commit(StartModelExecution{StepID: stepID}, commandID=startID,
          baseRevision=snapshot.Revision, grant=zero)
        if err == ErrStaleRuntime or err == ErrRunTerminal:
          continue outer loop
        if err != nil:
          return err
        if start is AlreadyApplied:
          continue outer loop
        modelStep := start.Snapshot.State.Current.(ModelStep)
        invoker, err := l.Models.Resolve(modelStep.Model)
        var completion AgentCommand
        completionID := freshCommandIDForThisCommand()
        if err != nil:
          completion = SubmitModelFailure{StepID: stepID, Failure: StepFailureForModel(err)}
        else:
          modelResult, invokeErr := invokeModel(invoker, workerCtx, modelStep.Request, l.Streaming, events)
          if invokeErr != nil and worker context was cancelled:
            completion = RecoverModelExecution{StepID: stepID}
          else if invokeErr != nil:
            completion = SubmitModelFailure{StepID: stepID, Failure: StepFailureForModel(invokeErr)}
          else:
            bindings, bindErr := bindToolCalls(modelResult, modelStep.Request, modelStep.Tools)
            if bindErr != nil:
              completion = RejectModelResult{StepID: stepID, Usage: modelResult.Usage,
                Failure: StepFailure{Class: FailureMalformedModel, Message: bindErr.Error()}}
            else:
              completion = SubmitModelResult{StepID: stepID, Result: modelResult, Calls: bindings}
        applied, err := commit(completion, commandID=completionID,
          baseRevision=start.Snapshot.Revision, grant=start.Grant)
        if err == ErrStaleRuntime or err == ErrRunTerminal:
          continue outer loop
        if err != nil:
          return err
        if applied is Accepted or AlreadyApplied:
          continue outer loop
        continue outer loop

      case StartToolCalls(stepID, callIDs):
        startedWorkers := []
        for each call in selectByPolicy(callIDs, l.Execution):
          tool, resolveErr := l.Tools.Resolve(call.ToolRef)
          argErr := nil
          policyErr := nil
          definitionErr := nil
          if resolveErr == nil:
            if tool.Ref() != call.ToolRef or DigestToolDefinition(tool.Definition()) != call.DefinitionDigest:
              definitionErr = definitionMismatch
            argErr = tool.ValidateArguments(call.Arguments)
            if tool.ResponsePolicy() != call.Policy:
              policyErr = definitionMismatch
          }
          if resolveErr != nil or argErr != nil or policyErr != nil or definitionErr != nil:
            failed, err := commit(SubmitToolFailure{StepID: stepID, CallID: call.CallID,
              Failure: ToolFailureFor(resolveErr, argErr, policyErr, definitionErr), Outcome: ToolOutcomeKnown},
              commandID=freshCommandIDForThisCommand(), baseRevision=snapshot.Revision, grant=zero)
            if err == ErrStaleRuntime or err == ErrRunTerminal:
              launchAndSettle(startedWorkers)
              continue outer loop
            if err != nil:
              return err
            if failed is Accepted or AlreadyApplied:
              continue with next call
            continue with next call
          startID := freshCommandIDForThisCommand()
          start, err := commit(StartToolCall{StepID: stepID, CallID: call.CallID}, commandID=startID,
            baseRevision=snapshot.Revision, grant=zero)
          if start is Accepted:
            worker := launch the resolved `l.Tools` ExecutableTool for callID,
              retaining start.Grant for completion
            startedWorkers.append(worker)
          else if err == ErrStaleRuntime or err == ErrRunTerminal:
            launchAndSettle(startedWorkers)
            continue outer loop
          else if err != nil:
            return err
          else if start is AlreadyApplied:
            continue with next call without launching it
        launch all accepted workers, then wait for every worker started by this Loop branch.
        Workers already in Executing state belong to another live or recoverable
        attempt; this branch neither launches nor waits on them.
        As each worker returns, immediately commit its SubmitToolResult or SubmitToolFailure,
        using that Call's start grant; a stale BaseRevision is eligible for Call-local rebase.
        If one worker reports Unknown, cancel the other workers and do not commit their late results.
        After all workers started by this branch have settled or been cancelled,
        continue outer loop; a subsequent Load decides whether the remaining
        Executing calls require recovery or whether more Pending calls can start.

      case WaitForResponse(requests):
        return LoopWaiting(requests)

      case WaitForExecutionRecovery:
        return LoopWaiting(ExecutionRecovery)

    // Every accepted event is followed by Load; Loop does not keep a
    // second authoritative MachineState.
```

外层 ctx 取消不会让 Loop 立即返回：模型 worker 随之取消并以 `RecoverModelExecution` 释放，工具 worker 按 6.1 的规则运行到自身结束并提交结果，随后循环顶部的 ctx 检查返回 `ctx.Err()`。取消只结束本次 Loop 执行，不改变 Run 状态。

`commit` 为每个 AgentCommand 生成一次 CommandID；如果提交响应丢失，使用同一个 CommandID 和 digest 重放。`CommitAlreadyApplied` 表示对应事件组已经落地，不能再次执行相应外部 effect；Loop 重新 Load 后根据权威状态决定下一动作。

伪代码中的 `commit(command, commandID, baseRevision, grant)` 是一个小型 Loop helper：它补齐 CommandEnvelope 的 Type、SchemaVersion 和 `DigestCommand`，然后调用唯一的 `Runtime.Commit`。构造 CommandEnvelope 与派生 ID 只能通过 agent 提供的 typed 构造函数与 helper；任何代码不得手工拼装信封字段。

`launchAndSettle(startedWorkers)` 是伪代码 helper：它等待已经接受 start 的 worker，并为每个 worker 提交完成/已知失败；worker 无法确定结果时提交 Unknown。它不能简单丢弃 worker 或 grant。

`freshCommandIDForThisCommand()` 为一次具体的 AgentCommand 生成新 ID，并在该提交重试范围内保存；同一 execution attempt 的 start、completion 和 recovery 是不同 command，因此各自拥有不同 ID。

一旦某个 `StartToolCall` 已被接受，当前分支必须启动并收束该 worker，或在无法启动时提交对应的已知失败/恢复结果；不能因为后续 Call 的 start 返回 stale/terminal 就遗弃已经返回的 grant。伪代码中的 `continue outer` 只有在先处理完本分支已接受的 start（或明确取消并提交其结果）后才允许执行。

`ToolFailureFor` 将 resolve error 映射为 `tool_lookup_failed`，参数校验 error 映射为 `invalid_arguments`，tool ref、response policy 或 definition digest 不匹配映射为 `tool_definition_mismatch`。

`invokeModel` 是说明性 Loop helper：开启 streaming 且 invoker 支持 `StreamingModelInvoker` 时消费 stream
并向 EventSink 发送 delta，否则调用 `Generate`；两条路径都只返回一个完整 `sdk.ModelResult`。

所有 completion commit 都遵循同一错误处理：`ErrStaleRuntime`/`ErrRunTerminal` 只触发重新 `Load` 并丢弃迟到结果，其他错误返回给调用方；只有 `CommitAccepted` 或相同 command 的 `CommitAlreadyApplied` 才表示该事实已被 authority 接受。接受后 Loop 可以把 `CommitResult.Events` 逐个包装为 `Event{Kind: EventAgentCommitted, Durability: EventCommitted, Canonical: &e}` 发送给 EventSink；发送失败不回滚提交。

### 6.3 模型调用

```text
NeedModelRequest
  -> Planner.Plan
  -> Commit(PrepareModelRequest)
  -> Commit(StartModelExecution)
  -> sdk.Generate 或 sdk.Stream
  -> Commit(SubmitModelResult、SubmitModelFailure 或 RejectModelResult)
```

Loop 的 `Streaming` 选项决定是否优先使用可选的 `StreamingModelInvoker`；未开启或没有该实现时使用 `Generate`。stream 中的文本 delta 可以实时发给 EventSink；只有完整 `ModelResult` 的完成提交会改变 MachineState。模型请求失败时由 Machine 的 Decide 决定是否将 Run 置为 RunFailed；provider transport retry 不穿透到 Machine。

### 6.4 工具调用

```text
ToolStep.Pending
  -> Commit(StartToolCall)
  -> ExecutableTool.Execute
  -> Commit(SubmitToolResult / SubmitToolFailure)
```

Loop 先解析 ToolRef、校验参数、工具定义 digest、response policy 和 binding digest，再提交 start command。多个独立 Pending Call 可以按 Loop 的 `ExecutionPolicy` 并行；每个 execution attempt 有自己的 CommandID 和 ExecutionGrant。如果解析或参数校验已经失败，直接提交 Pending Call 的已知 `SubmitToolFailure`，不经过 start barrier，也不调用外部工具。

如果 start command 已经成功但响应丢失，Loop 不能把 `AlreadyApplied` 当作新的执行授权；它必须重新 Load。旧执行仍被确认拥有时，新的 Loop 返回 `LoopWaiting(ExecutionRecovery)`；执行权失效后由 Memoh recovery 处理为 Unknown。这样即使崩溃发生在 start barrier 与真正调用之间，也不会无凭据地重复执行工具；代价是该 Call 按 Unknown 终止当前 Run。

### 6.5 等待与并行

```text
Pending + Waiting
  -> 先执行 Pending，Waiting 不阻塞

只有 Waiting，没有 Pending
  -> LoopWaiting(WaitingForResponse)
  -> 若同时有 Executing，response 和 execution recovery 都必须唤醒下一次 Loop

只有 Executing
  -> 仍有有效 owner/lease 时等待当前 worker；执行权失效后等待 Runtime recovery

全部 Call terminal
  -> Machine 自动关闭 ToolStep
```

不同 Call 的 response 可以并行到达。一个 response 不会取消或轮换其他 Call 的执行授权。每次重新唤醒都从 Runtime.Load 开始。

一个 Loop 在自己成功 start 的 worker 尚未结束时不会返回 `LoopWaiting`；它会等待该 worker
提交完成/失败，或提交 recovery。只有加载同一 Run 的后续 Loop，才可能看到
`Waiting + Executing` 并返回 `WaitingForResponse`；原 worker 的完成事件或 Runtime recovery
通过 Memoh outbox、scanner，或 in-process 的宿主唤醒下一次 Loop。LoopResult 不携带 worker
句柄，也不要求新 Loop 接管旧 Attempt。

### 6.6 错误、取消和 limit

| 情况 | 处理 |
| --- | --- |
| provider transport/rate limit | sdk 在一次模型调用内处理；最终失败由 Machine 记录。 |
| 结构性 malformed 模型结果 | 提交 `RejectModelResult`，Step 回到 Prepared 重试；超过 `ModelRejectLimit` 后 RunFailed。 |
| tool lookup/参数错误 | 提交已知 `SubmitToolFailure`，交给下一次模型请求。 |
| 工具明确失败 | 提交已知 `SubmitToolFailure`，模型决定是否在新 Step 重试。 |
| 工具结果未知 | 提交 Unknown，RunFailed；不自动重试，不创建下一 ModelStep。 |
| 外层 ctx 在工具 start 前取消 | 不再 start 新 Call；已 start 的 worker 运行到结束并提交，Loop 随后返回 `ctx.Err()`。 |
| 外层 ctx 在模型执行中取消 | 模型 worker 取消，提交 `RecoverModelExecution`，Loop 返回 `ctx.Err()`。 |
| 进程崩溃留下 Executing Call | Runtime recovery 按 §8.2 收束为 Unknown。 |
| application CancelRun | 宿主必须先提交 `CancelRun`（Run 置为 RunStopped），再取消 Loop 的 ctx；顺序颠倒会让执行中的工具收束为 Unknown，把用户停止错记为 `RunFailed(effect_unknown)`。 |
| stale Revision/grant | 丢弃本地结果，重新 Load；不重放旧 Step 的外部 effect。 |
| Commit 响应未知 | 同一 CommandID/digest 重放一次；仍未知则结束当前 Loop，后续 Load 读取 authority。 |

计划内停机（部署、滚动升级）不走崩溃路径：宿主收到停止信号后取消 Loop 的外层 ctx，按上表语义排空——不再 start 新 Call，已 start 的工具 worker 在停机 grace 内提交完成/失败，模型执行以 recovery 释放。grace 内未能结束的工具执行才留给 lease recovery 收束为 Unknown。

Run 创建时冻结 `RunConfig.ModelStepLimit` 和 `RunConfig.ModelRejectLimit`；Loop 不持有第二份计数器。`ModelStepLimit` 的 `0` 表示无限；达到正数上限后不创建新的 ModelStep，已打开 ToolStep 仍完成或等待，随后由 Machine 结束 Run。`ModelRejectLimit` 的 `0` 在 `Initialize` 时归一化为默认值 `2`，负值无效；不提供无限值——结构性 malformed 的无限重试是无上界的成本。

## 7. Tool、approval、response 和 MCP

### 7.1 Tool contract

`sdk.ToolDefinition` 只描述 provider 可发现的 schema，不依赖 agent，也不携带 `ResponsePolicy`。`agent.ExecutableTool` 描述应用如何执行工具并提供 response policy；模型返回后，agent 用 `ToolRef`、definition digest 和 policy 生成冻结的 `ToolCallBinding`。恢复时 schema、工具版本或 policy 不匹配都不能静默换版本。

```go
type ExecutableTool interface {
    Ref() ToolRef
    Definition() sdk.ToolDefinition
    ResponsePolicy() ResponsePolicy
    ValidateArguments(json.RawMessage) error
    Execute(context.Context, ToolExecutionRequest) ToolExecutionOutcome
}
```

`ResponsePolicy` 决定工具是直接执行、需要 approval，还是等待外部 response；`ValidateArguments` 在 start barrier 前运行并且不能产生外部 effect。工具定义及其 digest 同时承担工具版本绑定。工具进度通过 `ToolProgressSink` 进入 EventSink，只是实时观察。需要影响恢复的状态必须提交 AgentCommand；工具内部的瞬时观察仍只发送 EventSink provisional event。

Loop 为每个执行 worker 创建绑定了 RunID/StepID/CallID 的 `ToolProgressSink`；其 `Publish` 只是向同一个 `EventSink` 发出 provisional `EventToolProgress`，不调用 Runtime，也不产生 AgentCommand。

### 7.2 approval 和 ask-user

两者都是 ToolCall 的 response policy，不是新的 Step 类型：

```text
approval required -> Waiting(Approval)
ask_user         -> Waiting(ExternalResponse)
```

批准后仍要执行实际工具；拒绝产生已知 permission-denied 结果；ask-user 的答案直接完成对应 Call。多个 Waiting Call 同时存在时，每个响应按 CallID 独立推进。

### 7.3 MCP

agent 提供 MCP schema/call adapter，把 MCP tool 转换为 `sdk.ToolDefinition` 和 `ExecutableTool`。MCP server 的连接、认证、生命周期和产品权限由 Memoh/application 管理。迁移期可以保留旧 `sdk.MCPClient` wrapper；新 Loop 不依赖 SDK 的 MCP session。

## 8. MemoryRuntime 与 MemohRuntime

### 8.1 MemoryRuntime

MemoryRuntime 使用 `mutex + MachineState + AgentEvent map`：

```text
Load
  在锁内返回当前 MachineState 和 Revision

Commit
  在锁内判定 grant 有效性，调用共享 EvaluateCommit 并保存结果；
  接受 start command 时返回本进程的 opaque ExecutionGrant
```

MemoryRuntime 不需要 owner、fence、lease、outbox 或 Attempt 表。进程退出后状态可以丢失；它是本地会话、测试和 conformance reference。它仍须在同一把锁内记录每个已接受 start 的执行占用，防止两个本地 worker 同时执行同一个 Call。

MemoryRuntime 只保存 agent 的 MachineState、AgentEvent 和提交幂等记录，不自动保存产品 history。需要多轮上下文的 in-process
宿主应在 Runtime 外维护一个内存 history projection，并让自己的 RequestPlanner 读取它；这只是一个轻量的应用层配套，不是 MemoryRuntime 为 durable 语义模拟数据库。

worker context 的取消不等同于业务取消。若已提交 `StartToolCall` 后 worker 返回 Unknown，Loop 使用不受 worker cancellation 影响的 commit context 提交 `SubmitToolFailure{Outcome: ToolOutcomeUnknown}`；MemoryRuntime 在同一把锁内记录 Unknown 并结束 Run。若当前进程直接退出，MemoryRuntime 不承诺跨进程恢复；仍处于 Executing 的 Call 随内存状态一起丢失。

ModelStep 的执行授权被取消时，Loop 使用仍有效的 model grant 提交 `RecoverModelExecution`；MemoryRuntime 在同一把锁下把它恢复为 Prepared，下次 Loop 仍使用同一冻结 request。若进程在提交前退出，MemoryRuntime 随内存状态丢失，不承诺跨进程恢复。

### 8.2 MemohRuntime

MemohRuntime 用 PostgreSQL transaction/CAS 加上 Memoh 私有的 owner/fence/lease：

```text
Load
  只读取 canonical history、MachineState、ToolStep progress 和 session facts
  不创建 Attempt，不取得 lease；这些控制记录不出现在 snapshot

Commit
  判定内部 owner/fence/lease 的有效性
  对 StartModelExecution/StartToolCall 在同一事务内建立 Attempt/lease
  调用共享 EvaluateCommit（command identity、Revision、Decide/Evolve）
  原子保存 MachineState、AgentEvent 组、history projection、queue action、response record 和 outbox
```

并行 Call 的完成 transition 按提交先后获得递增的 Revision；这不改变模型上下文中的 Call 顺序。Memoh 先按 CallID 保存各自结果，ToolStep 关闭时再按 ModelResult 原始 Call 顺序写入 assistant tool-call 与 tool-result history。

Attempt、owner、fence、lease 和数据库 row 不进入 agent public state。它们只保证多个 Loop attempt 不会同时取得同一个 Call 的执行权。持久化的 MachineState snapshot 必须带 adapter 自己的 snapshot schema version；跨版本升级时按该版本解码或迁移，不复用事件的 `SchemaVersion` 字段。

MemohRuntime 的 worker 实例在构造时绑定当前 worker 的 owner identity；只有该实例可以提交自己接受的 model/tool start、completion 和主动 recovery。response 和 cancel 使用同一个 `Commit` 语义，但由 Memoh 创建不带 worker grant 的 ingress-scoped adapter；这些 command 不取得执行权，因此不需要伪造 Loop owner。新 Run admission 仍由 Memoh 的 admission 事务处理，不调用旧 Run 的 `Runtime.Commit`。

续租失败不是立即的工具失败。Memoh 先使用 backend-loss/recovery grace 判断旧 owner 是否已经失效；确认 lease 和 grace 都失效后，scanner 在事务中检查该 Executing Call 是否已有已接受结果：

```text
已有结果 -> 只释放旧 lease
没有结果 -> 生成 `ToolCallFailed{Outcome: ToolOutcomeUnknown, Failure: effect_unknown}` 并提交，RunFailed
```

scanner 使用由 StepID、CallID 和 system namespace 稳定生成的 CommandID，并遵守与普通 Commit 相同的 digest/idempotency。它只在自己的 recovery record 证明 lease 已失效后通过共享 `EvaluateCommit` 提交 Unknown；普通 Commit 调用者不能伪造这条 system command。Model recovery 也必须通过共享 Decide/Evolve 规则，而不能直接改写 Step 状态。scanner 不查询外部系统，也不重新执行工具。Pending Call 不走 Unknown 路径，可以由新 Loop 继续。

ModelStep 的 recovery 规则不同：模型请求没有工具那样的外部业务 effect。ModelStep 的执行 lease 失效后，Memoh 可以在事务中把它从 Executing 重置为 Prepared；新的 Loop 使用同一冻结 `sdk.Request` 重试，不创建新的 Step。只有已经接受的 `SubmitModelResult` 才能关闭该 ModelStep。

当 ToolStep 自动关闭且 Run 允许继续时，Memoh 先提交 history、queue action 和 context revision；下一次 Loop 通过 Planner 构造并提交下一份 `sdk.Request`。已经冻结的请求不会被后来的输入改变。

### 8.3 外部 effect 的保证

```text
AgentCommand commit -> AgentEvent 组
  effectively-once（identity + digest）

Model call
  lease 失效后可使用同一冻结 request 重复；不同 attempt 可能返回不同结果，只有先被
  Runtime 接受的结果推进 Run

Tool effect
  start command 提交后才发生，但结果可能在提交前丢失
```

通用 core 无法判断 Unknown effect 是否已经发生，也不假设支付系统或其他外部系统提供查询接口。因此 Unknown 保守地终止当前 Run；如果产品需要继续，只能创建新的 Run。非幂等工具不能由 Runtime 获得 exactly-once 保证。

## 9. Events 和 context

### 9.1 Canonical Event Plane

状态变化的提交路径固定为：

```text
Loop / response ingress
  -> AgentCommand
  -> Runtime.Commit (EvaluateCommit: 幂等/类别校验 + Machine.Decide + Machine.Evolve)
  -> MachineState + AgentEvent 组 in one atomic boundary
  -> EventSink / replay / projection / OTel
```

`AgentCommand` 表示“希望发生的状态变化”；`AgentEvent` 表示“authority 已接受并持久化的事实”。一个接受的 command 构成一次 transition，产出一个或多个事实。AgentEvent 必须具备：

```text
RunID + (Revision, Index)      全序身份；Revision 是 transition 计数，Index 是组内序
CommandID                      产生这次 transition 的 command
Digest                         该事实内容的 canonical digest
SchemaVersion + Type           wire 兼容和 sealed fact discriminator
Fact                           已接受的事实内容
```

MachineState 与 AgentEvent log 是同一个 transition 序列的两个 materialization（§5.1）：MachineState 是 execution authority，AgentEvent log 是 historical authority，由同一原子提交产生、共享 Revision。Runtime 必须把两者和需要一致的 Memoh projection/outbox 放在同一事务或锁边界。Durable adapter 必须保留 AgentEvent，使其可以按 RunID/(Revision, Index) replay；MemoryRuntime 可以只在进程内保留同样的记录。公共 `Runtime` 不增加 replay 方法，读取由实现或 application projection 提供。

Replay 按 RunID/(Revision, Index) 取出 AgentEvent，从初始状态（Revision=0）开始依次调用同一份 `Machine.Evolve` 折叠。折叠只依赖 Evolve，不重新运行 Decide——决策结果已经记录在事实里，Machine 决策规则的演进不影响历史事件的折叠。校验规则：折叠到 Revision N 的状态必须与该 Revision 的持久化状态一致；Revision、Index、identity 或 digest 不匹配时停止 replay 并报告 halt 级一致性违规，不静默修复状态，也不定义任何一方自动覆盖另一方。分歧后的恢复是人工按事件流重建并核对的 runbook 操作，不是自动路径。

Replay 的起点是 admission 已建立的初始 `MachineState`；`RunSeed` 的 admission 记录不作为任何 Run 的 AgentEvent 重放。需要重建 admission 链时，由 Memoh 的 session/queue 记录负责。

canonical event 只记录影响语义状态、恢复和审计的已接受事实：Step 的建立/启动/恢复/关闭、模型结果的接受与拒绝、工具结果、响应、active Run 的输入接受和 Run terminal（§3.6 的 Fact 词表）。新 Run 的 `RunSeed` 仍属于 admission record。模型文本 delta、工具 stdout、下载百分比和其他瞬时 progress 不进入 AgentEvent；它们仍可在提交前通过 EventSink 发送 provisional observation。

### 9.2 EventSink

EventSink 是实时观察出口，不是 canonical source。Loop 在收到 `CommitResult.Events` 后可以发送对应的 committed observation，也可以发送不改变权威状态的 provisional observation：

```text
ModelTextDelta、ToolProgress
  可以在完成提交前发送，都是 provisional

ToolStarted
  只能在 start command Accepted 后发送

ToolCompleted、Run terminal
  先由 Runtime Commit；Loop 随后发送观察事件，Memoh 的 durable projection/outbox
  由同一 Commit 事务保存对应事实
```

EventSink 丢失、重复或来自旧 Attempt 都不改变 MachineState 或 AgentEvent。客户端出现 gap 时从 durable AgentEvent、snapshot 或最终 `RunResult` 重建。Loop 默认忽略观察通道错误，不因此重试模型/工具。

并行工具的观察事件通过 `CallID` 关联到具体 ToolCall；模型事件的 `CallID` 为空。
`Event.Sequence` 只在一次观察流内单调递增，不是 `AgentEvent.Revision`，也不参加 canonical digest/idempotency。`Durability` 仅说明这条观察是否对应已提交事实；它不能替代 AgentEvent 的 identity/digest。

### 9.3 context transform

Request Planner 属于 Memoh/application。它可以在事务外读取 context，但 MemohRuntime adapter
必须以版本检查和 create-if-absent 冻结生成的 request。已冻结 ModelStep 不受后来输入影响。

这里的版本检查由 MemohRuntime 的 adapter 实现：agent.Runtime 只看到带有 `BaseRevision`
的 `PrepareModelRequest`，不理解 Memoh 的 context revision，也不读取 queue 或 history。

### 9.4 provider transport

一次 `sdk.Generate` 或 `sdk.Stream` 对应一次逻辑 provider request。transport retry 在 sdk/provider client 内部发生；agent 不记录它，也不为它创建新的 Step。

## 10. Memoh queue、session 和恢复

### 10.1 queue 归属

steer/follow-up 的 queue 数据结构、accepted order、重排、claim、apply、取消和 admission 全部属于 Memoh。两种 queue 可以共享稳定 item reference、accepted sequence、order version、取消状态和 claim provenance；消费策略不同。被选中的 item 在交给 core 前转换为只有 `InputID` 和 payload 的 `AgentInput`，Memoh 私下保留 item reference 与 claim provenance 的映射：

```text
steer      优先进入当前 eligible boundary；若当前 Run 已 terminal，则按 session policy 创建 continuation Run
follow-up  当前 Run 自然结束后创建新的 Run
```

active Run 的 steer 通过 `NextStep(input)` 生成 `AcceptInput` command；terminal Run 的 steer/follow-up 通过 `NextRun(input)` 生成 `RunSeed`，然后由 Memoh admission 创建新 Run。core 不接收 queue item、priority、order 或 claim。

重排必须带 order version；过期版本、未知 item、重复 item 和越过已 claim item 的操作都拒绝。

### 10.2 queue-safe boundary

Memoh 只在以下 boundary 仲裁 queue：

```text
ModelStep 完成且没有 tool calls
ToolStep 自动关闭
```

ModelStep 执行中、ToolStep 有 Pending/Executing/Waiting Call 时，不消费新的 queue 输入。queue action、对应 `InputAccepted` 事件以及 claim provenance 在 Memoh transaction 中一起提交，已 claim item 不能回退或越过。

没有 tool calls 的 ModelStep 会使当前 Run 到达 `RunCompleted`。Memoh 可以在这个 queue-safe boundary 消费 steer 并创建 continuation Run；这不改变已经完成的 Run，也不把 queue policy 放进 Machine。

### 10.3 R0/R1 与 session settled

Memoh 的 session 规则保持：

```text
R0 terminal 不等于 session settled
admission-active R1 存在时 session 仍 busy
```

follow-up 可以在 R1 的第一个 ModelStep 之前完成 durable admission claim；这是 Memoh session 操作，不是 Loop 中间读取 queue。R0 continuation 通过 Memoh outbox/scanner 唤醒；R1 admission 用 `NextRun(input)` 的 `RunSeed` 初始化新 Run，R1 identity 不进入 agent 的通用结果。

### 10.4 多 response 恢复

```text
ToolStep T1
  A Completed
  B Waiting(response=101)
  C Waiting(response=102)
  D Pending
```

response 101 只完成 B；D 仍可执行，不必等待 C。response 102 再完成 C；D 完成后 Machine 自动关闭 T1，并允许下一 ModelStep。每个 response 有自己的 row、CommandID 和 wake，不再受旧协议“一个 deferred 只能保存一个 approval”的限制。

## 11. 迁移现状与兼容策略

### 11.1 当前问题

当前 SDK 同时承载 provider API 和多步 loop：

```text
GenerateParams / GenerateResult
  混合请求、自动 tool loop、approval、steps、callbacks 和 max steps
```

Memoh native runtime 已拥有产品 context、queue 和 durable session，但旧 loop 让它看不到 ToolStep 内逐 Call 的 progress。新方案把多步执行统一到 `agent.Loop`，并要求 Memoh 增加 ToolCall progress/response projection。

本规范中的 `sdk.Request`、`sdk.ModelResult` 和 provider-neutral `sdk.ToolDefinition` 是迁移目标的闭合类型合同；当前 SDK 仍主要使用 `Tool`、`GenerateResult` 和 `StreamResult`。阶段 A 负责实现等价的新类型并保留显式 legacy wrapper，不能把目标类型误认为已经存在的兼容 API。

### 11.2 迁移目标

```text
旧调用者 -> legacy sdk wrapper（迁移期）
新调用者 -> agent.Loop + Runtime
Memoh    -> agent.Loop + MemohRuntime
```

生产环境最终只有一条多步执行路径：`agent.Loop`。

### 11.3 兼容原则

1. provider adapter 的现有请求/响应字段优先复用。
2. sdk 保留旧单次调用入口，直到调用方迁移完成。
3. 旧自动 loop 只在显式 legacy wrapper 中存在，不能由新 Loop 隐式调用。
4. 旧 `WithMaxSteps(0)` 保持一次模型调用、不自动执行 tools；`n>0` 映射为 `RunConfig.ModelStepLimit=n`；旧值 `-1` 由 legacy wrapper 先规范化为 `RunConfig.ModelStepLimit=0`（无限）。agent 的 `RunConfig` 不接受负数。Memoh 当前使用 `-1`，迁移后保持无限模型步骤语义。
5. 旧 deferred/approval 记录不能在线猜测为新的多 Call ToolStep。切换前必须排空，或以 `runtime_upgrade_required` 终态保留审计后再切换。
6. 新协议写入生产后不回滚旧 loop；Memoh 的 queue accepted order、重排、claim、admission、R0/R1 和 settled 保证不变。

## 12. 分阶段实施

### 阶段 A：sdk 单次调用边界

1. 固定 provider-neutral `Request`、`ModelResult`、stream parts 和 snapshot。
2. 让 Generate、Stream 各自对应一次 provider request；transport retry 留在 sdk。
3. 将旧自动 loop 隔离为 legacy wrapper。
4. 保留 blocking/streaming 等价测试。

### 阶段 B：Machine 和 MemoryRuntime

1. 实现 `MachineState`、ModelStep、ToolStep、ToolCall 状态和 Decide/Evolve/Next 规则。
2. 实现 `EvaluateCommit`、`Runtime.Load/Commit`、command/fact digest 与幂等和 opaque grant。
3. 实现 Loop 的 model/tool/approval/response 路径和并行执行策略。
4. 完成 MemoryRuntime conformance 测试（`agent/runtimetest`），并冻结 SchemaVersion 1 的 golden event stream。

### 阶段 C：Memoh storage groundwork

1. 增加 ToolCall progress、response set、event idempotency、history 和 outbox projection。
2. 保留 queue 的 accepted order、重排、claim、admission、R0/R1 和 settled 语义。
3. 为旧 deferred 数据建立排空/审计迁移窗口。

### 阶段 D：MemohRuntime adapter

1. 用 transaction/CAS 实现 `Load/Commit`。
2. 在 adapter 内加入 owner/fence/lease/Attempt 和 recovery scanner。
3. 实现逐 Call response、wake/outbox、unknown outcome 和 crash/fencing 测试。
4. 让 Memoh Request Planner 以 revision 冻结下一份 sdk.Request。

### 阶段 E：production cutover

1. 先以 shadow projection 验证 MachineState、history 和 queue action。
2. 排空或审计终结旧 deferred Run。
3. 将现有 Memoh NativeAgentLoop 保留为薄宿主 wrapper：由它组装 Request Planner、`agent.Loop` 和 `MemohRuntime`，但不再保留独立的多步执行算法。
4. 禁止旧 loop 写入新 Runtime projection，观察 recovery、duplicate commit 和多 response。

### 阶段 F：删除兼容残留

删除旧自动 loop、旧 approval/deferred 提交路径和只服务旧 loop 的 SDK 状态字段；保留仍有外部调用者使用的单次 provider API。

## 13. 并行工作边界

| 工作 | 负责方 | 依赖 |
| --- | --- | --- |
| sdk Request/ModelResult/stream | twilight-ai/sdk | provider adapter |
| Machine 规则和 Step 类型 | twilight-ai/agent | sdk types |
| Loop、工具 contract、EventSink | twilight-ai/agent | Machine |
| MemoryRuntime | twilight-ai/agent | Machine/Loop |
| ToolCall projection、history/outbox | Memoh | event contract |
| queue、session、R0/R1、owner/fence | Memoh | existing session spec |
| Request Planner/context | Memoh/application | sdk.Request |
| MemohRuntime | Memoh | Runtime contract + projections |

## 14. 测试矩阵

### 14.1 sdk

```text
Generate/Stream 都返回一次完整 ModelResult
stream finish、EOF、中断和 malformed part
provider retry 不改变一次调用语义
旧 wrapper 的单次调用兼容
```

### 14.2 Machine/Loop

```text
无当前 Step -> NeedModelRequest
NextStep(input) -> AcceptInput -> InputAccepted -> pending input appears in PlanningHint
NextRun(input) -> RunSeed，admission initializes new Run, without mutating old Run
PrepareModelRequest -> [ModelStepPrepared] -> ModelStep
ModelExecuting lease recovery -> same frozen ModelStep can start again
SubmitModelResult 有 tools -> [ModelStepCompleted, ToolStepOpened]，保存完整 Call set
SubmitModelResult 无 tools -> [ModelStepCompleted, RunEnded{RunCompleted}]，并返回该 ModelResult
结构性 malformed -> RejectModelResult -> [ModelStepRejected]，Step 回到 Prepared，usage 已累计
Rejects 超过 ModelRejectLimit -> [ModelStepRejected, RunEnded{RunFailed, malformed_model_result}]
参数无法解析的单个 Call -> Pending，start 前以 invalid_arguments 已知失败关闭
unknown ToolRef/invalid arguments -> Pending Call 的已知失败，不提交 start
approval approved -> [ToolCallApproved] -> Pending -> start -> tool execute
approval rejected -> [ToolCallFailed{Known, permission_denied}]
多个 Pending 并行；Waiting 不阻塞其他 Pending
Waiting 与 Executing 并存时，response 和 execution wake 都有效
response 只推进对应 Waiting Call
Waiting result carries RunID/StepID/CallID/ResponseID for response routing
最后一个 Call terminal -> 同一 transition 追加 ToolStepClosed
最后一个 Call terminal 且触发 step limit -> [..., ToolStepClosed, RunEnded{RunStopped, step_limit}]
RunEnded 只能是事实序列的最后一个事实
已知失败进入下一次模型上下文
Unknown -> [ToolCallFailed{Unknown}, RunEnded{RunFailed, effect_unknown}]，不创建下一 ModelStep
Pending + Executing 并存时不误关 ToolStep
RunStopped 与 worker cancellation 区分
外层 ctx 取消：模型执行以 ModelStepRecovered 释放，工具 worker 运行到结束后 Loop 返回 ctx.Err()
MachineState.Usage 逐字段累计 ModelStepCompleted 与 ModelStepRejected；terminal 时复制到 RunResult.Usage
Decide 拒绝时不产出部分事实；接受时事实组与 MachineState 原子提交
Evolve 不读 RunConfig、无 policy 分支；对 Decide 产出的全部事实全定义
AgentEvent 按 RunID/(Revision, Index) 可 replay，重复提交不产生第二组
replay 只经 Evolve 折叠，不重新运行 Decide；golden event stream 折叠出冻结的状态字节
EventSink provisional/committed 发射点
并行 EventSink 事件包含 CallID，Waiting result 可路由到目标 Call
Streaming=true 但 invoker 不支持 streaming -> Generate fallback
```

### 14.3 Runtime conformance

conformance 测试由 agent 以可运行测试包（`agent/runtimetest`）交付；MemoryRuntime 与 MemohRuntime 直接运行同一套件，不各自转写矩阵：

```text
same CommandID + digest -> CommitAlreadyApplied + 原事件组
same CommandID + different digest -> ErrCommandConflict
AlreadyApplied 不重新运行 Decide；事件组逐字节等于首次提交
并发 Commit 的 Revision/CAS 行为
同一 Run/Revision 的并发 Planner：相同请求 AlreadyApplied，不同请求 CommandConflict
Planner InputIDs 必须完整匹配 PendingInputs；Tools/ToolsDigest 与 sdk.Request 一一对应
并行 Call 的 start/response 在旧 Revision 上按目标 Call rebase
Pending Call 的 lookup/argument failure 在旧 Revision 上按目标 Call rebase
工具 ref/definition digest 变化 -> tool_definition_mismatch，且不调用工具
ToolCallState 非法字段组合 -> Runtime.Load/Evolve 拒绝
ToolCallState 的 BindingDigest 与 frozen ToolSpec 不匹配 -> tool_definition_mismatch
相同 ResponseID 不同 payload -> ErrCommandConflict
start Accepted 后才授予外部执行权
AlreadyApplied 不重新授予执行权
stale grant/Revision 被拒绝
一次 transition 的事件共享 Revision，Index 连续，提交后 State.Revision == transition Revision
ToolCall progress 按 CallID 合并
已关闭 ToolStep 不重复关闭或创建新 Step
model-step limit 在 authority 内生效
RunStopped/RunFailed 保留最近已接受的 ModelResult
Cancel 与 Unknown 的提交先后决定终态
CancelRun 在过期 BaseRevision 上对非 terminal Run 重新评估
RejectModelResult 必须带有效 Model grant；AlreadyApplied 重放不重复累计 usage
持久化状态与按 Evolve 折叠的事件流逐字段一致；不一致 -> halt，不自动修复
```

### 14.4 Memoh integration

```text
queue FIFO、accepted-order reorder、typed ID isolation
assigned follow-up 只由正确的 R1 admission claim
canonical history、AgentEvent 与 MachineState 同事务
assistant tool-call 和 tool result 只写一次
多 response rows 与逐次 wake/idempotency
lease expiry/recovery/unknown outcome
eligible steer 存在时 Prepare 在同一事务内被拒绝，AcceptInput 先应用，重新规划携带该输入
并行 Call 中一个 Unknown 后撤销其他 grant，迟到结果不改变终态
terminal 后迟到的工具结果投影到产品可见的审计视图
R0 terminal 与 session settled 分离
EventSink gap 后可由 durable snapshot 对账
```

## 15. Memoh queue spec 的后续改写

本次不编辑 `session-runtime-steer-followup.md`。后续应按以下边界修订：

Memoh queue/session/admission 的语义保持在 Memoh；现有 NativeAgentLoop 只保留为组装 Request Planner、`agent.Loop` 和 `MemohRuntime` 的薄宿主 wrapper，不包含第二套多步执行算法。queue 仲裁只发生在 queue-safe boundary，并与对应 AgentCommand/AgentEvent 在 Memoh transaction 中提交。旧的完整 Step 提交改为 ModelStep、ToolStep 以及逐 Call progress/response 记录；每次 response 只推进对应 Call。R0/R1、ownerless recovery、session settled 和 claim takeover 语义保持。

## 16. 实施前置条件

1. Memoh 增加 ToolCall progress、response set、event idempotency、history/outbox projection。
2. Memoh 冻结内部 Attempt、owner、fence、lease 和 recovery grace 规则；这些不进入 agent public API。
3. 工具失败不由 agent core 调度 retry timer；已知失败交给下一次模型，未知结果终止当前 Run。非幂等外部 effect 只能承诺 at-least-once。
4. Request Planner 必须能从已提交的 application context 构造完整、可冻结的 `sdk.Request`。

## 17. 待确认决策

实现前仍需确认：

1. queue capacity、expiry 和产品授权是否进入 Memoh queue contract。
2. breaking release 版本和 Memoh protocol upgrade window。
3. EventSink payload schema，以及是否需要在 Memoh outbox 中加入跨进程 execution epoch。

本规范已经固定：Cancel 与 Unknown 按提交先后决定终态；`sdk.Request` 冻结完整的 generation options，streaming 只是 `ModelInvoker` 的可选执行路径，不改变 AgentCommand/AgentEvent 语义。Machine 采用 Decide/Evolve 拆分：Decide 承载全部决策并在提交时产出结果事实，Evolve 是机械折叠、与事件编码同属永久兼容契约；MachineState 为 execution authority、AgentEvent log 为 historical authority，二者是同一 transition 的两个 materialization，分歧即 halt。结构性 malformed 的模型结果通过 `RejectModelResult` 在同一冻结 request 上有限重试；usage 在 MachineState 内逐字段累计；steer 由 MemohRuntime 的 Prepare gate 保证进入下一个 ModelStep；工具不做效果分级，计划内停机以排空代替，Unknown 语义只覆盖崩溃和 lease 失效。

重新评估 event log 为唯一权威（纯 ES 翻转）的触发条件：出现跨多个发布周期存活的长生命周期 Run；fork/任意历史时点重建成为产品功能；外部消费方要求自描述的事件日志。上述条件出现前，保持双物化模型。

## 附录 A：最小 public API 草案

```go
package agent

import (
    "context"
    "encoding/json"
    "errors"

    "github.com/memohai/twilight-ai/sdk"
)

type RunID string
type StepID string
type CallID string
type CommandID string
type ResponseID string
type InputID string
type ToolRef string
type ModelRef string
type Digest string
// PlanningToken is opaque to agent; the application uses it to identify the
// context revision from which a RequestPlan was built.
type PlanningToken string

type RunConfig struct {
    Model ModelRef
    // Zero means unlimited. A positive value caps ModelSteps in this Run;
    // negative values are invalid.
    ModelStepLimit int
    // Max RejectModelResult per ModelStep before the Run fails. Zero is
    // normalized to 2 by Initialize; negative values are invalid. There is no
    // unlimited value.
    ModelRejectLimit int
}

type RunStatus uint8

const (
    RunActive RunStatus = iota
    RunCompleted
    RunStopped
    RunFailed
)

type RunReason string

const (
    ReasonCancelled      RunReason = "cancelled"
    ReasonStepLimit      RunReason = "step_limit"
    ReasonProviderFailure RunReason = "provider_failure"
    ReasonMalformedModel RunReason = "malformed_model_result"
    ReasonEffectUnknown  RunReason = "effect_unknown"
)

type RunFailure struct {
    Class   string
    Message string
    CallID  CallID
}

type RunResult struct {
    Status  RunStatus
    Reason  RunReason
    Failure *RunFailure
    Model   *sdk.ModelResult
    Usage   sdk.Usage // MachineState.Usage 在 terminal 时的副本
}

type StepRef struct {
    RunID  RunID
    ID      StepID
    Digest  Digest // immutable step binding digest; progress is not included
}

// Step is sealed by the agent package. Runtime implementations return values
// created by the Machine rules; callers cannot add another Step variant.
type Step interface {
    step()
    Ref() StepRef
}

type ModelStep struct {
    RefValue      StepRef
    Request       sdk.Request
    RequestDigest Digest
    Model         ModelRef
    Tools         []ToolSpec
    ToolsDigest   Digest
    Status        ModelStepStatus
    Rejects       int // accepted ModelStepRejected count; progress, not part of RefValue.Digest
}

func (ModelStep) step() {}
func (s ModelStep) Ref() StepRef { return s.RefValue }

type ModelStepStatus uint8

const (
    ModelPrepared ModelStepStatus = iota
    ModelExecuting
)

type ToolStep struct {
    RefValue StepRef
    Source   StepID
    Calls    []ToolCallState
}

func (ToolStep) step() {}
func (s ToolStep) Ref() StepRef { return s.RefValue }

type ToolCallStatus uint8

const (
    ToolPending ToolCallStatus = iota
    ToolExecuting
    ToolWaiting
    ToolCompleted
    ToolFailed
)

type ToolCallState struct {
    CallID           CallID
    ToolRef          ToolRef
    DefinitionDigest Digest
    BindingDigest    Digest
    Arguments        json.RawMessage
    Policy           ResponsePolicy
    Status           ToolCallStatus
    Result           *ToolExecutionResult
    Failure          *ToolCallFailure
    Waiting          *ResponseRequest
}

func ValidateToolCallState(ToolCallState) error

type ToolCallFailure struct {
    Failure ToolFailure
    Outcome ToolFailureOutcome
}

type ToolFailure struct {
    Class   string
    Message string
}

type ToolFailureOutcome uint8

const (
    ToolOutcomeKnown ToolFailureOutcome = iota
    ToolOutcomeUnknown
)

const (
    FailurePermissionDenied = "permission_denied"
    FailureToolLookup       = "tool_lookup_failed"
    FailureInvalidArguments = "invalid_arguments"
    FailureMalformedModel   = "malformed_model_result"
    FailureDefinitionMismatch = "tool_definition_mismatch"
    FailureExecution        = "execution_failed"
    FailureEffectUnknown    = "effect_unknown"
)

type ResponsePolicy uint8

const (
    DirectExecution ResponsePolicy = iota
    ApprovalRequired
    ExternalResponse
)

// MaxParallel: 1 means sequential; values greater than 1 enable bounded
// parallelism. Zero is normalized to 1; negative values are rejected by Loop
// construction.
type ExecutionPolicy struct { MaxParallel int }

type ResponseRequest struct {
    RunID        RunID
    StepID       StepID
    CallID       CallID
    ID           ResponseID
    Kind         ResponseKind
    Payload      json.RawMessage
    RequestDigest Digest // digest of the request payload, not a user response
}

type ResponseKind string

const (
    ResponseApproval ResponseKind = "approval"
    ResponseExternal ResponseKind = "external_response"
)

type MachineState struct {
    RunID           RunID
    Status          RunStatus
    Config          RunConfig
    Current         Step
    PendingInputs   []AgentInput
    ModelSteps      int
    Usage           sdk.Usage // 已接受 ModelStepCompleted/ModelStepRejected 的逐字段累计
    LastModelResult *sdk.ModelResult
    Result          *RunResult
}

// AgentCommand is the intent submitted through Runtime.Commit for an existing
// Run. Accepting one command constitutes one transition.
type AgentCommand interface { agentCommand() }

// Fact is one committed outcome produced by Machine.Decide. Facts are wrapped
// as AgentEvents; Machine.Evolve folds them mechanically.
type Fact interface { fact() }

type AgentInput struct {
    ID      InputID
    Payload json.RawMessage
}

// NextStep creates the command consumed by an active Run at a safe boundary.
func NextStep(input AgentInput) AcceptInput

// NextRun creates the admission seed used by application admission. It does
// not allocate a RunID, claim a queue item, or mutate an existing Run, and it
// is never submitted through Runtime.Commit.
type RunSeed struct { Input AgentInput }
func NextRun(input AgentInput) RunSeed

// --- Commands (intent) ---

type PrepareModelRequest struct {
    StepID         StepID
    Model          ModelRef
    Request        sdk.Request
    RequestDigest  Digest
    InputIDs       []InputID
    PlanningToken  PlanningToken
    Tools          []ToolSpec
    ToolsDigest    Digest
}
func (PrepareModelRequest) agentCommand() {}

type StartModelExecution struct { StepID StepID }
func (StartModelExecution) agentCommand() {}

// Releases or recovers model execution: no provider result was accepted, so
// the same frozen request may be prepared for another attempt.
type RecoverModelExecution struct { StepID StepID }
func (RecoverModelExecution) agentCommand() {}

type SubmitModelResult struct {
    StepID StepID
    Result sdk.ModelResult
    Calls  []ToolCallBinding
}
func (SubmitModelResult) agentCommand() {}

type SubmitModelFailure struct { StepID StepID; Failure StepFailure }
func (SubmitModelFailure) agentCommand() {}

// A structurally malformed model result: usage is accumulated, the step's
// reject counter is incremented, and the step returns to Prepared until
// RunConfig.ModelRejectLimit is exceeded. Requires the model start grant.
type RejectModelResult struct {
    StepID  StepID
    Usage   sdk.Usage
    Failure StepFailure
}
func (RejectModelResult) agentCommand() {}

type StartToolCall struct { StepID StepID; CallID CallID }
func (StartToolCall) agentCommand() {}

type SubmitToolResult struct {
    StepID StepID
    CallID CallID
    Result ToolExecutionResult
}
func (SubmitToolResult) agentCommand() {}

type SubmitToolFailure struct {
    StepID  StepID
    CallID  CallID
    Failure ToolFailure
    Outcome ToolFailureOutcome
}
func (SubmitToolFailure) agentCommand() {}

// ResponseDigest is the canonical digest of this approval decision payload.
type ApproveToolCall struct { StepID StepID; CallID CallID; ResponseID ResponseID; ResponseDigest Digest }
func (ApproveToolCall) agentCommand() {}

// ResponseDigest is the canonical digest of this rejection payload. Decide
// records the outcome as ToolCallFailed{Known, permission_denied}.
type RejectToolCall struct { StepID StepID; CallID CallID; ResponseID ResponseID; ResponseDigest Digest; Reason string }
func (RejectToolCall) agentCommand() {}

type SubmitToolResponse struct {
    StepID         StepID
    CallID         CallID
    ResponseID     ResponseID
    ResponseDigest Digest // digest of the answer payload
    Payload        json.RawMessage
}
func (SubmitToolResponse) agentCommand() {}

type CancelRun struct { Reason RunReason }
func (CancelRun) agentCommand() {}

type AcceptInput struct { Input AgentInput }
func (AcceptInput) agentCommand() {}

// --- Facts (committed outcomes) ---

type ModelStepPrepared struct {
    StepID        StepID
    Model         ModelRef
    Request       sdk.Request
    RequestDigest Digest
    InputIDs      []InputID
    Tools         []ToolSpec
    ToolsDigest   Digest
}
func (ModelStepPrepared) fact() {}

type ModelStepStarted struct { StepID StepID }
func (ModelStepStarted) fact() {}

type ModelStepRecovered struct { StepID StepID }
func (ModelStepRecovered) fact() {}

type ModelStepRejected struct {
    StepID  StepID
    Usage   sdk.Usage
    Failure StepFailure
}
func (ModelStepRejected) fact() {}

type ModelStepCompleted struct {
    StepID StepID
    Result sdk.ModelResult
}
func (ModelStepCompleted) fact() {}

type ToolStepOpened struct {
    StepID StepID // the new ToolStep
    Source StepID // the completed ModelStep
    Calls  []ToolCallBinding
}
func (ToolStepOpened) fact() {}

type ToolCallStarted struct { StepID StepID; CallID CallID }
func (ToolCallStarted) fact() {}

type ToolCallApproved struct { StepID StepID; CallID CallID; ResponseID ResponseID; ResponseDigest Digest }
func (ToolCallApproved) fact() {}

type ToolCallCompleted struct {
    StepID StepID
    CallID CallID
    Result ToolExecutionResult
}
func (ToolCallCompleted) fact() {}

type ToolCallAnswered struct {
    StepID         StepID
    CallID         CallID
    ResponseID     ResponseID
    ResponseDigest Digest
    Payload        json.RawMessage
}
func (ToolCallAnswered) fact() {}

type ToolCallFailed struct {
    StepID  StepID
    CallID  CallID
    Failure ToolFailure
    Outcome ToolFailureOutcome
}
func (ToolCallFailed) fact() {}

type ToolStepClosed struct { StepID StepID }
func (ToolStepClosed) fact() {}

type InputAccepted struct { Input AgentInput }
func (InputAccepted) fact() {}

// Terminal fact. Always the last fact of its transition.
type RunEnded struct {
    Status  RunStatus // RunCompleted, RunStopped or RunFailed
    Reason  RunReason
    Failure *RunFailure
}
func (RunEnded) fact() {}

type StepFailure struct { Class string; Message string }

type ToolCallBinding struct {
    CallID           CallID
    ToolRef          ToolRef
    DefinitionDigest Digest
    BindingDigest    Digest // definition, policy and canonical arguments
    Arguments        json.RawMessage
    Policy           ResponsePolicy // unresolved ToolRef uses DirectExecution
    Response         *ResponseRequest // Decide 在 ToolStepOpened 中派生并填充；调用方提交时留空
}

// ToolSpec is the agent-side sidecar for a provider-neutral sdk.ToolDefinition.
// ResponsePolicy is intentionally kept out of sdk to preserve package layering.
type ToolSpec struct {
    Ref              ToolRef
    Definition       sdk.ToolDefinition
    DefinitionDigest Digest
    Policy           ResponsePolicy
}

func Next(MachineState) (Effect, error)
func Decide(MachineState, AgentCommand) ([]Fact, error)
func Evolve(MachineState, Fact) (MachineState, error)
func Initialize(RunID, RunConfig, RunSeed) (MachineState, error)

type Effect interface { effect() }

type NeedModelRequest struct { Hint PlanningHint }
func (NeedModelRequest) effect() {}

type StartModelCall struct { StepID StepID }
func (StartModelCall) effect() {}

type StartToolCalls struct { StepID StepID; CallIDs []CallID }
func (StartToolCalls) effect() {}

type WaitForResponse struct { Requests []ResponseRequest }
func (WaitForResponse) effect() {}

type WaitForExecutionRecovery struct{}
func (WaitForExecutionRecovery) effect() {}

type PlanningHint struct {
    RunID         RunID
    Model         ModelRef
    SourceStep    StepID
    Inputs        []AgentInput
}

type RequestPlanner interface {
    Plan(context.Context, PlanningHint) (RequestPlan, error)
}

type RequestPlan struct {
    Model         ModelRef
    Request       sdk.Request
    InputIDs      []InputID
    PlanningToken PlanningToken
    Tools         []ToolSpec
}

type Runtime interface {
    Load(context.Context) (RuntimeSnapshot, error)
    Commit(context.Context, CommitRequest) (CommitResult, error)
}

// The representation is implementation-defined. Callers only pass it back;
// it is not a Step identity or a user credential.
type ExecutionGrant string

type RuntimeSnapshot struct {
    State    MachineState
    Revision uint64 // 已接受的 transition 数；初始状态为 0
}

type CommandEnvelope struct {
    SchemaVersion uint16
    Type          string
    RunID         RunID
    ID            CommandID
    Digest        Digest
    Command       AgentCommand
}

type AgentEvent struct {
    SchemaVersion uint16
    Type          string
    RunID         RunID
    Revision      uint64
    Index         uint16
    CommandID     CommandID
    CommandDigest Digest
    Digest        Digest
    Fact          Fact
}

type CommitRequest struct {
    BaseRevision uint64
    Grant        ExecutionGrant
    Command      CommandEnvelope
}

type CommitStatus uint8

const (
    CommitAccepted CommitStatus = iota
    CommitAlreadyApplied
)

type CommitResult struct {
    Status   CommitStatus
    Snapshot RuntimeSnapshot
    Events   []AgentEvent   // 该 transition 的完整事件组
    Grant    ExecutionGrant // 仅 Accepted 的 start command 会返回
}

type DecisionKind uint8

const (
    DecisionApply DecisionKind = iota
    DecisionAlreadyApplied
    DecisionConflict
    DecisionStale
    DecisionTerminal
)

type CommitDecision struct {
    Kind     DecisionKind
    NewState MachineState
    Events   []AgentEvent
}

// Shared, pure commit evaluation. Both runtimes call this single
// implementation inside their own critical section / transaction.
func EvaluateCommit(
    cur MachineState, curRevision uint64,
    prior []AgentEvent,
    req CommitRequest,
    grantValid bool,
    recoveryValid bool,
) (CommitDecision, error)

func EncodeCommand(CommandEnvelope) ([]byte, error) // 不包含 Digest 字段
func DigestCommand(schemaVersion uint16, typ string, command AgentCommand) (Digest, error)
func EncodeFact(schemaVersion uint16, typ string, fact Fact) ([]byte, error)
func DigestFact(schemaVersion uint16, typ string, fact Fact) (Digest, error)
func EncodeRunSeed(RunSeed) ([]byte, error)
func DigestRunSeed(schemaVersion uint16, seed RunSeed) (Digest, error)
func DigestRequest(sdk.Request) (Digest, error)
func DigestToolDefinition(sdk.ToolDefinition) (Digest, error)
func DigestToolSpec(ToolSpec) (Digest, error)
func DigestToolSpecs([]ToolSpec) (Digest, error)
func DigestModelStepBinding(model ModelRef, requestDigest, toolsDigest Digest) (Digest, error)
func DeriveModelRequestCommandID(RunID, uint64) CommandID
func DeriveModelStepID(RunID, CommandID, Digest) StepID
func DeriveToolStepID(StepID, Digest) StepID
func DeriveResponseID(RunID, StepID, CallID, ResponseKind) ResponseID
func DeriveResponseCommandID(RunID, StepID, CallID, ResponseID) CommandID
func DeriveInputCommandID(RunID, InputID) CommandID

var ErrCommandConflict = errors.New("agent: command identity conflict")
var ErrStaleRuntime = errors.New("agent: stale runtime version or grant")
var ErrRunTerminal = errors.New("agent: run is terminal")

type ModelCatalog interface { Resolve(ModelRef) (ModelInvoker, error) }

type ModelInvoker interface {
    Generate(context.Context, sdk.Request) (sdk.ModelResult, error)
}

// Optional optimization. It must produce the same final ModelResult as Generate.
type StreamingModelInvoker interface {
    Stream(context.Context, sdk.Request) (sdk.ModelStream, error)
}

type ToolCatalog interface { Resolve(ToolRef) (ExecutableTool, error) }

type ToolExecutionRequest struct {
    RunID            RunID
    StepID           StepID
    CallID           CallID
    ToolRef          ToolRef
    DefinitionDigest Digest
    Arguments        json.RawMessage
    Progress         ToolProgressSink
}

type ExecutableTool interface {
    Ref() ToolRef
    Definition() sdk.ToolDefinition
    ResponsePolicy() ResponsePolicy
    ValidateArguments(json.RawMessage) error
    Execute(context.Context, ToolExecutionRequest) ToolExecutionOutcome
}

type ToolExecutionResult struct { Output json.RawMessage }

type ToolExecutionOutcome interface { toolExecutionOutcome() }

type ToolExecutionSucceeded struct { Result ToolExecutionResult }
func (ToolExecutionSucceeded) toolExecutionOutcome() {}

type ToolExecutionFailed struct { Failure ToolFailure }
func (ToolExecutionFailed) toolExecutionOutcome() {}

type ToolExecutionUnknown struct { Failure ToolFailure }
func (ToolExecutionUnknown) toolExecutionOutcome() {}

type ToolProgressSink interface { Publish(context.Context, ToolProgress) }
type ToolProgress struct { Payload json.RawMessage }

type EventSink interface { Emit(context.Context, Event) error }

type Event struct {
    RunID      RunID
    StepID     StepID
    CallID     CallID
    Sequence   uint64
    Kind       EventKind
    Durability EventDurability
    Payload    json.RawMessage
    Canonical  *AgentEvent // set for a committed observation; nil for provisional
}

type EventDurability uint8

const (
    EventProvisional EventDurability = iota
    EventCommitted
)

type EventKind string

const (
    EventAgentCommitted EventKind = "agent_committed"
    EventModelTextDelta EventKind = "model_text_delta"
    EventToolProgress   EventKind = "tool_progress"
    EventToolStarted    EventKind = "tool_started"
    EventToolCompleted  EventKind = "tool_completed"
    EventRunFinished    EventKind = "run_finished"
)

type LoopDisposition uint8

const (
    LoopWaiting LoopDisposition = iota
    LoopFinished
)

type WaitReason string

const (
    WaitingForResponse    WaitReason = "waiting_for_response"
    ExecutionRecovery     WaitReason = "execution_recovery" // 当前执行仍在运行或等待失效恢复
)

type LoopResult struct {
    Disposition LoopDisposition
    Reason      WaitReason
    Waiting     []ResponseRequest
    Result      *RunResult
}

type Loop struct {
    Models    ModelCatalog
    Tools     ToolCatalog
    Planner   RequestPlanner
    Execution ExecutionPolicy
    Streaming bool
}

func (l *Loop) Run(context.Context, Runtime, EventSink) (LoopResult, error)
```

实现必须保证所有返回的 Step、Call、Request、Result 和等待 payload 具有只读快照语义；调用方不能通过修改 slice、map 或 `json.RawMessage` 改变 Runtime 状态。`AgentCommand`、`Fact`、`Effect` 和 ToolExecutionOutcome 使用 agent 的 sealed interface，外部实现不能添加未定义变体。构造 CommandEnvelope 与派生 CommandID/ResponseID 只能通过 agent 提供的 typed 构造函数；手工拼装信封字段属于实现错误。

## 附录 B：核心不变量

1. sdk 的一次 `Generate` 或 `Stream` 对应一次 provider request；transport retry 不创建新的 Step。
2. `agent.Loop` 是唯一的多步执行算法；Run 的权威状态由 Runtime 持有，Loop 不保存第二份。
3. Runtime 对 Loop 只公开 `Load` 和 `Commit`；Planner、queue 和工具入口不是 Runtime 的隐藏第三、第四个方法。
4. Machine 是完整的 Run/Step/ToolCall 语义规则；决策只在 `Decide` 中、只在提交时运行一次，`Evolve` 是机械折叠。Runtime 通过共享 `EvaluateCommit` 调用它们，不复刻规则。
5. Step 是 durable resume boundary，只有 ModelStep 和 ToolStep；ToolCall 是 ToolStep 内的 progress。
6. ModelStep 完成有 tool calls 时产出 `ToolStepOpened`；全部 Call 到达可关闭终态时，同一 transition 产出 `ToolStepClosed`。终态一律以 `RunEnded` 显式产出，且它是其 transition 的最后一个事实。
7. Pending Call 必须在 start command Accepted 后才可执行；多个独立 Pending Call 可按 ExecutionPolicy 并行。
8. Waiting response 只推进对应 Call；approval approved 先变 Pending，随后由 Loop 执行工具。日志记录结果事实（`ToolCallFailed{permission_denied}`、`RunEnded{cancelled}`），不记录请求本身。
9. 幂等按 command 判定：相同 CommandID/digest 重放返回 CommitAlreadyApplied 与原事件组（不重新运行 Decide），不重复写入 projection、history、queue action 或 outbox；相同 CommandID 不同 digest 冲突。
10. 一次接受的 transition 使 Revision 恰好加一；其全部事实共享该 Revision，Index 组内连续，提交后 `Snapshot.Revision` 等于该 Revision。
11. MachineState 是 execution authority，AgentEvent log 是 historical authority；二者是同一 transition 序列的两个 materialization，由同一原子提交产生。对任意 Revision，状态必须等于初始状态经 `Evolve` 折叠事件流的结果；分歧即 halt，不定义自动覆盖方向。
12. Evolve 的折叠语义与事件编码同属永久兼容契约，按 SchemaVersion 冻结；Decide 的决策规则可随版本演进，因为决策结果已记录为事实。
13. 已知工具失败交给下一次模型请求；Unknown 终止 Run，不自动重试、不查询外部系统。
14. worker cancellation 不等于 RunStopped；业务停止必须提交控制 command。宿主的业务停止先提交 `CancelRun`，再取消 Loop 的 ctx；ctx 取消本身只结束执行尝试，工具 worker 运行到自身结束。
15. EventSink 只是实时观察；AgentEvent、durable snapshot 和 outbox 才是 replay/recovery 依据。
16. MemoryRuntime 用进程内同步；MemohRuntime 用事务、CAS 和内部 Attempt/owner/fence/lease；两者共享 `EvaluateCommit` 与 Machine 规则，但不共享存储实现。
17. 结构性 malformed 的模型结果以 `RejectModelResult` 回到 Prepared，在同一冻结 request 上重试并累计 usage；超过 `RunConfig.ModelRejectLimit` 才 RunFailed。单个 Call 的参数解析失败不是 malformed，按已知 `invalid_arguments` 进入下一次模型请求。
