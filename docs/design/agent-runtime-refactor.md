# Twilight AI Agent Runtime 重构设计规范

状态：重构方案

本文定义 Twilight AI 的目标 agent domain 边界：`sdk/` 负责一次模型调用，`agent/es` 提供通用 event-sourcing 机制，`agent/run` 保存单次 Run 的 execution ES，`agent/session` 保存长期会话语义 ES，`agent/queue` 提供通用调度/claim/dedup 机制。Application/Product 负责把这些 domain 编排起来。

本文不再把 Twilight 定义成一个“大 Agent Core”。当前代码中的 root `agent` 包语义上应收敛为目标 `agent/run` 包；它回答“一次 computation 执行到哪里、下一步是否合法、如何 replay/recover”。Session、Queue、Context、Artifact、Memory、Workspace、Scheduler 等是独立 domain 或 application policy；其中本规范先正式拆出 `run`、`session`、`queue` 和共享 `es` substrate。

正文中的 Go 片段用于说明协议；附录 A 是 public API 草案。正文各节为规范文本，附录是汇总视图；两者不一致时以正文为准，并修订附录。

## 1. 目标

### 1.1 核心目标

Twilight AI 的 agent 侧被拆成几个可组合 domain，而不是一个单体 Agent：

```text
Application / Product
        │
        ├── conversation/session semantics
        ├── steer / follow-up policy
        ├── artifact/context/memory/workspace policy
        └── orchestration
             │
   ┌─────────┼─────────┐
   │         │         │
agent/     agent/    agent/
session    queue     run
长 ES      调度机制   短 Run ES
```

三个已经明确的 domain authority：

| Domain | 回答的问题 | Authority 形态 | 生命周期 |
| --- | --- | --- | --- |
| `agent/session` | 这个会话长期发生过什么？ | append-only Session ES；**产品跨 Run 的语义主 authority** | 跨多个 Run 长期存在 |
| `agent/run` | 这一次执行如何跑到当前位置？ | RunHeader + TransitionRecord log；**该 Run 存活期间的 execution authority** | 单次 Run，materialize/finalize 后可归档/GC |
| `agent/queue` | 哪些工作待处理、谁 claim、如何 dedup？ | transactional queue state | 调度生命周期 |

Session 不需要保存每一个执行微步骤，但必须记录每一次 Run 的 admission、终态和继续理解会话所需的结果/引用。Run log 只服务执行、恢复和审计；它不是跨 Run 语义的最终来源。

`agent/es` 是机制库，不是第四个语义 domain。它只提供 canonical envelope、digest、record completeness、revision/index 校验和 fold runner 等可复用机制；它不认识 ModelStep、Session message、queue claim 或产品 policy。

本版本固定五个执行层级，但这些层级属于 `agent/run`：

```text
1. Run Machine
   定义 Run、Step、ToolCall 的语义状态和合法状态变化

2. Run Loop
   解释 Machine 产生的 effect，执行模型/工具，再提交结果事件

3. Run Runtime
   加载权威 MachineState，并原子提交 AgentCommand，产生 TransitionRecord

4. Model / Tool
   执行一次模型请求或一次工具调用

5. Request Planner
   把 application/session/context projection 投影为下一次模型所需的 sdk.Request
```

这里的 “Run Machine” 是语义层名称，不引入名为 `Agent` 的核心对象；公共执行入口仍是 `Loop`。

整体关系：

```text
                Session ES / Context / Queue
             (Application-owned planning inputs)
                         │
                         v
                 Request Planner
                         │ sdk.Request
                         v
                  run.Loop --SDK()--> Model / Tool
                         │
                         │ AgentCommand carries frozen run.ModelRequest / ModelResult
                         v
                 run.Runtime
                 Load + atomic Commit
                /                  \
       run.MemoryRuntime      Durable runtime adapter
        memory + mutex      DB + private lease/fence/outbox
                \                  /
                 \                /
             RunHeader + TransitionRecord log
             MachineState projection
                         │
                         v
      EventSink / Run replay / Session materializer / OTel
```

Loop 与 Runtime 的职责相互独立：Loop 决定如何执行，Runtime 决定权威状态在哪里以及如何安全提交。Machine 决定什么状态变化合法；Planner 决定模型看到的 application/session/context projection；Model/Tool 只执行一次外部 effect。

目标目录边界：

```text
twilight-ai/
  sdk/             一次 LLM request/response 的边界/transport 类型
  agent/
    es/            通用 ES envelope、digest、record、fold 机制
    run/           单次 Run 的 execution ES：Machine、Loop、Runtime、Step/ToolCall
    session/       长期语义 Session ES substrate
    queue/         通用 transactional queue/claim/dedup 机制
    jsonstable/    canonical JSON value 与解析/编码
    harness/       可选：in-process 组合器，不是新的 Runtime 语义
```

依赖方向固定为：

```text
agent/es        ---> agent/jsonstable（可选）
agent/session   ---> agent/es
agent/run       ---> agent/es + agent/jsonstable + sdk
agent/queue     ---> 不依赖 agent/run；默认不依赖 ES
Application     ---> sdk + agent/run + agent/session + agent/queue
sdk             不依赖 agent 或 Application
```

禁止出现 `agent/es -> agent/run` 或 `agent/session -> agent/run` 这类反向依赖；共享机制不能 import domain 语义。

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
        TransitionRecord + new MachineState（同一 Revision）
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
| ModelStep | 一次冻结的 run-owned `ModelRequest`，直到接受模型结果。 |
| ToolStep | 一个模型结果产生的一组 ToolCall 及其 progress。 |
| ToolCall | ToolStep 内的一个结构化工具调用。 |
| AgentCommand | Loop 或外部入口希望 Machine 接受的意图；接受后构成一次 transition。 |
| AgentEvent | Runtime 已接受的单个事实；一次 transition 产出一个或多个，带 (Revision, Index) 身份，可作为外部事件流消费。 |
| TransitionRecord | Runtime 持久化的原子 authority record；封装一次 transition 的完整 AgentEvent 组和 transition digest。 |
| Revision | 每 Run 单调递增的 transition 计数；第 N 次接受的 transition 产出 Revision=N 的 TransitionRecord 和 Revision=N 的状态。 |
| Effect | Machine 根据当前状态返回的至多一个待执行动作；不表示一定有外部副作用。 |
| Attempt | Runtime 为一次进程执行建立的内部执行租约。 |
| Runtime | MachineState 的 execution authority、AgentCommand 的原子提交和 TransitionRecord 的产生者。 |
| Request Planner | application context 到 `sdk.Request` 的投影器。 |

### 1.4 成功标准

| 场景 | 期望结果 |
| --- | --- |
| 单次模型调用 | 直接使用 `sdk`，不自动执行工具。 |
| 本地 Run | `run.Loop` 配合 `run.MemoryRuntime`。 |
| durable Run | 同一个 `run.Loop` 配合 durable runtime adapter。 |
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
| request/result boundary | provider-neutral 的一次调用输入输出；可由 agent 在 Loop 边界冻结。 |

`sdk.Request` 是一次模型调用的完整输入，不是 session、history 或 queue。`sdk.ModelResult` 是一次完整模型响应，保留旧 `GenerateResult` 的单次调用字段（文本、reasoning parts、tool calls、finish reason、usage、sources/files 和 provider metadata），但不包含自动 tool loop、approval 或多次调用累加。多步执行的 steps 和 messages 由 agent/application 另行保存。

`sdk` 类型只允许存在于 Planner/ModelInvoker/provider 边界；不得作为 Run event、MachineState、AgentCommand 或 digest input 的持久化形态。Loop 必须在提交前把 `sdk.Request`、`sdk.ModelResult` 和 `sdk.ToolDefinition` 分别转换成 run-owned 的 `ModelRequest`、`ModelResult` 和 `ToolDefinition`；调用 provider/tool 前再由这些 frozen value 构造新的 `sdk` 值。

run-owned 冻结形态遵守以下规则；`FreezeModelRequest`、`FreezeModelResult`、`FreezeToolDefinition`、`DigestRequest`、StepID 派生和提交幂等都建立在这套规则上：

1. 冻结值是纯数据。它不包含 provider client、接口值、回调或 `Execute` 句柄；模型以 provider 作用域内的字符串 ID 表示，provider 绑定发生在 `ModelCatalog`/`ModelInvoker` 解析时。
2. 工具以 provider-neutral 的 `ToolDefinition{Name, Description, Parameters}` 表示；`Parameters` 是解析完成并 canonicalized 的 JSON Schema 文档。由 Go struct 推导 schema 的工作在冻结前完成，冻结后的 Request 不依赖推导或反射。
3. `ToolChoice` 是封闭类型 `{Mode: auto|none|required|tool, Tool string}`，不使用 `any`。
4. 消息 part 是 sealed union 的 agent value（text/reasoning/image/file/tool-call/tool-result），不持久化 `sdk.MessagePart` interface。
5. 消息中的二进制内容有两种形式：inline bytes（canonical 编码为 base64），或稳定的内容寻址引用 `BlobRef{Digest, MediaType, ByteSize}`。`BlobRef` 的字节解析由组装 `ModelInvoker` 的一方负责；带时效的 URL 等不稳定引用不能进入冻结请求。两种形式产生不同的 digest，Planner 对同一内容必须确定性地选择一种形式。
6. provider metadata、provider options、tool input/result、response format schema 等扩展字段的值必须是 JSON 值。进入 `agent/run` 前必须 parse/canonicalize 成 opaque `CanonicalJSON`（由 `agent/jsonstable.Value` 承载，内部 bytes 不可被 run core 或 caller 直接构造/修改）；不能保存 caller-owned map/slice/RawMessage 或 `any`。canonicalization 必须拒绝会把不同 payload 折叠成同一值的输入，包括重复 object key、trailing data、invalid UTF-8 和 escaped lone surrogate（`\ud800`..`\udfff`）。
7. `DigestRequest` 覆盖 frozen `ModelRequest` 的全部字段，不设排除项。cache 配置等只影响成本的字段同样参与摘要；排除任何字段都会把不同请求判成同一事件，产生错误的 `CommitAlreadyApplied`。

边界的 SDK 类型可以保持以下形态；agent 的持久化 `ModelRequest`/`ToolDefinition` 是相同语义的 concrete mirror，字段中所有接口/any/JSON 原文先在 `Freeze*` 中 canonicalize 为 `CanonicalJSON`。

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

新的单次调用入口是 `sdk.Generate(ctx, model, Request)` / `(*Model).Generate(ctx, Request)` 和对应的 `Stream` / `(*Model).Stream`，返回 `ModelResult` / `ModelStream`，不执行工具、不累计多步状态。provider 可以选择实现 additive `ModelInvoker` / `StreamingModelInvoker`；未实现时 SDK 通过兼容 adapter 调用旧 `Provider.DoGenerate` / `DoStream`。

旧的 `GenerateText`、`StreamText` 和自动 tool loop 在迁移期只能作为显式 legacy wrapper；新 Loop 不依赖它们。

### 2.2 `agent/es`

`agent/es` 是两个 ES domain 共用的机制层，只抽机制，不抽语义。它不能 import `agent/run`、`agent/session` 或 application 类型。

`agent/es` 负责：

| 能力 | 内容 |
| --- | --- |
| envelope | schema version、stream identity、revision、index、type、causation id、payload digest 的通用封装。 |
| canonical digest | 对 version/type/payload/record body 做稳定编码和 digest；拒绝 ambiguous JSON。 |
| record completeness | 校验一次 append 的完整 event group：revision、index、stream id、type、digest、record digest。 |
| replay runner | 给定 initial state、record log 和 domain-provided `EvolveVersion`，执行 `fold(initial, events)`。 |
| conformance helpers | 通用的 record truncation、digest mismatch、revision gap、duplicate/ambiguous JSON 测试工具。 |

`agent/es` 不负责：

```text
ModelStep / ToolStep / ToolCall / RunEnded
Session MessageAdded / Compact / Artifact
Queue claim / lease / visibility timeout
Runtime.Commit / ExecutionGrant
Planner / provider / tool execution
产品 policy
```

建议 API 形态是泛型和 hook，而不是把 domain 类型塞进 `es`：

```go
package es

type StreamID string
type EntryID string
type Revision uint64
type Index uint16
type Digest string
type CausationID string

type Event[T any] struct {
    SchemaVersion uint16
    StreamID      StreamID
    Revision      Revision
    Index         Index
    Type          string
    // CausationID links this event to the operation/event that caused it.
    // It is opaque to es; the domain/application defines its interpretation.
    CausationID   CausationID
    Payload       T
    PayloadDigest Digest
}

type Record[T any] struct {
    SchemaVersion uint16
    StreamID      StreamID
    Revision      Revision
    Events        []Event[T]
    RecordDigest  Digest
}

type Codec[T any] interface {
    Type(T) string
    DigestPayload(schemaVersion uint16, typ string, payload T) (Digest, error)
}

type Folder[S any, E any] func(schemaVersion uint16, state S, event E) (S, error)

func BuildRecord[T any](codec Codec[T], events []Event[T]) (Record[T], error)
func ValidateRecord[T any](codec Codec[T], record *Record[T]) error
func FoldRecords[S any, E any](initial S, records []Record[E], folder Folder[S, E]) (S, Revision, error)
```

具体字段名可以在实现时调整，但边界原则固定：`es` 提供 append-only/replay 的机械不变量；domain 提供 type discriminator、payload encoding、Evolve 语义和 store adapter。

### 2.3 `agent/run`

`agent/run` 是当前 root `agent` 包的目标形态。它是单次 Run 的 execution ES，不是 Session ES。

`agent/run` 负责：

| 能力 | 内容 |
| --- | --- |
| Run Machine | `MachineState`、`Step`、`ToolCallState`、`AgentCommand`、`Fact`、`AgentEvent` 和共享的 Decide/Evolve/Next 规则。 |
| Transition | 把一次 accepted command 产生的完整 fact/event group 封装为 `TransitionRecord`。实现上可直接使用 `agent/es.Record` 或在其上包 domain metadata。 |
| RunHeader | 正式持久协议的一部分：RunID、initial state/schema/digest、admission causation/provenance。完整 **execution authority** 是 `RunHeader + TransitionRecord log`。 |
| Runtime contract | `Load`/`Commit` authority 接口；所有语义状态变化只通过 Commit。 |
| Loop | 唯一的多步执行算法：解释 Effect，调用 Model/Tool，再提交结果 command。 |
| Tool contract | ToolRef、ExecutableTool、参数校验、结果分类和 response policy。 |
| Planner port | 供 Loop 注入 application Request Planner 的最小接口；规划实现不在 run。 |
| EventSink | canonical run event 的实时观察出口，也承载 provisional delta；它不是 authority。 |
| MemoryRuntime | 进程内 reference runtime；它实现同一 Run ES 语义，但不承诺跨进程 durable recovery。 |
| runtimetest | durable runtime 必须复用的 conformance suite。 |

`agent/run` 不拥有：

```text
Session history / compact / memory / artifacts
Queue admission / accepted order / claim policy
Product prompt/context/workspace construction
Provider clients / API keys
Durable DB schema / owner/fence/lease/outbox implementation
```

Run execution source of truth（不替代 Session 的长期语义 authority）：

```text
RunHeader(initial MachineState at Revision 0)
+
TransitionRecord log (Revision 1..N)
```

状态不变量：

```text
MachineState_N = Fold(initial, flatten(TransitionRecord[1..N].Events))
```

Run ES 是短生命周期的 execution authority。它完成后不能再作为下一 Run 的 context 来源；只有当 Session 已记录该 Run 的 lifecycle 与所需长期语义，并且 artifact/usage 等引用可恢复时，才可以 finalization 后 archive/GC。

### 2.3.1 run-owned persisted model data

Runtime 的 authority boundary 不能靠“调用者不要修改快照”这类规范约束来成立，必须由类型和提交路径保证。`sdk.Request`、`sdk.ModelResult`、`sdk.ToolDefinition` 及其内部的 `map`、`slice`、`json.RawMessage`、`any`、interface value 属于 transport boundary，不能直接进入 `run` 的 command/fact/state/event。

因此 `agent/run` 必须定义自己的 JSON-stable persisted value：`ModelRequest`、`ModelResult`、`ToolDefinition`、`Usage`、`Message`、`MessagePart`、`ProviderMetadata` 等。Loop/provider 边界仍使用 `sdk.Request`/`sdk.ModelResult`；进入 Runtime 前必须调用 `FreezeModelRequest`、`FreezeModelResult`、`FreezeToolDefinition`，把动态 JSON parse/canonicalize 成 opaque `CanonicalJSON`；离开 Runtime 调 provider 时通过 `.SDK()` 构造新的 SDK 值。

这不是防御性 deep clone 的局部补丁，而是分层边界：`sdk` 是一次调用的 transport API，`agent/run` 是可重放、可审计、可长期兼容的 execution event protocol。任何新增进入 Run event/MachineState 的 provider 数据，必须先落到 run-owned sealed/JSON-stable 类型或 `CanonicalJSON`。

### 2.4 `agent/session`

`agent/session` 是长期语义历史的 append-only ES substrate。它不写死 Twilight/Memoh 的 message ontology，而是用泛型承载上层语义事件：

```go
package session

type SessionID string
type EntryID string
type BranchID string

// Session is a typed view of one semantic stream. The package owns stream
// mechanics, not the meaning of E.
type Session[E any] struct {
    ID       SessionID
    BranchID BranchID
    Head     EntryID
    Revision es.Revision
}

type Entry[E any] struct {
    SessionID   SessionID
    EntryID     EntryID
    BranchID    BranchID
    Parent      EntryID
    Revision    es.Revision
    SchemaVersion uint16
    Type        string
    CausationID es.CausationID
    Payload     E
    Digest      es.Digest
}

type Store[E any] interface {
    Append(context.Context, AppendRequest[E]) (Entry[E], error)
    Head(context.Context, SessionID, BranchID) (EntryID, es.Revision, error)
    Replay(context.Context, SessionID, BranchID) ([]Entry[E], error)
    Fork(context.Context, ForkRequest) (BranchID, error)
}
```

上层可以定义自己的 session event，例如：

```text
UserMessageAdded
AssistantMessageAdded
ToolResultAdded
RunAdmitted        // RunID + RunHeader digest + admission provenance
RunCompleted       // RunID + terminal summary + durable output/artifact refs
RunFinalized       // 此 Run 的长期语义已吸收，可回收 execution log
CompactCreated
ArtifactLinked
MemoryUpdated
Custom product event
```

`RunAdmitted`、`RunCompleted` 和 `RunFinalized` 是 Application 定义的 Session ontology，不是 `agent/session` 预置的类型；但每一个 Run 至少必须以这种 lifecycle entry 被 Session 记录。

`agent/session` 只保证：

```text
append-only
revision/head CAS
parent/fork lineage
causation id / digest / schema version
replay order
```

它不决定：

```text
什么算 assistant message
哪些 Run event 应该变成长期消息
compact 策略
memory extraction 策略
artifact retention 策略
```

这些属于 Application/Product。

### 2.5 `agent/queue`

`agent/queue` 是通用 transactional queue/claim/dedup 机制，不默认使用 ES。Queue 回答“哪些工作待处理、谁拥有处理权、如何去重/过期/重排”，不是长期语义历史。

建议 API 形态：

```go
package queue

type QueueID string
type ItemID string
type ClaimID string
type DedupKey string

type Item[T any] struct {
    QueueID     QueueID
    ItemID      ItemID
    Payload     T
    PayloadDigest es.Digest
    DedupKey    DedupKey
    CausationID es.CausationID
}

type Store[T any] interface {
    Enqueue(context.Context, EnqueueRequest[T]) (Item[T], error)
    Claim(context.Context, ClaimRequest) (Claim[T], error)
    Ack(context.Context, ClaimID) error
    Release(context.Context, ClaimID, ReleaseReason) error
}
```

`SteerItem`、`FollowUpItem`、`RunQueueItem` 等不是 queue core 语义，而是 application payload：

```text
queue[T]       通用机制
       ↑
SteerItem / FollowUpItem / RunQueueItem   上层类型 + policy
```

Queue 可以被 Memoh durable adapter 放进数据库事务，也可以有 in-memory implementation；但它不进入 Run Machine，不成为 `MachineState` 字段。

### 2.6 Cross-domain materialization and finalization

Session ES 是跨 Run 的语义主 authority。Run ES 可以短命的前提，是 Session 已吸收该 Run 的 lifecycle 和下一 Run/用户可见语义所需的内容；artifact、usage、trace 等长期 store 由 Session entry 引用或以同一 provenance 关联。Run log 不是长期 conversation/history 的后备来源。

需要跨 Run 保留的 Run 语义 materialize 到长期 domain：

```text
Run ES
ModelStepCompleted
ToolCallCompleted
RunEnded
      │
      ├──→ Session ES
      │     AssistantMessageAdded
      │     ToolResultAdded
      │
      ├──→ Artifact Store
      │     files / large outputs / reasoning artifact
      │
      └──→ Usage / Trace
            cost / latency / debug
```

Run → Session/Artifact/Usage 的投影不能是 best-effort listener。Durable 场景必须有 transaction/outbox/inbox 或等价机制，并使用稳定 `causation_id` 做幂等：

```text
Run source event ID    = run_id + revision + index（或其 canonical digest）
Session source_event_id = same stable identity; unique/inbox key
Artifact source_event_id = same stable identity; unique/inbox key
Usage source_event_id   = same stable identity; unique/inbox key

Run event causation_id  = inherited application/session/queue/Run cause
Session causation_id    = preserved Run event causation_id
```

必须存在 finalization barrier：

```text
RunEnded
   ↓
Session 追加 RunCompleted；确保长期消息/结果、artifact、usage/outbox 已提交或可幂等恢复
   ↓
Session 追加 RunFinalized（或事务耦合的 application control-plane marker）
   ↓
Run ES execution log 才允许 archive / GC
```

`RunFinalized` 的语义归属是 Session/Application control plane，并携带 RunID 与 materialization watermark；它不是 `agent/run.Fact`，不参与 `MachineState` fold。它证明 Session 已能在不读取该 Run execution log 的情况下继续会话和构造后续 Run。

### 2.7 类型归属

| 类型或能力 | 所属层 |
| --- | --- |
| `sdk.Request`、`sdk.ModelResult`、`sdk.ToolDefinition`（provider-neutral 边界类型） | `sdk` |
| canonical JSON parser/value | `agent/jsonstable`（可被 `es`、`run`、`session` 使用） |
| generic event envelope/record/fold/digest mechanics | `agent/es` |
| `ModelRequest`、`ModelResult`、`ToolDefinition`、`Usage`、`Message`、`MessagePart`（run persisted frozen values） | `agent/run` |
| `Step`、`ToolCallState`、`AgentCommand`、`Fact`、`AgentEvent`、`TransitionRecord`、`Effect` | `agent/run` |
| `Runtime`、`MemoryRuntime`、`Loop`、`runtimetest` | `agent/run` |
| `SessionID`、`EntryID`、session entry envelope、fork/replay/store | `agent/session` |
| queue item/claim/dedup/visibility mechanism | `agent/queue` |
| Request Planner 实现和 context transformer | Application/Product |
| fixed model policy、step budget、malformed retry budget | Application/Product 或 run `ExecutionPolicy`；不进入 MachineState |
| owner、fencing、lease、outbox、DB schema | Durable adapter/Application |
| MCP server 连接和生命周期 | Application；schema/call adapter 可在 `agent/run` |
| provider transport retry | sdk/provider client |

## 3. Run Machine

### 3.1 Machine 的边界

Machine 是 `agent/run` package 中的纯 execution 语义规则。它只读取完整 `MachineState`、待决策的 `AgentCommand` 和待折叠的 `AgentEvent`，不访问 IO：

```text
MachineState + AgentCommand
          |
Machine.Decide -> 事实序列（决策，只在提交时运行一次）
          |
Machine.Evolve 逐个折叠（机械） -> new MachineState
Machine.Next(state) -> Effect
```

`Next(state)` 根据当前事实产生至多一个待执行的 `Effect`。`Decide(state, command)` 校验一个意图并产出这次 transition 的完整事实序列——所有决策（是否接受、派生哪些后果、是否进入终态）都发生在这里，且只在提交时运行一次，输出即冻结。`Evolve(state, event)` 把单个事实机械地折叠进状态：它不读产品配置或 Loop policy，不含 IO，对 Decide 产出的每种事实全定义；replay 只依赖 versioned Evolve。Runtime 接受一个 command 后，把 Decide 的事实序列包装为同一 Revision 的 AgentEvent 组，与折叠后的新状态放在同一提交边界。两种 Runtime 不能各自复制规则；Loop 重新 `Load` 后再次调用 `Next`，不会依赖一次提交响应中的 effect。

决策与折叠的分工是协议的兼容边界：Machine 的决策规则（自动关闭、终态转换、command disposition）可以随版本演进，因为历史事件已把这些决策的结果记录在案；Evolve 与事件编码一起构成永久兼容契约，已发布 SchemaVersion 的折叠语义不再修改。

Run terminal、Step successor、ToolStep 自动关闭和等待条件都由 Machine 的 Decide 决定，并显式产出对应事实（`ToolStepClosed`、`RunEnded`）。固定模型、max-step budget 和 malformed retry limit 是 host/Loop policy，不进入 MachineState；这些策略若要改变状态，必须提交显式 command（例如 `StopRun{step_limit}` 或带 fail-run disposition 的 `RejectModelResult`）。Runtime 不再维护另一套 Run 终态和等待判断；Memoh 只把已经接受的 AgentEvent 投影到自己的 history、queue 和 outbox，按事件驱动，不做提交前后的状态差分。

Machine 不知道 PostgreSQL、mutex、lease、fencing、provider client、queue 或产品 history。Runtime 可以在自己的临界区内调用这套规则，但不改变规则。

### 3.2 输入语义

Queue 不属于 Machine，但 Machine 需要知道“一个输入何时已经被接受，以及它对后续执行的影响”。因此 core 只定义 active Run 的输入提交边界；terminal follow-up 由 Memoh admission 创建新 Run 后再提交首个输入：

```text
NextStep(input)
  当前 Run 仍 active 且处于可接收边界；输入进入下一次 ModelStep 的规划上下文

terminal follow-up
  Memoh admission -> InitializeRun(new RunID) -> AcceptInput(input)
```

`AgentInput` 只有稳定的 `InputID` 和不可变 payload，不包含 queue item、priority、order、claim 或 lease。`NextStep` 构造 `AcceptInput` command；被 Runtime 接受后产出 `InputAccepted` 事实，与新的 MachineState 一起提交，Memoh 可以在同一事务中把 queue claim 标记为 applied。

Run admission 使用 `InitializeRun(RunID)` 建立最小 Revision-0 state，然后把初始用户输入作为该 Run 的第一条 `AcceptInput` transition 提交。Application 负责 Session/Queue admission 和新 Run 的身份分配；Run replay 不依赖从当前产品配置重新计算 initial state。

Machine 不在 ToolStep 执行中接受 `NextStep`，也不把 terminal follow-up 解释成旧 Run 的状态变化。输入的具体文本如何进入 Planner 生成的 `sdk.Request` 仍由 Request Planner 决定；Loop 在提交前冻结为 agent `ModelRequest`，Machine 只保证输入边界和一次性接受语义。

### 3.3 MachineState

MachineState 至少包括：

```text
RunID
Run status
current Step
ToolStep 中每个 ToolCall 的 progress/result
等待中的 ResponseRequest
已接受但尚未用于冻结下一请求的 AgentInput
model-step counter
累计 usage（对已接受 ModelStepCompleted 和 ModelStepRejected 的 agent Usage 逐字段求和）
最近一次已接受的 agent ModelResult
terminal RunResult（如果已结束）
```

MachineState 不包含固定模型、prompt/history、step limit、malformed retry limit 或产品配置。模型选择由每个 `ModelStepPrepared` 冻结的 `ModelRequest.Model` 表示；limits 属于 host/Loop policy，并通过显式 command 形成事实。

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

AgentCommand（15 种）：

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
StopRun                 host-owned 非取消停止策略，例如 step_limit
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

新 Run 的初始用户输入也必须作为 `AcceptInput` transition 记录，不存在绕过 Runtime.Commit 的 seed input。Run 创建本身由 `RunHeader` 表达；输入接受属于 Run ES。

二者的关系固定为：

```text
Loop / response ingress
        -> AgentCommand (intent)
Runtime EvaluateCommit: 幂等/类别校验 + Machine.Decide + Machine.EvolveVersion
        -> MachineState + TransitionRecord (committed facts, one Revision)
```

意图与事实由 Decide 显式转换，日志永远记录结果而非请求：`RejectToolCall` 记录为 `ToolCallFailed{permission_denied}`；`CancelRun` 记录为 `RunEnded{RunStopped, cancelled}`；host step limit 记录为 `StopRun{step_limit} -> RunEnded{RunStopped, step_limit}`。

`PrepareModelRequest.RequestDigest` 和 `ResponseDigest` 分别是请求/响应 payload 的内容摘要，不是提交身份。`ApproveToolCall`/`RejectToolCall` 使用 `DigestToolResponseDecision(kind, decision, reason)`，`SubmitToolResponse` 使用 `DigestToolResponsePayload(payload)`；Decide 必须校验 digest 与实际 decision/payload 匹配。一个 command 被重试时复用同一 CommandID 和 digest；Runtime 不会为重试生成第二组 AgentEvent。

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

1. `PrepareModelRequest` 只能在 Run active 且没有当前 Step 时接受；其 `InputIDs` 必须按 `PendingInputs` 的当前顺序完整匹配。`Request` 必须已经是 frozen agent `ModelRequest`，且 `Request.Model` 必须等于 command 的 `ModelRef`；`Tools` 必须与其中的 provider tool definitions 按 Ref、顺序和 definition digest 一一对应，`ToolsDigest` 覆盖 Ref、schema、顺序与 policy。产出 `[ModelStepPrepared]`，事实中携带冻结的请求、ToolSpec、被消费的 InputIDs 与 Decide 算好的 step binding digest（事实自包含条件：Evolve 不重算 digest）。
2. `StartModelExecution` 只能作用于 Prepared ModelStep，产出 `[ModelStepStarted]`。`RecoverModelExecution` 只能作用于没有已接受结果的 Executing ModelStep，产出 `[ModelStepRecovered]`；它只能由持有该 Model grant 的当前 Loop，或由 Runtime 自己确认 lease 失效后的 recovery 逻辑提交，普通 response ingress 不能提交。
3. `SubmitModelResult` 只能作用于对应的 Executing ModelStep。结果没有 tool calls 时产出 `[ModelStepCompleted, RunEnded{RunCompleted}]`；有 tool calls 时按冻结 `ToolSpec` 绑定 policy 和 binding digest，产出 `[ModelStepCompleted, ToolStepOpened]`——`ToolStepOpened` 携带完整 Call 集合：DirectExecution 的 Call 为 Pending，ApprovalRequired/ExternalResponse 的 Call 为带稳定 request 的 Waiting，每个 Waiting request 都包含目标 RunID、StepID、CallID、ResponseID、Kind 和 RequestDigest。
4. `SubmitModelFailure` 只能作用于对应的 Executing ModelStep，产出 `[RunEnded{RunFailed, provider_failure}]`，保留稳定失败原因。
5. `RejectModelResult` 只能作用于对应的 Executing ModelStep，必须携带该 start 的 Grant。`Disposition=ModelRejectRetry` 时产出 `[ModelStepRejected]`（Step 回到 Prepared，同一冻结 request 可再次 start）；`Disposition=ModelRejectFailRun` 时产出 `[ModelStepRejected, RunEnded{RunFailed, malformed_model_result}]`。Loop/host policy 可用当前 `ModelStep.Rejects` 和自己的 malformed limit 选择 disposition；limit 不进入 MachineState。被拒绝的结果不写入 `LastModelResult`。
6. `StartToolCall` 只能作用于 Pending Call，产出 `[ToolCallStarted]`。
7. `SubmitToolResult` 只能作用于 Executing Call，产出 `[ToolCallCompleted]`。`SubmitToolResponse` 只能作用于 Waiting(ExternalResponse) Call，产出 `[ToolCallAnswered]`。
8. `SubmitToolFailure`（known）可以作用于 Pending 或 Executing Call：Pending 的已知失败使用空 Grant，Executing 的必须使用对应 Grant，产出 `[ToolCallFailed{Known}]`。`SubmitToolFailure`（unknown）只能作用于 Executing Call，产出 `[ToolCallFailed{Unknown}, RunEnded{RunFailed, effect_unknown}]`，`RunResult.Failure` 记录 `effect_unknown` 和对应 CallID；scanner 提交它时必须先有实现内部的失效执行记录。
9. `ApproveToolCall` 必须匹配目标 Waiting(Approval) Call 保存的 ResponseID 和 kind，产出 `[ToolCallApproved]`（Call 变为 Pending，Loop 随后执行）。`RejectToolCall` 同样必须匹配，产出 `[ToolCallFailed{Known, permission_denied}]`。日志记录的是结果事实，不是请求本身。
10. 一次响应只推进对应 Call，不能修改其他 Call；ResponseID 和 kind 必须匹配该 Call 保存的请求。响应 payload 的 digest 用于内容冲突检测，不需要等于请求 payload 的 digest。
11. 使 ToolStep 内最后一个 Call 到达可关闭终态的 command，其事实序列追加 `ToolStepClosed`。ToolStep 关闭前不能创建下一 ModelStep。
12. `CancelRun` 只能作用于非 terminal Run，产出 `[RunEnded{RunStopped, cancelled}]`；`StopRun` 只能记录 host-owned 非取消停止原因，目前为 `[RunEnded{RunStopped, step_limit}]`。
13. `AcceptInput` 只能作用于 active 且没有当前 Step 的 Run，且 `InputID` 不得与当前 `PendingInputs` 中任何输入重复；重复 ID 是 identity conflict，不能记录一个 Evolve 会丢弃的 no-op fact。接受后产出 `[InputAccepted]`。Planner 由 `PlanningHint.Inputs` 收到这些输入并在 `RequestPlan.InputIDs` 中明确消费它们；遗漏或伪造 ID 的 `PrepareModelRequest` 被拒绝。
14. `InitializeRun(RunID)` 只在 application admission 创建新 Run 时使用；它建立最小初始 `MachineState`（Revision=0），不包含 initial input、fixed model、limits 或产品配置。Application 随后通过正常 `Runtime.Commit` 提交首个 `AcceptInput`。

`InputID` 在一个 Run 内唯一。相同 `InputID` 和相同 payload 的重复 `AcceptInput` 是语义 no-op，Runtime 返回原已接受的事件组；相同 ID 携带不同 payload 返回冲突。Memoh 的 queue claim 仍负责防止同一个 queue item 被多个输入入口同时消费。

#### 3.7.2 Evolve 折叠表（事实 -> 状态）

`Evolve(state, event)` 对每种事实执行固定的机械折叠，不读产品配置或 Loop policy，不含 policy 分支：

| 事实 | 折叠 |
| --- | --- |
| ModelStepPrepared | 若 Current 非空则拒绝折叠（损坏日志不能覆盖活跃 Step）；否则设置 Current 为 Prepared ModelStep（StepRef.Digest 取事实携带的 BindingDigest）；`ModelSteps+1`；按事实中的 InputIDs 从 `PendingInputs` 移除 |
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

`Next` 的主要映射是：无当前 Step -> `NeedModelRequest(PlanningHint{Inputs: PendingInputs})`；Prepared ModelStep -> `StartModelCall`；ModelStep 正在 Executing -> `WaitForExecutionRecovery`；有 Pending Call 的 ToolStep -> `StartToolCalls`；没有 Pending 且存在 Waiting Call -> `WaitForResponse`（即使另有 Executing Call，也等待 response 或 execution wake）；没有 Pending/Waiting 但存在 Executing Call -> `WaitForExecutionRecovery`。Loop/host 可以在处理 `NeedModelRequest` 前按自己的 step budget 提交 `StopRun{step_limit}`。Runtime 接受 `StartModelExecution` 或 `StartToolCall` 后，在 CommitResult 中返回一次性 `ExecutionGrant`，Loop 使用该授权调用对应的 ModelInvoker 或 ExecutableTool。Model execution lease 失效后，Runtime 通过仅限 Runtime/recovery 使用的 `RecoverModelExecution` 把 ModelStep 恢复为 Prepared，Loop 才能再次 start。

这里的 `WaitForExecutionRecovery` 是一个统一等待结果：它既表示已有 execution 仍可能由原 Loop 持有，也表示该 execution 已失效、等待 Runtime recovery。公共 `LoopResult.Reason` 不暴露 owner、lease 或 Attempt 的细节。

## 4. Step 与 ToolCall progress

### 4.1 ModelStep

ModelStep 冻结：

```text
StepID
agent.ModelRequest 及其 digest
ModelRef
本次请求使用的 provider-neutral tool definitions 及 digest
与这些 definition 对应的 agent `ToolSpec`（包含 response policy）
`ToolsDigest`（按 provider definition 顺序覆盖 schema、Ref 和 policy）
执行状态：Prepared / Executing
reject counter（progress，不参与冻结 digest）
```

`SubmitModelResult` 被接受后，当前 ModelStep 立即被 ToolStep 替换（`ToolStepOpened`），或因没有 tool calls 而关闭 Run（`RunEnded`）；因此模型完成状态不作为当前 Step 状态保存。接受的结果保存在 `LastModelResult`、RunResult 和 application history 中。

一个 ModelStep 代表一次模型调用。Loop 默认调用 `ModelInvoker.Generate`；如果实现提供可选的 `StreamingModelInvoker`，Loop 可以用 `Stream` 发送实时 delta，但两条路径必须得到同一种边界 `sdk.ModelResult`，并在提交前冻结为 agent `ModelResult`；transport retry 不创建新的 Step。

Request Planner 生成完整 `sdk.Request` 后，Loop 先调用 `FreezeModelRequest` 得到 agent `ModelRequest`，再提交 `PrepareModelRequest`。Runtime 以 revision/CAS 或事务保证只接受一份冻结请求；新 ModelStep 的 StepID 由 RunID、command identity 和 frozen request digest 稳定派生。已经冻结的请求不受后来 queue、history 输入或 Planner 持有的 SDK 对象 mutation 影响。

Loop 在 `SubmitModelResult` 时只校验 tool-call ID 与顺序，并从匹配的冻结 `ToolSpec` 生成 `ToolCallBinding`。这里不调用 ExecutableTool；未知工具保留为 `DirectExecution`、空 definition digest 的 unresolved binding，应用级参数错误留到 `StartToolCalls`，作为 Pending Call 的已知失败处理。Runtime 只校验 binding 与冻结请求、冻结模型结果和 Step 身份的一致性，不重复解析工具目录。

模型响应的结构性 malformed（重复/错序 CallID、违反 provider 协议）使 Call 集合无法建立：Loop 不提交 `SubmitModelResult`，而以 start grant 提交 `RejectModelResult{Failure.Class: malformed_model_result, Disposition: ...}`。Decide 产出 `ModelStepRejected`（usage 累计、`Rejects` 加一、Step 回到 Prepared）或再追加 `RunEnded{RunFailed, malformed_model_result}`；由 Loop/host policy 根据自己的 `MalformedModelResultLimit` 选择 retry/fail-run disposition。被拒绝的结果不写入 `LastModelResult`，不创建 ToolStep。

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
一个 AgentCommand 如何安全地提交成一个 TransitionRecord，并对外产生 AgentEvent 流？
```

它不组装 prompt，不调用模型或工具，不定义 Machine 规则，不实现 queue policy。Runtime 可以在同一事务中更新 Memoh 的 history、queue projection、response record 和 outbox，但这些是 adapter 的原子投影，不是 Runtime 的语义职责。

`Commit` 必须在 authority 的临界区内调用共享的 `EvaluateCommit`（内部执行幂等/类别校验、`Machine.Decide` 和 versioned `Machine.Evolve`），因为“读取状态、验证 command、计算事实与新状态、保存完整 transition、更新 projection”不能在 durable 实现中拆成几个由 Loop 拼接的公开操作。这个必要的原子边界不等于 Runtime 拥有 Machine 规则；规则仍只有 agent 一份，也不等于 Runtime 拥有 Memoh 的产品数据。

因此 Runtime 的接口很小，但一次 `Commit` 的事务范围可以很大：它必须让一个 AgentCommand、产出的 `TransitionRecord` 及其必要的产品投影一起成功或一起失败；这不意味着 Runtime 获得了 history、queue 或 prompt 的所有权。`CommitResult.Events` 只是该 transition 的事件流视图，方便 Loop、UI 和 observability 消费；Runtime 的 authority storage 是完整 transition aggregate。

Run execution 状态的 source of truth 是 immutable `RunHeader` 加 `TransitionRecord` log；它只负责该次执行的恢复与审计，不替代 Session ES 的长期语义 authority；`RunHeader` 固化 Revision-0 initial state 及其 schema/digest/admission causation。MachineState 是必需的同事务 projection（execution cache）：提交验证与 Loop 执行从它读取，因此它必须与 transition log 在同一原子提交内更新，但它可以从 RunHeader 和日志重建。对任意 Revision N，状态必须等于 `RunHeader.InitialState` 按 `flatten(TransitionRecord[].Events)` 调用 versioned `Evolve` fold 到 N 的结果——这是可自动恢复的不变量，不是 halt 条件。

这个权威声明成立的三个稳定条件（本规范的规范性条款）：

1. Event ontology 稳定：Fact 词表 sealed，已发布 SchemaVersion 的事实结构永不修改，新增字段进入新版本。
2. Evolve 语义稳定：折叠是机械的（不读产品配置/Loop policy、无 IO），已发布版本的折叠语义与事件编码一起永久冻结；replay 通过 `EvolveVersion(SchemaVersion, state, fact)` 选择历史语义，会演进的决策语义全部在 Decide，其结果记录为事实。
3. 事实自包含：折叠一条事实所需的全部信息在事实自身与折叠前状态之内，不访问外部系统，不重新计算依赖当前代码版本的派生值（digest 一律在 Decide 时算好并携带在事实中）。
4. 持久化值归属稳定：Run event 和 MachineState 只保存 run-owned frozen values；任何来自 SDK/provider/application 的引用在进入 Runtime 前必须被 canonicalize + detach，返回给 caller 的 snapshot/event 也必须是独立副本。

Runtime 实现还必须在代码层面维护这些条件：`EvaluateCommit` 对 Decide 产出的 facts 做 `snapshotFact` 后再 fold/persist，并构造带 transition digest 的 `TransitionRecord`；`Load`、`CommitResult` 和 AlreadyApplied replay 返回的 snapshot/event 不得共享 authority 内部引用；MemoryRuntime 这类参考实现保存 accepted transition 后，从 stored transition 的 events fold 出新的 authority state，而不是直接保存调用栈里算出的 `decision.NewState`。durable adapter 可以用数据库事务替代 mutex，但不能把未冻结 SDK 对象、浅拷贝 snapshot 或 caller-owned bytes 写入事件表/状态表。

每 Run 维护一个不可丢弃的 revision 水位（watermark）：每次提交与 transition log 同步推进的单调计数，语义为"transition log 至少完整到此"。它是日志尾部完整性的末端见证；单个 transition 内部完整性由 `TransitionRecord.Events` 和 `TransitionDigest` 绑定，transition 之间用 Revision 连续性检测缺洞。水位是控制平面数据，不进入 MachineState，重建不清除它。

分歧仲裁规则固定为：

```text
snapshot 与 fold(transitions) 不一致，或 snapshot 缺失，且 transitionLog.maxRevision >= watermark
  -> transition log 为准，自动重建（纯 EvolveVersion 折叠，零副作用，不重放命令，不产生外部 effect），
     并记录一次重建事件供运维审计——实现正确时这条路径不触发，每次触发都意味着
     Evolve bug、越权写入或 snapshot 损坏真实发生过

transitionLog.maxRevision < watermark，或尾部 transition 的 digest/事件组不完整
  -> 日志尾部缺失/损坏，halt 该 Run——已接受的事实永久消失无法凭空恢复，
     继续推进会把丢失升级为错误的重复执行；恢复属于灾难恢复范畴（备份、复制）
```

主动 truncate snapshot 强制全量重建是合法运维操作（例如 MachineState 存储布局变更时替代迁移）。水位与日志同库整体回退（全量备份恢复）不在检测范围内：内部自洽的一致回退需要外部见证，v1 不做。

一个 Runtime 实例服务一个 Run；多个 Run 由上层创建多个 Runtime 实例。Run 的创建和身份分配由 Application admission 完成；admission 调用 `InitializeRun`，构造并原子保存 `RunHeader`。Runtime 从这个已存在的 header 开始，Loop 是对该 Run 的一次进程执行。

### 5.1.1 RunHeader

`RunHeader` 是正式持久协议，不是可从当前代码、当前默认值或当前产品配置重新生成的临时参数：

```go
type RunHeader struct {
    SchemaVersion       uint16
    RunID               RunID
    InitialStateVersion uint16
    InitialState        MachineState
    InitialStateDigest  es.Digest
    CausationID         es.CausationID // creating session/queue/application operation
    HeaderDigest        es.Digest
}
```

规范性要求：

1. `RunHeader` 创建后 immutable；普通 `Runtime.Commit` 不修改它。
2. `InitialState` 必须是 `InitializeRun(RunID)` 产生的最小 Revision-0 state，不包含 initial input、fixed model、limits、session history 或产品配置。
3. initial input 通过 Revision 1 的正常 `AcceptInput` transition 提交。
4. `InitialStateDigest` 绑定 initial state 的 canonical wire bytes；`HeaderDigest` 绑定 header 中除自身外的全部协议字段。
5. durable adapter 必须让 header 创建、Run identity admission 和必要的 application causation 记录原子或可幂等恢复。
6. local Run 上传/迁移时传输 `RunHeader + TransitionRecord log + referenced artifacts`；目标 runtime 先验证 header/log，再重建 projection，不能信任上传的 MachineState snapshot。

`RunHeader.CausationID` 只提供跨 domain 关联，不让 `agent/run` import `agent/session` 或 `agent/queue` 的具体 ID 类型。Application 可以维护 SessionID/EntryID/QueueItemID 到 causation ID 的映射。

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

Runtime 的语义范围仍然只有 authority 和 commit。Durable application adapter 为了保持 Run transition、MachineState projection、Session materialization outbox 和 artifact/usage projection 的一致性，可以在自己的数据库事务内一起写入这些投影；这不是 Runtime 对外暴露的通用业务 API，也不让 Runtime 获得 prompt、queue 或 session history 的所有权。

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
    // Opaque cross-domain lineage; Run Machine does not interpret it.
    CausationID   es.CausationID
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
    CausationID   es.CausationID // inherited from accepted command
    Digest        Digest // canonical digest of this fact
    Fact          Fact
}

// TransitionRecord is the atomic authority record for one accepted command.
// It binds the complete ordered event group for one Revision.
type TransitionRecord struct {
    SchemaVersion    uint16
    RunID            RunID
    Revision         uint64
    CommandID        CommandID
    CommandDigest    Digest
    Events           []AgentEvent // complete ordered event group for this revision
    TransitionDigest Digest       // digest of transition identity + complete event group
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
func BuildRunHeader(RunID, es.CausationID) (RunHeader, error)
func ValidateRunHeader(*RunHeader) error
func DigestRunHeader(*RunHeader) (es.Digest, error)
func BuildTransitionRecord([]AgentEvent) (TransitionRecord, error)
func ValidateTransitionRecord(*TransitionRecord) error
func DigestTransitionRecord(*TransitionRecord) (Digest, error)
func FoldTransitions(header RunHeader, records []TransitionRecord) (MachineState, uint64, error)
func DigestRequest(ModelRequest) (Digest, error)
func DigestToolDefinition(ToolDefinition) (Digest, error)
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

`ExecutionGrant` 是 Runtime 返回的 opaque capability，只用于证明当前 Loop 获得了执行许可；调用方只保存并原样传回，不依赖其内容。它不是 Step 的业务字段，不暴露 AttemptID、FenceToken、lease 或数据库类型，也不是用户认证凭证。Runtime 必须生成不可预测且绑定到单个 Step/Call 和当前执行所有者的值，并在完成 command 中校验它。`run.MemoryRuntime` 可以用 mutex 加随机 generation 实现它；durable adapter 可以用 owner/fence/lease 实现它。

`Attempt` 只表示一次进程对当前 ModelStep 或 ToolCall 的执行占用，不是 MachineState，也不是恢复边界。一个 Step 可以先后有多个 Attempt；旧 Attempt 失效后，新的 Loop 重新读取同一个 Step。MemoryRuntime 的 Attempt 是内存中的占用记录，durable adapter 的 Attempt 是私有 lease/fence 记录。

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
    Kind       DecisionKind // Apply | AlreadyApplied | Conflict | Stale | Terminal
    NewState   MachineState
    Events     []AgentEvent     // event-stream view of Transition.Events
    Transition TransitionRecord // authority aggregate for this commit
}

// grantValid/recoveryValid 由 Runtime 依据自己的 lease/occupancy 记录判定后传入；
// prior 是相同 (RunID, CommandID) 的已有 transition（若有）。
func EvaluateCommit(
    cur MachineState, curRevision uint64,
    prior *TransitionRecord,
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
4. 校验 BaseRevision、当前 Step、CallID 和 Grant。BaseRevision 只对 `PrepareModelRequest` 是硬校验——其 CommandID 由 Revision 派生，StepID 由该 CommandID 与 Decide 得到的 binding digest 派生，Revision 即它的并发控制，过期即返回 `ErrStaleRuntime`。其余 command 在 BaseRevision 过期时按类别前置条件基于当前状态重新评估：start（`StartModelExecution` 目标仍须为同一 Prepared ModelStep；`StartToolCall` 与 Pending 的已知失败目标 Call 仍须为 Pending，均空 Grant）；owner 完成（Executing Call 的完成/失败、`SubmitModelResult`/`SubmitModelFailure`/`RejectModelResult`，以及持有效 Model grant 的 `RecoverModelExecution`，以提交者仍持有对应有效 Grant 为条件）；system recovery（无 grant 的 `RecoverModelExecution` 和 scanner 的 Unknown，以 Runtime 自己的 recovery record 为条件）；ingress（approval/external response 目标 Call 仍须为对应 Waiting 且 ResponseID/kind 匹配；`AcceptInput` 要求 Run 仍 active 且没有当前 Step，连续多条输入互不拒绝；均空 Grant）；run-control（`CancelRun` 只要求 Run 非 terminal）。前置条件不满足时按具体原因返回 stale/terminal/冲突。start command 建立 grant/lease 的动作与状态提交属于同一个原子操作。
5. 调用 `Machine.Decide` 产出事实序列，逐个 `Machine.EvolveVersion` 折叠出新状态；为这次 transition 分配 `Revision = curRevision + 1`，事实按序获得 `Index = 0..k-1`，全部携带产生它们的 CommandID，再封装为带 `TransitionDigest` 的 `TransitionRecord`。
6. Runtime 在自己的原子边界内保存 MachineState、TransitionRecord 及需要一致的 Memoh projection；`CommitResult.Events` 返回该 transition 的 `Events` 视图。
7. 非重复 command 若目标 Run 已经 terminal，返回 `ErrRunTerminal`；迟到的 worker 结果不会重新打开 Run。其他提交成功后返回新 snapshot。

`EvaluateCommit` 用 `DecisionKind` 表达结果；`Runtime.Commit` 把非成功结果映射为对外错误：`DecisionConflict -> ErrCommandConflict`、`DecisionStale -> ErrStaleRuntime`、`DecisionTerminal -> ErrRunTerminal`。Loop 与 ingress 只依赖这三个错误值和 `CommitStatus`，不接触 DecisionKind。

新 Run admission 使用 `InitializeRun(RunID)` 建立最小 `MachineState`（Revision=0）；初始用户输入作为该 Run 的第一条 `AcceptInput` transition 进入日志。不存在持久化 `RunConfig` 或绕过 Commit 的 `RunSeed`。普通 Runtime 只实现已有 Run 的 `Load/Commit`，admission 路径负责创建 RunID、构造并持久化 `RunHeader`。

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

model request CommandID
  `PrepareModelRequest` 的 CommandID 由 RunID 和 BaseRevision 稳定派生；StepID 由 RunID、该 CommandID 和 frozen request/tool binding digest 派生。相同 revision 的并发 planner 因此竞争同一个 identity：内容相同 replay，内容不同 conflict。

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

canonical 编码和 digest 函数由 agent 提供；Memoh 只保存和比较结果，不重新实现排序或编码。编码必须包含 sealed command/fact discriminator、按声明顺序编码有序 slice、对 map key 排序，并对 `CanonicalJSON` 原样写入其 canonical bytes；重复 object key、trailing data、invalid UTF-8、escaped lone surrogate 一律在构造 `CanonicalJSON` 或 decode wire document 时拒绝，不能让 `encoding/json` 的 replacement behavior 把不同输入合并为同一 digest；不把 `Digest`、BaseRevision、Revision、Index 或 ExecutionGrant 编入 digest。

已发布 `SchemaVersion` 的 canonical 编码和 digest 规则永久冻结；字段增删只能进入新的 SchemaVersion，旧事件按其自带版本校验。同一个 Run 不允许由写入不同 SchemaVersion 的进程混跑：升级窗口内先全量部署可读写新版本的代码，再开始写入新版本；否则同一 command 的重放会因编码不同被误判为 `ErrCommandConflict`。

CommandEnvelope 和 AgentEvent 的 `SchemaVersion`、`Type`、`CausationID` 是 Run 持久化协议字段；Type 必须与 sealed AgentCommand/Fact 的具体变体一致，未知版本或类型直接拒绝。`CausationID` 是跨 domain 的 opaque lineage：Application 可将 session entry、queue claim、前一 Run event 或 system recovery record 关联到 command；Run Machine 不解释其内容。接受 command 后，其产出的 AgentEvent 继承该 causation ID。`agent/run` 必须提供正式 wire codec：decode 时先 canonicalize 整个 document，再读 `Type`，恢复具体 command/fact variant，校验 command/fact digest，并要求 decoded value 重新 canonical marshal 后与输入 canonical document 等价；不能依赖 `encoding/json` 自动反序列化 interface 字段，也不能接受 duplicate key、大小写模糊字段名或其他会被 Go decoder 合并/宽容的形态。`DigestCommand`/`DigestFact` 对 `SchemaVersion`、`Type`、causation 和内容做 canonical digest，但不把 `Digest` 字段自身纳入摘要，保证 durable scanner、MemoryRuntime 和不同进程使用同一身份规则。`Revision` 只用于 authority 的 CAS，不进入任何 digest。

Evolve 的折叠语义与事件编码同属永久兼容契约：已发布 SchemaVersion 的事件必须永远能被折叠出与写入当时相同的状态。conformance kit 为每个已发布 SchemaVersion 冻结 golden transition stream 与对应的状态字节，任何 versioned Evolve 实现变更都必须通过全部历史版本的 golden 校验。

### 5.6 Runtime 不拥有 Planner

agent 只声明供 Loop 依赖注入的 `RequestPlanner` port；Planner 的实现和语义属于
application/Memoh。Runtime 不调用这个 port，也不读取 Planner 的 context：

```go
type RequestPlanner interface {
    Plan(context.Context, PlanningHint) (RequestPlan, error)
}

type RequestPlan struct {
    Model          ModelRef
    Request        sdk.Request // boundary value; Loop freezes to ModelRequest before PrepareModelRequest
    InputIDs       []InputID
    Tools          []ToolSpec   // agent frozen ToolSpec/ToolDefinition sidecars
    PlanningToken  PlanningToken // application-owned freshness token
}

type PlanningHint struct {
    RunID       RunID
    Model       ModelRef
    SourceStep  StepID
    Inputs      []AgentInput
}
```

Planner 可以读取 application 自己的 history、memory、workspace 和 queue-safe 输入，但不直接修改 Runtime。它返回 `RequestPlan`（完整边界 `sdk.Request`、ModelRef、已消费的 InputID 集合和 application-owned `PlanningToken`）；Loop 先 freeze 成 agent `ModelRequest`，随后提交 `PrepareModelRequest`。Memoh 在事务外构造请求，在提交时用自己的 context revision/CAS 和 create-if-absent 确保并发 Planner 只冻结一份结果。`PlanningToken` 只是供 adapter 验证 planner 输入是否新鲜的 opaque token；agent 不解释其内容，也不把它当成 authority revision。

Planner 所需的 history 必须由宿主提供：Memoh 从 durable history projection 读取；in-process
调用者可以用一个简单的内存 history projection 或 planner 自己持有的会话上下文。Runtime
不负责把 AgentEvent 推送给 Planner，也不因此新增 history/store 方法。

`PrepareModelRequest` 必须使用 Planner 开始前 Load 得到的 BaseRevision，并按当前顺序携带完整的 `PendingInputs` `InputIDs` 集合。相同 RunID 和 Revision 的并发 Planner 使用同一个 `DeriveModelRequestCommandID`：相同请求得到 `CommitAlreadyApplied`，不同请求得到 `ErrCommandConflict`；如果 Planner 在新的 Revision 上重试，则生成新的 CommandID。Memoh adapter 同时检查 `PlanningToken` 的 context revision，后到者不能覆盖已经冻结的请求。

Loop 校验 `RequestPlan.Model`（若提供）与 frozen `ModelRequest.Model` 一致；Runtime 通过共享 Decide 规则再次校验 `PrepareModelRequest.Model` 与 `PrepareModelRequest.Request.Model` 一致。固定模型限制若存在，属于 Memoh/host policy，不进入 agent MachineState。Planner 的 context 一致性由 Memoh 在自己的 queue-safe admission/planning 边界保证，不由 agent 解释。

### 5.7 外部 response

agent 不提供第三个 Loop 操作。Memoh/application 的 response ingress：

1. 验证用户权限、Run/Step/Call 身份和 payload。
2. 读取 authority snapshot，确认目标 Call 仍为 Waiting；这个 ingress 读取不取得 Loop 的执行租约。
3. 用 `ResponseRequest.RunID/StepID/CallID/ID` 路由响应。将 approval 转成
   `ApproveToolCall`/`RejectToolCall`，将外部结果转成 `SubmitToolResponse`；
   `ResponseRequest.RequestDigest` 是原请求的摘要，用户决定或答案的摘要单独放在
   command 的 `ResponseDigest`，由 `DigestToolResponseDecision` / `DigestToolResponsePayload`
   计算，并用 `DeriveResponseCommandID` 生成稳定 CommandID。
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

steer 必须进入下一个 ModelStep 的规划上下文，而不是延迟到更晚的边界。Loop 在 boundary 处的 Plan/Prepare 提交与 Application 的 queue 仲裁存在竞态；durable adapter 用以下 gate 消除它：处理 `PrepareModelRequest` 的同一事务内检查是否存在 eligible 的 steer item，存在时不接受该次 Prepare，先在事务内应用对应的 `AcceptInput`（Revision 递增，Prepare 按 `ErrStaleRuntime` 返回）。Loop 重新 Load 后由 `PlanningHint.Inputs` 携带该输入重新规划。这条 gate 是 Application orchestration 行为，不进入 Run Machine 规则；in-process harness 没有 queue 时由宿主在 Loop 空闲边界提交。

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
        frozenRequest, err := FreezeModelRequest(plan.Request)
        if err != nil:
          return err
        requestDigest, err := DigestRequest(frozenRequest)
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
            StepID: stepID, Model: plan.Model, Request: frozenRequest,
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
          sdkRequest, err := modelStep.Request.SDK()
          if err != nil:
            return err
          modelResult, invokeErr := invokeModel(invoker, workerCtx, sdkRequest, l.Streaming, events)
          if invokeErr != nil and worker context was cancelled:
            completion = RecoverModelExecution{StepID: stepID}
          else if invokeErr != nil:
            completion = SubmitModelFailure{StepID: stepID, Failure: StepFailureForModel(invokeErr)}
          else:
            bindings, bindErr := bindToolCalls(modelResult, modelStep.Request, modelStep.Tools)
            if bindErr != nil:
              completion = RejectModelResult{StepID: stepID, Usage: UsageFromSDK(modelResult.Usage),
                Failure: StepFailure{Class: FailureMalformedModel, Message: bindErr.Error()}}
            else:
              frozenResult, err := FreezeModelResult(modelResult)
              if err != nil:
                return err
              completion = SubmitModelResult{StepID: stepID, Result: frozenResult, Calls: bindings}
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
            frozenDefinition, freezeErr := FreezeToolDefinition(tool.Definition())
            if freezeErr != nil or tool.Ref() != call.ToolRef or DigestToolDefinition(frozenDefinition) != call.DefinitionDigest:
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
并向 EventSink 发送 delta，否则调用 `Generate`；两条路径都只返回一个完整边界 `sdk.ModelResult`，Loop 在提交前用 `FreezeModelResult` 转为 agent `ModelResult`。

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
| 结构性 malformed 模型结果 | 提交 `RejectModelResult`；Loop/host 通过 Disposition 选择回到 Prepared 重试或同 transition RunFailed。 |
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

固定模型、model-step budget 和 malformed retry budget 不进入 MachineState。Loop/host 可以使用非持久化 `ExecutionPolicy`：`ModelStepLimit=0` 表示无限，达到正数上限时在下一次 planning 前提交 `StopRun{ReasonStepLimit}`；`MalformedModelResultLimit=0` 归一化为默认值 `2`，Loop 根据当前 `ModelStep.Rejects` 选择 `RejectModelResult` 的 retry/fail-run disposition。Memoh 也可以在自己的 host policy 中实现同样逻辑。

## 7. Tool、approval、response 和 MCP

### 7.1 Tool contract

`sdk.ToolDefinition` 只描述 provider 可发现的 schema，不依赖 run，也不携带 `ResponsePolicy`。`run.ExecutableTool.Definition()` 可以返回 SDK 边界类型，Loop 必须先 `FreezeToolDefinition` 再计算 `DigestToolDefinition` 或写入 `ToolSpec`；MachineState/AgentEvent 中保存的是 run `ToolDefinition`。`run.ExecutableTool` 描述应用如何执行工具并提供 response policy；模型返回后，run 用 `ToolRef`、definition digest 和 policy 生成冻结的 `ToolCallBinding`。恢复时 schema、工具版本或 policy 不匹配都不能静默换版本。

```go
type ExecutableTool interface {
    Ref() ToolRef
    Definition() sdk.ToolDefinition
    ResponsePolicy() ResponsePolicy
    ValidateArguments(CanonicalJSON) error
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

## 8. 同一 Run ES 的 Runtime implementations

### 8.1 `run.MemoryRuntime`

`run.MemoryRuntime` 是 in-process reference runtime：

```text
RunHeader + mutex + MachineState projection + TransitionRecord log
```

```text
Load
  在锁内返回当前 MachineState 和 Revision

Commit
  在锁内判定本进程 grant 有效性，调用共享 EvaluateCommit，
  原子保存完整 TransitionRecord、projection、watermark 和幂等索引；
  接受 start command 时返回本进程的 opaque ExecutionGrant
```

它可以省掉 durable **机制**：

```text
lease / fencing / distributed claim / worker heartbeat
DB transaction / durable outbox / crash 后跨机器 recovery
persistent execution queue
```

但不能省掉 durable **语义**：

```text
RunHeader
TransitionRecord complete append
AgentCommand -> Decide -> Fact[] -> EvolveVersion
execution grant/start barrier
idempotency
replay/fold invariants
```

因此 in-process 和 durable 不是两种 Agent，而是同一 `run.Runtime` contract 的不同实现。Local Run 可以导出：

```text
RunHeader + TransitionRecord log + referenced artifacts
```

供 durable adapter 验证、replay 后继续执行。

MemoryRuntime 不保存 Session history、context、queue、long-term memory 或 product artifacts。需要多轮上下文的 in-process harness 在 Runtime 外组合 `session` memory store、可选 `queue` memory store、materializer 和 RequestPlanner；这不是把产品 memory 偷塞进 Runtime。

### 8.2 Durable Run runtime adapter

Durable adapter 位于 Application/Product（例如 Memoh），通过 PostgreSQL transaction/CAS 或等价存储实现同一个 `run.Runtime`：

```text
Load
  读取 RunHeader、MachineState projection、revision 和必要的私有 control-plane metadata
  不创建 Attempt，不取得 lease；control-plane 不出现在 RuntimeSnapshot

Commit
  判定私有 owner/fence/lease/recovery record 的有效性
  对 StartModelExecution/StartToolCall 在同一事务内建立 Attempt/lease
  调用共享 EvaluateCommit
  原子保存 TransitionRecord、MachineState projection、watermark、idempotency index、
  Session/artifact/usage materialization outbox 和必要的 queue claim outcome
```

Attempt、owner、fence、lease、outbox row 和数据库 schema 不进入 `run.MachineState`。它们只保证多个 Loop attempt 不会同时取得同一个 Step/Call 的执行权。snapshot storage schema 与 Run event schema 独立版本化；snapshot 可被 truncate 后从 `RunHeader + TransitionRecord log` 重建，watermark 不得随 rebuild 清除。

Durable worker 实例可绑定 owner identity；只有当前 owner 能提交其取得的 start/completion/recovery。response、cancel、host StopRun 使用不带 worker grant 的 ingress/control adapter。lease/recovery scanner 必须通过正常 command + `EvaluateCommit` 提交 Unknown 或 Model recovery，不能直接改写 MachineState。

### 8.3 外部 effect 的保证

```text
AgentCommand commit -> TransitionRecord
  effectively-once（identity + digest）

Model call
  lease 失效后可使用同一冻结 request 重复；不同 attempt 可能返回不同结果，
  只有先被 Runtime 接受的结果推进 Run

Tool effect
  start command 提交后才发生，但结果可能在提交前丢失
```

Run core 无法判断 Unknown effect 是否已经发生，也不假设支付系统或其他外部系统提供查询接口。因此 Unknown 保守地终止当前 Run；如果产品需要继续，只能创建新的 Run。非幂等工具不能由 Runtime 获得 exactly-once 保证。

## 9. Events 和 context

### 9.1 Canonical Event Plane

状态变化的提交路径固定为：

```text
Loop / response ingress
  -> AgentCommand
  -> Runtime.Commit (EvaluateCommit: 幂等/类别校验 + Machine.Decide + Machine.EvolveVersion)
  -> MachineState + TransitionRecord in one atomic boundary
  -> EventSink / replay / projection / OTel
```

`AgentCommand` 表示“希望发生的状态变化”；`AgentEvent` 表示“authority 已接受的事实”，`TransitionRecord` 是持久化的 authority aggregate。一个接受的 command 构成一次 transition，产出一个或多个事实，并以完整 `TransitionRecord` 原子保存。AgentEvent 必须具备：

```text
RunID + (Revision, Index)      全序身份；Revision 是 transition 计数，Index 是组内序
CommandID                      产生这次 transition 的 command
Digest                         该事实内容的 canonical digest
SchemaVersion + Type           wire 兼容和 sealed fact discriminator
Fact                           已接受的事实内容
```

RunHeader + TransitionRecord log 是 Run execution source of truth；MachineState 是必需的同事务 projection（§5.1）。Runtime 必须把 transition log、snapshot、水位和需要一致的 Application materialization outbox 放在同一事务或锁边界。Durable adapter 必须保留完整 TransitionRecord，使其可以按 RunID/Revision replay；MemoryRuntime 可以只在进程内保留同样的记录。公共 `Runtime` 不增加 replay 方法，读取由实现或 application projection 提供。

Replay 按 RunID/Revision 取出 TransitionRecord，从经 `ValidateRunHeader` 验证的 `RunHeader.InitialState`（Revision=0）开始依次展开其中的 AgentEvent，并调用对应 `SchemaVersion` 的 `Machine.EvolveVersion` 折叠。折叠只依赖 Evolve，不重新运行 Decide——决策结果已经记录在事实里，Machine 决策规则的演进不影响历史事件的折叠；折叠不产生任何外部 effect。仲裁按 §5.1 的规则：transition log 完整（maxRevision >= watermark 且每条 transition digest 正确）时日志为准，snapshot 分歧或缺失自动重建并记录重建事件；日志尾部低于水位或尾部 transition 不完整时 halt。事件流内部的 RunID 不匹配、SchemaVersion/Type 不支持、同一 transition 的 CommandID/CommandDigest 不一致、Revision/Index 缺洞、fact digest 或 transition digest 不匹配同样按日志损坏处理，halt 该 Run。

Replay 的起点是 `RunHeader` 中已建立并持久化的最小 `MachineState`；初始用户输入通过 `AcceptInput` transition 重放。Session/Queue admission lineage 由各自 domain 记录，并通过 causation/provenance 与 RunHeader 关联。

canonical event 只记录影响语义状态、恢复和审计的已接受事实：Step 的建立/启动/恢复/关闭、模型结果的接受与拒绝、工具结果、响应、active Run 的输入接受和 Run terminal（§3.6 的 Fact 词表）。模型文本 delta、工具 stdout、下载百分比和其他瞬时 progress 不进入 AgentEvent；它们仍可在提交前通过 EventSink 发送 provisional observation。

### 9.2 EventSink

EventSink 是实时观察出口，不是 canonical source。Loop 在收到 `CommitResult.Events` 后可以发送对应的 committed observation，也可以发送不改变权威状态的 provisional observation：

```text
ModelTextDelta、ToolProgress
  可以在完成提交前发送，都是 provisional

ToolStarted
  只能在 start command Accepted 后发送

ToolCompleted、Run terminal
  先由 Runtime Commit；Loop 随后发送观察事件，durable adapter 的
  Session/artifact/usage materialization outbox 由同一事务保存对应事实
```

EventSink 丢失、重复或来自旧 Attempt 都不改变 MachineState 或 AgentEvent。客户端出现 gap 时从 durable AgentEvent、snapshot 或最终 `RunResult` 重建。Loop 默认忽略观察通道错误，不因此重试模型/工具。

并行工具的观察事件通过 `CallID` 关联到具体 ToolCall；模型事件的 `CallID` 为空。
`Event.Sequence` 只在一次观察流内单调递增，不是 `AgentEvent.Revision`，也不参加 canonical digest/idempotency。`Durability` 仅说明这条观察是否对应已提交事实；它不能替代 AgentEvent 的 identity/digest。

### 9.3 context transform

Request Planner 属于 Application。它可以在事务外读取 context，但 durable runtime adapter
必须以版本检查和 create-if-absent 冻结生成的 request。已冻结 ModelStep 不受后来输入影响。

这里的版本检查由 durable runtime adapter 实现：`run.Runtime` 只看到带有 `BaseRevision`
的 `PrepareModelRequest`，不理解 Memoh 的 context revision，也不读取 queue 或 history。

### 9.4 provider transport

一次 `sdk.Generate` 或 `sdk.Stream` 对应一次逻辑 provider request。transport retry 在 sdk/provider client 内部发生；run 不记录它，也不为它创建新的 Step。

## 10. Application queue、session 和恢复

### 10.1 queue 归属

steer/follow-up 的 queue 数据结构、accepted order、重排、claim、apply、取消和 admission 全部属于 Application queue/session policy。两种 queue 可以共享稳定 item reference、accepted sequence、order version、取消状态和 claim provenance；消费策略不同。被选中的 item 在交给 Run core 前转换为只有 `InputID` 和 payload 的 `AgentInput`，Application 私下保留 item reference 与 claim provenance 的映射：

```text
steer      优先进入当前 eligible boundary；若当前 Run 已 terminal，则按 session policy 创建 continuation Run
follow-up  当前 Run 自然结束后创建新的 Run
```

active Run 的 steer 通过 `NextStep(input)` 生成 `AcceptInput` command；terminal Run 的 follow-up 由 Application admission 创建新 Run，再对新 Run 提交首个 `AcceptInput`。Run core 不接收 queue item、priority、order 或 claim。

重排必须带 order version；过期版本、未知 item、重复 item 和越过已 claim item 的操作都拒绝。

### 10.2 queue-safe boundary

Application 只在以下 boundary 仲裁 queue：

```text
ModelStep 完成且没有 tool calls
ToolStep 自动关闭
```

ModelStep 执行中、ToolStep 有 Pending/Executing/Waiting Call 时，不消费新的 queue 输入。queue action、对应 `InputAccepted` transition 以及 claim provenance 在 Application transaction 或可幂等 outbox 中一起提交，已 claim item 不能回退或越过。

没有 tool calls 的 ModelStep 会使当前 Run 到达 `RunCompleted`。Application 可以在这个 queue-safe boundary 消费 steer 并创建 follow-up Run；这不改变已经完成的 Run，也不把 queue policy 放进 Machine。

### 10.3 Session settled 与 follow-up admission

具体产品可以定义 session settled/busy 规则，例如 terminal Run 不等于 session settled，或 admission-active follow-up Run 存在时 session 仍 busy。这些是 `agent/session` 上层 policy，不是 Run Machine 状态。

follow-up 可以在新 Run 的第一个 ModelStep 之前完成 durable admission claim；这是 Session/Queue operation，不是 Loop 中间读取 queue。Application outbox/scanner 可以唤醒新 Run；admission 用 `InitializeRun` 创建最小 state，并把 follow-up 输入提交为首个 `AcceptInput` transition。

### 10.4 多 response 恢复

```text
ToolStep T1
  A Completed
  B Waiting(response=101)
  C Waiting(response=102)
  D Pending
```

response 101 只完成 B；D 仍可执行，不必等待 C。response 102 再完成 C；D 完成后 Machine 自动关闭 T1，并允许下一 ModelStep。每个 response 有自己的 row、CommandID 和 wake，不再受旧协议“一个 deferred 只能保存一个 approval”的限制。

## 11. Domain 拆分与实施顺序

当前 root `agent` 包是 Run execution ES 的实现雏形；它不应继续吸收 Session、Queue、Artifact、Context 或产品 orchestration。拆包以 target package 为准，不为尚未合并的 API 保留 agent-level compatibility façade。

### 阶段 A：抽取 `agent/es`

1. 从现有 Run codec/transition/rebuild 中抽出不认识 run 语义的机制：canonical record encoding、payload/record digest hook、revision/index 校验、complete-record validation 和 generic fold runner。
2. `es` 不 import `run`、`session`、`queue`。
3. Run 的 `TransitionRecord` 先适配/包装 `es.Record`；保持 Run-specific `CommandID`、`CommandDigest`、sealed Fact codec 在 `run`。
4. 为 `es` 添加独立 conformance：partial record tail、revision/index gap、digest mismatch、schema/type mismatch、canonical JSON ambiguity。

### 阶段 B：移动现有 core 到 `agent/run`

1. 将现有 root `agent` 中的 Machine、Loop、Runtime、MemoryRuntime、model data、tool contract、codec、runtimetest 移到 `agent/run`。
2. 删除 `RunConfig`、`RunSeed`、`NextRun`、旧 `Initialize(run, config, seed)` 以及对应 codec/digest；目标 API 只有 `InitializeRun(runID)`，初始输入通过 `AcceptInput` transition。
3. 把 `RunHeader` 实现为正式 execution authority record；rebuild/fold API 以 header 为起点。
4. 让 `MemoryRuntime` 成为同一 Run ES 的最轻 reference runtime，而不是另一种 agent；它可以没有 lease/DB/heartbeat，但不能跳过 execution event semantics。
5. 更新 import path、examples、conformance 和 golden streams；此时尚未合并，不保留 root `agent` compatibility wrapper。

### 阶段 C：实现 `agent/session`

1. 定义 generic append-only Session store、entry envelope、head CAS、parent/fork lineage、schema/digest/causation。
2. 先提供 memory store 和 replay/fork conformance；不在 package 内写死 MessageAdded/Compact/Artifact ontology。
3. Application 定义 session event，并实现 Run event -> Session event 的 materializer。

### 阶段 D：实现 `agent/queue`

1. 定义 generic enqueue/claim/ack/release/dedup/visibility contract 与 memory implementation。
2. `SteerItem`、`FollowUpItem`、`RunQueueItem` 保持在 Application payload/policy。
3. 不将 queue state 或 claim identity 加入 `run.MachineState`。

### 阶段 E：simple in-process harness

`agent/harness`（或 application example package）组合：

```text
session memory store
+ queue memory store（可选）
+ run.MemoryRuntime
+ run.Loop
+ in-memory EventSink
+ application RequestPlanner/materializer
```

它不是第二个 Runtime interface，不复制 Run Machine，不把 chat history 塞进 `MemoryRuntime`。它用于 local example、test、prototype，以及证明 local/durable 是同一 Run ES semantics 的不同 runtime implementation。

### 阶段 F：durable application adapter

Durable adapter 在 Application/Memoh 实现：

1. 持久化 `RunHeader`、TransitionRecord log、MachineState projection 和 watermark。
2. 用 transaction/CAS 实现 `run.Runtime.Load/Commit`，私有实现 owner/fence/lease/Attempt/recovery。
3. 将 Run transition、Session materialization outbox、artifact/usage projection 和 finalization state 放在同一事务，或使用可幂等 inbox/outbox 恢复；Application 要把每一个 Run 的 lifecycle 记录为 Session event。
4. Session 已追加 `RunFinalized`（或存在与之事务耦合的 marker）后才 archive/GC Run log；Session/Artifact/Usage 的长期语义不依赖保留旧 Run log。

## 12. Cross-domain orchestration contract

Application 是唯一允许同时依赖 `run`、`session`、`queue`、artifact/context/memory 的层。它负责：

```text
Session semantic history + artifacts + memory + compact
    -> ContextView
    -> RequestPlanner
    -> run.PrepareModelRequest

Session RunAdmitted
    -> new RunHeader + first AcceptInput
    -> run TransitionRecord (short-lived execution trace)
    -> durable materializer/outbox
    -> Session RunCompleted + messages/results/artifact refs/usage projection
    -> Session RunFinalized
    -> Run log archive / GC is now permitted

queue claim
    -> queue-safe admission policy
    -> Session lifecycle entry + run.AcceptInput or new RunHeader
```

每个 ModelStep 应记录足以审计输入来源的 `ContextManifest`（位置可为 `ModelStepPrepared` 中的 immutable reference 或 companion artifact）：

```text
ContextManifest {
    session_revision
    artifact_refs
    memory_revision
    compact_revision
}
```

它不是完整 prompt/history 的重复副本，而是 provenance。下一 Run 的 context 必须由 Session ES、Artifacts、Memory/Compact projection 构造，不能依赖读取已完成 Run 的 execution log。

## 13. 并行工作边界

| 工作 | 目标包/层 | 依赖 |
| --- | --- | --- |
| sdk Request/ModelResult/stream | `sdk` | provider adapter |
| generic record/digest/fold mechanism | `agent/es` | jsonstable |
| Run Machine、Loop、Runtime、MemoryRuntime | `agent/run` | es + sdk |
| RunHeader、run codec、runtimetest | `agent/run` | es |
| generic Session store/memory store/fork conformance | `agent/session` | es |
| generic Queue store/memory store/claim conformance | `agent/queue` | optional es digest types |
| Session event ontology/materializer/context planner | Application/Product | session + run + artifacts |
| Steer/follow-up/run queue policy | Application/Product | queue + session + run |
| durable runtime, DB schema, owner/fence/lease/outbox | Application/Memoh | run contract |
| simple in-process harness | `agent/harness` or examples | run + session + queue |

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
terminal follow-up -> Application admission creates new Run with InitializeRun, then AcceptInput(input), without mutating old Run
PrepareModelRequest -> [ModelStepPrepared] -> ModelStep
ModelExecuting lease recovery -> same frozen ModelStep can start again
SubmitModelResult 有 tools -> [ModelStepCompleted, ToolStepOpened]，保存完整 Call set
SubmitModelResult 无 tools -> [ModelStepCompleted, RunEnded{RunCompleted}]，并返回该 ModelResult
结构性 malformed -> RejectModelResult -> [ModelStepRejected]，Step 回到 Prepared，usage 已累计
RejectModelResult{Disposition: FailRun} -> [ModelStepRejected, RunEnded{RunFailed, malformed_model_result}]
参数无法解析的单个 Call -> Pending，start 前以 invalid_arguments 已知失败关闭
unknown ToolRef/invalid arguments -> Pending Call 的已知失败，不提交 start
approval approved -> [ToolCallApproved] -> Pending -> start -> tool execute
approval rejected -> [ToolCallFailed{Known, permission_denied}]
多个 Pending 并行；Waiting 不阻塞其他 Pending
Waiting 与 Executing 并存时，response 和 execution wake 都有效
response 只推进对应 Waiting Call
Waiting result carries RunID/StepID/CallID/ResponseID for response routing
最后一个 Call terminal -> [..., ToolStepClosed]；host step limit 在下一 planning 边界提交 StopRun(step_limit)
RunEnded 只能是事实序列的最后一个事实
已知失败进入下一次模型上下文
Unknown -> [ToolCallFailed{Unknown}, RunEnded{RunFailed, effect_unknown}]，不创建下一 ModelStep
Pending + Executing 并存时不误关 ToolStep
RunStopped 与 worker cancellation 区分
外层 ctx 取消：模型执行以 ModelStepRecovered 释放，工具 worker 运行到结束后 Loop 返回 ctx.Err()
MachineState.Usage 逐字段累计 ModelStepCompleted 与 ModelStepRejected；terminal 时复制到 RunResult.Usage
Decide 拒绝时不产出部分事实；接受时事实组与 MachineState 原子提交
Evolve 不读产品配置/Loop policy、无 IO；对 Decide 产出的全部事实全定义
TransitionRecord 按 RunID/Revision 可 replay，内部 AgentEvent 按 (Revision, Index) 保持事件流顺序，重复提交不产生第二组
replay 只经 EvolveVersion 折叠，不重新运行 Decide；golden transition stream 折叠出冻结的状态字节
EventSink provisional/committed 发射点
并行 EventSink 事件包含 CallID，Waiting result 可路由到目标 Call
Streaming=true 但 invoker 不支持 streaming -> Generate fallback
```

### 14.3 Runtime conformance

conformance 测试由 `agent/run` 以可运行测试包（`agent/run/runtimetest`）交付；MemoryRuntime 与任意 durable runtime adapter 直接运行同一套件，不各自转写矩阵：

```text
same CommandID + digest -> CommitAlreadyApplied + 原事件组
same CommandID + different digest -> ErrCommandConflict
AlreadyApplied 不重新运行 Decide；事件组逐字节等于首次提交
并发 Commit 的 Revision/CAS 行为
同一 Run/Revision 的并发 Planner：相同请求 AlreadyApplied，不同请求 CommandConflict
Planner InputIDs 必须完整匹配 PendingInputs；Tools/ToolsDigest 与 frozen ModelRequest 一一对应
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
Loop/host model-step limit 通过 StopRun(step_limit) 形成事实，不进入 MachineState
RunStopped/RunFailed 保留最近已接受的 ModelResult
Cancel 与 Unknown 的提交先后决定终态
CancelRun 在过期 BaseRevision 上对非 terminal Run 重新评估
RejectModelResult 必须带有效 Model grant；AlreadyApplied 重放不重复累计 usage
持久化状态与按 EvolveVersion 折叠的 TransitionRecord log 一致；snapshot 分歧或缺失且日志完整 -> 自动重建并报告，重建不改变健康状态
transition log 尾部低于 revision 水位或尾部 transition 不完整 -> ErrLogTruncated/log damage，halt 该 Run
TransitionRecord 内部 Revision/Index 缺洞、事实 digest 或 transition digest 不匹配 -> fold 拒绝，按日志损坏处理
ModelStepPrepared/ToolStepOpened 自包含：携带的 digest 折叠后可重现 Step 身份
golden transition stream：固定 v1 命令序列折叠出冻结的状态字节
```

### 14.4 `agent/es`

```text
record 内 event 的 stream/revision/index/type/payload digest 一致性
record digest 覆盖完整 event group，partial tail 必须拒绝
revision gap、index gap、重复/冲突 identity、schema/type mismatch 拒绝
canonical JSON duplicate key/trailing data/invalid UTF-8/lone surrogate 拒绝
generic FoldRecords 不运行 domain Decide 或任何 IO
```

### 14.5 `agent/session` 与 `agent/queue`

```text
session append/head CAS/replay order
branch/fork parent lineage
相同 causation/source event 不重复 materialize
queue enqueue/dedup/claim/ack/release/visibility
过期 claim 不允许 ack；重复 claim/ack 幂等或明确冲突
SteerItem/FollowUpItem 只作为 application payload，不污染 queue core
```

### 14.6 Durable application integration

```text
queue FIFO、accepted-order reorder、typed ID isolation
assigned follow-up 只由正确的 admission claim
RunHeader、TransitionRecord、MachineState projection 与 watermark 一致
Session materialization、artifact/usage outbox 与 Run transition 原子或可幂等恢复
assistant tool-call 和 tool result 只 materialize 一次
多 response rows 与逐次 wake/idempotency
lease expiry/recovery/unknown outcome
eligible steer 存在时 Prepare 在同一事务内被拒绝，AcceptInput 先应用，重新规划携带该输入
并行 Call 中一个 Unknown 后撤销其他 grant，迟到结果不改变终态
terminal Run 与 session settled 分离
RunEnded 后未写入 Session RunFinalized 不允许 archive/GC
Session RunFinalized 后 Session/Artifact/Usage 仍可完整构造下一 Run context
EventSink gap 后可由 durable snapshot 对账
```

## 15. Memoh queue spec 的后续改写

本次不编辑 `session-runtime-steer-followup.md`。后续应按以下边界修订：

Application 的 queue/session/admission 语义属于各自 domain；Application loop host 只组装 Request Planner、`run.Loop` 和 durable runtime adapter，不包含第二套多步执行算法。queue 仲裁只发生在 queue-safe boundary，并与对应 Run command/transition 在 application transaction 或可幂等 outbox 中提交。Step 提交使用 ModelStep、ToolStep 以及逐 Call progress/response 记录；每次 response 只推进对应 Call。具体产品的 branch/claim/recovery 语义保持在 Application，不进入 `agent/run`。

## 16. 实施前置条件

1. Durable Application adapter 增加 ToolCall progress、response set、event idempotency、Run→Session/artifact/usage outbox projection 和 finalization marker。
2. Durable adapter 冻结内部 Attempt、owner、fence、lease 和 recovery grace 规则；这些不进入 `run` public API。
3. 工具失败不由 Run core 调度 retry timer；已知失败交给下一次模型，未知结果终止当前 Run。非幂等外部 effect 只能承诺 at-least-once。
4. Request Planner 必须能从已提交的 Session/context projection 构造完整、可冻结的边界 `sdk.Request`，由 Loop freeze 为 `run.ModelRequest`。

## 17. 待确认决策

实现前仍需确认：

1. queue capacity、expiry 和产品授权是否进入 Application queue policy。
2. breaking release 版本和 durable protocol upgrade window。
3. EventSink payload schema，以及是否需要在 durable outbox 中加入跨进程 execution epoch。

本规范已经固定：Cancel 与 Unknown 按提交先后决定终态；`run.ModelRequest` 冻结完整的 generation options，streaming 只是 `run.ModelInvoker` 的可选执行路径，不改变 Run command/event 语义。Run Machine 采用 Decide/Evolve 拆分：Decide 承载全部决策并在提交时产出结果事实，Evolve 是机械折叠、与事件编码同属永久兼容契约；`RunHeader + TransitionRecord log` 为 Run execution source of truth，MachineState 为必需的同事务 projection，分歧仲裁按 §5.1（transition log 完整则自动重建，日志尾部低于水位或 transition 不完整则 halt）。结构性 malformed 的模型结果通过 `RejectModelResult` 的 disposition 在同一冻结 request 上重试或失败；fixed model/limits 不进入 MachineState；usage 在 MachineState 内逐字段累计；steer 由 Application 的 queue-safe admission gate 保证进入下一个 ModelStep；工具不做效果分级，计划内停机以排空代替，Unknown 语义只覆盖崩溃和 lease 失效。

本规范采用 `RunHeader + TransitionRecord log` 为 Run execution source of truth（§5.1）：三个稳定条件（ontology 冻结、versioned Evolve 冻结、事实自包含）由 Decide/Evolve 拆分保障，revision 水位保护 transition log 尾部完整性，TransitionDigest 保护单个 transition 内部的完整事件组。MachineState 保持为必需的同事务 projection——提交验证要求当前状态在临界区内可得，这与日志权威并不冲突。跨 Run 语义不从旧 Run log 读取，而由 Session ES、artifact、memory/context projection 构造。

## 附录 A：最小 public API 草案

```go
package run

import (
    "context"
    "encoding/json"
    "errors"

    "github.com/memohai/twilight-ai/agent/es"
    "github.com/memohai/twilight-ai/agent/jsonstable"
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

// Run-owned JSON-stable persisted data. SDK request/result/tool values are
// converted at Loop/provider boundaries via Freeze* and SDK(); Runtime never
// stores sdk.MessagePart interfaces, map[string]any provider metadata, or
// caller-owned JSON bytes. CanonicalJSON is an opaque immutable value constructed
// only by parsing/canonicalizing JSON at the boundary.
type CanonicalJSON = jsonstable.Value
type ProviderMetadata map[string]CanonicalJSON
type CacheControl struct { Type string; TTL string }
type Message struct { Role MessageRole; Content []MessagePart; Usage *Usage }
type MessagePart struct { /* sealed text/reasoning/image/file/tool-call/tool-result fields */ }
type ResponseFormat struct { Type ResponseFormatType; JSONSchema CanonicalJSON }
type ToolChoice struct { Mode ToolChoiceMode; Tool string }
type ToolDefinition struct { Name string; Description string; Parameters CanonicalJSON; CacheControl *CacheControl }
type ModelRequest struct { /* Model/System/Messages/Tools/options/ProviderOptions */ }
type Usage struct { /* token counters and details; Add is field-wise */ }
type ModelToolCall struct { ToolCallID string; ToolName string; Input CanonicalJSON; ProviderMetadata ProviderMetadata }
type ReasoningPart struct { /* text/id/format/model/provider metadata */ }
type Source struct { /* source identity and provider metadata */ }
type GeneratedFile struct { Data string; MediaType string }
type ResponseMetadata struct { ID string; ModelID string; Timestamp string; Headers map[string]string }
type ModelResult struct { /* text/reasoning/finish/usage/sources/files/tool calls/response */ }

func FreezeModelRequest(sdk.Request) (ModelRequest, error)
func (ModelRequest) SDK() (sdk.Request, error)
func FreezeModelResult(sdk.ModelResult) (ModelResult, error)
func (ModelResult) SDK() (sdk.ModelResult, error)
func FreezeToolDefinition(sdk.ToolDefinition) (ToolDefinition, error)
func (ToolDefinition) SDK() sdk.ToolDefinition

type RunResult struct {
    Status  RunStatus
    Reason  RunReason
    Failure *RunFailure
    Model   *ModelResult
    Usage   Usage // MachineState.Usage 在 terminal 时的副本
}

type StepRef struct {
    RunID  RunID
    ID      StepID
    Digest  Digest // immutable step binding digest; progress is not included
}

// Step is sealed by the run package. Runtime implementations return values
// created by the Machine rules; callers cannot add another Step variant.
type Step interface {
    step()
    Ref() StepRef
}

type ModelStep struct {
    RefValue      StepRef
    Request       ModelRequest
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
    Arguments        CanonicalJSON
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

// ExecutionPolicy is host-owned Loop policy and is not persisted in MachineState.
type ExecutionPolicy struct {
    MaxParallel int
    ModelStepLimit int              // 0 unlimited; Loop submits StopRun(step_limit)
    MalformedModelResultLimit int   // 0 default; Loop chooses reject disposition
}

type ResponseRequest struct {
    RunID        RunID
    StepID       StepID
    CallID       CallID
    ID           ResponseID
    Kind         ResponseKind
    Payload      CanonicalJSON
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
    Current         Step
    PendingInputs   []AgentInput
    ModelSteps      int
    Usage           Usage // 已接受 ModelStepCompleted/ModelStepRejected 的逐字段累计
    LastModelResult *ModelResult
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
    Payload CanonicalJSON
}

// NextStep creates the command consumed by an active Run at a safe boundary.
func NextStep(input AgentInput) AcceptInput

// --- Commands (intent) ---

type PrepareModelRequest struct {
    StepID         StepID
    Model          ModelRef
    Request        ModelRequest
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
    Result ModelResult
    Calls  []ToolCallBinding
}
func (SubmitModelResult) agentCommand() {}

type SubmitModelFailure struct { StepID StepID; Failure StepFailure }
func (SubmitModelFailure) agentCommand() {}

type ModelRejectDisposition uint8
const (
    ModelRejectRetry ModelRejectDisposition = iota
    ModelRejectFailRun
)

// A structurally malformed model result. Disposition records the host/Loop
// policy decision: retry same frozen request or fail the Run.
type RejectModelResult struct {
    StepID      StepID
    Usage       Usage
    Failure     StepFailure
    Disposition ModelRejectDisposition
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
    Payload        CanonicalJSON
}
func (SubmitToolResponse) agentCommand() {}

type CancelRun struct { Reason RunReason }
func (CancelRun) agentCommand() {}

type StopRun struct { Reason RunReason } // currently ReasonStepLimit
func (StopRun) agentCommand() {}

type AcceptInput struct { Input AgentInput }
func (AcceptInput) agentCommand() {}

// --- Facts (committed outcomes) ---

type ModelStepPrepared struct {
    StepID        StepID
    Model         ModelRef
    Request       ModelRequest
    RequestDigest Digest
    InputIDs      []InputID
    Tools         []ToolSpec
    ToolsDigest   Digest
    BindingDigest Digest // Decide 算好携带；Evolve 折叠时不重算（事实自包含）
}
func (ModelStepPrepared) fact() {}

type ModelStepStarted struct { StepID StepID }
func (ModelStepStarted) fact() {}

type ModelStepRecovered struct { StepID StepID }
func (ModelStepRecovered) fact() {}

type ModelStepRejected struct {
    StepID  StepID
    Usage   Usage
    Failure StepFailure
}
func (ModelStepRejected) fact() {}

type ModelStepCompleted struct {
    StepID StepID
    Result ModelResult
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
    Payload        CanonicalJSON
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
    Arguments        CanonicalJSON
    Policy           ResponsePolicy // unresolved ToolRef uses DirectExecution
    Response         *ResponseRequest // Decide 在 ToolStepOpened 中派生并填充；调用方提交时留空
}

// ToolSpec is the agent-side sidecar for a provider-neutral frozen ToolDefinition.
// ResponsePolicy is intentionally kept out of sdk to preserve package layering.
type ToolSpec struct {
    Ref              ToolRef
    Definition       ToolDefinition
    DefinitionDigest Digest
    Policy           ResponsePolicy
}

func Next(MachineState) (Effect, error)
func Decide(MachineState, AgentCommand) ([]Fact, error)
func Evolve(MachineState, Fact) (MachineState, error)
func EvolveVersion(uint16, MachineState, Fact) (MachineState, error)
func InitializeRun(RunID) (MachineState, error)

type RunHeader struct {
    SchemaVersion       uint16
    RunID               RunID
    InitialStateVersion uint16
    InitialState        MachineState
    InitialStateDigest  es.Digest
    CausationID         es.CausationID
    HeaderDigest        es.Digest
}

func BuildRunHeader(RunID, es.CausationID) (RunHeader, error)
func ValidateRunHeader(*RunHeader) error
func DigestRunHeader(*RunHeader) (es.Digest, error)

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
    RunID      RunID
    SourceStep StepID
    Inputs     []AgentInput
}

type RequestPlanner interface {
    Plan(context.Context, PlanningHint) (RequestPlan, error)
}

type RequestPlan struct {
    Model         ModelRef
    Request       sdk.Request // boundary value; Loop freezes before PrepareModelRequest
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
    // Opaque cross-domain lineage; Run Machine does not interpret it.
    CausationID   es.CausationID
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
    CausationID   es.CausationID
    Digest        Digest
    Fact          Fact
}

type TransitionRecord struct {
    SchemaVersion    uint16
    RunID            RunID
    Revision         uint64
    CommandID        CommandID
    CommandDigest    Digest
    Events           []AgentEvent
    TransitionDigest Digest
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
    Kind       DecisionKind
    NewState   MachineState
    Events     []AgentEvent
    Transition TransitionRecord
}

// Shared, pure commit evaluation. Both runtimes call this single
// implementation inside their own critical section / transaction.
func EvaluateCommit(
    cur MachineState, curRevision uint64,
    prior *TransitionRecord,
    req CommitRequest,
    grantValid bool,
    recoveryValid bool,
) (CommitDecision, error)

func EncodeCommand(CommandEnvelope) ([]byte, error) // 不包含 Digest 字段
func DigestCommand(schemaVersion uint16, typ string, command AgentCommand) (Digest, error)
func EncodeFact(schemaVersion uint16, typ string, fact Fact) ([]byte, error)
func DigestFact(schemaVersion uint16, typ string, fact Fact) (Digest, error)
func BuildRunHeader(RunID, es.CausationID) (RunHeader, error)
func ValidateRunHeader(*RunHeader) error
func DigestRunHeader(*RunHeader) (es.Digest, error)
func BuildTransitionRecord([]AgentEvent) (TransitionRecord, error)
func ValidateTransitionRecord(*TransitionRecord) error
func DigestTransitionRecord(*TransitionRecord) (Digest, error)
func FoldTransitions(header RunHeader, records []TransitionRecord) (MachineState, uint64, error)
func DigestRequest(ModelRequest) (Digest, error)
func DigestToolDefinition(ToolDefinition) (Digest, error)
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
    Arguments        CanonicalJSON
    Progress         ToolProgressSink
}

type ExecutableTool interface {
    Ref() ToolRef
    Definition() sdk.ToolDefinition
    ResponsePolicy() ResponsePolicy
    ValidateArguments(CanonicalJSON) error
    Execute(context.Context, ToolExecutionRequest) ToolExecutionOutcome
}

type ToolExecutionResult struct { Output CanonicalJSON }

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

实现必须保证所有返回的 Step、Call、Request、Result 和等待 payload 具有只读快照语义；调用方不能通过修改 slice、map 或 JSON bytes view 改变 Runtime 状态。`AgentCommand`、`Fact`、`Effect` 和 ToolExecutionOutcome 使用 agent 的 sealed interface，外部实现不能添加未定义变体。构造 CommandEnvelope 与派生 CommandID/ResponseID 只能通过 agent 提供的 typed 构造函数；手工拼装信封字段属于实现错误。

## 附录 B：核心不变量

1. sdk 的一次 `Generate` 或 `Stream` 对应一次 provider request；transport retry 不创建新的 Step。
2. `agent/run.Loop` 是唯一的 Run 多步执行算法；Run 的权威状态由 Runtime 持有，Loop 不保存第二份。
3. Runtime 对 Loop 只公开 `Load` 和 `Commit`；Planner、queue 和工具入口不是 Runtime 的隐藏第三、第四个方法。
4. Machine 是完整的 Run/Step/ToolCall 语义规则；决策只在 `Decide` 中、只在提交时运行一次，`Evolve` 是机械折叠。Runtime 通过共享 `EvaluateCommit` 调用它们，不复刻规则。
5. Step 是 durable resume boundary，只有 ModelStep 和 ToolStep；ToolCall 是 ToolStep 内的 progress。
6. ModelStep 完成有 tool calls 时产出 `ToolStepOpened`；全部 Call 到达可关闭终态时，同一 transition 产出 `ToolStepClosed`。终态一律以 `RunEnded` 显式产出，且它是其 transition 的最后一个事实。
7. Pending Call 必须在 start command Accepted 后才可执行；多个独立 Pending Call 可按 ExecutionPolicy 并行。
8. Waiting response 只推进对应 Call；approval approved 先变 Pending，随后由 Loop 执行工具。日志记录结果事实（`ToolCallFailed{permission_denied}`、`RunEnded{cancelled}`），不记录请求本身。
9. 幂等按 command 判定：相同 CommandID/digest 重放返回 CommitAlreadyApplied 与原事件组（不重新运行 Decide），不重复写入 projection、history、queue action 或 outbox；相同 CommandID 不同 digest 冲突。
10. 一次接受的 transition 使 Revision 恰好加一；其全部事实共享该 Revision，Index 组内连续，提交后 `Snapshot.Revision` 等于该 Revision。
11. `RunHeader + TransitionRecord log` 是 Run execution source of truth；MachineState 是必需的同事务 projection，可按 `EvolveVersion` 从经验证的 header 和 transition log 重建。对任意 Revision，状态必须等于 `RunHeader.InitialState` 经 `flatten(TransitionRecord[].Events)` 折叠的结果；snapshot 分歧或缺失且日志完整时自动重建并记录，日志尾部低于 revision 水位或 transition digest/事件组不完整时 halt 该 Run。
12. Evolve 的折叠语义与事件编码同属永久兼容契约，按 SchemaVersion 冻结；Replay 通过 `EvolveVersion` 选择历史语义；Decide 的决策规则可随版本演进，因为决策结果已记录为事实。
13. 已知工具失败交给下一次模型请求；Unknown 终止 Run，不自动重试、不查询外部系统。
14. worker cancellation 不等于 RunStopped；业务停止必须提交控制 command。宿主的业务停止先提交 `CancelRun`，再取消 Loop 的 ctx；ctx 取消本身只结束执行尝试，工具 worker 运行到自身结束。
15. EventSink 只是实时观察；TransitionRecord、durable snapshot 和 outbox 才是 replay/recovery 依据，AgentEvent 是 transition 内部和观察出口的事实流视图。
16. `run.MemoryRuntime` 用进程内同步；durable runtime adapter 用事务、CAS 和内部 Attempt/owner/fence/lease；两者共享 `EvaluateCommit` 与 Run Machine 规则，但不共享存储实现。
17. 结构性 malformed 的模型结果以 `RejectModelResult` 累计 usage；Disposition 决定回到 Prepared 重试或同 transition RunFailed。单个 Call 的参数解析失败不是 malformed，按已知 `invalid_arguments` 进入下一次模型请求。
