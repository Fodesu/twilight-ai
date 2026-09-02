# Twilight Agent Session Chatlog Module

状态：设计草案，payload 字段、输入 limits 与 golden fixtures 尚未冻结。

本文定义 `agent/session/chatlog` first-party Module，依赖 [Session](agent-session.md) 与 [Session Module Framework](agent-session-extension.md)。回合生命周期由 [Turn](agent-turn.md) 拥有。文中的“必须”“不得”“应该”是草案冻结时应保留的协议约束；canonical JSON 与 digest 遵循 `agent/jsonstable`、`agent/es`。

## 1. module 与 ontology

```text
Source       = twilight
ModuleID     = chatlog
EventType    = twilight/chatlog/<name>
Projections  = twilight/chatlog/surface, twilight/chatlog/context
```

Chatlog 保存对话内容：Input、assistant、tool_result、summary、checkpoint。Surface 与 Context 是对这些 events 的纯投影。`assistant` 与 `tool_result` 携带 `TurnID`；Input 在 `input_delivered` 之后挂上 TurnID；summary 与 checkpoint 不携带 TurnID。回合的创建、结束与 Run linkage 由 `twilight/turn/` 事件表达。外部内容经 `ReferencePart` 关联 Artifact BindingID。

流式 `text_delta` / `reasoning_delta` 由 Loop EventSink 发送，属于临时观察。Chatlog 权威是已提交的条目。

**CHT-SCP-1** 本模块拥有对话内容。Application 拥有模型调用、provider transport、发送策略与审计。`turn` 拥有回合与 Run linkage。

## 2. stable entity 与生命周期

```go
type TurnID string
type InputID string
type AssistantID string
type ToolResultID string
type SummaryID string
type CallID string
type CheckpointID string
```

`TurnID` 与 turn 模块同一 identity。InputID、AssistantID、ToolResultID、SummaryID 在 resolved ancestry 内唯一；CallID 在同一 Turn 内唯一。replacement graph 无环，一个实体至多一个直接 replacement。

| 实体 | 创建 | 可变过程 | 终态/替换 | 不变量 |
|---|---|---|---|---|
| Input | `input_submitted` | 无 | delivered / withdrawn / rejected | 只终结一次；delivered 后进入 Context |
| Assistant | `assistant` | 无 | — | immutable；ID 单次创建 |
| Tool result | `tool_result` | 无 | 可被 `tool_result_superseded` | 同一 Turn、同一 CallID 至多一条 active |
| Summary | `summary` | 无 | 随 checkpoint 失效 | checkpoint 的摘要正文 |
| Checkpoint | `checkpoint_created` | 无 | invalidated | 指向已有 EventPosition |

**CHT-LIF-1** reducer 拒绝 identity mutation、非法状态迁移、replacement conflict 与重复 ID。模型步骤进行中走 EventSink；定稿写入 `assistant` 或 `tool_result`。

## 3. parts 与条目

```go
type PartKind string
type ContentRefKind string
const (
    PartText      PartKind = "twilight/chatlog/text"
    PartReasoning PartKind = "twilight/chatlog/reasoning"
    PartToolCall  PartKind = "twilight/chatlog/tool_call"
    PartReference PartKind = "twilight/chatlog/reference"
    RefArtifactBinding ContentRefKind = "twilight/chatlog/artifact_binding_ref"
)
type Part interface { PartKind() PartKind }
type ContentRef interface { RefKind() ContentRefKind }
type ArtifactBindingRef struct { BindingID artifact.BindingID }
func (ArtifactBindingRef) RefKind() ContentRefKind
type ReferencePart struct { Ref ContentRef; Name string }
func (ReferencePart) PartKind() PartKind
type TextPart struct { Text string }
func (TextPart) PartKind() PartKind
type ReasoningPart struct { Text string }
func (ReasoningPart) PartKind() PartKind
type ToolCallPart struct { CallID CallID; Name string; Input jsonstable.Value }
func (ToolCallPart) PartKind() PartKind

type ToolResultStatus string
const (
    ToolSuccess ToolResultStatus = "success"
    ToolError ToolResultStatus = "error"
    ToolUnknown ToolResultStatus = "unknown"
    ToolIndeterminate ToolResultStatus = "indeterminate"
)

type Input struct {
    ID InputID
    TurnID TurnID // delivered 之后赋值；与 turn 模块同一 identity
    Content jsonstable.Value
    Digest es.Digest
}
type Assistant struct {
    ID AssistantID
    TurnID TurnID
    Parts []Part
    Digest es.Digest
}
type ToolResult struct {
    ID ToolResultID
    TurnID TurnID
    CallID CallID
    Status ToolResultStatus
    Parts []Part
    Digest es.Digest
}
type Summary struct {
    ID SummaryID
    Parts []Part
    Digest es.Digest
}

type EntryKind string
const (
    EntryInput      EntryKind = "input"
    EntryAssistant  EntryKind = "assistant"
    EntryToolResult EntryKind = "tool_result"
    EntrySummary    EntryKind = "summary"
)
type Entry struct {
    Kind EntryKind
    ID string
    Digest es.Digest
    Input *Input
    Assistant *Assistant
    ToolResult *ToolResult
    Summary *Summary
}
```

**CHT-ENT-1** Parts 有序。`ArtifactBindingRef` 的 identity 为 discriminator 与 BindingID。interface value 非 nil；part kind 与 concrete value 匹配。ReferencePart 的 MediaType 来自 Artifact Ref。

**CHT-ENT-2** 每个 `(TurnID,CallID)` 在 assistant 中至多一个 ToolCall。`tool_result` 对应同 Turn 已有的 call。CallID 在同一 Turn 内唯一：同一 Turn 的后续 ModelStep 不得复用已出现的 CallID。`unknown` 为 v1 mapper 写入的未决终态；`indeterminate` 保留给历史条目，v1 mapper 不产出。active Context 视 unresolved call 为未解决，直到 Application 在 Turn 尚未 `twilight/turn/completed` 或 `twilight/turn/failed` 时写入 `tool_result_superseded`，换成 `success` 或 `error`。每个 unresolved result 至多一个 replacement。v1 FactMapper 不写 `tool_result_superseded`。

**CHT-ENT-3** ToolResult 的 nested Parts 为单层 TextPart 或 ReferencePart。外部内容使用 `ReferencePart`。

**CHT-ENT-4** 用户侧内容是 Input。`input_delivered` 把 Input 挂到 TurnID；Context 将已 delivered 的 Input 作为用户条目。

## 4. canonical wire codec

运行时是 typed model；wire 是 static registry 的 discriminated union。

```go
type PartCodec interface {
    Kind() PartKind
    EncodePart(Part) (jsonstable.Value, error)
    DecodePart(jsonstable.Value) (Part, error)
}
type ContentRefCodec interface {
    Kind() ContentRefKind
    EncodeRef(ContentRef) (jsonstable.Value, error)
    DecodeRef(jsonstable.Value) (ContentRef, error)
}
```

**CHT-COD-1** Decode 先检查 object、discriminator、Session protocol profile、unknown fields 和 limits，再构造 typed value。Encode/Decode 拒绝 nil、kind mismatch、未知 discriminator、cycle 和超限输入。有效值满足 `Encode → Decode → Encode` canonical-equivalent。

**CHT-COD-2** registry 在启动时固定，kind 有唯一 codec。BindingExtractor 按 appearance order 返回 assistant、tool_result、summary 中 ReferencePart 的 BindingID。

**CHT-COD-3** EventType 为 `twilight/chatlog/<name>`。条目 Digest 的 domain 与 EventType 相同，覆盖 ID、TurnID（若有）、有序 parts 或 Content、ref identity。wire 与 digest 形状由 Session `ProtocolVersion` 决定：

```text
Digest("twilight/chatlog/input_submitted", ...)
Digest("twilight/chatlog/assistant", ...)
Digest("twilight/chatlog/tool_result", ...)
Digest("twilight/chatlog/summary", ...)
```

v1 freeze 前开放项：wire field names、输入 limits、golden fixtures。

## 5. event payloads

payload 为 object，identity 为 string，整数按 Session profile 编码。未列字段在 v1 拒绝。

```go
type InputSubmittedPayload struct {
    InputID InputID
    Content jsonstable.Value
    SubmittedAtUnixMilli int64
}
type InputDeliveredPayload struct { InputID InputID; TurnID TurnID }
type InputWithdrawnPayload struct { InputID InputID; Reason string }
type InputRejectedPayload struct { InputID InputID; Reason string }

type AssistantPayload struct { Assistant Assistant }

type ToolResultPayload struct { ToolResult ToolResult }
type ToolResultSupersededPayload struct { ToolResultID ToolResultID; ReplacementToolResultID ToolResultID }

type SummaryPayload struct { Summary Summary }

type CheckpointCreatedPayload struct {
    CheckpointID CheckpointID
    CoveredThrough session.EventPosition
    BaseContextDigest es.Digest
    SummaryID SummaryID
    SummaryDigest es.Digest
    Retained []EntryDigestPair
    Digest es.Digest
}
type EntryDigestPair struct { Kind EntryKind; ID string; Digest es.Digest }
type CheckpointInvalidatedPayload struct { CheckpointID CheckpointID; Reason string }
```

`assistant`、`tool_result`、`summary` 的 EventDefinition 注册 parts registry：

```go
extension.BindingReferenceDefinition{
    RegistryID: "twilight/chatlog/parts",
    Cardinality: extension.Cardinality{Min: 0},
    AllowedSchemes: nil,
    RequiredDurability: artifact.EventBound,
}
```

`AllowedSchemes` 为空表示任意已注册且支持 `EventBound` 的 scheme。

**CHT-EVT-1** EventType：

```text
twilight/chatlog/input_submitted
twilight/chatlog/input_delivered
twilight/chatlog/input_withdrawn
twilight/chatlog/input_rejected
twilight/chatlog/assistant
twilight/chatlog/tool_result
twilight/chatlog/tool_result_superseded
twilight/chatlog/summary
twilight/chatlog/checkpoint_created
twilight/chatlog/checkpoint_invalidated
```

**CHT-EVT-2** `input_submitted` 创建 Input。Delivered、Withdrawn、Rejected 各终结一次。`input_delivered` 要求 Input 仍为 submitted，并写入非空 TurnID。同一 Start commit 中 `twilight/turn/started` 列出这些 InputIDs。AssistantID、ToolResultID、SummaryID 在 ancestry 内单次创建。

**CHT-EVT-3** checkpoint Digest 的 domain 为 `twilight/chatlog/checkpoint_created`。`BaseContextDigest` 覆盖截至 `CoveredThrough` 的有序 active Context 序列 `(Kind, ID, Digest)`。`CoveredThrough` 早于该 checkpoint。`SummaryID` 落在 `CoveredThrough` 与 checkpoint 之间，且已由 `summary` 创建。该间隙内仅有这一条 summary。`Retained` 为 base 序列的有序子集。合法 checkpoint 下 Context 为 `[Summary] + Retained`，再 fold checkpoint 之后的 tail。checkpoint 在显式 invalidate，或 summary / Retained / base source 被 supersede 之后失效；projection 回退到更早合法 checkpoint，或从全量 events 重折。

## 6. Surface projection

```go
type SurfaceEntry struct {
    Kind EntryKind
    ID string
    Position session.EventPosition
}
type Surface struct {
    Inputs map[InputID]Input
    Assistants map[AssistantID]Assistant
    ToolResults map[ToolResultID]ToolResult
    Summaries map[SummaryID]Summary
    EntryOrder []SurfaceEntry
}
```

**CHT-SUR-1** SurfaceFold 消费 chatlog decoded events。`EntryOrder` 为 resolved replay 顺序下的 delivered input、assistant、tool_result、summary，并带 Position。回合列表由 turn 投影提供，按 `TurnID` 连接。

## 7. Context projection

```go
func ContextFold(events []extension.DecodedEvent) ([]Entry, error)
```

**CHT-CTX-1** 输入为已验证、按 ancestry 排序的 chatlog events。输出为 delivered input、assistant、tool_result、summary 经 supersession 与 checkpoint 处理后的有序 `[]Entry`。ContextFold 为纯函数。

**CHT-CTX-2** fold 执行 ID 单次创建、CallID pairing、unresolved-call 与 replacement 规则。合法 checkpoint 按 CHT-EVT-3 应用。Context 只含已 delivered 的 Input。

## 8. materializer port

```go
type MaterializationTarget struct { Name string; Capabilities []string }
type MaterializationDecision struct {
    Kind EntryKind; ID string
    BindingID artifact.BindingID
    Operation string; Result string; Detail string
}
type MaterializedEntry struct { Kind EntryKind; Parts []jsonstable.Value }
type MaterializationResult struct { Entries []MaterializedEntry; Decisions []MaterializationDecision }
type ContextMaterializer interface {
    Materialize(context.Context, []Entry, MaterializationTarget) (MaterializationResult, error)
}
```

**CHT-MAT-1** materializer 把语义条目转为目标模型表示。committed chatlog Events 保持不变。provider capability 与发送策略由 Application 决定。Planner 组装见 [参考组装](agent-reference-assembly.md)。

## 9. conformance

- **CHT-LIF-1、CHT-EVT-1、CHT-EVT-2**：所列 EventType、Input 终结一次、delivered 带 TurnID、ID 单次创建；
- **CHT-ENT-1 至 CHT-ENT-4**：parts、CallID pairing、replacement、用户侧为 Input；
- **CHT-COD-1 至 CHT-COD-3**：codec；Digest domain 与 EventType 相同；
- **CHT-SUR-1、CHT-CTX-1、CHT-CTX-2**：EntryOrder 与 checkpoint；
- **CHT-MAT-1**：materializer 为 IO 边界。
