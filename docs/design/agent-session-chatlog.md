# Twilight Agent Session Chatlog Module

状态：设计草案，payload 字段、输入 limits 与 golden fixtures 尚未冻结。

本文定义 `agent/session/chatlog` first-party Module，依赖 [Session](agent-session.md) 与 [Session Module Framework](agent-session-extension.md)。文中的“必须”“不得”“应该”是草案冻结时应保留的协议约束；canonical JSON 与 digest 遵循 `agent/jsonstable`、`agent/es`。

## 1. module 与 ontology

```text
ModuleID     = twilight.chatlog
Event prefix = twilight.chatlog/
Projections  = twilight.chatlog/surface, twilight.chatlog/context
```

committed chatlog Events 是 persisted chatlog facts；Surface 和 Context 都是从这些 Events fold 的 pure projections。Attachment 是独立 domain；外部内容只能经 Message 的 `ReferencePart` 关联 BindingID。

**CHT-SCP-1** Module 的 authority 是会话语义；Application/provider adapter 负责模型调用、provider transport、发送策略、外部内容读取与审计 policy；`twilight.turn` 负责 Turn execution linkage，chatlog payload 必须限于会话语义。

## 2. stable entity 与生命周期

```go
type InputID string
type TurnID string
type ItemID string
type MessageID string
type CallID string
type CheckpointID string
type ItemKind string
type EntityStatus string
```

InputID、TurnID、ItemID、MessageID 在 resolved ancestry 内唯一；CallID 在 Turn 内唯一。replacement graph 无环，且一个实体至多一个直接 replacement。

| 实体 | 创建 | 可变过程 | 终态/替换 | 不变量 |
|---|---|---|---|---|
| Input | InputSubmitted | 无 | Delivered / Withdrawn / Rejected | 只终结一次 |
| Turn | TurnOpened | 无 | Completed / Failed / Superseded | replacement 指向新 Turn |
| Item | ItemOpened | ItemUpdated* | Completed / Failed / Superseded | `(TurnID, Sequence)` 唯一；version 连续 |
| Message | MessageCommitted | 无 | 可被 MessageSuperseded | immutable；ID 单次创建 |
| Checkpoint | ContextCheckpointCreated | 无 | Invalidated | 仅指向已有 EventPosition |

**CHT-LIF-1** reducer 必须拒绝 identity mutation、非法状态迁移、version gap、sequence collision、replacement conflict 与重复 MessageID（即使 canonical value 相同）。一个 item 的 Completed 必须携带 final version 与 final content digest。

## 3. semantic Message interfaces

```go
type MessagePartKind string
type ContentRefKind string
type MessageRole string
const (
    RoleUser MessageRole = "user"
    RoleAssistant MessageRole = "assistant"
    RoleToolResult MessageRole = "tool_result"
    PartText MessagePartKind = "twilight.chatlog/text"
    PartReasoning MessagePartKind = "twilight.chatlog/reasoning"
    PartToolCall MessagePartKind = "twilight.chatlog/tool_call"
    PartToolResult MessagePartKind = "twilight.chatlog/tool_result"
    PartReference MessagePartKind = "twilight.chatlog/reference"
    RefArtifactBinding ContentRefKind = "twilight.chatlog/artifact_binding_ref"
)
type MessagePart interface { PartKind() MessagePartKind }
type ContentRef interface { RefKind() ContentRefKind }
type ArtifactBindingRef struct { BindingID artifact.BindingID }
func (ArtifactBindingRef) RefKind() ContentRefKind
type ReferencePart struct { Ref ContentRef; Name string }
func (ReferencePart) PartKind() MessagePartKind
type TextPart struct { Text string }
func (TextPart) PartKind() MessagePartKind
type ReasoningPart struct { Text string }
func (ReasoningPart) PartKind() MessagePartKind
type ToolCallPart struct { CallID CallID; Name string; Input jsonstable.Value }
func (ToolCallPart) PartKind() MessagePartKind
type ToolResultStatus string
const (
    ToolSuccess ToolResultStatus = "success"; ToolError ToolResultStatus = "error"
    ToolUnknown ToolResultStatus = "unknown"; ToolIndeterminate ToolResultStatus = "indeterminate"
)
type ToolResultPart struct { CallID CallID; Status ToolResultStatus; Parts []MessagePart }
func (ToolResultPart) PartKind() MessagePartKind
type Message struct {
    ID MessageID; TurnID TurnID; Role MessageRole
    InputIDs []InputID; ItemIDs []ItemID; Parts []MessagePart; Digest es.Digest
}
```

**CHT-MSG-1** Parts、InputIDs、ItemIDs 均有序。`ArtifactBindingRef` 的 identity 为 discriminator 与 exact BindingID。所有 interface value 必须非 nil 且非 typed nil；part kind 必须匹配 concrete value。ReferencePart 不重复声明 MediaType；consumer 从 Binding 的 identity-bound Ref 得到它，并仍将其作为 untrusted metadata。

**CHT-MSG-2** assistant message 可有 Text、Reasoning、Reference、ToolCall；每个 `(TurnID,CallID)` 只有一个 ToolCall。user message 的每个 InputID 必须属于该 Turn 的 `TurnOpened.InputIDs`，已被 `InputDelivered` 指向该 Turn，且尚未被其他 user message 消费；不得消费未声明、未 delivered 或已消费 input。

`tool_result` role 只可有对应既有 call 的一个 ToolResult。`unknown` 与 `indeterminate` 是 terminal historical result，但 active Context 视其 call unresolved。唯一处置是 `MessageSuperseded` 将该结果 Message 指向同一 Turn、同一 CallID 的新 replacement tool-result Message；replacement 必须为 `success` 或 `error`，且成功/错误后该 active call resolved。每个 unresolved historical result 最多一个 replacement，replacement chain 中同一 call 只有一个 active Message；不得以新 call、另一 event 或删除历史结果处置。

**CHT-MSG-3** ToolResult 的 v1 nested Parts 只允许单层 TextPart 或 ReferencePart；不得嵌套 Reasoning、ToolCall、ToolResult 或另一层 child。ItemIDs 必须指向同一 Turn 中已 completed 且 final digest 匹配的 item；message role、TurnID、item sequence 必须符合已折叠状态。`ItemOpened.Content` 与 `ItemUpdated.Content` 在 v1 只承载 inline canonical content，不得承载 durable external BindingID 或 artifact reference；外部内容必须使用 Message `ReferencePart`。

## 4. canonical wire codec

运行时 interface 是 typed model；wire 是 static registry 的 discriminated union。

```go
type PartCodec interface {
    Kind() MessagePartKind
    EncodePart(MessagePart) (jsonstable.Value, error)
    DecodePart(jsonstable.Value) (MessagePart, error)
}
type ContentRefCodec interface {
    Kind() ContentRefKind
    EncodeRef(ContentRef) (jsonstable.Value, error)
    DecodeRef(jsonstable.Value) (ContentRef, error)
}
type MessageBindingExtractor struct { /* immutable Message registry implementation */ }
func (MessageBindingExtractor) BindingIDs(value any) ([]artifact.BindingID, error)

type MessageRegistry interface {
    extension.RuntimeRegistry
    RegistryID() extension.RegistryID
    EncodeMessagePreimage(Message) (jsonstable.Value, error)
    EncodeMessage(Message) (jsonstable.Value, error)
    DecodeMessage(jsonstable.Value) (Message, error)
}
```

**CHT-COD-1** Decode 必须 wire-first：先检查 object、explicit discriminator、Session protocol profile、unknown fields 和 limits，再构造 typed value。Encode/Decode 均拒绝 nil/typed nil、kind mismatch、未知 discriminator、cycle 和超限输入。有效值必须 `Encode → Decode → Encode` canonical-equivalent。

**CHT-COD-2** registry 是 startup static、immutable，kind 有唯一 codec，不依赖 Go type name。Message registry 实现 `extension.RuntimeRegistry.BindingExtractor()`，且必须返回 `(MessageBindingExtractor, true)`，不得以 typed nil 表示缺失。该 extractor 只接受 typed `MessageCommittedPayload`，并按 appearance order 返回其 `Message` 中顶层 ReferencePart 与 ToolResult 单层 child 的全部 ArtifactBindingRef；错误的 typed value 必须拒绝。它是 Chatlog Module binding extraction 的唯一入口。当前 draft 的 discriminator 为本节定义的 `Part*` 和 `RefArtifactBinding` 值；后续 Module kind 使用其 owner namespace。

**CHT-COD-3** `EncodeMessagePreimage` 编码 ID、TurnID、role、有序 InputIDs/ItemIDs、parts、每个 ref identity 和声明的附加字段，排除 Digest。`MessageDigest=Digest("twilight.chatlog/message/v1", registry ID, preimage)`；`EncodeMessage` 编码同一 preimage 加 Digest，Decode 必须重算。MessageCommitted payload 的 typed shape 是 `MessageCommittedPayload{Message Message}`；它的 wire codec 必须将 `message` 编码为 `EncodeMessage(Message)` 得到的 canonical message value，不得 generic re-marshal 或重复其中任一字段。

`twilight.chatlog/message/v1` 是 Message digest 的 domain identity，沿用 Session `ProtocolVersion` 的兼容性规则；它不构成独立的 event schema version。

Note：本规范保持 draft；v1 freeze 前准确开放项仅为 wire field names、输入 limits 与 golden fixtures。

## 5. event payloads

下列 payload 均为 object，identity 为 string，整数按 Session profile 编码；未列字段在 v1 默认拒绝。

```go
type InputSubmittedPayload struct { InputID InputID; Content jsonstable.Value; SubmittedAtUnixMilli int64 }
type InputDeliveredPayload struct { InputID InputID; TurnID TurnID }
type InputWithdrawnPayload struct { InputID InputID; Reason string }
type InputRejectedPayload struct { InputID InputID; Reason string }
type TurnOpenedPayload struct { TurnID TurnID; InputIDs []InputID }
type TurnCompletedPayload struct { TurnID TurnID }
type TurnFailedPayload struct { TurnID TurnID; FailureClass string }
type TurnSupersededPayload struct { TurnID TurnID; ReplacementTurnID TurnID }
type ItemOpenedPayload struct { ItemID ItemID; TurnID TurnID; Sequence uint32; Kind ItemKind; Version uint32; Content jsonstable.Value; ContentDigest es.Digest }
type ItemUpdatedPayload struct { ItemID ItemID; Version uint32; Content jsonstable.Value; ContentDigest es.Digest }
type ItemCompletedPayload struct { ItemID ItemID; FinalVersion uint32; FinalDigest es.Digest }
type ItemFailedPayload struct { ItemID ItemID; FailureClass string }
type ItemSupersededPayload struct { ItemID ItemID; ReplacementItemID ItemID }
type MessageCommittedPayload struct { Message Message }
type MessageSupersededPayload struct { MessageID MessageID; ReplacementMessageID MessageID }
type ContextCheckpointCreatedPayload struct {
    CheckpointID CheckpointID; CoveredThrough session.EventPosition
    BaseContextDigest es.Digest; SummaryMessageID MessageID; SummaryMessageDigest es.Digest
    Retained []MessageDigestPair; Digest es.Digest
}
type MessageDigestPair struct { MessageID MessageID; Digest es.Digest }
type ContextCheckpointInvalidatedPayload struct { CheckpointID CheckpointID; Reason string }
```

`ModuleDescriptor` 中 `twilight.chatlog/message_committed` 的 EventDefinition 必须注册 Message registry requirement，并声明唯一 Binding reference：

```go
extension.BindingReferenceDefinition{
    RegistryID: "twilight.chatlog/message",
    Cardinality: extension.Cardinality{Min: 0},
    AllowedSchemes: nil,
    RequiredDurability: artifact.EventBound,
}
```

`AllowedSchemes:nil`（或 empty）明确表示任意 Catalog-registered 且支持 `EventBound` 的 scheme；非空 slice 才是显式允许列表。它不表示允许 ephemeral。该 declaration 使 Message extractor 覆盖的每一个 Binding 都接受 Module admission。

**CHT-EVT-1** Event names依次为 `input_submitted`、`input_delivered`、`input_withdrawn`、`input_rejected`、`turn_opened`、`turn_completed`、`turn_failed`、`turn_superseded`、`item_opened`、`item_updated`、`item_completed`、`item_failed`、`item_superseded`、`message_committed`、`message_superseded`、`context_checkpoint_created`、`context_checkpoint_invalidated`，均以 `twilight.chatlog/` 为前缀。

**CHT-EVT-2** InputSubmitted 创建 input；Delivered/Withdrawn/Rejected 只能一次终结它。`InputDelivered` 必须指向已存在、尚未终结的 TurnOpened，且该 TurnOpened.InputIDs 明确声明该 InputID。TurnOpened 的 InputIDs 必须是 submitted、distinct 且未终结 inputs；Turn 终结后不得新增 Item 或 Message。ItemOpened 从 version 1 起，ItemUpdated 每次加一。Item ContentDigest 的唯一算法为 `Digest("twilight.chatlog/item-content/v1", ItemID, TurnID, Kind, Version, canonical Content)`：ItemOpened 用 payload 的 ItemID/TurnID/Kind/Version/Content 重算并匹配其 ContentDigest；ItemUpdated 用当前 item 的 ItemID/TurnID/Kind 和该 update 的 Version/Content 重算并替换当前 digest；ItemCompleted 的 FinalVersion 和 FinalDigest 必须分别匹配当前/latest version 与该 latest digest。MessageCommitted payload codec 必须 decode/validate typed `MessageCommittedPayload.Message`，再创建其 MessageID；同一 resolved ancestry 中只能创建一次。

**CHT-EVT-3** `BaseContextDigest` 覆盖截至 `CoveredThrough`（必须早于该 checkpoint event）的 canonical ordered active Message sequence，即有序 `(MessageID, MessageDigest)`，而非仅 ID；该 base sequence 不包含 summary。`SummaryMessageID` 必须在 `CoveredThrough` 之后、checkpoint event 之前已由 `MessageCommitted` 创建且仍 active，其 digest 必须等于 `SummaryMessageDigest`。在 `CoveredThrough` 与 checkpoint event 之间，除该指定 summary 外不得有其他 context-contributing `MessageCommitted` 或 checkpoint；surface/log-only facts 可以出现。`Retained` 必须是 base sequence 的有序子集，digest 均匹配且不得重复。checkpoint digest 覆盖 registry identity、CoveredThrough、BaseContextDigest、summary pair 和 ordered retained pairs。应用有效 checkpoint 的结果固定为 `[SummaryMessage] + Retained`；tail 仅从 checkpoint event 之后开始 fold，绝不再次折叠 summary。显式 invalidation，或 checkpoint 之后 summary、任一 retained 或任一 base source message 的 supersession，都会使该 checkpoint incompatible；projection 必须回退到更早 compatible checkpoint 或完整 fold，不得对 checkpoint 内部结果局部应用 supersession。

## 6. Surface projection

```go
type SurfaceItem struct { ItemID ItemID; TurnID TurnID; Sequence uint32; Status EntityStatus; Version uint32; Digest es.Digest }
type SurfaceMessage struct { MessageID MessageID; Position session.EventPosition }
type ConversationTurn struct { TurnID TurnID; Status EntityStatus; ItemOrder []ItemID; SupersededBy TurnID }
type Surface struct {
    Turns []ConversationTurn; Items map[ItemID]SurfaceItem; Messages map[MessageID]Message
    MessageOrder []SurfaceMessage
}
```

**CHT-SUR-1** SurfaceFold 是 pure deterministic fold，只消费 chatlog decoded events。它按 stable IDs 聚合 active、terminal 与 superseded 状态；`MessageOrder` 是 resolved replay order 的稳定完整 MessageCommitted sequence，并以 Position 保留 source event identity，调用方不得从 map iteration 推导顺序。展示 grouping 由调用方从 ItemOrder 和 MessageOrder 派生。

## 7. Context projection

```go
func ContextFold(events []extension.DecodedEvent) ([]Message, error)
```

**CHT-CTX-1** 输入必须是已验证、decode 后、按 resolved ancestry segment/event order 的 events。ContextFold 只输出有效 MessageCommitted 经 supersession/checkpoint 处理后的有序 `[]Message`；它不得接收 raw commits、resolve content、访问 IO 或 provider transport。

**CHT-CTX-2** fold 必须再次执行 ancestry-wide MessageID single-creation、role/call pairing、active unresolved-call 检查以及 unknown/indeterminate replacement 规则。checkpoint 只在 CHT-EVT-3 的 coverage gap、base/summary/retained digest 与 retained order 均匹配且仍 compatible 时使用；应用后按 `[SummaryMessage] + Retained` 继续仅 fold checkpoint event 之后的 tail。

## 8. materializer port

```go
type MaterializationTarget struct { Name string; Capabilities []string }
type MaterializationDecision struct { MessageID MessageID; BindingID artifact.BindingID; Operation string; Result string; Detail string }
type MaterializedMessage struct { Role MessageRole; Parts []jsonstable.Value }
type MaterializationResult struct { Messages []MaterializedMessage; Decisions []MaterializationDecision }
type ContextMaterializer interface {
    Materialize(context.Context, []Message, MaterializationTarget) (MaterializationResult, error)
}
```

**CHT-MAT-1** materializer 是 IO boundary，可把 semantic message 转为 target 表示并报告 decisions；它不得改写 Message 或 committed chatlog Events。provider capability、fallback、audit 以及何时持久化 decision 都是 Application policy，本规范不要求发送前追加任何事实。

## 9. conformance

Conformance 必须验证：

- **CHT-LIF-1、CHT-EVT-1、CHT-EVT-2**：entity lifecycle、InputDelivered 的 opened/declaration 约束、sequence/version、Item ContentDigest 的 opened/update 重算与 latest completion matching、replacement 与 ancestry-wide MessageID；
- **CHT-MSG-1、CHT-MSG-2、CHT-MSG-3**：role/turn/order、declared-delivered-unconsumed input、CallID pairing、unknown/indeterminate 的唯一 replacement resolution、nested v1 grammar 与无 Item external BindingID；
- **CHT-COD-1、CHT-COD-2、CHT-COD-3**：wire-first codec、nil/kind/limit rejection、Message registry RuntimeRegistry extractor、`message_committed` 的 EventBound registry declaration、typed MessageCommittedPayload canonical message wire、digest closure 与完整 BindingIDs；
- **CHT-SUR-1、CHT-CTX-1、CHT-CTX-2**：pure Surface/Context、稳定 MessageOrder、decoded input、checkpoint 的 base 不含 summary、summary placement/coverage gap、retained ordered subset、`[Summary] + Retained` replacement、checkpoint 后 tail，以及 invalidation 或 source/summary/retained supersession 的回退；
- **CHT-MAT-1**：materializer port 不越过 Application policy boundary。
