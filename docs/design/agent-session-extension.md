# Twilight Agent Session Extension Framework

状态：设计规范

本文定义建立在 `agent/session` 与 `agent/artifact` 之上的静态 `agent/session/extension`。文中的“必须”“不得”“应该”是协议约束；JSON canonicalization 与 digest 遵循 `agent/jsonstable`、`agent/es`。

## 1. 范围与依赖

```text
agent/artifact  ←  agent/session/extension  →  agent/session
                                      ↑
                            first-party 与 Application modules
```

Framework 负责 Module ownership、static startup composition、immutable Catalog、event codec/schema/upcast、Binding declaration、SemanticAppender 和 pure projection。first-party modules 可包括 `twilight.chatlog` 与 `twilight.turn`。它不支持运行时动态代码加载；新组合必须构建新 immutable Catalog。

**EXT-SCP-1** Session Store protocol 由 Session kernel 负责；多 Session transaction、package import、saga、operation log 与 provider policy 由 Application/adapter 负责。未知 event 的 archive Binding manifest 由 Application archive coordinator 处理。

## 2. Module、Catalog 与版本

```go
type ModuleID string
type ModuleVersion uint16
type ProjectionID string
type ProjectionVersion uint16
type RegistryID string

type ModuleDescriptor struct {
    ID ModuleID; Version ModuleVersion
    Events []EventDefinition
    Projections []ProjectionDefinition
    Registries []CodecRegistryDescriptor
    Schemes []artifact.SchemeDefinition
}
type Catalog struct { /* immutable, built only by BuildCatalog */ }
type CatalogBuildRequest struct { Modules []ModuleDescriptor; Registries []RuntimeRegistry }
type RuntimeRegistry interface {
    Descriptor() CodecRegistryDescriptor
    BindingExtractor() (BindingExtractor, bool)
}
func BuildCatalog(CatalogBuildRequest) (*Catalog, error)
```

**EXT-CAT-1** ModuleID 使用小写 ASCII namespace；`twilight.*` 保留 first-party。一个 Catalog 中 ModuleID、EventType、ProjectionID、Scheme、RegistryID 均唯一。Build defensive-copy 所有 descriptor 和 runtime registry，成功后只读且与注册顺序无关。

| 变化 | ModuleVersion | Event SchemaVersion | ProjectionVersion |
|---|---:|---:|---:|
| 新 EventType | 增加 | 从 1 开始 | 受影响时增加 |
| 修改 payload 或 Binding 声明 | 增加 | 增加 | fold 受影响时增加 |
| 修改 reducer/state codec | 增加 | 不变 | 增加 |
| 增加无关 Module | Catalog composition 改变 | 不变 | 不变 |

**EXT-CAT-2** Build 必须拒绝 duplicate owner、namespace mismatch、schema gap、重复版本、registry requirement mismatch、upcast gap/cycle/ambiguity、非法 Binding declaration、以及任何 Session event declaration 的 `RequiredDurability < EventBound`。Binding declaration 引用 RegistryID 时，Catalog 必须找到匹配 runtime registry，并要求其 `BindingExtractor()` 返回 `(extractor, true)` 且 extractor 非 nil/非 typed nil；`false` 明确表示该 registry 不支持 Binding extraction，不能被该 declaration 引用。

## 3. event schema、codec 与 upcast

```go
type EventDefinition struct { Type session.EventType; Schemas []EventSchema; Upcasters []Upcaster }
type EventSchema struct {
    Version uint16
    Codec PayloadCodec
    Bindings []BindingReferenceDefinition
    RegistryRequirements []CodecRegistryRequirement
}
type PayloadCodec interface {
    Encode(value any) (jsonstable.Value, error)
    Decode(wire jsonstable.Value) (any, error)
    Validate(value any) error
}
type CodecRegistryDescriptor struct {
    ID RegistryID; Version uint16; WireManifest jsonstable.Value; WireProfile string
}
type CodecRegistryRequirement struct { ID RegistryID; Version uint16; WireManifest jsonstable.Value }
type Upcaster interface {
    FromVersion() uint16; ToVersion() uint16
    Upcast(jsonstable.Value) (jsonstable.Value, error)
}
type DecodedEvent struct {
    Event session.SessionEvent; ModuleID ModuleID
    PersistedVersion uint16; TargetVersion uint16; Value any
}
```

**EXT-COD-1** descriptor 是 wire manifest/profile；runtime registry 是显式提供的 immutable implementation。descriptor 不声称 hash 或证明代码。Catalog 必须逐字段匹配 requirement 和 runtime descriptor。

**EXT-COD-2** codec、Validate、Upcast、Binding extraction 必须纯、确定、无 IO，不读 clock/random/environment/mutable global。Decode wire-first：先验证 object、discriminator（如适用）、kind、schema、limits 和 unknown policy，再构造 value。Encode/Decode 必须拒绝 nil、typed nil、kind mismatch、未知 kind 和非 canonical value；有效值须满足 `Encode → Decode → Encode` 的 canonical round-trip。

**EXT-COD-3** 一个 EventType 的 upcast 路径只能线性向前；upcast 仅为 projection 生成 typed view，不改写 committed Session events。Catalog 对未知 EventType 不作 decode。

## 4. Binding reference declaration

```go
type Cardinality struct { Min uint32; Max *uint32 }
type BindingReferenceDefinition struct {
    JSONPointer string // 仅普通 JSON payload 路径
    RegistryID RegistryID // 仅 custom runtime-registry 路径
    Cardinality Cardinality
    AllowedSchemes []artifact.Scheme
    RequiredDurability artifact.Durability
}
type BindingOccurrence struct { BindingID artifact.BindingID; Location string }
type BindingExtractor interface { BindingIDs(value any) ([]artifact.BindingID, error) }
```

**EXT-REF-1** declaration 恰选一种：非空 JSONPointer，或非空 RegistryID。JSONPointer 路径仅在普通 canonical JSON payload 上执行；RegistryID 路径把该 event 的 decoded typed value 交给对应 RuntimeRegistry 的 BindingExtractor。custom registry（包括 Message registry）必须以 `BindingIDs(value)` 返回内部全部引用，不能回退为 JSONPointer 猜测。提取保留 occurrence appearance order，随后 append group 才 sorted-unique。

**EXT-REF-2** Catalog 验证 cardinality、pointer grammar、RegistryID extractor availability 与 scheme/durability declaration。admission 解析每个 Binding，验证 Scheme、最低 durability、resolvability 与 host access policy；任何遗漏或违反均拒绝整个 group。所有 Session event declaration 的最低 durability 至少为 `EventBound`。

## 5. SemanticAppender

普通 producer 只持有 SemanticAppender；raw `session.Store` 只能注入它或由 trusted adapter/recovery 使用。

```go
type TypedEvent struct {
    EventID session.EventID; Type session.EventType; SchemaVersion uint16
    RecordedAtUnixMilli int64; SourceEvents []session.EventID; Value any
}
type SemanticAppendRequest struct {
    SessionID session.SessionID; ExpectedHead session.Head; CommitID session.CommitID
    CausationID es.CausationID; CorrelationID string; Events []TypedEvent
}
type SemanticAppendOutcome string
const (
    SemanticApplied SemanticAppendOutcome = "applied"
    SemanticAlreadyApplied SemanticAppendOutcome = "already_applied"
    SemanticHeadConflict SemanticAppendOutcome = "head_conflict"
    SemanticCommitConflict SemanticAppendOutcome = "commit_conflict"
    SemanticInvalid SemanticAppendOutcome = "invalid"
    SemanticIndeterminate SemanticAppendOutcome = "indeterminate"
)
type SemanticAppendResult struct {
    Outcome SemanticAppendOutcome; Commit *session.SessionCommit
    BindingSet *artifact.BindingSet; Reconciled bool
}
type SemanticAppender interface {
    AppendSemantic(context.Context, SemanticAppendRequest) (SemanticAppendResult, error)
}
```

为闭合进程崩溃后只有 claim、却没有完整 append request 的状态，单次 semantic append 使用下列最小 durable journal；multi-Session package saga 由 Application coordination 负责。

```go
type SemanticAppendIntentState string
const (
    IntentPending SemanticAppendIntentState = "pending"
    IntentCompleted SemanticAppendIntentState = "completed"
    IntentAborted SemanticAppendIntentState = "aborted"
)
type SemanticAppendIntent struct {
    ClaimID artifact.ClaimID
    ClaimOwner artifact.ClaimOwner
    BindingSet artifact.BindingSet
    Fingerprint es.Digest
    CanonicalRequest session.AppendRequest
    State SemanticAppendIntentState
}
type SemanticAppendJournal interface {
    Prepare(context.Context, SemanticAppendIntent) (SemanticAppendIntent, error)
    Lookup(context.Context, artifact.ClaimID) (SemanticAppendIntent, bool, error)
    Pending(context.Context, artifact.ClaimCursor) ([]SemanticAppendIntent, *artifact.ClaimCursor, error)
    MarkTerminal(context.Context, artifact.ClaimID, SemanticAppendIntentState) error
}
// Before an Event is visible, this capability atomically persists either the
// exact full intent and its matching Prepared claim, or the commit and matching
// Active claim; each outcome leaves a recoverable claim plan.
type AtomicSemanticCommitter interface {
    CommitSemanticAtomic(context.Context, SemanticAppendIntent) (session.AppendResult, error)
}
```

**EXT-APP-1** request 的 Events 是完整 group，不能为空。Appender 对每个 TypedEvent lookup schema、validate、canonical encode、decode-round-trip 和 extract；任一失败不作 IO。它将全部 occurrence 组成 sorted-unique union，并通过 `artifact.BindingSetBuilder.Build(ctx, union)` 构造完整 BindingSet；Extension 只对 Build 已 resolve 的 Binding 按 declaration 执行 Scheme、最低 durability 与 host access admission。Appender 必须将 TypedEvent 的 `EventID`、`RecordedAtUnixMilli`、`SourceEvents` 与 canonical Payload、Type、SchemaVersion 逐字段映射为 `session.UncommittedEvent`，不得生成或替换其中任一值。`SourceEvents` 仍由 Session 的 bytewise sorted-unique、same-resolved-stream 规则验证。

**EXT-APP-2** 对 nonempty BindingSet，先经 `Store.Header(ctx, SessionID)` 读取并验证该 Session 的 Header，唯一派生为：

```text
ClaimID = Digest("twilight.session-extension/claim/v1",
                 claim-profile-version "1",
                 extension-canonical-string(Header.ProtocolVersion),
                 SessionID, CommitID, BindingSet.RefSetDigest)
ClaimOwner = {Kind:"twilight.session/commit",
              Authority:string(SessionID), Identity:string(CommitID)}
```

claim profile version 固定为 `1`；整数使用 Extension canonical string profile（无前导零的十进制），其中 Session `ProtocolVersion` 只能取自已验证 Header。Artifact `WireVersion` 已由 `BindingSet.RefSetDigest` 覆盖，绝不另入 preimage。ClaimID 不含 ExpectedHead。先 Build set，再派生 claim；调用者不得提供或覆盖它们。相同 CommitID retry 的 typed event identity/time/source/payload 或 BindingSet 不同均为 conflict；完全相同 immutable request 的 append fingerprint 稳定。`ExpectedHead` 不入该 fingerprint，且是同一 pending intent 唯一可更新的 CanonicalRequest 字段。retry、recovery 及导入 `twilight.session/commit` active claim 时都必须以该 Header 和同一 preimage 重算并验证 ClaimID。

**EXT-APP-3** 没有 Binding 时不创建 BindingSet、intent 或 claim。否则先构造含 ClaimID、ClaimOwner、BindingSet、Fingerprint、完整 CanonicalRequest 和 Pending State 的 intent，且非原子流程严格为 `journal.Prepare → ledger.Prepare → Store.Commit`。journal `Prepare` 对既有同一 ClaimID 的记录，必须逐字段验证 Fingerprint、ClaimOwner、BindingSet 及 CanonicalRequest 的所有 immutable 字段；只有全部相同才幂等，并且仅可更新 Pending intent 的 `CanonicalRequest.ExpectedHead`，其他差异均为 conflict。`ledger.Prepare` 必须使用 intent 内的 ClaimID、ClaimOwner、BindingSet。支持 `AtomicSemanticCommitter` 的 adapter 可跳过单独 choreography，但必须原子持久化同一完整 intent 与匹配 Prepared claim，或直接原子 commit 并建立匹配 Active claim；两种路径均不得在 Event 可见前缺少可恢复 claim plan，并使用等价 terminal handling。

```text
AppendSemantic(request):
  validate/encode group → extract → build BindingSet → map canonical AppendRequest
  derive ClaimID/ClaimOwner and append fingerprint
  if AtomicSemanticCommitter exists:
      result = capability.CommitSemanticAtomic(intent)
  else:
      journal.Prepare(intent); ledger.Prepare(intent.ClaimID, intent.ClaimOwner, intent.BindingSet)
      result = Store.Commit(intent.CanonicalRequest)
  Applied/AlreadyApplied: ledger.Activate(ClaimID); journal.MarkTerminal(Completed)
  HeadConflict: retain Prepared claim and pending intent; return actual head
  Invalid/CommitConflict: journal.MarkTerminal(Aborted); ledger.AbortPrepared(ClaimID)
  unknown result: retain Prepared claim and pending intent; return Indeterminate
```

只有 claim 已 Activate，`SemanticApplied` 或 `SemanticAlreadyApplied` 才能返回；Activate 或 terminal marking 的未知/失败必须保留 Prepared/pending state 并返回 `SemanticIndeterminate`。`HeadConflict`、`CommitConflict`、`Invalid` 分别映射为同名 Semantic outcome，后两者仅在 journal 已标记 Aborted 且 AbortPrepared 成功后返回。A `HeadConflict` retry 保持相同 CommitID、fingerprint、ClaimID 和所有 event identity/time/source/payload/BindingSet，仅更新 ExpectedHead。`LookupCommit` 找到 canonical commit 时 Activate；返回 NotFound 时保留 Prepared/pending state，留待 retry 或 recovery。明确的 terminal `Invalid` 或 `CommitConflict` 才可先 journal 标记 `Aborted`，再 AbortPrepared。

**EXT-APP-4** recovery 扫描 journal Pending 与 ledger PreparedClaims 的并集。

对每个 journal Pending intent，第一步总是以 `intent.ClaimOwner` 和 `intent.BindingSet` 幂等调用 `ledger.Prepare(intent.ClaimID, ...)`，并逐字段验证返回 claim 的 ID、owner、set；只有其 state 为 Prepared 或 Active 且完全匹配，才可 `LookupCommit` 或 retry `Store.Commit(intent.CanonicalRequest)`。

journal 已存在而 claim 尚不存在是正常 crash point：NotFound 只表示该 Prepare 之前尚无 claim，且上述 Prepare 必须重建 Prepared claim。找到且与 intent fingerprint 匹配的 canonical commit 时 Activate 并 MarkTerminal(Completed)；只有 journal 已 durable 标记 Aborted 才可 AbortPrepared。

对 ledger 的每个 Prepared claim，journal 缺失或无法提供同 ID、owner、set 的 intent 时，必须保持 Prepared 并返回 `SemanticIndeterminate`/运维错误，绝不得 Commit 或 Abort。claim/journal 不匹配、非 Prepared/Active state、LookupCommit NotFound、unknown 或查询失败时，同样保留可恢复 state 并返回 indeterminate。journal 的 durable `Aborted` 标记授权 `AbortPrepared`。因此每个 crash point 都保有可恢复状态，直到完整 request 的 terminal 状态得到 durable confirmation。

## 6. pure projection 与 snapshot

```go
type Consumption struct { Type session.EventType; Versions []uint16 }
type ProjectionDefinition struct {
    ID ProjectionID; Version ProjectionVersion
    Consumes []Consumption; Ignores []session.EventType; RequireComplete []ModuleID
    Initial func() (any, error)
    Apply func(any, DecodedEvent) (any, error)
    StateCodec PayloadCodec
}
type ProjectionRunRequest struct { Definition ProjectionDefinition; Events []session.SessionEvent; InitialState any }
type ProjectionRunResult struct { State any; Applied uint64; Ignored uint64 }
type ProjectionRunner interface { Run(ProjectionRunRequest) (ProjectionRunResult, error) }
```

**EXT-PRJ-1** Initial、Apply、StateCodec、runner decode/upcast 都必须 pure。runner 只接受已验证的 complete commit sequence；一个 commit 内任一 event 失败，不得发布该 commit 的 partial state。

**EXT-PRJ-2** `Consumes` 表示必须 decode/upcast/handle 的类型版本，`Ignores` 是显式已知跳过，二者不得重叠。出现属于 `RequireComplete` module 的 unknown 或未分类 event 必须失败。其他 module event 可忽略。

**EXT-PRJ-3** snapshot 使用 Session snapshot envelope。只有 ProjectionID、ProjectionVersion、StateCodec canonical validation 和 Session coverage 都匹配时可复用；否则从 log 重建。

## 7. errors 与 conformance

```go
type ErrorCode string
const (
    ErrInvalid ErrorCode = "invalid"; ErrUnknownEvent ErrorCode = "unknown_event"
    ErrSchema ErrorCode = "schema"; ErrCodec ErrorCode = "codec"
    ErrBinding ErrorCode = "binding"; ErrConflict ErrorCode = "conflict"
    ErrIndeterminate ErrorCode = "indeterminate"
)
type Error struct { Code ErrorCode; Type session.EventType; Detail string }
func (Error) Error() string
```

Conformance 必须验证：

- **EXT-CAT-1、EXT-CAT-2**：static immutable Catalog、所有 ownership/version conflict、拒绝低于 EventBound 的 Session declaration、optional BindingExtractor availability/typed-nil rejection；
- **EXT-COD-1、EXT-COD-2、EXT-COD-3**：wire-first、安全 codec、canonical round-trip 和 linear upcast；
- **EXT-REF-1、EXT-REF-2**：pointer/custom registry 全量提取、cardinality、extractor requirement、scheme/durability admission；
- **EXT-APP-1、EXT-APP-2、EXT-APP-3、EXT-APP-4**：TypedEvent→UncommittedEvent 全字段映射、由 Header ProtocolVersion/claim profile v1/RefSetDigest 派生的 stable ClaimID、完整 intent 的逐字段 journal idempotency（仅 ExpectedHead 可更新）、durable intent-before-claim、claim-before-commit、journal-only claim rebuild、claim-only journal-missing indeterminate、terminal abort evidence、active-claim import revalidation、atomic capability 等价性与 crash recovery；
- **EXT-PRJ-1、EXT-PRJ-2、EXT-PRJ-3**：pure fold、commit boundary、Consumes/Ignores/RequireComplete 和 snapshot equivalence。
