# Twilight Agent Session Module Framework

状态：设计规范

本文定义建立在 `agent/session` 与 `agent/artifact` 之上的静态 Session Module Framework。实现包路径暂为 `agent/session/extension`；文中的“必须”“不得”“应该”是协议约束；JSON canonicalization 与 digest 遵循 `agent/jsonstable`、`agent/es`。

## 1. 范围与依赖

```text
agent/artifact  ←  Session Module Framework  →  agent/session
                                      ↑
                            first-party 与 Application modules
```

Framework 负责 Module ownership、static startup composition、immutable Catalog、typed event
codec、Binding declaration、SemanticAppender 和 pure projection。first-party Source 为
`twilight`，其 Module 为 `chatlog` 与 `turn`。每次进程启动构建一套与 Session protocol
version 绑定的 immutable Catalog。Application 注册自己的 Source（例如 `acme`）与 Module。

**EXT-SCP-1** Session Store protocol 由 Session kernel 负责；多 Session transaction、package import、saga、operation log 与 provider policy 由 Application/adapter 负责。未知 event 的 archive Binding manifest 由 Application archive coordinator 处理。

`SemanticAppender` 是 Session Module Framework 面向 typed producer 的写入协调器：它执行 codec、binding
admission、claim/journal 编排，并调用 Session Store 的 canonical append。claim、journal 与
artifact binding 的一致性由 Session Module Framework 维护；Session kernel 继续维护 commit、CAS、digest 与
replay 规则。

## 2. Module、Catalog 与版本

```go
type SourceID string
type ModuleID string
type ProjectionID string
type ProjectionVersion uint16
type RegistryID string

const SourceTwilight SourceID = "twilight"

type ModuleDescriptor struct {
    Source SourceID
    ID ModuleID
    Events []EventDefinition
    Projections []ProjectionDefinition
    Registries []CodecRegistryDescriptor
    Schemes []artifact.SchemeDefinition
}
type Catalog struct {
    ProtocolVersion uint16
    Profile session.ProtocolProfile
    // immutable indexes for modules, events, projections, registries and schemes
}
type CatalogBuildRequest struct {
    ProtocolVersion uint16
    Profile session.ProtocolProfile
    Modules []ModuleDescriptor
    Registries []RuntimeRegistry
}
type RuntimeRegistry interface {
    Descriptor() CodecRegistryDescriptor
    BindingExtractor() (BindingExtractor, bool)
}
func BuildCatalog(CatalogBuildRequest) (*Catalog, error)
func (c *Catalog) LookupEvent(session.EventType) (SourceID, ModuleID, EventDefinition, bool)
func (c *Catalog) ModuleForEvent(session.EventType) (SourceID, ModuleID, bool)
func (c *Catalog) LookupProjection(ProjectionID, ProjectionVersion) (ProjectionDefinition, bool)
```

`Catalog` 是进程启动时的 Module 目录：它把一个协议版本下可用的 Module、EventType、
payload codec、projection、runtime registry 与 artifact scheme 组合成只读索引。它供
typed append、decode、binding admission 与 projection lookup 使用；Session 数据、Run
状态和事件日志继续保存在各自的 authority 中。`Catalog.Profile` 是同一版本的
`session.ProtocolProfile`，两者在 `BuildCatalog` 时绑定。

**EXT-CAT-1** `SourceID` 与 `ModuleID` 为小写 ASCII。first-party Source 为 `twilight`，
其 Module 为 `chatlog` 与 `turn`。Application 注册自己的 Source（例如 `acme`）。
EventType 为 `<SourceID>/<ModuleID>/<local-name>`，例如 `twilight/chatlog/assistant`、
`twilight/turn/started`。Digest domain 与 EventType 相同；wire 与 digest 形状由 Catalog
绑定的 Session `ProtocolVersion` 决定。一个 Catalog 中 `(Source, ModuleID)`、EventType、
ProjectionID、Scheme、RegistryID 均唯一。Build defensive-copy 所有 descriptor
和 runtime registry，成功后只读且与注册顺序无关。

事件类型在一个 Catalog 中只有一个当前定义。`ProtocolVersion` 覆盖已经持久化的
payload 格式与 Session wire；ProjectionVersion 只标识 projection 的派生状态格式。Catalog
本身属于进程启动时的组合配置，不进入 Session wire、event digest 或 commit identity。
现有 EventType 的 wire identity 由其 ProtocolVersion 固定。版本判断遵循旧 reader 的
语义兼容性：旧 reader 读取新 writer 的 log 后仍能保持 canonical validation、commit/fork/
replay 顺序、history/context/recovery 与 digest chain 语义时，继续使用当前 ProtocolVersion。
安全可忽略的新 persisted EventType 与真正 optional 的 payload 字段属于这一类；非持久化
projection、registry 或 scheme 的增加沿用当前版本。

旧 reader 无法保持上述语义的 event、payload、envelope 或 commit 变化提升
`ProtocolVersion`。影响 Session 核心重建的新增 EventType 也属于这一类。ProjectionVersion
管理单个派生快照的 state codec；projection 实现变化只有在改变 Session 核心重建语义时
才推动 ProtocolVersion。

**EXT-CAT-2** Build 必须拒绝 duplicate owner、namespace mismatch、无效 ProtocolVersion、registry requirement mismatch、非法 Binding declaration、以及任何 Session event declaration 的 `RequiredDurability < EventBound`。Binding declaration 引用 RegistryID 时，Catalog 必须找到匹配 runtime registry，并要求其 `BindingExtractor()` 返回 `(extractor, true)` 且 extractor 非 nil/非 typed nil；`false` 明确表示该 registry 不支持 Binding extraction，不能被该 declaration 引用。

**EXT-CAT-3** `BuildCatalog` 要求 `Profile` 非 nil（含 typed nil）且
`Profile.Version() == ProtocolVersion`。一个 Catalog 只服务一个 ProtocolVersion；typed
append、event decode、projection run 与 binding admission 先校验 Session Header 的版本
与 Catalog 版本一致。当前版本之外的 Session 先经外部 migration 转换，再进入该 Catalog。

## 3. event codec

```go
type EventDefinition struct {
    Type session.EventType
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
    ID RegistryID; WireManifest jsonstable.Value; WireProfile string
}
type CodecRegistryRequirement struct { ID RegistryID; WireManifest jsonstable.Value }
type DecodedEvent struct {
    Event session.SessionEvent; ModuleID ModuleID
    Value any
}
```

**EXT-COD-1** descriptor 是 wire manifest/profile；runtime registry 是显式提供的 immutable implementation。descriptor 不声称 hash 或证明代码。Catalog 必须逐字段匹配 requirement 和 runtime descriptor。

**EXT-COD-2** codec、Validate、Binding extraction 必须纯、确定、无 IO，不读 clock/random/environment/mutable global。Decode wire-first：先验证 object、discriminator（如适用）、kind、ProtocolVersion/profile 和 limits，再构造 value。Encode/Decode 必须拒绝 nil、typed nil、kind mismatch、未知 kind 和非 canonical value；有效值须满足 `Encode → Decode → Encode` 的 canonical round-trip。

**EXT-COD-3** EventDefinition 的 Codec 遵循 Catalog 的 ProtocolVersion。已提交事件的
payload 保持原始 canonical bytes。旧 reader 可以安全保留并忽略的新 EventType 或真正
optional 的 payload 字段沿用当前 ProtocolVersion；旧 reader 无法保持 Session 核心语义的
变化提升 ProtocolVersion，并由外部 migration tool 生成当前版本 stream。Catalog 对未知
EventType 保留 raw payload，projection 按 `RequireComplete` 规则决定是否接受该事件。

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
    EventID session.EventID; Type session.EventType
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

**EXT-APP-1** request 的 Events 是完整 group，不能为空。Appender 对每个 TypedEvent lookup event definition、validate、canonical encode、decode-round-trip 和 extract；任一失败不作 IO。它将全部 occurrence 组成 sorted-unique union，并通过 `artifact.BindingSetBuilder.Build(ctx, union)` 构造完整 BindingSet；Session Module Framework 只对 Build 已 resolve 的 Binding 按 declaration 执行 Scheme、最低 durability 与 host access policy。Appender 必须将 TypedEvent 的 `EventID`、`RecordedAtUnixMilli`、`SourceEvents` 与 canonical Payload、Type 逐字段映射为 `session.UncommittedEvent`，不得生成或替换其中任一值。`SourceEvents` 仍由 Session 的 bytewise sorted-unique、same-resolved-stream 规则验证。

**EXT-APP-2** 对 nonempty BindingSet，先经 `Store.Header(ctx, SessionID)` 读取并验证该 Session 的 Header，唯一派生为：

```text
ClaimID = Digest("twilight/session-extension/claim",
                 claim-profile-version "1",
                 extension-canonical-string(Header.ProtocolVersion),
                 SessionID, CommitID, BindingSet.RefSetDigest)
ClaimOwner = {Kind:"twilight/session/commit",
              Authority:string(SessionID), Identity:string(CommitID)}
```

claim profile version 固定为 `1`；整数使用 Session Module canonical string profile（无前导零的十进制），其中 Session `ProtocolVersion` 只能取自已验证 Header。Artifact `WireVersion` 已由 `BindingSet.RefSetDigest` 覆盖，绝不另入 preimage。ClaimID 不含 ExpectedHead。先 Build set，再派生 claim；调用者不得提供或覆盖它们。相同 CommitID retry 的 typed event identity/time/source/payload 或 BindingSet 不同均为 conflict；完全相同 immutable request 的 append fingerprint 稳定。`ExpectedHead` 不入该 fingerprint，且是同一 pending intent 唯一可更新的 CanonicalRequest 字段。retry、recovery 及导入 `twilight/session/commit` active claim 时都必须以该 Header 和同一 preimage 重算并验证 ClaimID。

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
type ProjectionDefinition struct {
    ID ProjectionID; Version ProjectionVersion
    Consumes []session.EventType; Ignores []session.EventType; RequireComplete []ModuleID
    Initial func() (any, error)
    Apply func(any, DecodedEvent) (any, error)
    StateCodec PayloadCodec
}
type ProjectionRunRequest struct {
    Catalog *Catalog
    ProtocolVersion uint16
    Definition ProjectionDefinition
    Events []session.SessionEvent
    InitialState any
}
type ProjectionRunResult struct { State any; Applied uint64; Ignored uint64 }
type ProjectionRunner interface { Run(ProjectionRunRequest) (ProjectionRunResult, error) }
```

**EXT-PRJ-1** Initial、Apply、StateCodec 和 runner decode 都必须 pure。runner 要求
`Catalog` 非 nil、`Catalog.ProtocolVersion == ProtocolVersion`，并只接受已验证的 complete
commit sequence；一个 commit 内任一 event 失败，不得发布该 commit 的 partial state。

**EXT-PRJ-2** `Catalog` 为每个 EventType 提供唯一 `(Source, ModuleID)` 与 EventDefinition；
`ModuleForEvent` 按 EventType 的 `<SourceID>/<ModuleID>/` 前缀识别 owner，因此 required module 的未知事件可以被
发现。`Consumes` 表示必须 decode/handle 的 EventType，`Ignores` 是显式已知跳过，二者不得重叠。
出现属于 `RequireComplete` module 的 unknown 或未分类 event 必须失败；其他 module event
可忽略。

**EXT-PRJ-3** snapshot 使用 Session snapshot envelope。只有 ProjectionID、ProjectionVersion、StateCodec canonical validation 和 Session coverage 都匹配时可复用；否则从 log 重建。

## 7. errors 与 conformance

```go
type ErrorCode string
const (
    ErrInvalid ErrorCode = "invalid"; ErrUnknownEvent ErrorCode = "unknown_event"
    ErrCodec ErrorCode = "codec"; ErrBinding ErrorCode = "binding"
    ErrConflict ErrorCode = "conflict"
    ErrUnsupportedProfile ErrorCode = "unsupported_profile"
    ErrIndeterminate ErrorCode = "indeterminate"
)
type Error struct { Code ErrorCode; Type session.EventType; Detail string }
func (Error) Error() string
```

Conformance 必须验证：

- **EXT-CAT-1、EXT-CAT-2、EXT-CAT-3**：static immutable Catalog、Profile/ProtocolVersion binding、所有 ownership/protocol conflict、拒绝低于 EventBound 的 Session declaration、optional BindingExtractor availability/typed-nil rejection；
- **EXT-COD-1、EXT-COD-2、EXT-COD-3**：wire-first、安全 codec、ProtocolVersion 匹配、兼容性分类与 canonical round-trip；
- **EXT-REF-1、EXT-REF-2**：pointer/custom registry 全量提取、cardinality、extractor requirement、scheme/durability admission；
- **EXT-APP-1、EXT-APP-2、EXT-APP-3、EXT-APP-4**：TypedEvent→UncommittedEvent 全字段映射、由 Header ProtocolVersion/claim profile v1/RefSetDigest 派生的 stable ClaimID、完整 intent 的逐字段 journal idempotency（仅 ExpectedHead 可更新）、durable intent-before-claim、claim-before-commit、journal-only claim rebuild、claim-only journal-missing indeterminate、terminal abort evidence、active-claim import revalidation、atomic capability 等价性与 crash recovery；
- **EXT-PRJ-1、EXT-PRJ-2、EXT-PRJ-3**：pure fold、commit boundary、Consumes/Ignores/RequireComplete 和 snapshot equivalence。
