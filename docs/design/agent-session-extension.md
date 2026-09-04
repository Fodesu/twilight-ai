# Twilight Agent Session Module Framework

状态：设计草案。无实现；Registry、SemanticAppender 与 projection 在 Memory reference implementation 通过 conformance 前不冻结。2026-09-04 第二次修订：v1 收缩为 first-party 固定注册表与单事务 append；Application module、通用 Catalog 与两阶段 journal 移入附录，不进入 v1 conformance。

本文定义建立在 `agent/session` 与 `agent/artifact` 之上的 Session Module Framework。实现包路径为 `agent/session/extension`；文中的"必须""不得""应该"是协议约束；JSON canonicalization 与 digest 遵循 `agent/jsonstable`、`agent/es`。

## 1. 范围与依赖

```text
agent/artifact  ←  Session Module Framework  →  agent/session
                                      ↑
                          first-party modules: chatlog、turn、run
```

Framework 负责 typed event codec 与 payload 版本、Binding declaration 与 admission、claim 与 commit 的同事务写入、pure projection。first-party Source 为 `twilight`，其 Module 为 `chatlog`、`turn` 与 `run`。

**EXT-SCP-1** v1 只有一个写入路径：`SemanticAppender`。它有两个入口，`AppendSemantic`（CAS）与 `AppendSemanticIn`（临界区），两者执行同一套 codec、admission 与 claim 规则，都在 Session Store 的一个事务内完成。Run 的 `Runtime` 经 `AppendSemanticIn` 写入；Turn 的 Start、Retry、Settle 经任一入口写入。raw `session.Store` 只由 Appender、Application 的 trusted adapter 与测试持有。

**EXT-SCP-2** v1 的模块集合在编译期固定为三个 first-party module。Application 自定义 Source 与 Module、通用 `Catalog` 构建校验、`RuntimeRegistry` 与自定义 `BindingExtractor` 见附录 B；远程或跨存储 adapter 的两阶段提交见附录 C。

## 2. Registry 与版本

```go
type SourceID string
type ModuleID string
type ProjectionID string
type ProjectionVersion uint16
type PayloadVersion uint16

const SourceTwilight SourceID = "twilight"

type EventDefinition struct {
    Type session.EventType
    Current PayloadVersion                  // Encode 使用的版本
    Codecs map[PayloadVersion]PayloadCodec  // Decode 支持的全部版本
    Bindings []BindingReferenceDefinition
}
type ModuleDescriptor struct {
    ID ModuleID
    Events []EventDefinition
    Projections []ProjectionDefinition
}
type Registry struct {
    ProtocolVersion uint16
    Profile session.ProtocolProfile
    // immutable indexes for modules, events and projections
}
func FirstPartyRegistry(profile session.ProtocolProfile, parts PartsRegistry) (*Registry, error)
func (r *Registry) LookupEvent(session.EventType) (ModuleID, EventDefinition, bool)
func (r *Registry) ModuleForEvent(session.EventType) (ModuleID, bool)
func (r *Registry) LookupProjection(ProjectionID, ProjectionVersion) (ProjectionDefinition, bool)
func (r *Registry) Decode(session.SessionEvent) (DecodedEvent, error)
```

**EXT-REG-1** EventType 为 `twilight/<ModuleID>/<local-name>`，例如 `twilight/chatlog/assistant`、`twilight/turn/started`、`twilight/run/model_step_prepared`。Digest domain 与 EventType 相同。一个 Registry 中 EventType、ProjectionID 均唯一；`FirstPartyRegistry` 构建后只读，`Profile.Version()` 必须等于 `ProtocolVersion`。

**EXT-REG-2** payload 版本与 kernel 版本分离（SES-VER-1）。每个 payload object 的第一层携带整数字段 `v`；`Encode` 写入 `Current`，`Decode` 读取 `v` 并选择 `Codecs[v]`。同一 EventType 的旧版本 codec 永久保留在 Registry 中，旧事件不迁移。一个模块的 payload 非兼容变化只增加该 EventType 的 `Current` 与一个新 codec，不影响其他模块，不触发 kernel 版本变化。

**EXT-REG-3** `Decode` 对未注册的 EventType，或已注册 EventType 的未注册 `v`，返回 `DecodedEvent{Unknown:true}` 并保留原始 payload；是否接受由 projection 的 `RequireComplete` 决定（EXT-PRJ-2）。Encode 对 `Current` 之外的版本拒绝。

## 3. event codec

```go
type PayloadCodec interface {
    Encode(value any) (jsonstable.Value, error) // 不含 v；Registry 负责加入
    Decode(wire jsonstable.Value) (any, error)
    Validate(value any) error
}
type DecodedEvent struct {
    Event session.SessionEvent
    ModuleID ModuleID
    Version PayloadVersion
    Value any
    Unknown bool
}
```

**EXT-COD-1** codec、Validate、Binding extraction 必须纯、确定、无 IO，不读 clock/random/environment/mutable global。Decode wire-first：先验证 object、discriminator（如适用）、kind 与 limits，再构造 value。Encode/Decode 必须拒绝 nil、typed nil、kind mismatch、未知 kind 和非 canonical value；有效值须满足 `Encode → Decode → Encode` 的 canonical round-trip。

**EXT-COD-2** 已提交事件的 payload 保持原始 canonical bytes。`v` 字段由 Registry 在 Encode 后加入、Decode 前取出，codec 自身不读写它；payload 的其他第一层字段不得命名为 `v`。

## 4. Binding reference declaration 与 admission

```go
type Cardinality struct { Min uint32; Max *uint32 }
type BindingReferenceDefinition struct {
    JSONPointer string   // 普通 JSON payload 路径；与 Parts 二选一
    Parts bool           // 使用 PartsRegistry 提取（chatlog 的 assistant、tool_result、summary）
    Cardinality Cardinality
    AllowedSchemes []artifact.Scheme
    RequiredDurability artifact.Durability
}
type PartsRegistry interface {
    BindingIDs(value any) ([]artifact.BindingID, error) // appearance order
}
```

**EXT-REF-1** declaration 恰选一种：非空 JSONPointer，或 `Parts=true`。JSONPointer 路径在 canonical JSON payload 上执行；Parts 路径把 decoded typed value 交给 `PartsRegistry`，它必须返回内部全部引用，不能回退为 JSONPointer 猜测。提取保留 appearance order，随后 group 才 sorted-unique。

**EXT-REF-2** Registry 构建时验证 cardinality、pointer grammar 与 scheme/durability declaration；所有 declaration 的最低 durability 至少为 `EventBound`。admission 解析每个 Binding，验证 Scheme、最低 durability、resolvability 与 host access policy；任何遗漏或违反均拒绝整个 group，不作任何写入。

## 5. SemanticAppender

```go
type TypedEvent struct {
    EventID session.EventID; Type session.EventType
    RecordedAtUnixMilli int64; SourceEvents []session.EventID; Value any
}
type SemanticGroup struct {
    CommitID session.CommitID
    CausationID es.CausationID; CorrelationID string
    Events []TypedEvent
}
type SemanticAppendRequest struct {
    SessionID session.SessionID; ExpectedHead session.Head
    Group SemanticGroup
}
// SemanticTx 是 session.SessionTx 加 typed decode；所有方法在同一事务内生效。
type SemanticTx interface {
    session.SessionTx
    Decode(session.SessionEvent) (DecodedEvent, error)
}
type SemanticCommitFn func(SemanticTx) (*SemanticGroup, error)

type SemanticAppendOutcome string
const (
    SemanticApplied SemanticAppendOutcome = "applied"
    SemanticAlreadyApplied SemanticAppendOutcome = "already_applied"
    SemanticHeadConflict SemanticAppendOutcome = "head_conflict"
    SemanticCommitConflict SemanticAppendOutcome = "commit_conflict"
    SemanticInvalid SemanticAppendOutcome = "invalid"
    SemanticNoop SemanticAppendOutcome = "noop" // fn 返回 nil
)
type SemanticAppendResult struct {
    Outcome SemanticAppendOutcome
    Commit *session.SessionCommit
    Claim *artifact.RetentionClaim // group 含 Binding 时非空
}
type SemanticAppender interface {
    AppendSemantic(context.Context, SemanticAppendRequest) (SemanticAppendResult, error)
    AppendSemanticIn(context.Context, session.SessionID, SemanticCommitFn) (SemanticAppendResult, error)
}
```

**EXT-APP-1** group 的 Events 是完整 group，不能为空。Appender 对每个 TypedEvent lookup EventDefinition、Validate、canonical Encode（加入 `v`）、decode round-trip 和 Binding extraction；任一失败不作写入。它将全部 occurrence 组成 sorted-unique union，并通过 `artifact.BindingSetBuilder.Build(ctx, union)` 构造完整 BindingSet，再按 declaration 执行 Scheme、最低 durability 与 host access policy。Appender 必须将 TypedEvent 的 `EventID`、`RecordedAtUnixMilli`、`SourceEvents` 与 canonical Payload、Type 逐字段映射为 `session.UncommittedEvent`，不得生成或替换其中任一值。

**EXT-APP-2** 对 nonempty BindingSet，以已验证 Header 唯一派生：

```text
ClaimID = Digest("twilight/session-extension/claim",
                 claim-profile-version "1",
                 canonical-string(Header.ProtocolVersion),
                 SessionID, CommitID, BindingSet.RefSetDigest)
ClaimOwner = {Kind:"twilight/session/commit",
              Authority:string(SessionID), Identity:string(CommitID)}
```

ClaimID 不含 ExpectedHead 与时间戳。相同 CommitID 的重试若 typed event identity、source、payload 或 BindingSet 不同为 conflict。

**EXT-APP-3** 两个入口都在 `Store.CommitIn` 的一个事务内完成，顺序为：

```text
AppendSemanticIn(sessionID, fn):
  Store.CommitIn(sessionID, func(tx):
    group = fn(SemanticTx{tx})            // nil → Noop，不追加
    if existing := tx.LookupCommit(group.CommitID): 
        fingerprint 相同 → AlreadyApplied；ledger.ActivateIn(tx.control, claim) 幂等重放；返回
        否则 → CommitConflict
    validate/encode/extract → build BindingSet → admission
    if BindingSet 非空: ledger.ActivateIn(tx.control["twilight/artifact/claim"], ClaimID, Owner, Set)
    return AppendRequest{ExpectedHead: tx.Head(), ...})

AppendSemantic(request):
  同上，fn 固定为：tx.Head() == request.ExpectedHead ? request.Group : HeadConflict
```

claim 在 `Active` 状态写入控制面 KV，与 commit 同一事务；没有 `Prepared` 状态，也没有 journal。事务失败时 commit 与 claim 都不可见；事务成功时两者同时可见。`HeadConflict`、`CommitConflict`、`Invalid` 分别映射为同名 Semantic outcome，且不留下任何写入。claim 的 Release 由 Application 在事件 retention 结束时经 `RetentionLedger.ReleaseActive` 执行，不属于 Appender。

**EXT-APP-4** 崩溃恢复不需要扫描：没有 in-flight 状态。Appender 调用返回未知结果时，调用方以同一 CommitID 重试，得到 `AlreadyApplied` 或首次 `Applied`。

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
    Registry *Registry
    Definition ProjectionDefinition
    Events []session.SessionEvent
    InitialState any
}
type ProjectionRunResult struct { State any; Applied uint64; Ignored uint64 }
type ProjectionRunner interface { Run(ProjectionRunRequest) (ProjectionRunResult, error) }
```

**EXT-PRJ-1** Initial、Apply、StateCodec 和 runner decode 都必须 pure。runner 只接受已验证的 complete commit sequence；一个 commit 内任一 event 失败，不得发布该 commit 的 partial state。

**EXT-PRJ-2** `Consumes` 表示必须 decode/handle 的 EventType，`Ignores` 是显式已知跳过，二者不得重叠。出现属于 `RequireComplete` module 的 Unknown event 必须失败；其他 module 的 event 可忽略。读取时以 `Consumes` 与 `RequireComplete` module 的前缀作为 `Types` 过滤（SES-REP-2、SessionTx.Tail），使读取代价与 projection 消费的事件数成正比。

**EXT-PRJ-3** snapshot 使用 Session snapshot envelope。只有 ProjectionID、ProjectionVersion、StateCodec canonical validation 和 `Through` 前缀校验都通过时可复用；否则从 log 重建。写入策略由 projection 自定，可以在 `SemanticTx.SaveSnapshot` 中与 commit 同事务写入，也可以异步写入。

## 7. errors 与 conformance

```go
type ErrorCode string
const (
    ErrInvalid ErrorCode = "invalid"; ErrUnknownEvent ErrorCode = "unknown_event"
    ErrCodec ErrorCode = "codec"; ErrBinding ErrorCode = "binding"
    ErrConflict ErrorCode = "conflict"
    ErrUnsupportedProfile ErrorCode = "unsupported_profile"
)
type Error struct { Code ErrorCode; Type session.EventType; Detail string }
func (Error) Error() string
```

v1 conformance 必须验证：

- **EXT-REG-1、EXT-REG-2、EXT-REG-3**：immutable Registry、Profile/ProtocolVersion binding、`v` 字段的写入与选择、多版本 codec 共存、Unknown 事件保留 raw payload；
- **EXT-COD-1、EXT-COD-2**：wire-first、安全 codec、canonical round-trip、`v` 保留字段；
- **EXT-REF-1、EXT-REF-2**：pointer 与 Parts 全量提取、cardinality、scheme/durability admission、拒绝时无写入；
- **EXT-APP-1 至 EXT-APP-4**：TypedEvent→UncommittedEvent 全字段映射、由 Header ProtocolVersion 与 RefSetDigest 派生的 stable ClaimID、claim 与 commit 同事务（崩溃点注入后两者同时存在或同时缺失）、AlreadyApplied 的 claim 幂等、两个入口产生等价 commit、Noop 不追加；
- **EXT-PRJ-1、EXT-PRJ-2、EXT-PRJ-3**：pure fold、commit boundary、Consumes/Ignores/RequireComplete、Types 过滤读取与全量读取等价、snapshot equivalence。

## 附录 B：Application module 与通用 Catalog（不进入 v1）

Application 注册自己的 `SourceID`（例如 `acme`）与 Module 时，EventType 为 `<SourceID>/<ModuleID>/<local-name>`；`BuildCatalog(CatalogBuildRequest)` 在启动时把多个 Source 的 ModuleDescriptor、RuntimeRegistry 与 artifact SchemeDefinition 组合为只读索引，拒绝 duplicate owner、namespace mismatch、registry requirement mismatch、非法 Binding declaration 与低于 `EventBound` 的 declaration。`RuntimeRegistry` 以 `CodecRegistryDescriptor{ID, WireManifest, WireProfile}` 描述自定义 codec 与 `BindingExtractor`，Catalog 逐字段匹配 requirement 与 descriptor。v1 的 `PartsRegistry` 是这一机制的唯一实例，直接作为 `FirstPartyRegistry` 的参数注入。

## 附录 C：两阶段 semantic append（不进入 v1）

当 Session Store、RetentionLedger 或 BindingStore 不在同一事务域（远程 Store、跨数据库）时，claim 与 commit 无法同事务写入，需要 `Prepared` claim 状态与 durable `SemanticAppendJournal`：

```text
journal.Prepare(intent{ClaimID, Owner, BindingSet, Fingerprint, CanonicalRequest, Pending})
→ ledger.Prepare(ClaimID, Owner, Set)      // Prepared claim 为 in-flight 写入提供 GC 保护
→ Store.Commit(CanonicalRequest)
Applied/AlreadyApplied → ledger.Activate；journal.MarkTerminal(Completed)
Invalid/CommitConflict → journal.MarkTerminal(Aborted)；ledger.AbortPrepared
未知结果 → 保留 Prepared 与 Pending，返回 Indeterminate
```

恢复扫描 journal Pending 与 ledger Prepared 的并集：先以 intent 幂等重建 Prepared claim，再 `LookupCommit` 决定 Activate 或重试；只有 journal 的 durable `Aborted` 标记授权 `AbortPrepared`；claim 存在而 intent 缺失时保持 Prepared 并报告 indeterminate。支持 `AtomicSemanticCommitter` 的 adapter 可以跳过 choreography，但必须原子持久化同一完整 intent 与匹配 claim。这些规则对应 artifact 附录中的 `Prepared` 状态与 reconciler。
