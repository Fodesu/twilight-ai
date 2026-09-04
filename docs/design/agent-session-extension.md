# Twilight Agent Session Module Framework

状态：设计草案。无实现；Registry、SemanticAppender、Lease 与 projection 在 Memory reference implementation 通过 conformance 前不冻结。v1 为 first-party 固定注册表与单事务 append；Application module、通用 Catalog 与两阶段 journal 在附录中，不进入 v1 conformance。

本文定义建立在 `agent/session` 与 `agent/artifact` 之上的 Session Module Framework。实现包路径为 `agent/session/extension`；文中的"必须""不得""应该"是协议约束；JSON canonicalization 与 digest 遵循 `agent/jsonstable`、`agent/es`。

## 1. 范围与依赖

```text
agent/artifact  ←  Session Module Framework  →  agent/session
                                      ↑
                          first-party modules: chatlog、turn、run
```

Framework 负责 typed event codec 与 payload 版本、Binding declaration 与 admission、claim 与 commit 的同事务写入、pure projection。first-party Source 为 `twilight`，其 Module 为 `chatlog`、`turn` 与 `run`。

**EXT-SCP-1** v1 只有一个写入路径：`SemanticAppender`。它有两个入口，`AppendSemantic`（CAS）与 `AppendSemanticIn`（临界区），两者执行同一套 codec、admission 与 claim 规则，都在 Session Store 的一个事务内完成。Run 的 `Runtime` 经 `AppendSemanticIn` 写入；Turn 的 Start、Retry、Settle 经任一入口写入。`session.Store` 的 append port（`Commit`、`CommitIn`）只由 Appender 调用；模块读取投影经 `ProjectionReader`（第 6 节），控制面 KV 的临界区外操作经 `Lease`（第 7 节）。

**EXT-SCP-2** 模块集合由组装代码在启动时传入 `BuildRegistry`，运行期不变；v1 的组装恰为三个 first-party module，本层不 import 任何模块包。Application 自定义 Source 与 Module、通用 `Catalog` 构建校验与 `RuntimeRegistry` 见附录 B；远程或跨存储 adapter 的两阶段提交见附录 C。

**EXT-SCP-3** 建立在 kernel 机制之上、供模块共用的类型化设施属于本层，不属于 kernel。v1 有三个：`SemanticAppender`（封装 `Store.CommitIn`）、artifact claim 的 KV 适配（封装控制面 KV）、`Lease`（封装控制面 KV 的条件写与 deadline 枚举，第 7 节）。kernel 对这些设施只提供事务、条件写与 deadline 枚举（SES-API-3），不持有其语义。run 是 `Lease` 的第一个消费者；`Lease` 的 API 不得出现 run 的概念。

**EXT-SCP-4** 模块间依赖单向、固定，以 `Requires` 声明并由 Registry 校验（EXT-REG-4）。v1 三个模块的声明：

| 模块 | Requires |
|---|---|
| `chatlog` | 无。事件字段 `TurnID` 是 opaque 字符串，不需要 turn 的 codec |
| `run` | 无。Run fact 只记录内容 digest，内容由 `Companion` 写成其他模块的事件；`Companion` 是 Runtime 的构造参数，不是模块依赖 |
| `turn` | `run`（`twilight/run/created` v1、`twilight/run/input_accepted` v1、`twilight/run/ended` v1）、`chatlog`（存在即可） |

本层不提供按模块启停的机制；v1 的组装总是传入全部三个模块。

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
    Requires []ModuleRequirement
    Events []EventDefinition
    Projections []ProjectionDefinition
}
// ModuleRequirement 声明对另一模块的依赖：消费它的哪些事件、能处理哪些 payload 版本。
type ModuleRequirement struct {
    Module ModuleID
    Events map[session.EventType][]PayloadVersion // 空表示只要求该模块已注册
}
type Registry struct {
    ProtocolVersion uint16
    Profile session.ProtocolProfile
    // immutable indexes for modules, events and projections
}
func BuildRegistry(profile session.ProtocolProfile, modules ...ModuleDescriptor) (*Registry, error)
func (r *Registry) LookupEvent(session.EventType) (ModuleID, EventDefinition, bool)
func (r *Registry) ModuleForEvent(session.EventType) (ModuleID, bool)
func (r *Registry) LookupProjection(ProjectionID, ProjectionVersion) (ProjectionDefinition, bool)
func (r *Registry) Decode(session.SessionEvent) (DecodedEvent, error)
```

**EXT-REG-1** EventType 为 `twilight/<ModuleID>/<local-name>`，例如 `twilight/chatlog/assistant`、`twilight/turn/started`、`twilight/run/model_step_prepared`。Digest domain 与 EventType 相同。一个 Registry 中 ModuleID、EventType、ProjectionID 均唯一；`BuildRegistry` 校验每个 EventDefinition 的 Type 前缀等于其模块，构建后只读，`Profile.Version()` 必须等于 `ProtocolVersion`。

**EXT-REG-2** payload 版本与 kernel 版本分离（SES-VER-1）。每个 payload object 的第一层携带整数字段 `v`；`Encode` 写入 `Current`，`Decode` 读取 `v` 并选择 `Codecs[v]`。同一 EventType 的旧版本 codec 永久保留在 Registry 中，旧事件不迁移。一个模块的 payload 非兼容变化只增加该 EventType 的 `Current` 与一个新 codec，不影响其他模块，不触发 kernel 版本变化。

**EXT-REG-3** `Decode` 对未注册的 EventType，或已注册 EventType 的未注册 `v`，返回 `DecodedEvent{Unknown:true}` 并保留原始 payload；是否接受由 projection 的 `RequireComplete` 决定（EXT-PRJ-2）。Encode 对 `Current` 之外的版本拒绝。

**EXT-REG-4** 模块间依赖由 `Requires` 声明，Registry 构建时校验，任一失败拒绝构建：

1. `Requires` 指向的模块都已注册，依赖图无环；
2. 每个 projection 的 `Consumes` 与 `Ignores` 中的 EventType 属于本模块或 `Requires` 中的模块；`RequireComplete` 是本模块加 `Requires` 的子集；
3. `Requires.Events` 声明的每个 EventType，被依赖模块该类型的 `Current` 版本必须在声明的版本列表中。被依赖模块升版本而依赖方未声明能处理时，在启动期失败并指出是哪个依赖，而不是在 Apply 时失败。

`Requires` 只表达事件消费依赖。一个模块需要另一模块提供的接口实现（例如 run 的 `Companion` 由 turn 实现）是普通的构造参数，由组装代码注入、为 nil 时构造失败，不进入 `Requires`，Registry 不校验。

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
// BindingExtractor 由声明它的模块提供，从 decoded typed value 中返回全部 Artifact 引用。
type BindingExtractor interface {
    BindingIDs(value any) ([]artifact.BindingID, error) // appearance order
}
type BindingReferenceDefinition struct {
    JSONPointer string          // 普通 JSON payload 路径；与 Extractor 二选一
    Extractor BindingExtractor  // typed value 上的模块自定义提取（chatlog 的 parts）
    Cardinality Cardinality
    AllowedSchemes []artifact.Scheme
    RequiredDurability artifact.Durability
}
```

**EXT-REF-1** declaration 恰选一种：非空 JSONPointer，或非 nil `Extractor`。JSONPointer 路径在 canonical JSON payload 上执行；`Extractor` 接收 decoded typed value，必须返回内部全部引用，不能回退为 JSONPointer 猜测。提取保留 appearance order，随后 group 才 sorted-unique。Extractor 由模块随 EventDefinition 一起声明，本层不维护提取器注册表。

**EXT-REF-2** `BuildRegistry` 验证 cardinality、pointer grammar、Extractor 非 nil（若声明）与 scheme/durability declaration；所有 declaration 的最低 durability 至少为 `EventBound`。admission 解析每个 Binding，验证 Scheme、最低 durability、resolvability 与 host access policy；任何遗漏或违反均拒绝整个 group，不作任何写入。

## 5. SemanticAppender

```go
type TypedEvent struct {
    Type session.EventType
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

**EXT-APP-1** group 的 Events 是完整 group，不能为空。Appender 对每个 TypedEvent lookup EventDefinition、Validate、canonical Encode（加入 `v`）、decode round-trip 和 Binding extraction；任一失败不作写入。它将全部 occurrence 组成 sorted-unique union，并通过 `artifact.BindingSetBuilder.Build(ctx, union)` 构造完整 BindingSet，再按 declaration 执行 Scheme、最低 durability 与 host access policy。Appender 必须将 TypedEvent 的 `RecordedAtUnixMilli`、`SourceEvents` 与 canonical Payload、Type 逐字段映射为 `session.UncommittedEvent`，不得替换其中任一值。

**EXT-APP-5** first-party 事件的 EventID 由 Appender 统一赋值：`EventID = Digest(EventType, CommitID, index)`，index 为该事件在 group 中的位置。TypedEvent 不携带 EventID。同一 CommitID 的重放得到同一组 EventID，因此 EventID 与 CommitID 一起构成幂等判定的一部分（SES-APP-1）。模块不得自行派生 EventID。

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

// ProjectionReader 是模块与 Coordinator 的读取入口：snapshot 加过滤 tail，返回投影状态与其覆盖到的 head。
type ProjectionReader interface {
    Load(ctx context.Context, sid session.SessionID, id ProjectionID, v ProjectionVersion) (state any, through session.Head, err error)
}
```

**EXT-PRJ-1** Initial、Apply、StateCodec 和 runner decode 都必须 pure。runner 只接受已验证的 complete commit sequence；一个 commit 内任一 event 失败，不得发布该 commit 的 partial state。

**EXT-PRJ-2** `Consumes` 表示必须 decode/handle 的 EventType，`Ignores` 是显式已知跳过，二者不得重叠。出现属于 `RequireComplete` module 的 Unknown event 必须失败；其他 module 的 event 可忽略。读取时以 `Consumes` 与 `RequireComplete` module 的前缀作为 `Types` 过滤（SES-REP-2、SessionTx.Tail），使读取代价与 projection 消费的事件数成正比。

**EXT-PRJ-3** snapshot 使用 Session snapshot envelope，`ProjectionKey` 等于 `ProjectionID`，`ProjectionVersion` 同名。只有 ProjectionID、ProjectionVersion、StateCodec canonical validation 和 `Through` 前缀校验都通过时可复用；否则从 log 重建。写入策略由 projection 自定，可以在 `SemanticTx.SaveSnapshot` 中与 commit 同事务写入，也可以异步写入。

**EXT-PRJ-4** `ProjectionReader.Load` 是临界区之外读取投影的唯一入口：读 snapshot、按 EXT-PRJ-3 校验、以过滤 replay 读取其后的 tail、fold，返回状态与 `through`。同一 `ProjectionRunner` 在 `SemanticTx` 内以 `LoadSnapshot` 加 `Tail` 得到相同结果。

## 7. Lease：占用与续期

```go
type LeaseToken string
type Lease struct {
    Namespace session.ControlNamespace
    Key string
    Holder string            // 模块提供的持有者标识；同一 Holder 的重复 Acquire 幂等
    Token LeaseToken         // 本层派生的凭证；Release 与 Renew 必须携带
    DeadlineUnixMilli int64  // 0 表示不超时
    Attrs jsonstable.Value   // 模块自用，本层不解释
}
type AcquireLeaseRequest struct {
    Namespace session.ControlNamespace; Key string
    Holder string; TTL time.Duration; Attrs jsonstable.Value
}

// SemanticTx 内，与本次 commit 同事务
func AcquireLease(tx SemanticTx, commitID session.CommitID, now int64, req AcquireLeaseRequest) (Lease, error)
func ReleaseLease(tx SemanticTx, ns session.ControlNamespace, key string, token LeaseToken) error
func LookupLease(tx SemanticTx, ns session.ControlNamespace, key string) (Lease, bool, error)

// 临界区之外
type Leases struct { Store session.Store }
func (Leases) Lookup(ctx, sid session.SessionID, ns session.ControlNamespace, key string) (Lease, bool, error)
func (Leases) Renew(ctx, sid session.SessionID, ns session.ControlNamespace, key string, token LeaseToken, ttl time.Duration, now int64) error
func (Leases) Expired(ctx, ns session.ControlNamespace, now int64, fn func(session.SessionID, Lease) (bool, error)) error
```

**EXT-LSE-1** 值编码为 canonical JSON `{holder, token, attrs}`，deadline 使用 KV 条目的 deadline 字段（now 加 TTL；TTL 为零时 deadline 为 0）。`Token = Digest("twilight/session-extension/lease", SessionID, Namespace, Key, Holder, commitID)`，由 Acquire 所在 commit 的 CommitID 派生，因此 Acquire 是纯函数，同一 commit 的重放得到同一 Token。

**EXT-LSE-2** `AcquireLease`：条目不存在时写入并返回新 Lease；条目存在且 Holder 相同时返回既有 Lease（幂等，不改 deadline）；Holder 不同时返回 `ErrConflict`，无论既有条目是否已过 deadline。本层不自动回收过期条目；过期的处置由消费模块经 `Expired` 枚举后在自己的 commit 中完成，通常以 `ReleaseLease` 结束。`ReleaseLease`：条目不存在或 Token 不匹配返回 `ErrStale`，否则删除。

**EXT-LSE-3** `Renew` 在临界区之外执行：`ControlGet` 读到条目并核对 Token 后，以读到的值为 `expected` 调用 `ControlCompareAndPut`，只改 deadline、不改值；条目不存在、Token 不匹配、或条件写返回 false（条目已被 Release 或改写）时返回 `ErrStale`。条件写保证续期不会把已删除的条目写回。TTL 为零时 `Renew` 只校验 Token，不改变 deadline。`Expired` 以 `ControlExpired` 枚举 deadline 已过的条目并解码为 Lease；fn 返回 false 停止。

## 8. errors 与 conformance

```go
type ErrorCode string
const (
    ErrInvalid ErrorCode = "invalid"; ErrUnknownEvent ErrorCode = "unknown_event"
    ErrCodec ErrorCode = "codec"; ErrBinding ErrorCode = "binding"
    ErrConflict ErrorCode = "conflict"; ErrStale ErrorCode = "stale"
    ErrUnsupportedProfile ErrorCode = "unsupported_profile"
)
type Error struct { Code ErrorCode; Type session.EventType; Detail string }
func (Error) Error() string
```

v1 conformance 必须验证：

- **EXT-REG-1、EXT-REG-2、EXT-REG-3、EXT-REG-4**：immutable Registry、Profile/ProtocolVersion binding、`v` 字段的写入与选择、多版本 codec 共存、Unknown 事件保留 raw payload、`Requires` 缺失或成环被拒绝、projection 消费未声明模块的事件被拒绝、被依赖事件版本不在声明范围被拒绝；
- **EXT-COD-1、EXT-COD-2**：wire-first、安全 codec、canonical round-trip、`v` 保留字段；
- **EXT-REF-1、EXT-REF-2**：pointer 与 Extractor 全量提取、cardinality、scheme/durability admission、拒绝时无写入；
- **EXT-APP-1 至 EXT-APP-5**：TypedEvent→UncommittedEvent 全字段映射、EventID 由 CommitID 与 index 统一派生、由 Header ProtocolVersion 与 RefSetDigest 派生的 stable ClaimID、claim 与 commit 同事务（崩溃点注入后两者同时存在或同时缺失）、AlreadyApplied 的 claim 幂等、两个入口产生等价 commit、Noop 不追加；
- **EXT-LSE-1、EXT-LSE-2、EXT-LSE-3**：Token 确定派生、同 Holder 幂等、异 Holder conflict、Acquire 与 commit 同事务、Release 的 Token 校验、Renew 与 Release 并发时不复活已删除条目、TTL 为零的 Renew 不改 deadline、Expired 只返回已过 deadline 的条目；
- **EXT-PRJ-1 至 EXT-PRJ-4**：pure fold、commit boundary、Consumes/Ignores/RequireComplete、Types 过滤读取与全量读取等价、snapshot equivalence、`ProjectionReader.Load` 与临界区内读取结果一致。

## 附录 B：Application module 与通用 Catalog（不进入 v1）

Application 注册自己的 `SourceID`（例如 `acme`）与 Module 时，EventType 为 `<SourceID>/<ModuleID>/<local-name>`；`BuildCatalog(CatalogBuildRequest)` 在启动时把多个 Source 的 ModuleDescriptor、RuntimeRegistry 与 artifact SchemeDefinition 组合为只读索引，拒绝 duplicate owner、namespace mismatch、registry requirement mismatch、非法 Binding declaration 与低于 `EventBound` 的 declaration。`RuntimeRegistry` 以 `CodecRegistryDescriptor{ID, WireManifest, WireProfile}` 描述由配置装载的自定义 codec 与提取器，Catalog 逐字段匹配 requirement 与 descriptor。v1 的模块以 Go 值直接传入 `BuildRegistry`，提取器随 EventDefinition 声明，不需要这一层间接。

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
