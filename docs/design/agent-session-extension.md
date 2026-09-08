# Twilight Agent Session Module Framework

状态：设计草案，第二版（2026-09-08）。第一版（双入口 Appender、Lease、事务内投影读写）已由 `agent/session/extension` 实现并验证，随后按 [agent-runtime-refactor.md](agent-runtime-refactor.md) 第 8 节收缩为本版：写入串行与幂等重放由进程内的 `Writer` 承担，kernel 只提供追加日志（[agent-session.md](agent-session.md)）。本版尚无实现；wire 在 conformance 通过前不冻结。

本文定义建立在 `agent/session` 与 `agent/artifact` 之上的 Session Module Framework。实现包路径为 `agent/session/extension`；文中的"必须""不得""应该"是协议约束；JSON canonicalization 与 digest 遵循 `agent/jsonstable`、`agent/es`。

## 1. 范围与依赖

```text
agent/artifact  ←  Session Module Framework  →  agent/session
                                      ↑
                          first-party modules: chatlog、turn、run
```

Framework 负责：typed event codec 与 payload 版本；Binding admission；进程内的写入串行、幂等重放与 claim 顺序（`Writer`）；pure projection 与投影缓存。first-party Source 为 `twilight`，Module 为 `chatlog`、`turn`、`run`。

**EXT-SCP-1** 一个 Session 在一个进程内恰有一个 `Writer`，它持有 kernel 的 `session.Writer`（所有权句柄）。全部写入经 `Writer.Commit`：Run 的 Runtime、Turn 的 Coordinator、接管恢复都是它的调用方。模块读取投影经 `ProjectionReader`。

**EXT-SCP-2** 模块集合由组装代码在启动时传入 `BuildRegistry`，运行期不变；v1 恰为三个 first-party module，本层不 import 任何模块包。Application 自定义 Source、通用 `Catalog` 见附录 B。

**EXT-SCP-3** 模块间依赖单向、固定，以 `Requires` 声明并由 Registry 校验（EXT-REG-4）。v1 三个模块的声明：

| 模块 | Requires |
|---|---|
| `chatlog` | 无 |
| `run` | 无。`Companion` 是 Runtime 的构造参数，不是模块依赖 |
| `turn` | `run`（`twilight/run/run_created`、`input_accepted`、`run_ended` v1）、`chatlog`（存在即可） |

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
    Current PayloadVersion
    Codecs map[PayloadVersion]PayloadCodec
    Bindings []BindingReferenceDefinition
    // Ignorable 为真的事件写入时带 session.SessionEvent.Ignorable，供不认识它的 reader 跳过。
    Ignorable bool
}
type ModuleDescriptor struct {
    ID ModuleID
    Requires []ModuleRequirement
    Events []EventDefinition
    Projections []ProjectionDefinition
}
type ModuleRequirement struct {
    Module ModuleID
    Events map[session.EventType][]PayloadVersion
}
type Registry struct { ProtocolVersion uint16 /* immutable indexes */ }
func BuildRegistry(protocolVersion uint16, modules ...ModuleDescriptor) (*Registry, error)
func (r *Registry) LookupEvent(session.EventType) (ModuleID, EventDefinition, bool)
func (r *Registry) ModuleOf(session.EventType) (ModuleID, bool) // 按 twilight/<module>/ 前缀
func (r *Registry) Encode(session.EventType, any) (jsonstable.Value, PayloadVersion, error)
func (r *Registry) Decode(session.SessionEvent) (DecodedEvent, error)
```

**EXT-REG-1** EventType 为 `twilight/<ModuleID>/<local-name>`。一个 Registry 中 ModuleID、EventType、ProjectionID 均唯一；`BuildRegistry` 校验每个 EventDefinition 的 Type 前缀等于其模块，构建后只读。

**EXT-REG-2** payload 版本与 kernel 版本分离（SES-VER-1）。payload object 第一层携带整数字段 `v`；`Encode` 写入 `Current`，`Decode` 读 `v` 并选择 `Codecs[v]`。旧版本 codec 永久保留，旧事件不迁移。

**EXT-REG-3** `Decode` 对未注册的 EventType 或未注册的 `v` 返回 `DecodedEvent{Unknown:true}` 并保留原始 payload。投影对 Unknown 的处置见 EXT-PRJ-2。

**EXT-REG-4** 模块间依赖由 `Requires` 声明，构建时校验：被依赖模块已注册、依赖图无环、投影消费的 EventType 属于本模块或 `Requires` 中的模块、被依赖事件的 `Current` 在声明的版本列表内。`Requires` 只表达事件消费依赖；接口实现（如 run 的 `Companion` 由 turn 实现）是构造参数，不进入 `Requires`。

## 3. event codec

```go
type PayloadCodec interface {
    Encode(value any) (jsonstable.Value, error) // 不含 v；Registry 加入
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

**EXT-COD-1** codec、Validate、Binding extraction 必须纯、确定、无 IO。Decode wire-first。Encode/Decode 拒绝 nil、typed nil、kind mismatch、未知 kind 与非 canonical value；有效值满足 `Encode → Decode → Encode` 的 canonical round-trip。

**EXT-COD-2** 已提交事件的 payload 保持原始 canonical bytes。`v` 由 Registry 在 Encode 后加入、Decode 前取出；payload 的其他第一层字段不得命名为 `v`。

## 4. Binding reference declaration 与 admission

```go
type Cardinality struct { Min uint32; Max *uint32 }
type BindingExtractor interface {
    BindingIDs(value any) ([]artifact.BindingID, error) // appearance order
}
type BindingReferenceDefinition struct {
    Extractor BindingExtractor
    Cardinality Cardinality
    AllowedSchemes []artifact.Scheme
    RequiredDurability artifact.Durability
}
```

**EXT-REF-1** 声明以 `Extractor` 提取 typed value 内的全部 Artifact 引用，保留 appearance order，随后 group 才 sorted-unique。Extractor 随 EventDefinition 声明，本层不维护提取器注册表。（第一版的 JSONPointer 路径提取无消费者，已删除。）

**EXT-REF-2** `BuildRegistry` 验证 cardinality、Extractor 非 nil 与 scheme/durability 声明；最低 durability 至少为 `EventBound`。admission 解析每个 Binding，验证 Scheme、最低 durability、resolvability；任何违反拒绝整个 group，不作任何写入。

## 5. Writer：进程内的写入串行与幂等

```go
type TypedEvent struct {
    Type session.EventType
    RecordedAtUnixMilli int64
    SourceSeqs []session.Seq
    Value any
}
type SemanticGroup struct {
    CommitID session.CommitID
    Events []TypedEvent
}
// View 是 Commit 回调内可读的一致视图：head、幂等索引、投影状态。
type View interface {
    Head() session.Head
    Epoch() session.Epoch
    LookupCommit(session.CommitID) ([]session.SessionEvent, bool)
    Projection(ProjectionID, ProjectionVersion) (any, error) // 折叠到当前 head 的状态
}
type CommitFn func(View) (*SemanticGroup, error) // nil 表示不写

type CommitOutcome string
const (
    CommitApplied CommitOutcome = "applied"
    CommitAlreadyApplied CommitOutcome = "already_applied" // 同 CommitID、同 fingerprint
    CommitConflict CommitOutcome = "conflict"             // 同 CommitID、不同 fingerprint
    CommitInvalid CommitOutcome = "invalid"
    CommitNoop CommitOutcome = "noop"
)
type CommitResult struct {
    Outcome CommitOutcome
    Events []session.SessionEvent
    Claim *artifact.RetentionClaim
    Detail string
}

type Writer interface {
    SessionID() session.SessionID
    Epoch() session.Epoch
    Commit(context.Context, CommitFn) (CommitResult, error)
    Projections() ProjectionReader   // 读取本 Writer 维护的投影
    Close(context.Context) error
}
func OpenWriter(ctx, store session.Store, registry *Registry, ledger artifact.RetentionLedger, sid session.SessionID, opts session.OpenOptions) (Writer, error)
```

**EXT-WRT-1** `OpenWriter` 调 `store.Open` 取得所有权，读取整条日志重建三样内存状态：幂等索引（CommitID → 该组的行与 fingerprint）、每个已注册投影的当前状态、head。之后 `Commit` 在 Writer 的互斥区内执行：调 fn 得到 group，做 codec、admission、claim，`session.Writer.Append`，再把新行折进投影并更新索引。fn 只能通过 `View` 读；fn 返回 nil 记 `Noop`。Writer 是并发的唯一入口：Run 的 worker、Coordinator、恢复流程都经它串行，kernel 不再需要临界区回调。

**EXT-WRT-2** 幂等：fn 返回的 group 若 CommitID 已在索引中，比对 fingerprint（Type、SourceSeqs、Payload 的有序序列，不含时间），相同返回 `AlreadyApplied` 与原行，不同返回 `Conflict`；两者都不写入，也不做 admission 与 claim。fn 内可先经 `View.LookupCommit` 判断，避免为重放重新构造 group。

**EXT-WRT-3** claim 顺序：group 含 Binding 时，Writer 在 `Append` 之前调用 `ledger.Activate(claimID, owner, set)`。顺序固定为先 claim 再 append，因此崩溃只可能留下孤儿 claim（有 claim 无 commit），不可能留下无 claim 的引用；孤儿由 artifact 的回收前核对释放（ART-RET-3）。`Append` 失败时 Writer 调用 `ledger.ReleaseActive(claimID)` 尽力回收，失败也只留孤儿。

**EXT-WRT-4** `Append` 返回 `ErrOwnershipLost` 时 Writer 进入失效状态：本次与之后的 `Commit` 返回该错误，调用方必须放弃该 Session 的执行。这是 Session 级 fencing 在进程内的表现；Runtime 与 Loop 对它的处理见 RUN-CMT-6。

**EXT-WRT-5** ClaimID 派生保持第一版规则：`Digest("twilight/session-extension/claim", "1", ProtocolVersion, SessionID, CommitID, RefSetDigest)`；`ClaimOwner = {Kind:"twilight/session/commit", Authority:SessionID, Identity:CommitID}`。

```go
// Writers 是宿主维护的 SessionID → Writer 映射；模块（run 的 Runtime、turn 的 Coordinator）经它取得 Writer。
type Writers interface {
    Writer(context.Context, session.SessionID) (Writer, error)
}
```

**EXT-WRT-6** 一个进程对同一 Session 只打开一个 Writer，`Writers` 负责这一唯一性：首次请求时 `OpenWriter`，之后返回同一实例；Writer 失效（EXT-WRT-4）或 Close 后再次请求返回错误，是否重新 Open 由宿主决定。模块不自行调用 `OpenWriter`。

## 6. pure projection 与缓存

```go
type ProjectionDefinition struct {
    ID ProjectionID; Version ProjectionVersion
    Consumes []session.EventType
    Initial func() (any, error)
    Apply func(any, DecodedEvent) (any, error)
    StateCodec PayloadCodec
}
type ProjectionReader interface {
    Load(ctx, sid session.SessionID, id ProjectionID, v ProjectionVersion) (state any, through session.Seq, err error)
}
// ProjectionCache 是可选的派生缓存，随时可删；Memory 与文件实现由本层提供。
type ProjectionCache interface {
    Load(ctx, sid, id, v) (state jsonstable.Value, through session.Seq, digest es.Digest, ok bool, err error)
    Save(ctx, sid, id, v, state jsonstable.Value, through session.Seq, digest es.Digest) error
}
func NewProjectionReader(store session.Store, registry *Registry, cache ProjectionCache) ProjectionReader
```

**EXT-PRJ-1** Initial、Apply、StateCodec 必须 pure。Fold 以组为单位：一组内任一 event 的 Apply 失败，不发布该组的部分状态。

**EXT-PRJ-2** 投影只处理 `Consumes` 中的 EventType。其他 EventType 按归属处理：属于本模块或 `Requires` 模块（EXT-REG-4 的范围）且 `Decode` 为 Unknown 的事件，`Ignorable` 为真则跳过，否则 Fold 失败；范围之外的模块的事件一律跳过。写入者对纯信息性事件声明 `Ignorable`（EXT-REG），默认不可忽略：忘记声明只会导致多拒绝，不会导致静默丢失。读取时以范围内模块的前缀作为 `Types` 过滤。

**EXT-PRJ-3** 缓存条目记录 `through`（已折叠到的最后一行 Seq）与该行的 `Digest`。复用条件：`Read(From: through)` 返回的首行 Seq 与 Digest 与缓存一致，且 `StateCodec.Decode` 成功；否则从头重折。写入策略由投影或其宿主决定（例如 run 的 `SnapshotPolicy`）；缓存不在 kernel，也不与 append 同事务，丢失或过期只影响读取代价。

**EXT-PRJ-4** `Writer.Projections()` 返回的 reader 直接读 Writer 内存中的状态，不经 Store；独立进程的观察者用 `NewProjectionReader` 从 Store 读，两者对同一 head 给出相同状态。

## 7. errors 与 conformance

```go
type ErrorCode string
const (
    ErrInvalid ErrorCode = "invalid"; ErrUnknownEvent ErrorCode = "unknown_event"
    ErrCodec ErrorCode = "codec"; ErrBinding ErrorCode = "binding"
    ErrConflict ErrorCode = "conflict"; ErrOwnershipLost ErrorCode = "ownership_lost"
)
```

v1 conformance 必须验证：

- **EXT-REG-1 至 4**：immutable Registry、`v` 的写入与选择、多版本 codec 共存、Unknown 保留 raw payload、`Requires` 缺失或成环被拒绝、投影消费范围外事件被拒绝、被依赖事件版本不在声明范围被拒绝；
- **EXT-COD-1/2**：wire-first、canonical round-trip、`v` 保留字段；
- **EXT-REF-1/2**：Extractor 全量提取、cardinality、scheme/durability admission、拒绝时无写入；
- **EXT-WRT-1 至 5**：OpenWriter 后索引与投影等于全量 fold；同 CommitID 重放 AlreadyApplied、不同内容 Conflict、两者无写入；并发调用方串行且各自看到前一次的结果；claim 先于 append，append 失败后 claim 被释放或可被核对回收；`ErrOwnershipLost` 后 Writer 失效；
- **EXT-PRJ-1 至 4**：pure fold、组边界、Consumes 与范围外跳过、Ignorable 与非 Ignorable 的 Unknown、缓存复用条件、Writer 内投影与 Store 读取一致。

## 附录 B：Application module 与通用 Catalog（不进入 v1）

Application 注册自己的 `SourceID` 与 Module 时，EventType 为 `<SourceID>/<ModuleID>/<local-name>`；`BuildCatalog` 在启动时把多个 Source 的 ModuleDescriptor 与 artifact SchemeDefinition 组合为只读索引。v1 的模块以 Go 值直接传入 `BuildRegistry`，不需要这一层。
