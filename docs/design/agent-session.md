# Twilight Agent Session Protocol

状态：设计草案。无实现；wire、digest preimage 与 conformance 在 Memory reference implementation 与 Input → Turn → Run → Session 纵向切片跑通前不冻结。v1 为单 stream kernel；Fork、ancestry、canonical import 在附录 A 中，不进入 v1 conformance。

本文定义 Twilight Session 的 Event Sourcing kernel。文中的"必须""不得""应该"是协议约束。

## 1. Events 与范围

```text
Events = committed SessionEvent stream
State  = Fold(Events)

Persistent representation
  = immutable SessionHeader
  + ordered atomic SessionCommit records containing ordered SessionEvents
  + control-plane KV（同事务写入，不进入 stream，不参与 digest chain）
```

Committed Events 构成 Session 的长期语义事实。进入 Fold 前，Session kernel 必须验证 Event 位于完整的 SessionCommit 中，且 Header、commit boundary、head 与 digest chain 有效。

```text
Session kernel   envelope、顺序、commit、临界区、snapshot、控制面 KV、integrity
Session modules  event ontology、typed codec、payload 版本、payload validation、projection
```

Payload 对 Session kernel 是 opaque canonical JSON。snapshot 与控制面 KV 都不是语义事实：snapshot 是可重建的派生数据；控制面 KV 保存执行占用、保留声明等运行控制信息，由模块解释，kernel 只保证它与同一 commit 原子写入、支持条件写与按 deadline 枚举。

**SES-SCP-1** kernel 不依赖领域 payload，也不对其执行 schema validation 或解释。replay 的唯一顺序是 `(revision, index)`；时钟只作 metadata。

**SES-SCP-2** v1 的范围是一条 stream 的 kernel：header、commit、两种 append 入口、local replay、snapshot、控制面 KV。Fork、ancestry archive、canonical import 与 resolved replay 见附录 A，v1 实现返回 `ErrUnsupported`，conformance 不覆盖。header 与 digest preimage 为它们保留字段位置，日后加入不升 ProtocolVersion。

## 2. 版本

`ProtocolVersion` 只覆盖 kernel wire：header、event envelope、commit、snapshot envelope、digest profile 与 codec 的 canonicalization 规则。它不覆盖 payload。

**SES-VER-1** payload 的版本由模块负责：每个 payload object 的第一层携带整数字段 `v`，模块按 `(EventType, v)` 选择 codec（EXT-REG-2）。同一 stream 内不同 event 可以携带不同的 `v`；kernel 不读取该字段。

**SES-VER-2** `ProtocolVersion` 在旧 kernel reader 读取新 writer 产生的 stream 后无法保持 envelope、commit 顺序或 digest chain 语义时递增。payload 字段、EventType 与模块 codec 的变化不触发 kernel 版本变化。kernel 版本变化由外部 migration tool 生成新版本 stream。

第一版运行实例绑定一个 `ProtocolProfile`，其 `Version()` 是实例接受的 `ProtocolVersion`。读取 Header、Commit 或 snapshot 时先校验 `ProtocolVersion` 与绑定 profile 相等，不匹配返回 `ErrUnsupportedProfile`。

## 3. wire types 与 profile

```go
type SessionID string
type CommitID string
type EventID string
type EventType string
type ProjectionKey string
type CursorToken string
type ControlNamespace string

type SessionHeader struct {
    ProtocolVersion uint16
    SessionID SessionID
    ParentFork *ForkPoint // v1 必须为 nil；见附录 A
    CausationID es.CausationID
    Metadata jsonstable.Value
    HeaderDigest es.Digest
}
type SessionEvent struct {
    EventID EventID
    Index uint16
    Type EventType
    RecordedAtUnixMilli int64
    SourceEvents []EventID
    Payload jsonstable.Value
    EventDigest es.Digest
}
type UncommittedEvent struct {
    EventID EventID
    Type EventType
    RecordedAtUnixMilli int64
    SourceEvents []EventID
    Payload jsonstable.Value
}
type SessionCommit struct {
    ProtocolVersion uint16
    SessionID SessionID
    Revision es.Revision
    PreviousDigest es.Digest
    CommitID CommitID
    CausationID es.CausationID
    CorrelationID string
    Events []SessionEvent
    CommitDigest es.Digest
}
type Head struct { Revision es.Revision; Digest es.Digest }
type EventPosition struct { Revision es.Revision; Index uint16; EventDigest es.Digest }
```

**SES-WIR-1** identity 非空且稳定；EventID 在同一 stream 内唯一。header 不可变，revision 从 1 连续递增，commit 至少有一个 event，event Index 从 0 连续递增。revision 1 的 `PreviousDigest=HeaderDigest`，其后为前一 CommitDigest。空 stream head 是 `{0, HeaderDigest}`。`Create` 对逐字段相同的 `CreateRequest` 幂等返回既有 header，同 SessionID 的不同请求为 `Conflict`。commit 与 header 的 `CausationID`、`CorrelationID` 是 producer 自行填写的 opaque metadata，kernel 不解释；它们进入 digest 与 append fingerprint。

**SES-WIR-2** `ProtocolProfile` 冻结 kernel wire：envelope、commit、snapshot envelope、null/omission、精确 integer encoding、unknown-field policy、array order 与下列 digest preimage；所有 digest 依 `agent/es` 的 versioned domain separator。

```go
type ProtocolProfile interface {
    Version() uint16
    EncodeHeader(SessionHeader) (jsonstable.Value, error)
    DecodeHeader(jsonstable.Value) (SessionHeader, error)
    EncodeEvent(SessionID, es.Revision, SessionEvent) (jsonstable.Value, error)
    DecodeEvent(SessionID, es.Revision, jsonstable.Value) (SessionEvent, error)
    EncodeCommit(SessionCommit) (jsonstable.Value, error)
    DecodeCommit(jsonstable.Value) (SessionCommit, error)
    EncodeSnapshot(Snapshot) (jsonstable.Value, error)
    DecodeSnapshot(jsonstable.Value) (Snapshot, error)
    ValidateCanonicalHeader(jsonstable.Value) (SessionHeader, error)
    ValidateCanonicalEvent(SessionID, es.Revision, jsonstable.Value) (SessionEvent, error)
    ValidateCanonicalCommit(jsonstable.Value) (SessionCommit, error)
    ValidateCanonicalSnapshot(jsonstable.Value) (Snapshot, error)
    FingerprintAppend(AppendRequest) (es.Digest, error)
}
```

**SES-WIR-3** 同一个 Session 的 `SessionHeader.ProtocolVersion`、所有 `SessionCommit.ProtocolVersion`、`Snapshot.ProtocolVersion` 与所选 `ProtocolProfile.Version()` 必须相等。Store 从已验证 Header 派生后续操作使用的版本，调用方提交的版本字段只能通过一致性校验。

Header、event、commit 分别覆盖自身以外的全部持久字段。event digest 额外覆盖 SessionID、revision 和 index。每个 `ValidateCanonical*` 必须执行 decode → encode 并要求 canonical-equivalent wire，同时验证对应 digest。`SourceEvents` 是 bytewise sorted-unique set，且只可引用同一 stream 内已存在的 EventID。opaque Payload 必须已经 canonical，完整 value 进入 event digest 和 append fingerprint。

## 4. Store API 与 errors

```go
type CreateRequest struct {
    ProtocolVersion uint16; SessionID SessionID; CausationID es.CausationID; Metadata jsonstable.Value
}
type AppendRequest struct {
    SessionID SessionID; ExpectedHead Head; CommitID CommitID
    CausationID es.CausationID; CorrelationID string; Events []UncommittedEvent
}
type AppendDisposition string
const (
    AppendApplied AppendDisposition = "applied"
    AppendAlreadyApplied AppendDisposition = "already_applied"
    AppendHeadConflict AppendDisposition = "head_conflict"
    AppendCommitConflict AppendDisposition = "commit_conflict"
    AppendInvalid AppendDisposition = "invalid"
)
type AppendResult struct { Disposition AppendDisposition; Commit *SessionCommit; ActualHead Head }

// 临界区内的读写视图。所有方法在同一事务内生效。
type SessionTx interface {
    Head() Head
    LookupCommit(CommitID) (SessionCommit, bool, error)
    // Tail 返回 after 之后的 local commits；types 非空时只返回含至少一个匹配 EventType 前缀的完整 commit。
    Tail(after Head, types []EventType) ([]SessionCommit, error)
    LoadSnapshot(ProjectionKey, ProjectionVersion uint16) (SnapshotResult, error)
    SaveSnapshot(Snapshot) error
    ControlGet(ControlNamespace, key string) (ControlEntry, bool, error)
    ControlPut(ControlNamespace, key string, value []byte, deadlineUnixMilli int64) error
    ControlDelete(ControlNamespace, key string) error
}
type CommitInFn func(SessionTx) (*AppendRequest, error)

type ReplayCursor struct { After *EventPosition; Token CursorToken }
type ReplayRequest struct {
    SessionID SessionID
    Types []EventType // 空为全部；非空为 EventType 前缀过滤
    Cursor *ReplayCursor
    Limit uint32
}
type ReplayPage struct { Header SessionHeader; Commits []SessionCommit; Next *ReplayCursor; Head Head }

type SnapshotRequest struct { SessionID SessionID; ProjectionKey ProjectionKey; ProjectionVersion uint16 }
type SnapshotResult struct { Snapshot *Snapshot; Found bool }
type SaveSnapshotRequest struct { Snapshot Snapshot }
type SaveSnapshotResult struct { Snapshot Snapshot; Replaced bool }

type ControlEntry struct {
    SessionID SessionID; Namespace ControlNamespace; Key string
    Value []byte
    DeadlineUnixMilli int64 // 0 表示无 deadline；kernel 只用于枚举，不据此删除
}

type Store interface {
    Create(context.Context, CreateRequest) (SessionHeader, error)
    Header(context.Context, SessionID) (SessionHeader, error)
    Head(context.Context, SessionID) (Head, error)
    LookupCommit(context.Context, SessionID, CommitID) (SessionCommit, bool, error)
    Commit(context.Context, AppendRequest) (AppendResult, error)
    CommitIn(context.Context, SessionID, CommitInFn) (AppendResult, error)
    Replay(context.Context, ReplayRequest) (ReplayPage, error)
    LoadSnapshot(context.Context, SnapshotRequest) (SnapshotResult, error)
    SaveSnapshot(context.Context, SaveSnapshotRequest) (SaveSnapshotResult, error)
    // 控制面 KV，临界区之外的读写；与 SessionTx 内的同名方法作用于同一存储。
    ControlGet(context.Context, SessionID, ControlNamespace, key string) (ControlEntry, bool, error)
    ControlPut(context.Context, SessionID, ControlNamespace, key string, value []byte, deadlineUnixMilli int64) error
    // 条件写：仅当条目存在且当前 Value 逐字节等于 expected 时写入；返回是否写入。
    ControlCompareAndPut(context.Context, SessionID, ControlNamespace, key string, expected, value []byte, deadlineUnixMilli int64) (bool, error)
    ControlDelete(context.Context, SessionID, ControlNamespace, key string) error
    ControlScan(context.Context, ControlNamespace, keyPrefix string, fn func(ControlEntry) (bool, error)) error    // 跨全部 Session，按前缀
    ControlExpired(context.Context, ControlNamespace, beforeUnixMilli int64, fn func(ControlEntry) (bool, error)) error // 跨全部 Session，deadline 非 0 且早于 before
}
```

```go
type ErrorCode string
const (
    ErrInvalid ErrorCode = "invalid"; ErrNotFound ErrorCode = "not_found"
    ErrConflict ErrorCode = "conflict"; ErrCorrupt ErrorCode = "corrupt"
    ErrUnsupportedProfile ErrorCode = "unsupported_profile"; ErrUnsupported ErrorCode = "unsupported"
    ErrUnavailable ErrorCode = "unavailable"
)
type Error struct { Code ErrorCode; Operation string; SessionID SessionID; CommitID CommitID; Detail string }
func (Error) Error() string
```

**SES-API-1** `Commit` 与 `CommitIn` 是仅有的两个 append port。它们必须原子持久化完整 commit、新 head，以及同一调用内的 snapshot 与控制面 KV 写入。普通 producer 的 capability exposure 由 Session Module Framework 提供（EXT-APP）。

**SES-API-2** `CommitIn` 是同一 Session 的 append 临界区：Store 在 per-Session 锁或数据库事务内向 fn 提供 `SessionTx`，fn 在其中读 head、按 CommitID 查 commit、读 snapshot 与 tail、读写控制面 KV、决定要追加的事件；返回的 `AppendRequest` 由 Store 在同一事务内按第 5 节规则追加。fn 返回 nil 表示不追加，此时 fn 已执行的 snapshot 与 KV 写入仍提交。section 内 head 不移动，`ExpectedHead` 与 head 不一致只可能是 fn 的实现错误，返回 `Invalid`。fn 必须纯且除 SessionTx 外无副作用；Store 不保证 fn 只被调用一次。

`CommitIn` 与 `Commit` 产生的 commit 逐字段等价，同一 CommitID 的幂等与 conflict 判定相同。两者都是 adapter 必须实现的 port：`Commit`（CAS）一次请求完成比对与写入，适用于 Store 实现为远程客户端、不能在持锁期间回调调用方的部署；`CommitIn` 适用于需要在写入前基于当前状态做决定的模块（例如 Run 的 Decide），以及需要把控制面 KV 写入与 commit 放在同一事务的 producer。进程内 adapter 可以用 `CommitIn` 加 head 比对函数实现 `Commit`，但 `Commit` 不因此从合同中移除。

**SES-API-3** 控制面 KV 以 `(SessionID, Namespace, Key)` 寻址，值为 opaque bytes 加一个可选 deadline，kernel 不解释值，也不因 deadline 到期而删除或修改条目。它用于需要与 commit 原子写入、但不属于语义事实的运行控制信息：执行占用、保留声明等。kernel 提供且只提供三种模块自己无法提供的保证：`SessionTx` 内的写入与该 commit 同事务；`ControlCompareAndPut` 是临界区之外的原子条件写，条目缺失或当前值不等于 `expected` 时不写入并返回 false；`ControlScan` 与 `ControlExpired` 跨 Session 枚举，前者按 key 前缀，后者按 deadline 早于给定时刻，fn 返回 false 停止。占用、凭证、续期、过期后的处置等语义由使用它的模块或 Module Framework 定义（EXT-SCP-3），不进入 kernel。Namespace 由模块以 `<SourceID>/<ModuleID>/<name>` 命名。KV 与 stream 在同一存储与事务域，不会单独丢失；它不进入 digest chain，因此不能通过 chain 校验，模块不得把语义事实放入 KV。

## 5. append 与 idempotency

**SES-APP-1** CommitID 的幂等键为 `(SessionID, CommitID)`。append fingerprint 覆盖 SessionID、CommitID、causation、correlation 和有序 UncommittedEvent group 的 `EventID`、`Type`、`SourceEvents`、`Payload`；**不**覆盖 ExpectedHead，也**不**覆盖 `RecordedAtUnixMilli`。时间是 metadata，重试可以携带新的时间戳而得到 `AlreadyApplied`，已提交 commit 中的时间保持首次写入值。

```text
append(request):
  if existing := lookup(SessionID, CommitID):
      if fingerprint(existing) == fingerprint(request): return AlreadyApplied(existing)
      return CommitConflict
  validate profile, identities, payloads and ExpectedHead
  assign next revision and contiguous indexes; calculate event/commit digests
  atomically write complete commit, new head, and pending snapshot / KV writes
  return Applied(commit)
```

**SES-APP-2** CAS 不匹配返回 `HeadConflict` 和 actual head，不得 last-write-wins。任何 invalid envelope、重复 EventID、非 canonical payload 或 digest-chain violation 返回 `Invalid` 或 `Corrupt`；不得部分写入。Store 必须拒绝同一 stream 内的 EventID duplicate。

## 6. replay

**SES-REP-1** Replay 只返回请求 `SessionID` 自己的 commits，按 `(Revision, Index)` 递增，page 只返回完整 commit。`After=nil` 表示起点，非 nil `After` 表示该精确 digest 的最后已返回 event，续页严格从它之后开始。

**SES-REP-2** `Types` 非空时返回至少含一个匹配 EventType 前缀的完整 commit，跳过其他 commit。过滤 replay 可以验证每个返回 commit 自身的 event digest 与 commit digest，不能验证 revision 连续性与 `PreviousDigest` 链；链完整性只由无过滤 replay 验证。adapter 应维护 `(SessionID, EventType 前缀)` 到 revision 的索引，使过滤 replay 的代价与匹配 commit 数成正比。

**SES-REP-3** 无过滤 replay 必须验证 header、revision gap、previous/commit/event digest、index 和 profile。损坏、缺口或不支持版本必须 fail loudly。cursor 的 `After` 必须是当前请求中存在且 digest 匹配的完整 `EventPosition`。Token 是 adapter 的 opaque continuation，必须绑定 `SessionID`、Types、完整 After 和 Limit，且不得替代这些可验证字段。

## 7. snapshot

```go
type Snapshot struct {
    ProtocolVersion uint16; SessionID SessionID
    ProjectionKey ProjectionKey; ProjectionVersion uint16
    Through Head
    State jsonstable.Value; SnapshotDigest es.Digest
}
```

**SES-SNP-1** snapshot 是派生 cache，任何时刻可以删除并从 stream 重建。`Through` 是该 snapshot 覆盖到的 head；因为 commit 经 `PreviousDigest` 链式覆盖全部前缀，`Through.Digest` 就是 coverage 的证明，不另设 coverage digest。SnapshotDigest 覆盖全部 snapshot field（自身除外）。

**SES-SNP-2** Load 时 `Through` 必须是当前 stream 的前缀（该 revision 的 CommitDigest 等于 `Through.Digest`），否则视为缺失并全量重折。写入频率由 projection 自己的策略决定；kernel 不要求每次 commit 都写 snapshot。同一 `(SessionID, ProjectionKey, ProjectionVersion)` 只保留最新一份。

## 8. boundaries 与 conformance

Session modules 对 opaque payload 作 typed encode/decode 并负责 payload 版本；Store 负责 commit、CommitID、digest、replay、snapshot 与控制面 KV 的机制。unknown event 保留原始 payload 并支持原样 replay。first-party module 为 `chatlog`、`turn` 与 `run`：Run 的执行事实以 `twilight/run/` 事件进入同一 stream，其状态机与投影由 [agent-run.md](agent-run.md) 定义。

一条 Session stream 承载多个模块的事件；每个 projection 只消费自己声明的 EventType（EXT-PRJ-2），并以 snapshot 加过滤 tail 的方式读取。读取代价由 projection 消费的事件数与 snapshot 之后的 tail 长度决定，与 stream 总长度无关。

v1 conformance 必须验证：

- **SES-SCP-2**：附录 A 的能力返回 `ErrUnsupported`，`ParentFork` 非 nil 的 header 被拒绝；
- **SES-VER-1、SES-VER-2**：kernel 不读取 payload `v`；不同 `v` 的 event 在同一 stream 共存；
- **SES-WIR-1、SES-WIR-2、SES-WIR-3**：wire/profile freeze、版本一致性、所有 Encode/Decode 与 `ValidateCanonical*` round-trip、digest preimage、same-stream SourceEvents、EventID uniqueness、complete commit；
- **SES-API-1、SES-API-2、SES-API-3、SES-APP-1、SES-APP-2**：atomic CAS、`CommitIn` 与 `Commit` 的 commit 等价、fn 返回 nil 时 KV 与 snapshot 仍提交、concurrent writer、CommitID exact idempotency（含不同时间戳的重试得到 AlreadyApplied）、failure classification、KV 与 commit 同事务、`ControlCompareAndPut` 在条目被并发删除或改写后返回 false、`ControlScan` 前缀与提前停止、`ControlExpired` 只返回 deadline 非 0 且已过的条目；
- **SES-REP-1、SES-REP-2、SES-REP-3**：顺序、完整 EventPosition After 语义、Types 过滤只返回匹配 commit、cursor/token binding、tamper/gap failure；
- **SES-SNP-1、SES-SNP-2**：Through 前缀校验、snapshot 加 tail 与全量 fold 等价、过期 snapshot 被忽略。

第一版包含 MemoryStore 与上述 Store conformance；不承诺特定 durable adapter。

## 附录 A：预留能力（不进入 v1）

以下能力保留设计，供后续版本在不升 kernel `ProtocolVersion` 的前提下加入。v1 实现对这些入口返回 `ErrUnsupported`。

**Fork。** `ForkPoint{ParentSessionID, Revision, HeadDigest}`；`Fork(ForkRequest)` 创建 immutable child header，`ParentFork` 精确绑定 parent 已验证 boundary，child local revision 从 1 开始。同 ForkID 逐字段相同的请求幂等，其他为 conflict。保留 child 时必须保留 ancestor header 与 boundary 内 commit（RetainClosure）。目前没有规范内的消费者：subagent 使用独立 Session。

**Resolved replay 与 AncestryDigest。** 有 Fork 后 replay 分 Local 与 Resolved 两种模式；Resolved 按 root-to-target segment order 返回。`AncestryDigest` 的 preimage 为 profile、target SessionID、root 到 target 的有序 HeaderDigest、每条 ForkPoint、每级可见 CommitDigest 与 target local Head。cursor 与 snapshot 绑定该 digest。v1 只有一个 segment，`EventPosition` 不携带 origin SessionID；加入 Fork 时以 optional 字段扩展，旧 reader 可忽略。

**Canonical import 与 ancestry archive。** `ImportCanonical` 只处理 Session records，不解释 payload，经 `ValidateCanonical*` 验证完整 chain 后导入完整 stream 或已有可验证 predecessor 的 contiguous tail。`ImmutableAncestryArchive` 的 preimage 为 ProtocolVersion、按 segment order 的 Headers 与 Commits。同 `(SessionID, CommitID)` 仅在 canonical commit 逐字段相同才幂等。

**Snapshot coverage 与 ancestry。** 有 Fork 后 snapshot 的 `Through` 扩展为 `ResolvedHead{SessionID, LocalHead, AncestryDigest}`，同 local head 而 ancestry 不同不等价。
