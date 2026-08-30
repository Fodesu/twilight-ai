# Twilight Agent Session Protocol

状态：设计规范

本文定义 Twilight Session 的 Event Sourcing kernel。文中的“必须”“不得”“应该”是协议约束。

本文冻结 Twilight 的 Session stream、并发、Fork ancestry 与 integrity 语义。

## 1. Events 与范围

```text
Events = resolved committed SessionEvent stream
State  = Fold(Events)

Persistent representation
  = immutable SessionHeader
  + ordered atomic SessionCommit records containing ordered SessionEvents
```

Committed Events 构成 Session 的长期语义事实。进入 Fold 前，Session kernel 必须验证：

- Event 位于完整的 SessionCommit 中；
- Header、commit boundary、CAS 与 digest chain 有效；
- Fork events 已按 root 到 target 解析为确定序列。

Header 与 Commit 是 Events 的 canonical persistent representation，提供原子提交、完整性验证与 fork/replay framing。职责边界为：

```text
Session kernel  envelope、顺序、CAS、fork、snapshot、canonical import、integrity
Extension       event ontology、typed codec、payload validation、projection
```

Payload 对 Session kernel 是 opaque canonical JSON。缓存和 snapshot 是可重建派生数据。

**SES-SCP-1** kernel 不依赖领域 payload，也不对其执行 schema validation 或解释。resolved replay 的唯一顺序是 root-to-target ancestry segment order，再在每个 segment 内按 `(revision, index)`；`EventPosition` 只标识 event/cursor，不定义跨 segment 的排序；时钟只作 metadata。

## 2. wire types 与 profile

```go
type SessionID string
type CommitID string
type EventID string
type EventType string
type ForkID string
type ProjectionKey string
type CursorToken string

type SessionHeader struct {
    ProtocolVersion uint16
    SessionID SessionID
    ParentFork *ForkPoint
    CausationID es.CausationID
    Metadata jsonstable.Value
    HeaderDigest es.Digest
}
type ForkPoint struct {
    ParentSessionID SessionID
    Revision es.Revision
    HeadDigest es.Digest
}
type SessionEvent struct {
    EventID EventID
    Index uint16
    Type EventType
    SchemaVersion uint16
    RecordedAtUnixMilli int64
    SourceEvents []EventID
    Payload jsonstable.Value
    EventDigest es.Digest
}
type UncommittedEvent struct {
    EventID EventID
    Type EventType
    SchemaVersion uint16
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
```

**SES-WIR-1** identity 非空且稳定；EventID 在 resolved ancestry 内唯一。header 不可变，revision 从 1 连续递增，commit 至少有一个 event，event Index 从 0 连续递增。revision 1 的 `PreviousDigest=HeaderDigest`，其后为前一 CommitDigest。

**SES-WIR-2** `ProtocolProfile` 冻结 envelope field、null/omission、精确 integer encoding、unknown-field policy、array order 与下列 digest preimage；所有 digest 依 `agent/es` 的 versioned domain separator。

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
    EncodeAncestryArchive(ImmutableAncestryArchive) (jsonstable.Value, error)
    DecodeAncestryArchive(jsonstable.Value) (ImmutableAncestryArchive, error)
    ValidateCanonicalHeader(jsonstable.Value) (SessionHeader, error)
    ValidateCanonicalEvent(SessionID, es.Revision, jsonstable.Value) (SessionEvent, error)
    ValidateCanonicalCommit(jsonstable.Value) (SessionCommit, error)
    ValidateCanonicalSnapshot(jsonstable.Value) (Snapshot, error)
    ValidateCanonicalAncestryArchive(jsonstable.Value) (ImmutableAncestryArchive, error)
    FingerprintAppend(AppendRequest) (es.Digest, error)
}
```

Header、event、commit 分别覆盖自身以外的全部持久字段。event digest 额外覆盖 origin SessionID、revision 和 index。每个 `ValidateCanonical*` 必须执行 decode → encode 并要求 canonical-equivalent wire，同时验证对应 digest；它是 adapter/import 的唯一 canonical round-trip validation 入口。`SourceEvents` 是 bytewise sorted-unique set，且只可引用同一 resolved Session stream 内已存在的 EventID；跨 stream provenance 必须编码在 payload 的 owner-defined `SourceRef` 中。opaque Payload 必须已经 canonical，完整 value 进入 event digest 和 append fingerprint。空 stream head 是 `{0, HeaderDigest}`。

## 3. Store API 与 errors

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

type ReplayMode string
const ( ReplayLocal ReplayMode = "local"; ReplayResolved ReplayMode = "resolved" )
// EventPosition is a verified event identity and is safe to expose in APIs.
type EventPosition struct { OriginSessionID SessionID; Revision es.Revision; Index uint16; EventDigest es.Digest }
type ReplayCursor struct { Mode ReplayMode; AncestryDigest es.Digest; After *EventPosition; Token CursorToken }
type ReplayRequest struct { SessionID SessionID; Mode ReplayMode; Cursor *ReplayCursor; Limit uint32 }
type ReplayPage struct { Header SessionHeader; Commits []SessionCommit; Next *ReplayCursor; Head ResolvedHead }

type ForkRequest struct {
    ForkID ForkID; ChildSessionID SessionID; ParentSessionID SessionID
    ExpectedParentBoundary ForkPoint; CausationID es.CausationID; Metadata jsonstable.Value
}
type ForkResult struct { Header SessionHeader; Created bool }

type CanonicalImportRequest struct { Header SessionHeader; Commits []SessionCommit; AncestryArchive *ImmutableAncestryArchive }
type CanonicalImportResult struct { Header SessionHeader; Imported uint64; Head Head; AlreadyPresent bool }
type ImmutableAncestryArchive struct { ProfileVersion uint16; Headers []SessionHeader; Commits []SessionCommit; ArchiveDigest es.Digest }

type SnapshotRequest struct { SessionID SessionID; ProjectionKey ProjectionKey; ProjectionVersion uint16; AtOrBefore ResolvedHead }
type SnapshotResult struct { Snapshot *Snapshot; Found bool; Covers bool }
type SaveSnapshotRequest struct { Snapshot Snapshot }
type SaveSnapshotResult struct { Snapshot Snapshot; Replaced bool }

type Store interface {
    Create(context.Context, CreateRequest) (SessionHeader, error)
    Header(context.Context, SessionID) (SessionHeader, error)
    Head(context.Context, SessionID) (Head, error)
    LookupCommit(context.Context, SessionID, CommitID) (SessionCommit, bool, error)
    Commit(context.Context, AppendRequest) (AppendResult, error)
    Replay(context.Context, ReplayRequest) (ReplayPage, error)
    Fork(context.Context, ForkRequest) (ForkResult, error)
    ImportCanonical(context.Context, CanonicalImportRequest) (CanonicalImportResult, error)
    LoadSnapshot(context.Context, SnapshotRequest) (SnapshotResult, error)
    SaveSnapshot(context.Context, SaveSnapshotRequest) (SaveSnapshotResult, error)
}
```

```go
type ErrorCode string
const (
    ErrInvalid ErrorCode = "invalid"; ErrNotFound ErrorCode = "not_found"
    ErrConflict ErrorCode = "conflict"; ErrCorrupt ErrorCode = "corrupt"
    ErrUnsupportedProfile ErrorCode = "unsupported_profile"; ErrUnavailable ErrorCode = "unavailable"
)
type Error struct { Code ErrorCode; Operation string; SessionID SessionID; CommitID CommitID; Detail string }
func (Error) Error() string
```

**SES-API-1** Store.Commit 是唯一 append Store protocol port。它必须原子持久化完整 commit 与 head；普通 producer 的 capability exposure 是 Extension 的责任。`ImportCanonical` 只给 trusted adapter、recovery/import coordinator 或 test。

## 4. append 与 idempotency

**SES-APP-1** CommitID 的幂等键为 `(SessionID, CommitID)`。append fingerprint 覆盖 SessionID、CommitID、causation、correlation 和完整有序 UncommittedEvent group，**不**覆盖 ExpectedHead。

```text
append(request):
  if existing := lookup(SessionID, CommitID):
      if fingerprint(existing) == fingerprint(request): return AlreadyApplied(existing)
      return CommitConflict
  validate profile, identities, payloads and ExpectedHead
  assign next revision and contiguous indexes; calculate event/commit digests
  atomically write complete commit and new head
  return Applied(commit)
```

**SES-APP-2** CAS 不匹配返回 `HeadConflict` 和 actual head，不得 last-write-wins。任何 invalid envelope、重复 EventID、非 canonical payload 或 digest-chain violation 返回 `Invalid` 或 `Corrupt`；不得部分写入。Store 至少拒绝 resolved ancestry 内的 EventID duplicate。

## 5. replay

```go
type ResolvedHead struct { SessionID SessionID; LocalHead Head; AncestryDigest es.Digest }
type RetainClosure struct { Headers []es.Digest; Boundaries []ForkPoint; Commits []es.Digest }
```

**SES-REP-1** Local replay 只返回请求 `SessionID` 自己的 commits；其每个 `EventPosition.OriginSessionID` 必须等于该 SessionID，并按 `(Revision, Index)` 递增。Resolved replay 从 root 的可见 prefix 到 target 的 local commits，严格按 root-to-target ancestry segment order，再按每个 segment local `(Revision, Index)` 返回；每个 event 保留其实际 origin position。`EventPosition` 是 verified identity/cursor，不能替代该 segment order。两种模式的 page 都只返回完整 commit；`After=nil` 表示起点，非 nil `After` 表示该精确 digest 的最后已返回 event，续页严格从它之后开始。

**SES-REP-2** `AncestryDigest` 的 preimage 是 profile、target SessionID、root 到 target 的有序 HeaderDigest、每条完整 ForkPoint、每级可见 CommitDigest 和 target local Head；不含 cursor、snapshot、load time 或 projection state。Local 与 Resolved cursor 都绑定同一 target 的该 digest：Local 虽不返回 ancestor event，仍以它验证 fork closure；Resolved 则以它确定可见 ancestry。cursor 必须携带并在续页验证它，变更则 conflict。

**SES-REP-3** replay 必须验证 header、revision gap、previous/commit/event digest、index 和 profile。损坏、缺口或不支持版本必须 fail loudly。cursor 的 `After` 必须是当前 mode/ancestry 中存在且 digest 匹配的完整 `EventPosition`；Local mode 还必须满足其 origin 为请求 Session。Token 是 adapter 的 opaque continuation，必须绑定 `SessionID`、Mode、AncestryDigest、完整 After 和 Limit，且不得替代这些可验证字段。

## 6. fork

**SES-FRK-1** Fork 创建 immutable child header，ParentFork 精确绑定 parent 已验证 boundary；revision 0 的 boundary digest 为 parent HeaderDigest。child local revision 从 1 开始。

| 请求情况 | 结果 |
|---|---|
| 新 ForkID、未使用 ChildSessionID、boundary 已验证 | 创建 child |
| 同 ForkID、逐字段相同 request | 返回原 child，`Created=false` |
| 同 ForkID、请求不同 | conflict |
| ChildSessionID 已被另一 ForkID 使用 | conflict |
| parent/boundary 不存在、不匹配或损坏 | conflict/corrupt |

**SES-FRK-2** 保留 child 时必须保留 `RetainClosure`：所有 ancestor header 与每个 fork boundary 内 commit。实现可留存 ancestor prefix，或保存经验证 immutable ancestry archive；删除、归档或 compact parent 前必须证明 child 仍可 resolved replay。

## 7. snapshot

```go
type Snapshot struct {
    ProtocolVersion uint16; SessionID SessionID
    ProjectionKey ProjectionKey; ProjectionVersion uint16
    ThroughHead ResolvedHead; CoverageDigest es.Digest
    State jsonstable.Value; SnapshotDigest es.Digest
}
```

**SES-SNP-1** snapshot 是派生 cache。CoverageDigest 覆盖 `ThroughHead` 所解析的 root-to-target HeaderDigest/CommitDigest sequence；SnapshotDigest 覆盖全部 snapshot field（自身除外）。同 local head 而 ancestry 不同绝不等价。

**SES-SNP-2** Save 和 Load 必须重算 ThroughHead、AncestryDigest、coverage 和 snapshot digest。coverage 是当前 sequence 的完整前缀时才可从 snapshot tail replay；缺失、过旧、损坏或 projection 不兼容时全量 replay。

## 8. canonical import

**SES-IMP-1** canonical import 仅处理 Session records，不解释 payload。它必须经 ProtocolProfile 的 `ValidateCanonical*` 入口验证 profile、header、完整 chain、event uniqueness 和 supplied positions/digests；只接受完整 stream 或已有可验证 predecessor 的 contiguous tail。`ImmutableAncestryArchive` 的 canonical preimage 是 profile version、按 root-to-target segment order 的 Headers、每个 segment 按 local `(revision,index)` 的完整 Commits，排除 `ArchiveDigest`；ArchiveDigest 覆盖该 preimage。archive codec 同样必须 decode→encode canonical-equivalent 并验证 digest。

**SES-IMP-2** 同 `(SessionID,CommitID)` 仅在 canonical commit 逐字段相同才幂等；不同 header、revision、group 或 digest 为 conflict。fork child import 必须有本地 existing verified parent boundary，或随 request 提供经过验证 immutable ancestry archive；不得只信任 ParentFork 声明。

Application/import coordinator 负责跨 Session package、业务事务与恢复编排。

## 9. boundaries 与 conformance

Extension 可对 opaque payload 作 typed encode/decode；它不得改变 Store 的 CAS、CommitID、digest 或 replay 语义。unknown event 必须 raw-preserve 和 raw-replay。`agent/turn` 消费/追加 Events 并协调 Run；Session kernel 仍只处理纯 envelope 与 stream 机制。Run、queue、provider 与 Application policy 均在 kernel 外。

Conformance 必须验证：

- **SES-WIR-1、SES-WIR-2**：wire/profile freeze、所有 Encode/Decode 与 `ValidateCanonical*` round-trip、digest preimage、同-stream SourceEvents、EventID uniqueness、complete commit；
- **SES-API-1、SES-APP-1、SES-APP-2**：atomic CAS、concurrent writer、CommitID exact idempotency 和 failure classification；
- **SES-REP-1、SES-REP-2、SES-REP-3**：local/resolved replay 的 segment-first 顺序、完整 EventPosition After 语义、cursor/token binding、tamper/gap failure；
- **SES-FRK-1、SES-FRK-2**：ForkID idempotency、parent boundary 和 RetainClosure；
- **SES-SNP-1、SES-SNP-2**：coverage、ancestry validation 与 snapshot+tail 等价；
- **SES-IMP-1、SES-IMP-2**：complete/tail import、exact conflict 和 verified ancestry archive。

第一版包含 MemoryStore 与上述 Store conformance；不承诺特定 durable adapter。
