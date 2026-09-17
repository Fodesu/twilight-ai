# Twilight Agent Session Protocol

状态：v1 设计规范（commit ledger）。本文定义 Session ES kernel。本文档随 feat/agent-runtime 分支首次发布；定稿前的行格式（一行一个 SessionEvent、按行 digest）从未对外发布，由 commit ledger 取代。

本文定义 Twilight Session 的 Event Sourcing kernel。文中的"必须""不得""应该"是协议约束。

## 0. 设计原则

Twilight Session 是一个单写者、可接管、幂等提交的 Event-Sourced Aggregate：Event Stream 是事实的唯一权威，Writer 提供语义事务与串行化，kernel 只提供原子 append、ownership/fencing 与持久化，Projection 与 Snapshot 全部是可重建的派生状态。下列原则贯穿本文与 [Session Module Framework](agent-session-extension.md)、[Decision](agent-decision.md)、[Run](agent-run.md)、[Turn](agent-turn.md)、[Chatlog](agent-session-chatlog.md)、[Artifact](agent-artifact.md)、[Runtime](agent-runtime.md) 各规范；每条给出承载它的条款。

```text
                  Session

      ┌── Canonical Commit Ledger ─┐
      │                            │
Command                            │
  ↓                                │
Writer.Commit                      │
  │                                │
  ├─ semantic serialization        │
  ├─ transaction boundary          │
  ├─ validation                    │
  ├─ idempotency                   │
  ↓                                │
Atomic Commit ─────────────────────┘
             │
             ↓
      Projections / Snapshots
             │
             ↓
       Runtime / Context / UI
```

1. **唯一权威历史。** `Session = append-only Commit Ledger`，`State = Fold(Events)`。Turn、Run、Chatlog 不各自持有 Session 的权威状态，它们的事实同在一条 ledger 上；session 流与 run/&lt;RunID&gt; 流是这条 ledger 的两个逻辑流，CommitSeq 是权威全序，StreamSeq 只是读优化（§3）。stream 之外还有三类有明确 owner 的持久数据：内容寻址的 artifact `cas` ContentStore 存内容本体；artifact 的 `RetentionLedger` 保存 claim；Executor 的 durable Execution Store 保存已接受 Assignment 的执行状态与结果。前两类中被 Session 引用的内容以 digest 锚定，模型请求本体作为 Assignment 内容从 authority 传递到 executor，Run 事实只记其 digest，恢复不依赖 authority 的短期本体（RUN-WIR-4、RUN-CMT-7）。Execution Store 是效果层的 authority，不是 Session 事实的第二份来源；Session 只通过 Assignment/Outcome 与它交互。claim 先于 Append 建立（EXT-WRT-3）。

2. **语义串行化。** 同一 Session 的全部写入（Turn、Run、恢复、Checkpoint）经进程内唯一的 `Writer.Commit`，形成一个确定的全序（EXT-SCP-1、EXT-WRT-1）。kernel 不承担并发控制（SES-SCP-2）。

3. **事务边界。** `read state → decide → validate → append` 在 Writer 的互斥区内完成，不可被另一个语义提交插入：`CommitFn` 经 `View` 读取的 head、提交历史与投影状态即写入时的状态，fn 自身不做外部 IO（EXT-WRT-1）。validate 有两层：Binding admission（EXT-REF-2）与投影预折叠——任一投影拒绝则不落盘（EXT-PRJ-1）。这条边界是进程内的；跨进程的隔离由第 5 条提供，两者合起来才是完整的隔离。

4. **原子 Commit。** 一个领域动作产生的多个 event 要么全部出现，要么全部不存在（SES-APP-1）；底层事务或 fsync 只是它的物理实现。崩溃只可能留下一个不完整的尾 Commit，`Open` 在确立 head 之前把它截掉，reader 在任何时刻都看不到不完整的 Commit（SES-APP-2）。

5. **Session 级 Ownership 与 Fencing。** `Handle + Epoch`：同一 Session 同一时刻至多一个有效写者；接管使 Epoch 加一并持久化，旧 Handle 的迟到写入被拒（SES-OWN-1/2）。所有权是 Session 级而非执行目标级：接管者对全部执行中的目标查询，并根据结果重连、延迟或处置（SES-OWN-3、RUN-CMT-7）。何时接管是 kernel 之上的策略，kernel 不承载 TTL 或心跳。

6. **幂等语义提交。** `CommitID + semantic fingerprint`：同 ID 同内容为 `AlreadyApplied`，同 ID 不同内容为 `Conflict`，两者都不写入（EXT-WRT-2）。fingerprint 覆盖 CommitID、各 batch 的 stream 与其事件的 Type、Payload 有序序列，不含 SessionID（继承前缀经 fork 重放仍须判为已应用，SES-FRK-3）与时间。kernel 只拒绝重复 CommitID 并提供该索引的读侧（SES-APP-3、SES-REP-3/4），比对由 Writer 完成。恢复与重放因此不会重复写事实。

7. **Projection 与 Snapshot 只是派生状态。** Projection 可重建，Snapshot（投影缓存）可丢弃；复用条件是 Commit 边界对齐（EXT-PRJ-3），篡改或过期的条目只让下次多折，绝不成为第二份 authority（EXT-PRJ-5/7）。owner 进程内的投影与观察者从 Store 折出的投影对同一 head 给出相同状态（EXT-PRJ-4）。

8. **最小化、payload-opaque 的 kernel。** kernel 只懂 Open/ownership、Append、Read、Seq、CommitID 索引、digest 链（SES-SCP-1/3、第 4 至 6 节）。它不解释 payload，不知道 Turn、Run、Tool、Checkpoint 是什么；领域语义全部在 Module、Writer 与 Projection 层。

9. **可验证历史。** 每个 Commit 的 digest 覆盖 PrevDigest、Seq、CommitID、Epoch 与全部批次 digest，`H0 → C0 → C1 → …` 成链，删除、篡改、重排都使其后全部 Commit 失效（SES-WIR-2）。校验的义务点是 `Open`，读路径信任存储（SES-REP-1）。

10. **模块隔离与版本独立。** 事件按 `<source>/<module>/` 归属，`Requires` 图决定投影的消费范围：范围外事件跳过，范围内不可忽略的 Unknown 事件使折叠失败（EXT-REG-1/4、EXT-PRJ-2）。payload 版本 `v` 由模块携带，与 kernel 的 `ProtocolVersion` 分离（SES-VER-1）。application module 与 first-party 模块同构（EXT-APP）。

11. **事实只 canonical 一次。** 一个模型或工具结果在 ledger 上只有一份表达：Run 事实记录 digest，正文在 `FrozenValueStore`；对话条目与 Turn 结算是这些事实的纯投影，读取时经 materializer 取回正文（RUN-WIR-4、TRN-MAP-1、Chatlog 第 1 与第 8 节）。一次语义操作仍可以在一个 Commit 内写多个 domain 的事实（Run 的 `input_accepted` 与 Chatlog 的 `input_delivered`），它们是各自 domain 的真实事实，不是同一事实的两种表示。run 流因此是 canonical history 的一部分，不能独立于 session 流回收；正文可以迁移到冷存储，不得丢弃。

12. **崩溃后果的封闭集合。** Session 崩溃只可能留下不完整尾 Commit（第 4 条）与孤儿 claim（回收前核对释放，ART-RET-3）；Executor 崩溃还可能留下需要查询的 durable execution record。Session 接管者询问执行目标后重连或处置；两者都不触发自动重试。崩溃恢复、语义重试、重新生成回答和外部效果未知的身份边界见 TRN-DUR-1 至 4；Execution Store 的恢复由效果层合同负责。

13. **读不需要所有权。** 任何进程可随时读完整 Commit 构成的前缀（SES-OWN-4）；观察者用 `NewProjectionReader` 从 Store 折叠，与 owner 一致（第 7 条）。

## 1. 范围

```text
Events = 一条 Session 的 commit ledger：有序的原子 Commit 日志，一个 Commit 内含若干按逻辑流分组的 batch
State  = Fold(Events)

Session lineage DAG（第 8 节）：
  节点  = Segment：不可变的创建记录（SegmentHeader，不含任何 Session 身份）加它自己的只追加 commit，身份为 SegmentID = HeaderDigest
  边    = LedgerRef：子 Segment 到父 Segment 某个 commit 的引用（SegmentHeader.Parent）
  根    = SessionRecord：SessionID → 它追加到的 Segment（Tip）与 Session 自己的元数据
  路径  = Ancestry：一个根到 DAG 起点的显式 Segment 序列，及每段在拼接序列中贡献的区间

kernel 负责：Segment/LedgerRef/SessionRecord/Ancestry 的语义、Commit（Seq、CommitID、Epoch、批次）、原子的 Commit 追加、根级写者独占、按 Commit 的 digest 链、按 Ancestry 拼接的 CommitSeq 顺序读与流读、fork、删除、可达性回收
adapter 负责：Backend——LedgerStore（存节点：段的创建记录与自身 commit）与 SessionStore（存根：记录与 Lease）
modules 负责：event ontology、typed codec、payload 版本、投影、投影缓存、幂等重放、并发串行
```

kernel 的 `session.Ledger` 实现 `Store`，只依赖 `Backend` 端口；Memory 与文件 adapter 只实现该端口。DAG 由 Go 领域类型定义，存储持久化它，而不是从存储布局里产生。

**SES-SCP-1** kernel 不解释 payload，不校验 payload 的 schema，不知道模块、commit 的语义、投影或 lease。它保证四件事：日志只能追加；同一时刻一个 Session 至多一个有效写者；一次 `Append` 的整 Commit event 同时可见或同时不存在；每个 Commit 携带覆盖前一 Commit 的 digest。

**SES-SCP-2** 并发不在 kernel 解决。一个 Session 的全部写入者（Run 的 worker、Turn 的 Coordinator、恢复流程）在进程内经同一个 `writer.Writer` 串行（EXT-WRT），它持有 kernel 的所有权句柄 `session.Handle`。kernel 只拒绝不持有有效所有权的 `Append`。

**SES-SCP-3** kernel 的范围是 Session lineage DAG：header、Open/Append/ReadCommits/ReadStream、所有权与 epoch、按 Commit 的 digest 链、fork、删除与可达性回收（第 8、9 节）。canonical import 不属于当前合同。

**SES-SCP-4** adapter 端口是 `Backend = LedgerStore + SessionStore + CreateSession`。`LedgerStore` 存节点：`Segment`、`ListSegments`、`ReadSegment`（只读该段自身的 commit）、`Contains`、`LookupCommit`、对已封印 Commit 的 `Append(lease, segment, commit)`、`TruncateSegment`、`RemoveSegment`。`SessionStore` 存根：`Record`、`ListRecords`、`Acquire`（所有权与 torn tail 修复）、`Release`、`DeleteRecord`。两者共享一个一致性域，使 `Append` 能与 Lease 检查原子进行。adapter 不知道 fork、前缀与可达性；`Ledger` 在该端口之上一次实现 SES-FRK 与 SES-GC。conformance 以 `Store` 为参数运行，因此每个 adapter 得到同一套 DAG 语义。

## 2. 版本

`ProtocolVersion` 覆盖 kernel wire：header 字段、commit 字段、digest preimage、批次完整性规则。它不覆盖 payload。

**SES-VER-1** payload 的版本由模块负责：每个 payload object 第一层携带整数字段 `v`，模块按 `(EventType, v)` 选 codec（EXT-REG-2）。kernel 不读取该字段。

**SES-VER-2** `ProtocolVersion` 在旧 reader 无法保持 Commit 结构或 digest 语义时递增；payload、EventType、模块 codec 的变化不触发。kernel 版本变化由外部 migration tool 生成新版本日志，旧日志原样保留（adjacent migration）。

## 3. wire types

```go
type SessionID string
type CommitID string
type EventType string
type CommitSeq uint64   // Commit 在 ledger 中的位置，从 0 连续递增，是权威全序
type StreamSeq uint64   // event 在其逻辑流内的位置，从 0 连续递增；读优化，不进 digest
type Epoch uint64       // 写者所有权代数，从 1 递增

type StreamKind string
const (
    StreamKindSession StreamKind = "session" // 全 Session 共享的语义流，ID 必须为空
    StreamKindRun     StreamKind = "run"     // 一个 Run 的流，ID 为 RunID
)
type StreamRef struct { Kind StreamKind; ID string }

type SegmentHeader struct {          // 段的创建记录：ledger DAG 的节点，不含 Session 身份
    ProtocolVersion uint16
    Parent *LedgerRef                 // nil 为 root segment；非 nil 见第 8 节
    Nonce string                      // 128 位随机数的 hex；使两条字段相同的创建记录成为两个段
    CausationID es.CausationID
    Metadata jsonstable.Value
    HeaderDigest es.Digest            // = SegmentID
}
type SessionRecord struct {           // 根：Session 身份与它追加到的段
    ID SessionID
    Tip SegmentID
    CreatedAtUnixMilli int64
}

type Event struct {
    Type EventType
    RecordedAtUnixMilli int64
    Payload jsonstable.Value
}

type StreamBatch struct {
    Stream StreamRef
    Events []Event // 非空
}

type Commit struct {
    Seq CommitSeq
    CommitID CommitID
    Epoch Epoch
    Batches []StreamBatch // 非空；同一 Commit 内每个流至多一个 batch
    PrevDigest es.Digest
    Digest es.Digest
}
type Head struct { Next CommitSeq; Digest es.Digest } // 空日志为 LedgerSeed(header)：根 Session {0, HeaderDigest}，fork {Parent.Seq+1, Parent.Digest}
```

**SES-WIR-1** identity 非空、稳定、有效 UTF-8。`CommitSeq` 从 `LedgerSeed(header).Next` 连续（根 Session 从 0，fork 从 `Parent.Seq+1`，见第 8 节）；一次 `Append` 持久化恰好一个 `Commit`，`CommitID` 在同一 ledger 内唯一。每个 batch 的流归因必须合法：session 流不带 ID，run 流的 ID 是有效 RunID；同一 Commit 内同一流至多一个 batch，每个 batch 与每个 Commit 都非空。`Payload` 必须是 canonical JSON object（RFC 8785），完整字节进入 digest。event 不携带事务元数据（无 Seq、Index、SourceSeqs、Ignorable）：事件的权威顺序由 CommitSeq 加上其在 batch 内的位置决定。

**SES-WIR-4（四种身份）** `SessionID` 是根（分支）身份；`SegmentID` 是历史节点身份，由段的创建记录决定；`CommitID` 是语义操作身份，在一个 Session 的拼接历史内唯一；`Digest` 是完整性身份。段的创建记录与每个 commit 的 digest 预映像都不含 `SessionID`：段是 ledger DAG 的 canonical 对象，被根命名但不属于任何一个根。删除、重建、重命名 Session，或把段 DAG 与根集合一起迁移到另一个 Store，都不改变任何段或 commit 的身份。

**SES-WIR-2** digest preimage：

```text
HeaderDigest = Digest("twilight/session/header", ProtocolVersion, [Parent], Nonce, CausationID, Metadata)  // Parent 为 nil 时不进入预映像；SegmentID = HeaderDigest
BatchDigest  = Digest("twilight/session/batch", SegmentID, Stream, [{Type, RecordedAtUnixMilli, Payload}, ...])
CommitDigest = Digest("twilight/session/commit", PrevDigest, SegmentID, Seq, CommitID, Epoch, [BatchDigest, ...])
```

digest 依 `agent/es` 的 versioned domain separator。链条按 Commit 连接，batch digest 又把 batch 内的事件按序绑定；任何 Commit 被改写、删除或重排都使其后所有 Commit 的 digest 失效。封印（`SealCommit`）以 Handle 的 Epoch 与当前 head digest 计算，验证时重算比对。

**SES-WIR-3** 同一 Session 的 header 与每个 Commit 使用同一 `ProtocolVersion`；Store 从 header 派生 profile（`LedgerProfileFor`），调用方不传版本。

## 4. 所有权

```go
type OpenOptions struct {
    // Takeover 为假时，已有有效 Handle 的 Open 返回 ErrOwned；为真时接管：Epoch 加一，
    // 旧持有者被 fencing。何时允许接管是 kernel 之上的策略。
    Takeover bool
}
// Handle 是 kernel 的所有权句柄，由 Store.Open 返回；进程内的写入者是 writer.Writer，它持有一个 Handle。
type Proposal struct {
    CommitID CommitID
    Batches []StreamBatch // 非空；调用方按批归因流
}
type Handle interface {
    SessionID() SessionID
    Epoch() Epoch
    Head() Head
    Append(context.Context, Proposal) (Commit, error)
    Committed(CommitID) bool
    LookupCommit(CommitID) (Commit, bool, error)
    Close(context.Context) error
}
type Store interface {
    Create(context.Context, CreateRequest) (SegmentHeader, error)   // 返回 tip 段的 header
    Header(context.Context, SessionID) (SegmentHeader, error)       // tip 段的 header
    Record(context.Context, SessionID) (SessionRecord, error)       // 根
    Open(context.Context, SessionID, OpenOptions) (Handle, error)
    ReadCommits(context.Context, CommitReadRequest) (CommitPage, error)
    ReadStream(context.Context, StreamReadRequest) (StreamPage, error)
}
```

**SES-CRT-1** `Create` 建立一个根与它的 tip 段：kernel 解析 `Fork`（SES-FRK-1）、抽取 128 位随机 nonce、封印 `SegmentHeader`，以 `Backend.CreateSession` 一步落下段与根。nonce 只由 kernel 抽取，调用方不能指定：可写节点的身份不对外开放，因此两个根不可能被构造成共用一个 tip（SES-FRK-4）；`CreateSession` 对已存在的 SegmentID 也返回 `ErrConflict`。对已存在的 SessionID，请求所决定的每个字段（ProtocolVersion、解析后的边、CausationID、Metadata、CreatedAtUnixMilli）都与现有 Session 相同则幂等返回现有 tip 的 header，否则 `ErrConflict`；幂等判定不比较 SegmentID，因为 nonce 每次不同。wire 夹具以 `NewLedger(be, WithNonceSource(...))` 注入确定性 nonce。

**SES-OWN-1** 同一 Session 同一时刻至多一个有效 Handle。`Open` 在已有有效 Handle 且未声明 `Takeover` 时返回 `ErrOwned`；声明 `Takeover` 的 Open 接管所有权。接管的安全性由 Epoch fencing（SES-OWN-2）承担；何时允许接管（进程死亡判定、租约、人工指令）是 kernel 之上的策略，kernel 不承载 TTL 或心跳。

**SES-OWN-2** 每次成功的 Open 使该 Session 的 `Epoch` 加一并持久化。`Append` 携带 Handle 的 Epoch；Store 对落后于当前持久化 Epoch 的调用返回 `ErrOwnershipLost`，不写入任何内容。这是 fencing：被接管的旧 Handle 的迟到写入不可能进入日志。

**SES-OWN-3** 所有权是 Session 级的，不是执行目标级的。一个进程取得 Session 的所有权即拥有其中全部执行；接管者读日志后对所有仍在执行中的目标做询问后处置（RUN-CMT-7）。kernel 不知道"执行中"是什么，这一步由 run 模块在 Writer 上完成。

**SES-OWN-4** `ReadCommits` 与 `ReadStream` 不需要所有权，任何进程可以随时读；读到的是完整 Commit 构成的前缀（SES-APP-2）。

## 5. append

**SES-APP-1** `Append(proposal)` 原子：整个 Commit 同时可见或同时不存在。Store 为 Commit 赋 `Seq`（从当前 `Head.Next` 起连续，空 ledger 的 head 为 `LedgerSeed(header)`），以 Handle 的 Epoch 与当前 head digest 封印（SES-WIR-2），持久化，然后返回封印后的 Commit。返回即持久（文件 adapter 每次 Append 一次 `fsync`；数据库 adapter 一个事务）。写入开始之后的任何失败（write、fsync、事务提交返回错误）使该 Commit 是否落盘对句柄成为未知：句柄进入失效状态，本次与之后的 `Append` 返回 `ErrHandleFailed`，不再写入；调用方 Close 并重开，`Open` 按磁盘实况决定该 Commit 是否存在（完整则接纳进索引，残缺则按 SES-APP-2 截断），随后的重放由 `Committed`/`LookupCommit` 回答。adapter 只能在写入开始之前返回 ctx 错误；写入开始后的中断按未知结果报告。

**SES-APP-2** 崩溃只可能留下一个不完整的尾 Commit：文件 adapter 打开时把末尾帧不完整且没有后续 Commit 的尾部截掉；数据库 adapter 由事务保证不会出现。截断必须发生在 `Head` 确立之前：否则 `Head.Next` 落在残 Commit 内部，下一次 `Append` 会把残 Commit 与后续 Commit 焊成一个。reader 在任何时刻都不会看到不完整的 Commit。

**SES-APP-3** kernel 拒绝：空 Commit、空 batch、同一 Commit 内重复的流、非法流归因、重复 `CommitID`、非 canonical 或非 object 的 payload、无效 identity、落后的 Epoch。拒绝不写入任何内容，返回 `ErrInvalid`（重复 CommitID 为 `ErrConflict`）。kernel 不比对重复 CommitID 的内容，不返回"已应用"：幂等重放由 `writer.Writer` 比对 fingerprint 完成（EXT-WRT-2），它为此需要的 Commit 经 `LookupCommit` 从 kernel 取（SES-REP-4）。

## 6. read

```go
type CommitReadRequest struct {
    SessionID SessionID
    From CommitSeq      // 起点，含
    Limit uint32        // 0 为不限
}
type CommitPage struct { Header SegmentHeader; Commits []Commit; Head Head; HasMore bool }  // Header 为 tip 段的
type StreamReadRequest struct {
    SessionID SessionID
    Stream StreamRef   // 只读该逻辑流的事件
    From StreamSeq      // 起点，含
    Limit uint32        // 0 为不限
}
type StreamPage struct { Header SegmentHeader; Stream StreamRef; Events []Event; Head Head; HasMore bool }
```

**SES-REP-1** `ReadCommits` 按 `CommitSeq` 递增返回 `From` 起的完整 Commit；fork 的序列是继承前缀加自身 commit（SES-FRK-2）。`From` 大于等于 `Head.Next` 时返回空页且 `HasMore` 为假，这对 `CommitSeq` 的全部值域成立：实现必须以 `CommitSeq` 比较起点，不得先把它转换为 `int` 再索引日志。损坏检测的义务点在 `Open`：Open 在建立所有权前用 `ValidateLedger` 重算整条 digest 链，损坏必须 fail loudly（`ErrCorrupt`）；`ValidateLedger` 同时作为显式校验入口导出，对任何无法重算的 Commit（包括 profile 拒绝 reseal 的形状错误）返回带该 Commit 坐标的 `ErrCorrupt`，不暴露底层封装错误。Open 读取的 Session 元数据同属校验范围：header 归属另一 Session 或所有权记录无法解析时报 `ErrCorrupt`，不得报告为不存在。读路径信任存储，不逐次重算链。

**SES-REP-2** `StreamSeq` 是流内位置，由 Store 按 CommitSeq 顺序数出，是读侧的优化：`ReadStream` 只返回该流的事件，但其顺序与从 `ReadCommits` 折叠出的流内顺序完全一致。它不进 digest，也不是第二种排序。

**SES-REP-3** `Committed` 报告某个 `CommitID` 是否已在 ledger 中。`Append` 必须拒绝重复 `CommitID`（SES-APP-3），kernel 因此本来就持有这个索引；`Committed` 是该索引的读侧，只做索引查找，不触碰存储。调用者（`writer.Writer`、Run 的重放判定）不必自己再维护一份同样的索引。

**SES-REP-4** `LookupCommit` 返回某个已提交的 Commit，未提交时 `ok=false`。句柄不持有它时从存储读取：文件 adapter 按 `Open` 时记录的字节区间读该 Commit，代价与日志长度无关；内存 adapter 复制该 Commit。代价只落在命中，未命中是一次索引查找。这是幂等重放唯一需要的读取能力：重放不必读整条日志（EXT-WRT-2）。

## 7. errors 与 conformance

```go
type ErrorCode string
const (
    ErrInvalid ErrorCode = "invalid"; ErrNotFound ErrorCode = "not_found"
    ErrConflict ErrorCode = "conflict"; ErrCorrupt ErrorCode = "corrupt"
    ErrOwned ErrorCode = "owned"; ErrOwnershipLost ErrorCode = "ownership_lost"
    ErrHandleFailed ErrorCode = "handle_failed" // 前一次 Append 的持久结果未知，句柄已失效
    ErrUnsupportedProfile ErrorCode = "unsupported_profile"; ErrUnsupported ErrorCode = "unsupported"
)
```

conformance 以 `Store` 为参数，每个 adapter 跑同一套，必须验证：

- **SES-WIR-1/2/3**：CommitSeq 连续、批次非空、同 Commit 内流唯一且归因合法、CommitID 唯一、payload canonical、header/batch/commit digest 链、版本一致；
- **SES-OWN-1/2**：第二个 Open 返回 `ErrOwned`；Close 后可再 Open 且 Epoch 加一；声明 `Takeover` 的 Open 在所有权存续期间接管且 Epoch 加一；旧 Handle 的 Append 返回 `ErrOwnershipLost` 且不写入；
- **SES-APP-1/2/3**：整 Commit 可见性；在 Commit 中途注入崩溃后打开，尾 Commit 不出现；拒绝项无写入；注入持久化失败后句柄返回 `ErrHandleFailed`，重开后已落盘的完整 Commit 在索引中、链完整、同 CommitID 的 Append 为 `ErrConflict`；
- **SES-REP-1/2**：顺序、From、Limit 截断、ReadStream 与折叠一致、篡改任一 Commit 后下一次 Open 报 `ErrCorrupt`；`From` 取到 `CommitSeq` 最大值仍为空页；无法 reseal 的 Commit 经 `ValidateLedger` 报带坐标的 `ErrCorrupt`；header 归属另一 Session 或所有权记录无法解析时 Open 与 Header 报 `ErrCorrupt`；
- **SES-GC-1/2**：Delete 对持有中、未知的 Session 分别为 `ErrOwned`、`ErrNotFound`；删除后不可见、不可开、不可 fork、再次 Delete 为 `ErrNotFound`，同名 Session 立即可重建且得到新段；子仍读到已删除父的前缀；Collect 截掉最大 anchor 之后的自身 commit、整段删除不可达段、对存活 Session 无影响、幂等；
- **SES-WIR-4**：段 header 与 commit 的 digest 预映像不含 SessionID；在另一段 header 下校验同一批 commit 为 `ErrCorrupt`；
- **SES-FRK-1/2/3**：未知父、超出父 history 的 Seq、自身为父的 fork 被拒且不留根；相同 origin 重复 Create 幂等，不同 origin 为 `ErrConflict`；边指向贡献该 commit 的 Segment（在继承 commit 处 fork 的边直指持有它的祖先段）；空 fork 的 head 为 seed；`ReadCommits` 返回前缀加自身，`From`/`Limit` 跨越前缀边界计数；`ReadStream` 对 session 流返回前缀加自身且流内位置计入继承事件，对 run 流只返回自身段的事件、父的同名流不受影响（SES-FRK-5）；首个自身 commit 的 Seq 为 `Seq+1`、PrevDigest 为边的 digest；继承的 CommitID 对 `Committed`/`LookupCommit` 可见、对 `Append` 为 `ErrConflict`；父在 fork 之后的追加对子不可见，反之亦然；自身 commit 在子 header 下、前缀在父段 header 下各自通过 `ValidateLedger`；fork 的 fork 读穿两层前缀。

kernel 的 `ProtocolVersion` 覆盖 header 字段、commit 字段、digest preimage 与批次完整性规则（SES-VER-2）。

## 8. lineage DAG 与 fork

Session 的历史是一个 DAG 上的路径。节点是不可变的 commit 段（`Segment`），边是段到其父段某个 commit 的引用（`SegmentHeader.Parent`，类型 `LedgerRef`），Session 是指向自身 tip 段的根（`SessionRecord`）。fork 的单位是整条 ledger 的前缀 `Session @ CommitSeq N`：session 流与全部 run 流到该 Commit 为止的事实。对话与 Turn 状态是 run 事实的投影（第 11 条），只复制 session 流得不到完整的 canonical history，因此 fork 不复制任何 commit，而是新增一个节点和一条边。

```go
type SegmentID string                                       // = SegmentHeader.HeaderDigest
type LedgerRef struct { Segment SegmentID; Seq CommitSeq; Digest es.Digest }
type Segment struct { ID SegmentID; Header SegmentHeader }  // Header.Parent *LedgerRef 是边
type SessionRecord struct { ID SessionID; Tip SegmentID; CreatedAtUnixMilli int64 }
type Lease struct { Session SessionID; Epoch Epoch }
type ForkOrigin struct { Session SessionID; Seq CommitSeq }  // CreateRequest.Fork
type CreateRequest struct { ProtocolVersion; SessionID; CreatedAtUnixMilli; Fork *ForkOrigin; CausationID; Metadata }  // 段 nonce 由 kernel 抽取，调用方不能命名节点
type Ancestry struct { Segments []AncestrySegment }          // 根段在前，tip 在后；每段带 From/Through
func LoadAncestry(ctx, LedgerStore, tip SegmentID) (*Ancestry, error)
func (*Ancestry) Read / Lookup / Contains / Owner(seq)
func LedgerSeed(SegmentHeader) Head        // 根段 {0, HeaderDigest}；子段 {Seq+1, Digest}
func Reachable(nodes map[SegmentID]Segment, roots []SessionRecord) map[SegmentID]CommitSeq
```

**SES-FRK-1（创建）** `Create` 携带 `Fork{Session, Seq}` 时建立 fork。`Ledger` 解析父 Session 的 `Ancestry`，找到贡献 commit `Seq` 的段（`Owner`），把边记为 `Parent = LedgerRef{Segment: 该段, Seq, Digest: 该 commit 的 digest}`，然后以 `Backend.CreateSession` 一步落下新段与新根（SES-GC-4）。必须核对：父 Session 存活（否则 `ErrNotFound`）、父与子同一 ProtocolVersion、`Seq` 在父的 history 内（否则 `ErrInvalid`）、父不是子自身。任一不满足则不写根也不写段。边进入 header digest 预映像（SES-WIR-2），因此 `SegmentID` 由创建记录决定，相同 origin 的重复 Create 幂等、不同 origin 为 `ErrConflict`。段只追加，边一经建立永久有效；在继承 commit 处 fork，边直指持有该 commit 的祖先段，路径不会随 fork 层数增长。

**SES-FRK-2（读与链）** 段只存自身 commit，从 `LedgerSeed(header)` 起连续编号并从边的 digest 起链：首个自身 Commit 的 `Seq = Parent.Seq+1`、`PrevDigest = Parent.Digest`，其 digest 覆盖创建它的 SessionID（SES-WIR-2）。`ValidateLedger` 以 seed 为起点校验自身 commit；继承前缀由其所在段在自己的 header 下校验，边由子 header digest 钉住。`Open`、`ReadCommits`、`ReadStream` 先加载根段的 `Ancestry`，再在这条显式路径上迭代：每段读取一次自己贡献的区间，不递归读 Store。`From`、`Limit`、`HasMore` 与 `StreamSeq` 都按拼接后的序列计数，`Head` 为 tip 段的 head。父在 fork 之后追加的 Commit 不属于子；子的 Commit 不属于父。

**SES-FRK-3（身份）** `Ancestry` 内的每个 CommitID 都是该 Session 的 CommitID：`Committed` 与 `LookupCommit` 对继承 commit 返回命中，`Append` 对它们返回 `ErrConflict`。Writer 的幂等 fingerprint 因此不覆盖 SessionID（EXT-WRT-2）：前缀 commit 由祖先的 SessionID 封印，经子重放仍须判为 `AlreadyApplied`。Session 级派生身份（RunID、Start/Retry/Settle 的 CommitID、TakeoverClaim）在子中以子的 SessionID 派生，与父此后可能派生的同名身份不冲突。

**SES-FRK-4（所有权与恢复）** 所有权是根级的（`Lease{Session, Epoch}`），段不属于任何 Session：多个根可以经边共享同一历史段，但每个根有自己的 tip 段，两个根从不共用一个 tip，因此不同 Session 的写者从不向同一节点追加。`Append(lease, segment, commit)` 由 adapter 原子核对三件事：Lease 是该 Session 的当前 Lease、该 Session 的 `Tip == segment`、commit 封印于该段的 head。子有独立的 Lease，打开子不需要父的所有权，父的写者也不受子影响。前缀中处于 Executing 的目标属于父的执行：子的接管处置以子的 AssignmentKey 询问 Executor，得到 `missing` 后按 RUN-CMT-7 处置（模型步撤回重规划、工具 call 记 Unknown），不接管父的 attempt。子引用的冻结正文与 artifact 由 fork claim 保留（EXT-WRT-8）。

**SES-FRK-5（继承的是语义历史，不是执行状态）** ledger 血统与语义继承是两件事：`Ancestry` 让子读到父的完整 Commit 前缀（完整性、provenance、CommitID 身份，SES-FRK-2/3），但前缀里的各个流对子的意义不同。session 流是会话的语义历史，子继承它；`run/<id>` 等其他流是写入它们的那个段的执行历史，子不继承——`ReadStream` 对非 session 流只返回子自身段（`Seq > Parent.Seq`）的事件，流内位置从自身段起算；对 session 流仍返回前缀加自身。投影按各自声明折叠继承 commit（EXT-PRJ-8）：执行状态投影只折继承 commit 的 session 流批次，因此父在 fork 点仍处于 Executing 的 Run 在子中不存在、子的接管处置不会把它当作自己的执行来恢复；以 run 事实为语义内容的投影（chatlog 的 assistant/tool_result、turn 的 attempt 结算）声明折叠全部批次。上层据此把继承的 attempt 视为已结算：其 Run 在子中 `ErrRunNotFound`，结算只在 surface 上。fork 点必须是语义静止点——父在该 commit 没有活动中的 Turn——由 Authority 核对（AUTH-FRK-1）；kernel 的 `Create` 不核对。

## 9. 删除与回收

删除只是撤掉一个根；节点是否保留由可达性决定。

```go
func (Store) Delete(ctx, SessionID) error
func (Store) Collect(ctx) (CollectReport, error)
type CollectReport struct { Removed []SegmentID; Truncated map[SegmentID]CommitSeq }
```

**SES-GC-1（删除撤根）** `Delete(sid)` 删除 `SessionRecord`：此后 `Header`、`Open`、`ReadCommits`、`ReadStream`、以它为 origin 的 `Create` 都返回 `ErrNotFound`，第二次 `Delete` 为 `ErrNotFound`，被写者持有的 Session 为 `ErrOwned`。SessionID 立即可以重建，重建得到的是新根与新段，与旧段无关。段本身不动，也没有"已删除"状态：仍以它为前缀的 fork 继续经 `Ancestry` 读到它。

**SES-GC-2（可达性回收）** `Collect` 由 kernel 以 `Reachable(nodes, roots)` 计算每个段必须保留到的 CommitSeq：某个根的 tip 段保留全部自身 commit；只经边到达的段保留到到达它的最大 `Parent.Seq`，边沿 `Parent.Segment` 传递。未被任何根到达的段整段删除；被到达但无根的段截掉边之后的自身 commit。任何根 tip 的 commit 不被触碰，因此 `Collect` 可以在有写者打开时运行，且幂等。

**SES-GC-4（图变更的串行）** 改变根与节点集合的操作（`Create`、`Delete`、`Collect`）在 kernel 内互斥：`Create` 对父存活的核对与它的写入不会与回收该父或新节点的 `Collect` 交错；adapter 以 `CreateSession(Segment, SessionRecord)` 一步落下节点与根，不存在有根无段或有段无根的持久状态。`Append` 与读不取该锁：根 tip 的段从不被 `Collect` 触及。该互斥是进程内的：多个进程共享一个 Backend 时，根集合变更与可达性回收的互斥必须由该 Backend 的存储事务或 GC 权威提供，当前两个 adapter 都是单进程的。

**SES-GC-3（claim 与回收的分工）** `writer.Delete` 在撤根后释放该 Session 拥有的全部 claim（commit claim 与 fork claim，EXT-WRT-9）；继承前缀所引用的内容由每个存活 fork 自己的 fork claim 保留，所以父的 claim 释放不影响子。`Collect` 只回收段的存储，不再涉及 claim。

