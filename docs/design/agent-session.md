# Twilight Agent Session Protocol

状态：设计草案。本文是 Session ES kernel 的目标设计。

本文定义 Twilight Session 的 Event Sourcing kernel。文中的"必须""不得""应该"是协议约束。

## 0. 设计原则

Twilight Session 是一个单写者、可接管、幂等提交的 Event-Sourced Aggregate：Event Stream 是事实的唯一权威，Writer 提供语义事务与串行化，kernel 只提供原子 append、ownership/fencing 与持久化，Projection 与 Snapshot 全部是可重建的派生状态。下列原则贯穿本文与 [Session Module Framework](agent-session-extension.md)、[Decision](agent-decision.md)、[Run](agent-run.md)、[Turn](agent-turn.md)、[Chatlog](agent-session-chatlog.md)、[Artifact](agent-artifact.md) 各规范；每条给出承载它的条款。

```text
                  Session

      ┌── Canonical Event Stream ──┐
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
Atomic Event Group ────────────────┘
             │
             ↓
      Projections / Snapshots
             │
             ↓
       Runtime / Context / UI
```

1. **唯一权威历史。** `Session = append-only Event Stream`，`State = Fold(Events)`。Turn、Run、Chatlog 不各自持有权威状态，它们的事实同在一条 stream 上（§2.1 authority）。stream 之外只有两类持久数据，且都以 digest 被 stream 锚定：`FrozenValueStore` 存模型请求本体，Run 事实只记其 digest（RUN-WIR-4）；artifact 的 `RetentionLedger` 自持久化，claim 先于 Append 建立（EXT-WRT-3）。

2. **语义串行化。** 同一 Session 的全部写入（Turn、Run、恢复、Checkpoint）经进程内唯一的 `Writer.Commit`，形成一个确定的全序（EXT-SCP-1、EXT-WRT-1）。kernel 不承担并发控制（SES-SCP-2）。

3. **事务边界。** `read state → decide → validate → append` 在 Writer 的互斥区内完成，不可被另一个语义提交插入：`CommitFn` 经 `View` 读取的 head、提交历史与投影状态即写入时的状态，fn 自身不做外部 IO（EXT-WRT-1）。validate 有两层：Binding admission（EXT-REF-2）与投影预折叠——任一投影拒绝则不落盘（EXT-PRJ-1）。这条边界是进程内的；跨进程的隔离由第 5 条提供，两者合起来才是完整的隔离。

4. **原子 Semantic Group。** 一个领域动作产生的多个 event 要么全部出现，要么全部不存在（SES-APP-1）；底层事务或 fsync 只是它的物理实现。崩溃只可能留下一个不完整尾组，`Open` 在确立 head 之前把它截掉，reader 在任何时刻都看不到不完整的组（SES-APP-2）。

5. **Session 级 Ownership 与 Fencing。** `Handle + Epoch`：同一 Session 同一时刻至多一个有效写者；接管使 Epoch 加一并持久化，旧 Handle 的迟到写入被拒（SES-OWN-1/2）。所有权是 Session 级而非执行目标级：接管者对全部执行中的目标做一次性处置（SES-OWN-3、RUN-CMT-7）。何时接管是 kernel 之上的策略，kernel 不承载 TTL 或心跳。

6. **幂等语义提交。** `CommitID + semantic fingerprint`：同 ID 同内容为 `AlreadyApplied`，同 ID 不同内容为 `Conflict`，两者都不写入（EXT-WRT-2）。fingerprint 覆盖 Type、SourceSeqs、Payload，不含时间。kernel 只拒绝重复 CommitID 并提供该索引的读侧（SES-APP-3、SES-REP-3/4），比对由 Writer 完成。恢复与重放因此不会重复写事实。

7. **Projection 与 Snapshot 只是派生状态。** Projection 可重建，Snapshot（投影缓存）可丢弃；复用条件是组对齐（EXT-PRJ-3），篡改或过期的条目只让下次多折，绝不成为第二份 authority（EXT-PRJ-5/7）。owner 进程内的投影与观察者从 Store 折出的投影对同一 head 给出相同状态（EXT-PRJ-4）。

8. **最小化、payload-opaque 的 kernel。** kernel 只懂 Open/ownership、Append、Read、Seq、CommitID 索引、digest 链（SES-SCP-1/3、第 4 至 6 节）。它不解释 payload，不知道 Turn、Run、Tool、Checkpoint 是什么；领域语义全部在 Module、Writer 与 Projection 层。

9. **可验证历史。** 每行 digest 覆盖本行与前一行，`H0 → E0 → E1 → …` 成链，删除、篡改、重排都使其后全部行失效（SES-WIR-2）。校验的义务点是 `Open`，`Read` 信任存储（SES-REP-1）。

10. **模块隔离与版本独立。** 事件按 `<source>/<module>/` 归属，`Requires` 图决定投影的消费范围：范围外事件跳过，范围内不可忽略的 Unknown 事件使折叠失败（EXT-REG-1/4、EXT-PRJ-2）。payload 版本 `v` 由模块携带，与 kernel 的 `ProtocolVersion` 分离（SES-VER-1）。application module 与 first-party 模块同构（EXT-APP）。

11. **同组伴随写入。** Run 事实与它产生的对话内容（assistant、tool_result）写在同一组：内容只出现一次，事实只记 digest（RUN-WIR-4、TRN-CMP、Chatlog 第 1 节）。这是第 4 条最重要的应用。

12. **崩溃后果的封闭集合。** 崩溃只可能留下不完整尾组（第 4 条）与孤儿 claim（回收前核对释放，ART-RET-3）；执行中的目标由接管者一次性处置（第 5 条），处置结果是终态事实，不触发自动重试；崩溃恢复、语义重试、重新生成回答四种情形的身份边界见 TRN-DUR-1 至 4。没有其他需要修复的中间状态。

13. **读不需要所有权。** 任何进程可随时读完整组构成的前缀（SES-OWN-4）；观察者用 `NewProjectionReader` 从 Store 折叠，与 owner 一致（第 7 条）。

## 1. 范围

```text
Events = 一条 Session 的有序 SessionEvent 日志，追加式，一行一个 event
State  = Fold(Events)

kernel 负责：header、event 行、seq、原子的组追加、Session 级写者独占、按行 digest、顺序读
modules 负责：event ontology、typed codec、payload 版本、投影、投影缓存、幂等重放、并发串行
```

**SES-SCP-1** kernel 不解释 payload，不校验 payload 的 schema，不知道模块、commit 的语义、投影或 lease。它保证四件事：日志只能追加；同一时刻一个 Session 至多一个有效写者；一次 `Append` 的整组 event 同时可见或同时不存在；每行携带覆盖前一行的 digest。

**SES-SCP-2** 并发不在 kernel 解决。一个 Session 的全部写入者（Run 的 worker、Turn 的 Coordinator、恢复流程）在进程内经同一个 `writer.Writer` 串行（EXT-WRT），它持有 kernel 的所有权句柄 `session.Handle`。kernel 只拒绝不持有有效所有权的 `Append`。

**SES-SCP-3** kernel 的范围是单条 stream：header、Open/Append/Read、所有权与 epoch、按行 digest。Fork、ancestry 与 canonical import 建立在这条 stream 之上，见第 8 节。

## 2. 版本

`ProtocolVersion` 覆盖 kernel wire：header 字段、event 行字段、digest preimage、组完整性规则。它不覆盖 payload。

**SES-VER-1** payload 的版本由模块负责：每个 payload object 第一层携带整数字段 `v`，模块按 `(EventType, v)` 选 codec（EXT-REG-2）。kernel 不读取该字段。

**SES-VER-2** `ProtocolVersion` 在旧 reader 无法保持行结构或 digest 语义时递增；payload、EventType、模块 codec 的变化不触发。kernel 版本变化由外部 migration tool 生成新版本日志，旧日志原样保留（adjacent migration）。

## 3. wire types

```go
type SessionID string
type CommitID string
type EventType string
type Seq uint64      // 行号，从 0 连续递增
type Epoch uint64    // 写者所有权代数，从 1 递增

type SessionHeader struct {
    ProtocolVersion uint16
    SessionID SessionID
    CreatedAtUnixMilli int64
    ParentFork *ForkPoint // nil 为 root stream；非 nil 见第 8 节
    CausationID es.CausationID
    Metadata jsonstable.Value
    HeaderDigest es.Digest
}

type SessionEvent struct {
    Seq Seq
    CommitID CommitID   // 同一次 Append 的行相同
    Index uint16        // 组内序号，从 0 递增
    Last bool           // 组内最后一行
    Type EventType
    RecordedAtUnixMilli int64
    SourceSeqs []Seq    // 可选；语义由声明它的模块解释，kernel 不校验
    Ignorable bool      // 写者声明：不认识该 Type 的 reader 可以跳过它
    Payload jsonstable.Value
    Digest es.Digest    // 覆盖本行全部字段与前一行的 Digest
}

type UncommittedEvent struct {
    Type EventType
    RecordedAtUnixMilli int64
    SourceSeqs []Seq
    Ignorable bool
    Payload jsonstable.Value
}
type Group struct {
    CommitID CommitID
    Events []UncommittedEvent // 非空
}
type Head struct { Next Seq; Digest es.Digest } // 空日志为 {0, HeaderDigest}
```

**SES-WIR-1** identity 非空、稳定、有效 UTF-8。`Seq` 从 0 连续；一次 `Append` 写入的行 `CommitID` 相同，`Index` 从 0 连续，最后一行 `Last=true`；`CommitID` 在同一 stream 内唯一。`Payload` 必须是 canonical JSON object（RFC 8785），完整字节进入 digest。

**SES-WIR-2** digest preimage：

```text
HeaderDigest = Digest("twilight/session/header", ProtocolVersion, SessionID, CreatedAtUnixMilli, CausationID, Metadata)
Digest(row)  = Digest("twilight/session/event", prev, SessionID, Seq, CommitID, Index, Last, Type, RecordedAtUnixMilli, SourceSeqs, Ignorable, Payload)
              其中 prev 为前一行的 Digest，Seq 0 的 prev 为 HeaderDigest
```

digest 依 `agent/es` 的 versioned domain separator。链条按行连接；任何行被改写、删除或重排都使其后所有行的 digest 失效。

**SES-WIR-3** 同一 Session 的 header 与每一行使用同一 `ProtocolVersion`；Store 从 header 派生版本，调用方不传版本。

## 4. 所有权

```go
type OpenOptions struct {
    // Takeover 为假时，已有有效 Handle 的 Open 返回 ErrOwned；为真时接管：Epoch 加一，
    // 旧持有者被 fencing。何时允许接管是 kernel 之上的策略。
    Takeover bool
}
// Handle 是 kernel 的所有权句柄，由 Store.Open 返回；进程内的写入者是 writer.Writer，它持有一个 Handle。
type Handle interface {
    SessionID() SessionID
    Epoch() Epoch
    Head() Head
    Append(context.Context, Group) ([]SessionEvent, error)
    Committed(CommitID) bool
    LookupCommit(CommitID) ([]SessionEvent, bool, error)
    Close(context.Context) error
}
type Store interface {
    Create(context.Context, CreateRequest) (SessionHeader, error)
    Header(context.Context, SessionID) (SessionHeader, error)
    Open(context.Context, SessionID, OpenOptions) (Handle, error)
    Read(context.Context, ReadRequest) (ReadPage, error)
}
```

**SES-OWN-1** 同一 Session 同一时刻至多一个有效 Handle。`Open` 在已有有效 Handle 且未声明 `Takeover` 时返回 `ErrOwned`；声明 `Takeover` 的 Open 接管所有权。接管的安全性由 Epoch fencing（SES-OWN-2）承担；何时允许接管（进程死亡判定、租约、人工指令）是 kernel 之上的策略，kernel 不承载 TTL 或心跳。

**SES-OWN-2** 每次成功的 Open 使该 Session 的 `Epoch` 加一并持久化。`Append` 携带 Handle 的 Epoch；Store 对落后于当前持久化 Epoch 的调用返回 `ErrOwnershipLost`，不写入任何内容。这是 fencing：被接管的旧 Handle 的迟到写入不可能进入日志。

**SES-OWN-3** 所有权是 Session 级的，不是执行目标级的。一个进程取得 Session 的所有权即拥有其中全部执行；接管者读日志后对所有仍在执行中的目标做一次性处置（RUN-CMT-7）。kernel 不知道"执行中"是什么，这一步由 run 模块在 Writer 上完成。

**SES-OWN-4** `Read` 不需要所有权，任何进程可以随时读；读到的是完整组构成的前缀（SES-APP-2）。

## 5. append

**SES-APP-1** `Append(group)` 原子：整组 event 同时可见或同时不存在。Store 为组内每行赋 `Seq`（从当前 `Head.Next` 起连续）、`Index`、`Last`，计算 `Digest`，持久化，然后返回带完整字段的行。返回即持久（文件 adapter 每次 Append 一次 `fsync`；数据库 adapter 一个事务）。写入开始之后的任何失败（write、fsync、事务提交返回错误）使该组是否落盘对句柄成为未知：句柄进入失效状态，本次与之后的 `Append` 返回 `ErrHandleFailed`，不再写入；调用方 Close 并重开，`Open` 按磁盘实况决定该组是否存在（完整则接纳进索引，残缺则按 SES-APP-2 截断），随后的重放由 `Committed`/`LookupCommit` 回答。adapter 只能在写入开始之前返回 ctx 错误；写入开始后的中断按未知结果报告。

**SES-APP-2** 崩溃只可能留下一个不完整的尾组：文件 adapter 打开时把末尾 `Last=false` 且没有后续行的整组截掉；数据库 adapter 由事务保证不会出现。截断必须发生在 `Head` 确立之前：否则 `Head.Next` 落在残组内部，下一次 `Append` 会把残组与后续组焊成一组。reader 在任何时刻都不会看到不完整的组。

**SES-APP-3** kernel 拒绝：空组、重复 `CommitID`、非 canonical 或非 object 的 payload、无效 identity、落后的 Epoch。拒绝不写入任何内容，返回 `ErrInvalid`（重复 CommitID 为 `ErrConflict`）。kernel 不比对重复 CommitID 的内容，不返回"已应用"：幂等重放由 `writer.Writer` 比对 fingerprint 完成（EXT-WRT-2），它为此需要的行经 `LookupCommit` 从 kernel 取（SES-REP-4）。

## 6. read

```go
type ReadRequest struct {
    SessionID SessionID
    From Seq            // 起点，含
    Types []EventType   // 空为全部；非空为 EventType 前缀过滤（优化，不改变语义）
    Limit uint32        // 0 为不限
}
type ReadPage struct { Header SessionHeader; Events []SessionEvent; Head Head; HasMore bool }
```

**SES-REP-1** `Read` 按 `Seq` 递增返回 `From` 起的行，只返回完整组内的行；`Limit` 截断只发生在组边界。损坏检测的义务点在 `Open`：Open 在建立所有权前校验整条 `Digest` 链，损坏必须 fail loudly（`ErrCorrupt`）；`ValidateChain` 同时作为显式校验入口导出。`Read` 信任存储，不逐次重算链。

**SES-REP-2** `Types` 过滤是读取代价的优化：文件 adapter 全量扫描后过滤，数据库 adapter 用 `(SessionID, Type 前缀)` 索引。过滤与不过滤读到的事件集合对匹配类型完全一致。

**SES-REP-3** `Committed` 报告某个 `CommitID` 是否已在 stream 中。`Append` 必须拒绝重复 `CommitID`（SES-APP-3），kernel 因此本来就持有这个索引；`Committed` 是该索引的读侧，只做索引查找，不触碰存储。调用者（`writer.Writer`、Run 的重放判定）不必自己再维护一份同样的索引。

**SES-REP-4** `LookupCommit` 返回某个已提交组的行，未提交时 `ok=false`。句柄不持有这些行时从存储读取：文件 adapter 按 `Open` 时记录的字节区间读该组，代价与日志长度无关；内存 adapter 复制该组的行区间。代价只落在命中，未命中是一次索引查找。这是幂等重放唯一需要的读取能力：重放不必读整条日志（EXT-WRT-2）。

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

v1 conformance 以 `Store` 为参数，每个 adapter 跑同一套，必须验证：

- **SES-WIR-1/2/3**：Seq 连续、组内 Index/Last、CommitID 唯一、payload canonical、digest 链与 header 根、版本一致；
- **SES-OWN-1/2**：第二个 Open 返回 `ErrOwned`；Close 后可再 Open 且 Epoch 加一；声明 `Takeover` 的 Open 在所有权存续期间接管且 Epoch 加一；旧 Handle 的 Append 返回 `ErrOwnershipLost` 且不写入；
- **SES-APP-1/2/3**：整组可见性；在组中途注入崩溃后打开，尾组不出现；拒绝项无写入；注入持久化失败后句柄返回 `ErrHandleFailed`，重开后已落盘的完整组在索引中、链完整、同 CommitID 的 Append 为 `ErrConflict`；
- **SES-REP-1/2**：顺序、From、Limit 在组边界截断、过滤与全量对匹配类型一致、篡改任一行后下一次 Open 报 `ErrCorrupt`。

kernel 的 `ProtocolVersion` 覆盖 header 字段、event 行字段、digest preimage 与组完整性规则（SES-VER-2）。

## 8. fork、ancestry 与 canonical import

**Fork。** `ForkPoint{ParentSessionID, Seq, Digest}`；子 Session 以父在 `Digest` 处的状态为 seed，header 记 `ParentFork`，seed 之后第一行的 prev digest 为 `ForkPoint.Digest`。

**Canonical import。** 按行校验 digest 链后导入完整日志，或导入已有可验证前缀的连续尾部；同 `(SessionID, Seq)` 仅在行逐字节相同时幂等。

**投影缓存与 ancestry。** Fork 之后，投影缓存的 `Through` 绑定 ancestry 而非单个 `Seq`（EXT-PRJ-3）。
