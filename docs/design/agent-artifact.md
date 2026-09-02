# Twilight Agent Artifact Core

状态：设计草案。无实现；wire 与 claim 状态表在 Memory reference implementation 通过 conformance 前不冻结。

本文定义 `agent/artifact`。文中的“必须”“不得”“应该”是协议约束；canonical JSON、JCS 与 domain-separated digest 使用 `agent/jsonstable` 和 `agent/es` 的通则。

## 1. 模型与范围

Artifact Core 只有三个模型：

```text
Ref：定位并验证不可变内容
Binding：稳定 BindingID 到 immutable Ref 的映射
RetentionClaim：owner 对一个 BindingSet 的 durable 保留事实
```

`BindingSet` 是 claim 的内容集合。`Prepared` claim 为 in-flight owner operation 提供 GC 保护；`Active` claim 是已确认 owner fact 的 retention root；`Released` claim 不再保留任何内容。Core 不依赖 Session、Event、Chatlog 或 Application，且不解释 owner 的领域语义。Attachment 等 owner module 可以关联 `AttachmentID`、subject 与 `BindingID`，但该边界只使用 BindingID，不引入 Event 依赖。

**ART-SCP-1** Core 不得解释 `ClaimOwner`，不得要求某种数据库、文件系统或 provider 实现。第一版只要求 Memory reference implementation 和 conformance suite。

## 2. identity 与 wire

```go
type WireVersion uint16
type Scheme string
type Authority string
type Key string
type BindingID string
type BindingDigest string
type RefWireIdentity string
type ClaimID string
type RefSetDigest string
type ProviderKindID string
type ProviderInstanceID string

type Durability string
const (
    Ephemeral Durability = "ephemeral"
    EventBound Durability = "event_bound"
    Pinned Durability = "pinned"
)

type Integrity struct { Algorithm, Value string }
type Ref struct {
    Scheme Scheme; Authority Authority; Key Key
    MediaType string
    SizeBytes *uint64
    Integrity *Integrity
    Durability Durability
    ExpiresAtUnixMilli *int64
}
```

**ART-ID-1** 所有 identity 必须非空、稳定，按 bytewise UTF-8 比较；精确 identity、整数和 digest 在 JSON 中为 string。Ref 不得含 credential、临时签名 URL 或进程 handle。

**ART-REF-1** `LocatorIdentity=(Scheme, Authority, Key)`；`RefWireIdentity` 是完整 Ref 的版本化 canonical wire encoding，`RefIdentity(Ref)` 必须由 WireCodec 实现。因此 `MediaType` 是 identity-bound：它进入 RefWireIdentity 和 BindingDigest；但它仍是来自内容声明的 untrusted metadata，resolver、materializer 和安全策略不得仅据它判定可执行性、解析器或权限。相同 locator 的 size（含 presence）和 integrity（含 presence）必须一致，否则 admission 和 resolve 失败。

**ART-REF-2** `cas` 必须带 integrity；`ExpiresAtUnixMilli` 仅允许 `Ephemeral`；`EventBound` 和 `Pinned` 不得过期。durability 顺序为 `Ephemeral < EventBound < Pinned`，promotion 只能产生同级或更高的新 Ref。

**ART-WIR-1** `WireVersion` 冻结字段、required/omitted、array order、unknown-field policy 和 digest preimage。v1 省略 optional empty field、拒绝 null 和未知 envelope field；`size_bytes`、`expires_at_unix_milli` 使用无前导零十进制 string。codec 必须提供：

```go
type WireCodec interface {
    Version() WireVersion
    RefIdentity(Ref) (RefWireIdentity, error)
    EncodeRef(Ref) (jsonstable.Value, error)
    DecodeRef(jsonstable.Value) (Ref, error)
    EncodeBinding(Binding) (jsonstable.Value, error)
    DecodeBinding(jsonstable.Value) (Binding, error)
    EncodeManifest(BindingManifest) (jsonstable.Value, error)
    DecodeManifest(jsonstable.Value) (BindingManifest, error)
}
```

## 3. Ref 与 Binding

```go
type Binding struct { ID BindingID; Ref Ref; Digest BindingDigest }
type Info struct { MediaType string; SizeBytes *uint64; Integrity *Integrity; Durability Durability }
type PutRequest struct { MediaType string; Reader io.Reader; Durability Durability }
type PromoteRequest struct { TargetScheme Scheme; TargetAuthority Authority; Durability Durability }

type Resolver interface {
    Stat(context.Context, Ref) (Info, error)
    Open(context.Context, Ref) (io.ReadCloser, Info, error)
}
type Store interface { Put(context.Context, PutRequest) (Ref, error) }
type Promoter interface { Promote(context.Context, Ref, PromoteRequest) (Ref, error) }
type BindingResolver interface { ResolveBinding(context.Context, BindingID) (Binding, error) }
type BindingStore interface {
    CreateBinding(context.Context, Binding) (Binding, error)
    LookupBinding(context.Context, BindingID) (Binding, bool, error)
}
```

**ART-BND-1** Binding immutable。`BindingDigest` 覆盖 versioned domain separator、BindingID 和完整 RefWireIdentity。相同 BindingID 只可重建逐字段相同的 Binding；其他值为 conflict。

**ART-BND-2** Resolver 必须验证返回 bytes 与声明的 size/integrity 一致。Store 只有在 durable acknowledgement 后返回 Ref；同 immutable identity 和 bytes 的重复 Put 幂等。promotion 流程为 `resolve → promote → CreateBinding(target Ref)`，不得重写旧 Binding。

## 4. capability interfaces

**ART-CAP-1** Resolver、Store、Promoter 是 capability boundary：它们必须区分 `missing`、`expired`、`unauthorized`、`corrupt` 和 transient failure，并防护跨 `Authority` key confusion、path traversal、size amplification 与不安全 media-type trust。

**ART-CAP-2** `Scheme` 是 resolution contract；`Authority` 是逻辑 store instance；`Key` 由 scheme 解释。标准 scheme 为 `cas`（content digest）、`spill`（opaque temporary key）和 `workspace`（immutable revision + canonical path）。自定义 scheme 使用 `ext:<module-id>/<name>`，发布后不得破坏其 key、integrity、durability 或 resolution contract。

## 5. retention ledger

```go
type ClaimOwner struct { Kind, Authority, Identity string }
type ClaimState string
const (
    ClaimPrepared ClaimState = "prepared"
    ClaimActive ClaimState = "active"
    ClaimReleased ClaimState = "released"
)

// BindingSet is a canonical, resolved retention set.
type BindingSet struct { BindingIDs []BindingID; RefSetDigest RefSetDigest }
type BindingSetBuilder interface {
    Build(context.Context, []BindingID) (BindingSet, error)
}
type RetentionClaim struct {
    ID ClaimID; Owner ClaimOwner; BindingSet BindingSet; State ClaimState
}
type ClaimCursor struct { Watermark ClaimID; After ClaimID }
type ClaimPage struct { Items []RetentionClaim; Next *ClaimCursor }
type ClaimOwnerQuery struct { Kind, Authority string; Identities []string }

type RetentionLedger interface {
    Prepare(context.Context, ClaimID, ClaimOwner, BindingSet) (RetentionClaim, error)
    LookupClaim(context.Context, ClaimID) (RetentionClaim, bool, error)
    Activate(context.Context, ClaimID) (RetentionClaim, error)
    AbortPrepared(context.Context, ClaimID) error
    ReleaseActive(context.Context, ClaimID) error
    PreparedClaims(context.Context, ClaimCursor) (ClaimPage, error)
    ClaimsByOwner(context.Context, ClaimOwnerQuery, ClaimCursor) (ClaimPage, error)
    ImportActiveClaims(context.Context, []RetentionClaim) error
}
```

**ART-RET-1** `BindingSetBuilder.Build(ctx, ids)` 是构造 BindingSet 的唯一算法：它将 ids canonicalize 为 sorted-unique `BindingID`，逐个通过 BindingResolver resolve，并计算覆盖 profile、WireVersion 和按 BindingID 排序的 `(BindingID, BindingDigest)` 的 `RefSetDigest`。`BindingSet` 必须同时携带这两个值，不能由调用者单独拼接 digest。ledger 必须以自己的 BindingResolver 重建并精确验证传入 set。

**ART-RET-2** claim 只接受 `EventBound` 或 `Pinned` Binding；`Ephemeral` 必须先 promote。ClaimID 必须由 owner fact identity 与 BindingSet 稳定、确定地派生，并永久绑定该 owner 与 set：`Prepare` 对同 ID、同 owner、同 set 幂等，对任何其他组合 conflict。`Prepared` 与 `Active` 都是 GC roots；GC 只忽略 `Released` claim。未知 scheme 必须保守保留。

| 操作 | 前置状态 | 结果 |
|---|---|---|
| Prepare(new ID, owner, set) | 不存在 | Prepared |
| Prepare(existing ID, exact owner/set) | Prepared/Active | 返回既有 claim |
| Prepare(existing ID, exact owner/set) | Released | conflict |
| Prepare(existing ID, other owner/set) | 任意 | conflict |
| Activate | Prepared | Active |
| Activate | Active | 幂等成功 |
| Activate | Released/不存在 | conflict/not found |
| AbortPrepared | Prepared，且有 owner operation terminally aborted 证据 | Released |
| AbortPrepared | Released | 幂等成功 |
| AbortPrepared | Active | conflict |
| ReleaseActive | Active，且 owner retention 已结束 | Released |
| ReleaseActive | Released | 幂等成功 |
| ReleaseActive | Prepared | conflict |

**ART-RET-3** `AbortPrepared` 的调用方必须以证明 owner operation 已 terminally aborted 的 durable evidence 授权。owner operation 查询为 NotFound、unknown 或 transient failure 时，reconciler 必须保留原 `Prepared` claim 并留待后续 reconciliation。对同一 owner fact 的 retry 必须保留原 `Prepared` claim，绝不得先 abort 再复用 ID。`PreparedClaims` 与 `ClaimsByOwner` 使用 watermark cursor，按 ClaimID 稳定排序；空 owner identities 不匹配。reconciler 将已证实 owner fact 映射为 Activate、已证实 terminal abort 映射为 AbortPrepared、已证实 owner retention 结束映射为 ReleaseActive。

## 6. provider 与 scheme boundary

```go
type SchemeDefinition struct {
    Scheme Scheme; SupportedDurabilities []Durability
    ValidateRef func(Ref) error
}
type ProviderDescriptor struct {
    KindID ProviderKindID; Schemes []Scheme; ConfigSchema jsonstable.Value
}
type ProviderBinding struct {
    Scheme Scheme; Authority Authority; InstanceID ProviderInstanceID
}
```

**ART-PRO-1** registry 在 startup 组合后 immutable；每个 Scheme 有唯一 definition，verified use 需要已注册 Scheme 和唯一 `(Scheme,Authority)` provider binding。provider config、secret、物理位置与迁移属于 adapter/Application。

**ART-PRO-2** adapter 改变物理实现时必须保持 locator resolution 不变，并以 generation/fence 防止旧位置在新位置验证可恢复前回收。具体 filesystem、DB 与迁移步骤由 adapter/Application 负责。

## 7. archive 与 import/export

```go
type BindingManifest struct {
    WireVersion WireVersion; Bindings []Binding; ActiveClaims []RetentionClaim
}
type InspectionStatus string
const (
    InspectionAccepted InspectionStatus = "accepted"
    InspectionUnknownScheme InspectionStatus = "unknown_scheme"
    InspectionInvalid InspectionStatus = "invalid"
)
type InspectionResult struct { Status InspectionStatus; Detail string }
type VerifiedImportResult struct { Bindings []BindingID; Claims []ClaimID }
```

**ART-ARC-1** manifest 是精确 canonical wire：Bindings 按 BindingID 严格递增、ActiveClaims 按 ClaimID 严格递增，且只可携带 `Active` claims。inspection 可以无损接受未知 scheme record，但不创建可用 Binding 或 claim；verified import 要求 codec、排序、Binding digest、RefSetDigest、scheme/provider 和 object policy 全部通过。

**ART-ARC-2** `ImportActiveClaims` 是 all-or-nothing validation boundary：先验证所有 referenced Binding、durability、digest、owner、ClaimID 和 state，再全部写入或失败。它不接受 Prepared 或 Released records；逐字段相同 active record 幂等，同 identity 的不同 record 为 conflict。导出按 `ClaimsByOwner` 的完整 cursor 枚举 closure。

多 package coordination、quarantine 操作流程不属于本规范。

## 8. errors 与 conformance

```go
type ErrorCode string
const (
    ErrInvalid ErrorCode = "invalid"; ErrNotFound ErrorCode = "not_found"
    ErrConflict ErrorCode = "conflict"; ErrUnauthorized ErrorCode = "unauthorized"
    ErrExpired ErrorCode = "expired"; ErrCorrupt ErrorCode = "corrupt"
    ErrUnsupported ErrorCode = "unsupported"; ErrUnavailable ErrorCode = "unavailable"
)
type Error struct { Code ErrorCode; Operation string; Identity string; Detail string }
func (Error) Error() string
```

实现必须以可判别 `ErrorCode` 返回预期失败；`Detail` 不得承载 provider secret。

Conformance 必须验证：

- **ART-ID-1、ART-REF-1、ART-REF-2、ART-WIR-1**：canonical round-trip、拒绝歧义 wire、identity-bound/untrusted MediaType、locator/integrity 和 durability；
- **ART-BND-1、ART-BND-2、ART-CAP-1**：Binding conflict、promotion、resolver integrity 和 capability errors；
- **ART-RET-1、ART-RET-2、ART-RET-3**：BindingSetBuilder/ledger 独立重算与精确验证、RefSetDigest、不可复用 released claim、全状态表、cursor pagination、Prepared/Active GC protection 与保守 reconcile；
- **ART-PRO-1、ART-PRO-2**：immutable registry、provider-instance isolation 与迁移 fence；
- **ART-ARC-1、ART-ARC-2**：manifest wire、inspection、verified all-or-nothing active-claim import 和 exact idempotency。
