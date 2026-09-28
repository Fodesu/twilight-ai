# Rule Index（过渡文件）

本文档是设计文档去符号化的过渡桥梁。设计文档正文不再使用规则 ID（如 SES-OWN-6、EXT-WRT-4），规则由所在文档的章节承载。代码注释中遗留的 ID 引用在本表可查；当代码中不再有任何 ID 引用时，删除本文件。

位置列给出定义该规则的文档与章节；一句话规则仅用于索引定位，正文以文档为准。

| ID | 一句话规则 | 位置 |
|---|---|---|
| SES-SCP-1 | kernel 不解释 payload；保证只追加、单写者、整 Commit 原子可见 | agent-session.md · kernel 的三条对外保证 |
| SES-SCP-2 | 进程内并发由 writer.Writer 串行；kernel 只拒绝无有效所有权的 Append | 同上 |
| SES-SCP-3 | kernel 范围 = Session lineage 树：header、读写、所有权、fork、删除与回收 | 同上 |
| SES-SCP-4 | adapter 端口 = SegmentStore + RootStore + MaintenanceStore，共享一个一致性域 | agent-session.md · adapter 端口 |
| SES-VER-1 | payload 版本归事件类型；读侧按 (EventType, v) 选 codec 并 upcast | agent-session.md · payload 版本归事件类型 |
| SES-VER-2 | kernel header/commit 结构不带版本号；新可选信息走 Ext 槽 | agent-session.md · kernel 结构不设版本 |
| SES-VER-3 | 派生身份（ClaimID、CommitID、SessionID 等）的预映像不含 kernel 层版本 | agent-session.md · 派生身份不嵌版本 |
| SES-WIR-1 | wire 形状：CommitSeq 连续、批次非空、流归因合法、CommitID 唯一、payload 为 canonical JSON object | agent-session.md · wire 的形状约束 |
| SES-WIR-2 | header/commit 不含内容 hash 与前向链接；历史不可变由 Store 写入契约保证 | agent-session.md · 不含内容 hash |
| SES-WIR-4 | 四种身份：SessionID（根）、SegmentID（节点）、CommitID（操作）、(SegmentID, Seq)（位置）；段不含 SessionID | agent-session.md · 四种身份 |
| SES-WIR-5 | Ext 按模块分槽；kernel 原样保存、不解释，条目逐字节往返 | agent-session.md · 模块扩展槽 |
| SES-CRT-1 | Create 幂等：同 SessionID 且段字段相同返回现有 tip，否则 ErrConflict；SegmentID 由 kernel 抽取 | agent-session.md · 创建与幂等 |
| SES-OWN-1 | 同一 Session 至多一个有效 Handle；三种接管都使 Epoch 加一 | agent-session.md · 租约与接管 |
| SES-OWN-2 | Epoch fencing：落后持久化 Epoch 的 Append/Renew 返回 ErrOwnershipLost，不写入 | agent-session.md · Epoch fencing |
| SES-OWN-3 | 所有权是 Session 级；接管者对全部执行中目标询问后处置 | agent-session.md · 所有权是 Session 级 |
| SES-OWN-4 | 读不需要所有权 | agent-session.md · 读不需要所有权 |
| SES-OWN-5 | LeaseOf/ListLeases/ExpiredLeases 是读取，不改变 Epoch、不触发接管 | agent-session.md · 租约读取 |
| SES-OWN-6 | 租约到期判定与新到期时间由 adapter 时钟完成 | agent-session.md · 租约时间归 adapter |
| SES-APP-1 | Append 原子；写入开始后的失败使句柄失效（ErrHandleFailed），重开按磁盘实况裁决 | agent-session.md · 原子性 |
| SES-APP-2 | 崩溃只留不完整的尾 Commit；截断必须在 Head 确立之前 | agent-session.md · 崩溃只留残尾 Commit |
| SES-APP-3 | kernel 拒绝项：空 Commit/空 batch/重复流/非法归因/重复 CommitID/非 canonical payload/无效 identity/落后 Epoch | agent-session.md · kernel 的拒绝项 |
| SES-APP-4 | CommitID 由写者派生命名操作；同 ID 不同内容不是可判定冲突，先落下者生效 | agent-session.md · CommitID 命名操作 |
| SES-APP-5 | 只追加；(segment, seq) 与 (segment, commit_id) 唯一约束和 Lease 检查在同一事务内 | agent-session.md · 只追加 |
| SES-REP-1 | ReadCommits 按 CommitSeq 递增返回完整 Commit；空页语义对 CommitSeq 全值域成立 | agent-session.md · 按 CommitSeq 顺序读 |
| SES-REP-2 | StreamSeq 是读优化，顺序与折叠一致；流读取成本与该流 commit 数成正比 | agent-session.md · StreamSeq 是读优化 |
| SES-REP-3 | Committed/StreamHead 是 CommitID 索引的读侧；tip 段核对以句柄自己的 head 为界 | agent-session.md · Committed 与索引 |
| SES-REP-4 | LookupCommit 按索引字节区间读取；幂等重放不必读整条日志 | agent-session.md · LookupCommit |
| SES-REP-5 | CommitIndex 是段的组成部分；Open 核对摘要，不符则以 commit 重建并写回 | agent-session.md · CommitIndex |
| SES-LIN-1 | 单父不变量：每段至多一条父边，lineage 是森林，读路径只拼接一条前缀 | agent-session.md · 单父不变量 |
| SES-FRK-1 | fork 创建：EdgeAt 读边、Branch 建路径、CreateSession 同事务写入；父存活/Seq 在 history 内/父非子三核对 | agent-session.md · fork 的创建 |
| SES-FRK-2 | fork 读：LoadedPath 装载路径逐段读取；From/Limit/StreamSeq 按拼接序列计数 | agent-session.md · fork 的读 |
| SES-FRK-3 | fork 身份：继承 CommitID 对 Committed/LookupCommit 可见、对 Append 为 ErrConflict | agent-session.md · fork 的身份 |
| SES-FRK-4 | fork 所有权：Lease 是根级，两个根从不共用 tip；子的接管不接管父的执行 | agent-session.md · fork 的所有权与恢复 |
| SES-FRK-5 | 流的语义继承由 domain 模块声明；StreamHead 只计 tip 段自身 | agent-session.md · 流的 lineage |
| SES-GC-1 | Delete 同事务 tombstone 并删除路径区间；SessionID 不复用 | agent-session.md · 删除 |
| SES-GC-2 | 引用与截断：SpanBound 定保留上界；TruncateSegment 持段锁同事务再读最大区间 | agent-session.md · 引用与截断 |
| SES-GC-3 | claim 与回收分工：Delete/Collect 的 Removed/Dropped 报告驱动 writer 释放 claim | agent-session.md · claim 与回收的分工 |
| SES-GC-4 | 图变更串行：kernel 内同一把图锁；跨进程每次 Storage 调用各自成事务，按段 ID 锁序 | agent-session.md · 图变更的串行 |

## agent-session-extension.md

| ID | 一句话规则 | 位置 |
|---|---|---|
| EXT-SCP-1 | 一个 Session 在进程内恰有一个 Writer，持有 kernel Handle；全部写入经 Writer.Commit | agent-session-extension.md · 一个 Session 恰有一个 Writer |
| EXT-SCP-2 | 模块集合启动时经 BuildRegistry 传入，运行期不变；本层不 import 任何模块包 | agent-session-extension.md · 模块集合在启动时固定 |
| EXT-SCP-3 | 模块间依赖单向、固定，以 Requires 声明并由 Registry 校验 | agent-session-extension.md · 模块间依赖单向、固定 |
| EXT-SCP-4 | writer 依赖 extension、反向不得；声明与 Registry 同居一层（互引用才不成环） | agent-session-extension.md · 两个包平级 |
| EXT-REG-1 | EventType 为 <Source>/<ModuleID>/<local-name>；同一 Registry 内 (Source,ID)、EventType、ProjectionID 唯一 | agent-session-extension.md · EventType 与模块身份 |
| EXT-REG-2 | payload 版本归事件类型：Encode 写 v=Version，Decode 按 v 选 codec 并 upcast；旧 codec 永久保留 | agent-session-extension.md · payload 版本归事件类型 |
| EXT-REG-3 | 未注册 EventType 或未注册的 v 解码为 Unknown 并保留 raw payload | agent-session-extension.md · Unknown 事件 |
| EXT-REG-4 | Requires 构建校验：被依赖已注册、无环、事件归属正确、投影消费限于本模块与 Requires | agent-session-extension.md · Requires 的构建校验 |
| EXT-COD-1 | codec、Validate、Binding extraction 必须纯、确定、无 IO；round-trip 是模块的测试义务 | agent-session-extension.md · codec 必须纯且 round-trip |
| EXT-COD-2 | 已提交事件的 payload 保持原始 canonical bytes；v 由 Registry 加入与取出 | agent-session-extension.md · payload 字节不变 |
| EXT-STR-1 | 流 domain 由模块声明、Registry 内唯一；Writer encode 时按声明校验每个 batch | agent-session-extension.md · 流 domain 由模块声明 |
| EXT-REF-1 | Extractor 随 EventDefinition 声明、保留 appearance order；无注册表、无路径式提取 | agent-session-extension.md · 提取器随事件声明 |
| EXT-REF-2 | admission 违反拒绝整个 group 不作写入；含引用而 Bindings/Ledger 为 nil 是配置错误，返回 error 而非 CommitInvalid | agent-session-extension.md · admission 拒绝整个 group；配置错误返回 error |
| EXT-WRT-1 | OpenWriter 重建投影与 claim 后，Commit 在互斥区执行；fn 为纯函数、只经 View 读 | agent-session-extension.md · OpenWriter 与互斥区 |
| EXT-WRT-2 | CommitID 已提交则返回 AlreadyApplied 与原 commit，不重建事件、不做 admission 与 claim | agent-session-extension.md · 幂等重放 |
| EXT-WRT-3 | claim 在 Append 前 Activate；写入前拒绝尽力 Release；结果未知或冲突时保持 Active 至重开核对 | agent-session-extension.md · claim 先于 Append |
| EXT-WRT-4 | ErrOwnershipLost 或结果未知的 Append 错误使 Writer 失效；验证拒绝与写入前 ctx 错误不致失效 | agent-session-extension.md · Writer 的失效 |
| EXT-WRT-5 | commit claim 以持有它的段为 owner；Activate 幂等复用，Released 派生后继链重试 | agent-session-extension.md · commit claim 的派生与核对 |
| EXT-WRT-6 | Writers 保证每 Session 一个 Writer；OwnershipLost 后 sticky 至 CloseWriter；未知失效后自动重开 | agent-session-extension.md · Writers 负责 Writer 的唯一性 |
| EXT-WRT-7 | Observers 在 CommitApplied 后按提交全序、互斥区外通知；panic 捕获；是观察的唯一源头 | agent-session-extension.md · 提交观察 |
| EXT-WRT-8 | writer.Fork 不建立 claim、不解码父前缀；前缀内容由父段 commit claim 保留 | agent-session-extension.md · fork 不建立 claim |
| EXT-WRT-9 | writer.Delete/Collect 按 CollectReport 释放 claim；宿主必须先关闭该 Session 的 Writer | agent-session-extension.md · 删除与回收时释放 claim |
| EXT-WRT-11 | LeaseDuration 非零时启动心跳，每 LeaseDuration/3 调 Renew；Renew 失败按拍重试 | agent-session-extension.md · 心跳 |
| EXT-PRJ-1 | Initial、Apply、StateCodec 必须 pure；fold 以 commit 为单位，不发布部分状态 | agent-session-extension.md · fold 以 commit 为单位 |
| EXT-PRJ-2 | 投影只折 Consumes；范围内 Unknown 按 Ignorable 跳过或失败；范围外一律跳过 | agent-session-extension.md · Consumes 之外的事件按归属处置 |
| EXT-PRJ-3 | 缓存复用条件是 commit 对齐且不越过继承边界；不符即从 Initial 重折，缺失不产生错误 | agent-session-extension.md · 缓存条目的复用条件 |
| EXT-PRJ-4 | Writer 内存投影与 Store reader 两条路径对同一 head 交付相同状态与独立副本 | agent-session-extension.md · 两条折叠路径交付相同输入 |
| EXT-PRJ-5 | OpenWriter 两步重建：先判定各投影条目起点，再从最小起点读一次日志续折 | agent-session-extension.md · OpenWriter 的两步重建 |
| EXT-PRJ-6 | CachePolicy 只约束写入；读取不论条目谁写；Exclude 把投影交还宿主 | agent-session-extension.md · 写入与读取的权限不对称 |
| EXT-PRJ-7 | Save 尽力而为、在互斥区外刷新；CacheEvery(n) 给出落后上界；Save 单调 | agent-session-extension.md · 缓存写入尽力而为 |
| EXT-PRJ-8 | Inherits 谓词决定从继承前缀折叠哪些流；nil 按各 domain 声明的 lineage | agent-session-extension.md · 继承策略 |
| EXT-PRJ-9 | authoritative 投影 fold 失败使 commit invalid；derived 失败标记不健康但不阻止落盘 | agent-session-extension.md · authoritative 与 derived |
| EXT-PRJ-10 | authoritative 投影的缓存条目带 checkpoint digest，读取重算不符即视为不存在 | agent-session-extension.md · authoritative 条目是受校验的 checkpoint |
| EXT-APP-1 | app module 可依赖 first-party 事件；解码后的当前类型即稳定消费面 | agent-session-extension.md · 承诺面 |
| EXT-APP-2 | 投影范围规则双向保护；未注册模块的历史事件保留 payload、不参与折叠 | agent-session-extension.md · 隔离 |
| EXT-APP-3 | 只有"持久、可重放、参与投影"的事实才建 module | agent-session-extension.md · 适用判据 |

## agent-artifact.md

| ID | 一句话规则 | 位置 |
|---|---|---|
| ART-SCP-1 | Core 不解释 ClaimOwner，不要求任何数据库、文件系统或 provider 实现 | agent-artifact.md · Core 不解释 ClaimOwner |
| ART-SCP-2 | Core 范围 = Ref、Binding、capability、两态 RetentionLedger、SchemeDefinition 与 provider registry | agent-artifact.md · Core 的范围 |
| ART-ID-1 | identity 非空、稳定、bytewise UTF-8 比较；Ref 不得含 credential、临时签名 URL 或进程 handle | agent-artifact.md · identity |
| ART-REF-1 | LocatorIdentity=(Scheme, Authority, Key)；MediaType identity-bound 但 untrusted；同 locator 的 size/integrity 必须一致 | agent-artifact.md · locator、MediaType 与 integrity |
| ART-REF-2 | cas 必须带 integrity；ExpiresAt 仅 Ephemeral；durability 顺序 Ephemeral < EventBound < Pinned，promotion 只升不降 | agent-artifact.md · durability 与 promotion |
| ART-WIR-1 | WireVersion 冻结字段、omitted/拒绝策略与 digest preimage；v1 省略 optional empty、拒绝 null 与未知字段 | agent-artifact.md · WireVersion 与 wire codec |
| ART-BND-1 | Binding immutable；BindingDigest 覆盖 separator、BindingID 与完整 RefWireIdentity；同 ID 只重建逐字段相同的 Binding | agent-artifact.md · Binding immutable |
| ART-BND-2 | Resolver 校验 size/integrity/MediaType；Put durable ack 后返回、同 bytes 幂等；promotion 不重写旧 Binding | agent-artifact.md · Resolver 校验、Put 与 promotion 语义 |
| ART-CAP-1 | capability 必须区分 missing/expired/unauthorized/corrupt/transient，并防护 key confusion、path traversal 等 | agent-artifact.md · capability boundary |
| ART-CAP-2 | Scheme=resolution contract、Authority=逻辑 store、Key 由 scheme 解释；cas Key 逐字等于 Integrity | agent-artifact.md · scheme、authority 与 key |
| ART-RET-1 | BindingSetBuilder.Build 是构造 BindingSet 的唯一算法；ledger 独立重算并精确验证传入 set | agent-artifact.md · BindingSet 的唯一构造算法 |
| ART-RET-2 | claim 只接受 EventBound/Pinned；ClaimID 由 owner 与 set 派生；Active 是 GC root，未知 scheme 保守保留 | agent-artifact.md · claim 状态机 |
| ART-RET-3 | ClaimsByOwner 用 watermark cursor 稳定枚举；GC 前经 OwnerVerifier 核对释放孤儿 claim | agent-artifact.md · 枚举、游标与回收前核对 |
| ART-PRO-1 | provider registry 组合后 immutable；Verify 要求 scheme 注册、durability 支持、ValidateRef 通过与 binding 存在 | agent-artifact.md · provider registry 与 verified use |
| ART-ARC-1 | manifest 为精确 canonical wire、只携带 Active claims；inspection 无损接受未知 scheme 但不建 Binding/claim | agent-artifact.md · manifest 与 inspection |
| ART-ARC-2 | ImportActiveClaims 是 all-or-nothing 校验边界，不接受 Prepared/Released，逐字段相同幂等 | agent-artifact.md · verified import 的 all-or-nothing 边界 |
| ART-PRO-2 | adapter 迁移保持 locator resolution 不变，以 generation/fence 防止旧位置提前回收 | agent-artifact.md · adapter 迁移不变量 |

## agent-session-chatlog.md

| ID | 一句话规则 | 位置 |
|---|---|---|
| CHT-SCP-1 | chatlog 拥有对话事实与 surface/context 两投影；声明单例流 domain chatlog（LineageSession）；Requires 为 run 的六类事实 | agent-session-chatlog.md · 模块边界 |
| CHT-LIF-1 | reducer 拒绝 identity mutation、非法迁移、replacement conflict 与重复 ID；失败 Turn 条目全保留进 ContextFold | agent-session-chatlog.md · reducer 拒绝项与失败 Turn 的保留 |
| CHT-ENT-1 | assistant 条目 = ModelStepCompleted 结构投影，AssistantID=StepID，CallIDs 由同 commit ToolStepOpened 按序补入 | agent-session-chatlog.md · assistant 条目 |
| CHT-ENT-2 | tool_result 条目 = call 终态事实投影；unknown 视为未决，supersede 原位替换，每个条目至多一个 replacement | agent-session-chatlog.md · tool_result 条目与 supersede |
| CHT-ENT-3 | Summary 的 Parts 为单层 TextPart 或 ReferencePart | agent-session-chatlog.md · Summary 的 Parts |
| CHT-ENT-4 | 用户侧内容是 Input；input_delivered 挂 TurnID 后进入 Context | agent-session-chatlog.md · 用户侧内容是 Input |
| CHT-COD-1 | Parts 的 wire 是 discriminated union；v 由 Registry 处理，本模块 codec 不读写 | agent-session-chatlog.md · Parts 的 wire |
| CHT-COD-2 | PartsExtractor 提取 summary 引用；tool_result_superseded 的提取器返回 FrozenBindingID(OutputDigest) | agent-session-chatlog.md · Binding 提取器 |
| CHT-COD-3 | 事件 digest domain 与 EventType 相同；条目 digest domain 为 chatlog/assistant 与 chatlog/tool_result，覆盖全部结构字段不含 v | agent-session-chatlog.md · Digest domain |
| CHT-EVT-1 | EventType 清单：input_submitted/delivered/withdrawn/rejected、tool_result_superseded、summary、compaction_created/invalidated | agent-session-chatlog.md · EventType 清单 |
| CHT-EVT-2 | input_submitted 创建 Input，三种终结各一次；input_delivered 与交给 Run 的事实同组 | agent-session-chatlog.md · 输入生命周期事件 |
| CHT-EVT-3 | compaction 压缩 active Context 为 [Summary]+Retained；提交前预折叠校验四项；invalidate 只回退最近的 active compaction | agent-session-chatlog.md · compaction |
| CHT-SUR-1 | SurfaceFold 跨 chatlog 与 run/<RunID> 流折叠；EntryOrder 带 ledger Position；run_ended 释放 Runs 表 | agent-session-chatlog.md · SurfaceFold |
| CHT-CTX-1 | ContextFold 纯函数：chatlog+run 事件 → 经 supersession/compaction 处理的有序条目；不读 ContentStore | agent-session-chatlog.md · ContextFold 的输入输出 |
| CHT-CTX-2 | fold 执行 ID 单次创建、CallIDs 补入、原位 replacement；只含已 delivered 的 Input | agent-session-chatlog.md · ContextFold 的规则 |
| CHT-MAT-1 | materializer 是 IO 边界：每 digest 至多读一次，正文缺失返回 frozen.ErrMissing 不影响投影 | agent-session-chatlog.md · materializer 是 IO 边界 |

## agent-run.md

| ID | 一句话规则 | 位置 |
|---|---|---|
| RUN-SCP-1 | agentcore/run 只定义 Run 的 identity/事实/状态/命令/状态转移；子包分协议层与执行层，协议层不引用执行层 | agent-run.md · 包分层：协议层与执行层 |
| RUN-SCP-2 | Run 是 first-party Module，不知道上层 Turn；Requires 为空，不写其他模块的事件 | agent-run.md · Run 不知道它的上层实体 |
| RUN-WIR-1 | identity 非空稳定；EffectID 派生；start/settlement/recovery CommandID 以 EffectID 为 preimage；settlement 事实自足指认 effect | agent-run.md · effect 身份与 settlement 的指认 |
| RUN-WIR-2 | 事实是 Session event，第一层带 runId 与 v；codec 历史按版本保留并 upcast，已发布 codec 永不删除 | agent-run.md · 事实的 wire 形状与 codec 历史 |
| RUN-WIR-3 | 一个 command 恰产生一组事件（同一 CommitID）；事件无独立 EventID，Seq 即身份 | agent-run.md · command 与事件组一一对应 |
| RUN-WIR-4 | 内容与执行状态分离，fact 只留 digest；正文经信封存 frozen.Store，digest 即 cas Key | agent-run.md · 内容只以 digest 进入 fact |
| RUN-NEW-1 | run_created 是首个事实，初始状态确定；同 RunID 第二条 created 为 Evolve 错误 | agent-run.md · run_created 是首个事实 |
| RUN-NEW-2 | FoldRun 按 Seq 折叠完整事实序列重建状态，是权威读取 | agent-run.md · FoldRun 是权威读取 |
| RUN-MCH-1 | MachineState 是 Progress/Inbox/Effects/End 四维度折叠；Usage/Result 是投影；terminal 吸收未幂等命令 | agent-run.md · MachineState 是四个维度的折叠 |
| RUN-MCH-2 | ToolCallBinding 冻结 call 身份；Decide 校验派生值；未知工具收束为 lookup failure；Unknown outcome 记 ToolCallFailed(Unknown) | agent-run.md · ToolCallBinding 冻结 call 身份 |
| RUN-MCH-3 | Decide 执行全部验证并返回完整有序 fact 组；Evolve 机械折叠；terminal 组 RunEnded 在最后 | agent-run.md · Decide 与 Evolve 的分工 |
| RUN-MCH-4 | Action 不持久化；AcceptInput 任意非终态到达、全有或全无；Waiting call 禁止 Start | agent-run.md · Action 不持久化，输入在任意非终态到达 |
| RUN-CMT-1 | RunStore 按 RunID 寻址、Scope 由绑定决定；无 Create；终态 Run Load 读流重折、Commit 返回 ErrRunTerminal | agent-run.md · RunStore 的寻址与终态读取 |
| RUN-CMT-2 | machine 投影只含非终态 Run；Record 经 FoldRun 权威重建并与投影比对；缓存可丢弃 | agent-run.md · machine 投影 |
| RUN-CMT-3 | Commit 固定步骤：查重放→取状态→校验→hard CAS→Decide once→Evolve→出 batch | agent-run.md · Commit 的固定步骤 |
| RUN-CMT-4 | Prepare 是 hard CAS（Base==Position）；其他 command call-local rebase；replay 判定先于 terminal check | agent-run.md · Prepare 是 hard CAS，其余命令 call-local rebase |
| RUN-CMT-5 | 幂等键 (SessionID, CommitID)；同 CommandID 重放返回 AlreadyApplied 不再 Decide；EffectID 是 start/settlement/recovery CommandID 的 preimage | agent-run.md · 幂等键与重放判定 |
| RUN-CMT-6 | Run 语义提交的 ownership fencing：settlement 必须经 Session Writer，跨进程迟到写入被 Epoch fencing 拒绝 | agent-run.md · Run 语义提交的 ownership 围栏 |
| RUN-CMT-7 | 接管处置：RecoverInterrupted 一次性处置 Executing 目标，Reconciler 比较 Run 与 execution store 给出 keep/defer/redispatch/dispose | agent-run.md · 接管处置 |
| RUN-CMT-8 | Run 六契约是无版本包级单值；identity/digest 预映像冻结；事实形状靠 payload 版本 v 演进 | agent-run.md · Run 协议不设版本 |
| RUN-EXE-1 | Assignment 是 Owner 交给 Executor 的工作单元；AssignmentKey 是两者唯一连接；attempt 只由 Executor 持有 | agent-run.md · Assignment 是交给 Executor 的工作单元 |
| RUN-EXE-2 | Outcome 是 Executor 的唯一回答；不能冻结的结果以 malformed_result 交付；每 Assignment 至多一个 authoritative Outcome | agent-run.md · Outcome 是 Executor 的唯一回答 |
| RUN-EXE-3 | Dispatch 错误三分类（确定拒绝/Retryable/Unknown）；接受时 Prepare→持久化→Start；Attach 按 record 给 missing/active/orphaned/terminal | agent-run.md · Dispatch 与 Attach |
| RUN-EXE-4 | Loop.Deliver 以 Outcome.Key 定位 Executing 目标并提交结算；找不到目标丢弃不写事实 | agent-run.md · Outcome 的结算 |
| RUN-EXE-5 | start barrier 前经 Executor.Validate 校验工具/模型 Assignment；失败走 DeclineToolCall 或 ErrModelUnavailable，不产生外部效果 | agent-run.md · start barrier 之前的校验 |
| RUN-EXE-6 | 恢复语义归 Executor（RecoverExecution/Dispose 两原语），触发归观察到 orphaned 的一方；Worker 不跑扫描循环 | agent-run.md · 恢复原语与恢复触发 |
| RUN-EXE-7 | Dispatch 必须携带内联 payload；Attach/GetOutcome/Cancel 只按 key 定位；digest-only Assignment 仅限 Owner 内部重建 | agent-run.md · Assignment payload 必须内联 |
| RUN-EXE-8 | v1 假设同构 worker 池与 loopback 信任域；不规定推送通道；colocated 也经 Worker 与 record store | agent-run.md · 部署说明 |
| RUN-EXE-9 | ExecutionRef 是 attempt 的物理绑定；Prepare 幂等确定、Restart 分配下一代；Replay 裁决归 Worker | agent-run.md · ExecutionRef 与 Restart |
| RUN-EXE-10 | backend 选择在 Dispatch 时经 Route 表评估一次并以 execution_bound 持久化；ledger 是 execution identity 唯一来源 | agent-run.md · backend 选择与 ledger 的权威性 |
| RUN-EXE-11 | Replay（工具级能力）与重试（失败级属性）分离；RetryAllowed 表示无不可重复外部效果；Worker 按 RetryBudget 经 Restart 重发 | agent-run.md · 失败分类与重试 |
| RUN-EXE-12 | 进度帧是临时观察：内存环形缓冲、reset/end 帧、Generation 递增；帧不是事实 | agent-run.md · 进度帧 |
| RUN-EXE-13 | 回收由 Owner 确认驱动（Acknowledge）；确认后 record 对 Attach 为 terminal、GetOutcome 为 ErrOutcomeCollected；record 不删除 | agent-run.md · 结算确认 |
| RUN-EXE-14 | Execution Ledger 是第二个 event-sourced authority；Seq/CommitID/写前折叠三规则与 Session 相同；租约行是 fence authority | agent-run.md · Execution Ledger |
| RUN-EXE-15 | 重派由 dispatch ledger 记录 planned/dispatched/given_up；先记后派；RedispatchMissing 要求端口与 ledger 同时装配 | agent-run.md · 重派与 dispatch ledger |
| RUN-EXE-16 | Abort tombstone：execution_accepted 与 execution_aborted 争 Seq 0；aborted 的 key Dispatch/GetOutcome 拒绝，Acknowledge/Recover/Cancel 无操作 | agent-run.md · Abort tombstone：同一 EffectID 的接受与关闭互斥 |
| RUN-EXE-17 | GetOutcome（读取）与 Settlements（通知）分离；通知不携带 Outcome、丢失由读取兜底；全部通知源共用一个 notice.Ring | agent-run.md · 读取与通知分离 |
| RUN-LOP-1 | Settings 来自 AgentPreset；Scheduling 冻结在 ToolStepOpened 上；MalformedRetries 决定畸形结果处置 | agent-run.md · Settings 来自 AgentPreset |
| RUN-LOP-2 | NeedModelRequest 经 Plan/Freeze/验证后提交 Prepare（command 携带本体）；stale 后重新 Load；业务停止用 CancelRun | agent-run.md · NeedModelRequest 的规划与提交 |
| RUN-LOP-3 | StartModelCall 在 Validate 后 Commit start barrier；CommitAccepted/AlreadyApplied 授予执行；Dispatch 拒绝按三类处置 | agent-run.md · StartModelCall 的 start barrier |
| RUN-LOP-4 | Tool call 先 Validate 后逐 call Start+Dispatch；按冻结 Scheduling 分批；panic/Unknown 结算为该 call 的 Unknown | agent-run.md · Tool call 的校验、派发与结算 |
| RUN-LOP-5 | 结算使用独立 control context；ErrOwnershipLost 是终止性错误；业务停止先 CancelRun 再取消 ctx | agent-run.md · 结算使用独立 control context |
| RUN-LOP-6 | EventSink 是 realtime observation，可丢失/重复/断流；committed observation 携带完整组；sink 失败不影响 Commit | agent-run.md · EventSink 是实时观察 |
| RUN-LOP-7 | ModelRef 是冻结请求中的执行身份；同一 Run 生命周期内 ResolveModel 必须解析为等价执行语义 | agent-run.md · ModelRef 是冻结请求中的执行身份 |
| RUN-LOP-8 | WithdrawPrepared 提交 WithdrawPreparedStep 并释放本体；Loop 不为输入做其他事 | agent-run.md · WithdrawPrepared 放弃过期请求 |
| RUN-LOP-9 | TargetResolver 按 effect 在 start barrier 前调用；target 不进 Run 事实，只存在于 Assignment 与 Execution Record | agent-run.md · target 解析按 effect 发生 |
| RUN-LOP-10 | BeforePrepare 在 NeedModelRequest 时、PromptBuilder 读上下文前调用一次；经同一 Writer 改写上下文；不写 Run 事实 | agent-run.md · Prepare 之前的钩子 |
| RUN-CMP-1 | digest 与派生 ID 预映像永久冻结；wire 变化以 payload 版本 v 发布；Evolve 只有一份 | agent-run.md · 兼容性 |
| RUN-CMP-2 | SessionRunStore conformance 只断言 Run 语义；组原子性/所有权/幂等/缓存复用由 SES/EXT conformance 覆盖 | agent-run.md · conformance |
