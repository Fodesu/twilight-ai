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
