# Rule Index（过渡文件）

本文档是设计文档去符号化的过渡桥梁。设计文档正文不再使用规则 ID（如 SES-OWN-6），规则由所在文档的章节承载。代码注释中遗留的 ID 引用在本表可查；当代码中不再有任何 ID 引用时，删除本文件。

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
