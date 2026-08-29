# Memoh Durable Run Runtime Adapter 工作单

状态：交接文档（twilight 侧 → Memoh 侧）
对应 spec：`docs/design/agent-runtime-refactor.md` §5、§8.2、§11 阶段 C
依赖：twilight 模块 `github.com/memohai/twilight`（PR #47 合并后的 tag）

本文是 Memoh 实现 durable `run.Runtime` adapter 的工作单：表结构草案、Commit 事务伪代码、materialization 契约和验收清单。约定：本文中的 SQL 为形态说明，字段类型与索引命名以 Memoh 现有规范为准；`run.` 前缀指 `github.com/memohai/twilight/agent/run`。

## 0. 原则

1. adapter 只实现 `run.Runtime` 的 `Load/Commit` 两个方法；全部语义规则在 `run.EvaluateCommit`（单实现），adapter 不复刻任何准入判断。
2. adapter 不依赖 `agent/session`、`agent/queue` 包。materialization 契约落在 Memoh 现有 history/session 表上；steer/follow-up 复用 Memoh 现有 queue。
3. control-plane（owner/fence/lease/attempt）是 Memoh 私有实现，不进入 `run.MachineState`、不进入事件。
4. 验收门槛是 `run/runtimetest.RunConformance` 全绿；这不是参考项，是合并前置条件。

## 1. 表结构草案

```sql
-- Revision-0 权威记录，创建后 immutable（spec §5.1.1）
CREATE TABLE agent_run_header (
    run_id            TEXT PRIMARY KEY,
    schema_version    SMALLINT NOT NULL,
    header_json       JSONB    NOT NULL,  -- run.RunHeader 的完整 JSON（含 digest 字段）
    causation_id      TEXT,               -- 冗余列，便于按来源查询
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- TransitionRecord log：authority 的日志单元（spec §5.3）
CREATE TABLE agent_run_transition (
    run_id            TEXT     NOT NULL REFERENCES agent_run_header(run_id),
    revision          BIGINT   NOT NULL,
    command_id        TEXT     NOT NULL,
    command_digest    TEXT     NOT NULL,
    record_json       JSONB    NOT NULL,  -- run.TransitionRecord 的完整 JSON
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, revision),
    UNIQUE (run_id, command_id)           -- 兼作幂等查询索引（prior lookup）
);

-- MachineState projection + watermark。projection 可 truncate 后由
-- header + transition log 重建；watermark 不随重建清除（spec §5.1、§8.2）。
CREATE TABLE agent_run_state (
    run_id                  TEXT PRIMARY KEY REFERENCES agent_run_header(run_id),
    revision                BIGINT   NOT NULL,
    watermark               BIGINT   NOT NULL,  -- 每次 Commit 与日志同步推进
    snapshot_schema_version SMALLINT NOT NULL,  -- 与事件 SchemaVersion 独立版本化
    state_json              JSONB    NOT NULL,
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (watermark >= 0)
);

-- Memoh 私有 control-plane：执行占用。不进入 snapshot、不进入事件。
CREATE TABLE agent_run_lease (
    run_id        TEXT   NOT NULL,
    target_key    TEXT   NOT NULL,  -- "model/<stepID>" 或 "call/<stepID>/<callID>"
    grant_token   TEXT   NOT NULL,  -- 不可预测；即 run.ExecutionGrant 的载体
    owner         TEXT   NOT NULL,  -- worker identity
    fence         BIGINT NOT NULL,  -- 单调 fencing token
    expires_at    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (run_id, target_key)
);

-- 可选：recovery scanner 的失效执行记录（system command 的 CommandID 来源）
CREATE TABLE agent_run_recovery (
    run_id       TEXT NOT NULL,
    target_key   TEXT NOT NULL,
    record_ref   TEXT NOT NULL,   -- 稳定引用；同一记录重放复用同一 system CommandID
    expired_at   TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (run_id, target_key, record_ref)
);
```

Memoh 现有表的增量（materialization 契约，见 §4）：

```sql
ALTER TABLE <memoh_history> ADD COLUMN source_event_id TEXT;      -- run_id:revision:index
CREATE UNIQUE INDEX ... ON <memoh_history>(source_event_id) WHERE source_event_id IS NOT NULL;
-- RunFinalized 标记：落在 Memoh 自己的 run/session control 表，加一列或一行皆可
```

## 2. admission（创建 Run）

```text
BEGIN;
  header, _ := run.BuildRunHeader(runID, causationID)   -- causationID 关联 Memoh 的 session/queue 来源
  INSERT agent_run_header(run_id, header_json, ...)
  INSERT agent_run_state(run_id, revision=0, watermark=0, state_json=header.InitialState)
  -- Memoh 自己的 admission 记录（session lifecycle：RunAdmitted）同事务写入
COMMIT;
-- 首个输入不进 header：admission 后由正常 Commit 提交 Revision-1 的 AcceptInput
```

导入/迁移路径（local run 上传）：先 `run.ValidateRunHeader`，再 `run.FoldRun(header, records)` 重建 projection；上传的 snapshot 一律不信任。

## 3. Commit 事务骨架

对应 `run.Runtime.Commit`。全部语义判断在 `run.EvaluateCommit` 内；adapter 只负责锁、查、control-plane 判定和落盘。

```text
Commit(ctx, req CommitRequest) (CommitResult, error):
  BEGIN;
    -- 1. 锁权威行
    state := SELECT ... FROM agent_run_state WHERE run_id = $1 FOR UPDATE
    header 已在内存或按需读取（immutable，可缓存）

    -- 2. 幂等查询：按 (run_id, command_id) 查 prior transition
    prior := SELECT record_json FROM agent_run_transition
             WHERE run_id = $1 AND command_id = req.Command.ID
    priorRecord := run.DecodeTransitionRecord(prior)  -- 存在时

    -- 3. control-plane 判定（Memoh 私有）
    grantValid := lease 表中 target_key 的 grant_token == req.Grant 且未过期
                  且 owner == 本 worker（completion 类）
    recoveryValid := grantless recovery/system command 时，
                     agent_run_recovery 存在匹配的失效记录

    -- 4. 单实现评估
    decision, err := run.EvaluateCommit(state, revision, priorRecord,
                                        req, grantValid, recoveryValid)

    -- 5. 按 DecisionKind 分派
    switch decision.Kind:
      AlreadyApplied:
        ROLLBACK（无写入）; return {Status: AlreadyApplied, Events: prior.Events}
        -- 注意：AlreadyApplied 不重发 grant、不重写任何投影/outbox
      Conflict / Stale / Terminal:
        ROLLBACK; return 对应 sentinel error
      Apply:
        INSERT agent_run_transition(run_id, revision+1, command_id,
                                    command_digest, decision.Transition)
        UPDATE agent_run_state SET revision = revision+1,
               watermark = revision+1, state_json = decision.NewState
        -- start command：同事务建 lease（铸 grant_token、fence+1）
        -- completion/recovery：同事务删对应 lease
        -- terminal：删该 run 全部 lease
        -- materialization outbox（见 §4）：同事务写入
        -- Prepare gate（见 §5）：在此事务内先行检查
  COMMIT;
  return {Status: Accepted, Snapshot, Events: decision.Events, Grant: minted}
```

要点：

- `(run_id, revision)` 主键使 CAS 内建于 INSERT——并发提交同一 revision 的第二个事务在唯一约束上失败，重读后按 EvaluateCommit 的 rebase 规则重试或拒绝。
- `AlreadyApplied` 分支在只读路径完成，绝不重复 outbox/history/计数（spec §5.4）。
- lease 的建立/消费与状态提交同一事务（spec §5.4 step 4 末句）。
- Load 是纯读取：`SELECT state + revision`，不触 lease，不返回 busy。

## 4. materialization 契约（不引入 session 包）

Run 事件 → Memoh 长期语义的投影，走 outbox：

```text
同一 Commit 事务写 outbox 行:
  source_event_id = run_id + ":" + revision + ":" + index   （或其 canonical digest）
  causation_id    = 事件继承的 CausationID
  payload         = 需要投影的事实（ModelStepCompleted / ToolCallCompleted / RunEnded ...）

outbox consumer（幂等）:
  ToolCallCompleted / ModelStepCompleted -> Memoh history 表
      （INSERT ... ON CONFLICT (source_event_id) DO NOTHING）
  RunEnded -> Memoh session lifecycle（RunCompleted 条目）+ 触发 queue 仲裁
  usage    -> Memoh usage/trace 存储，同一 source_event_id 幂等
```

finalization barrier（spec §2.6）：

```text
RunEnded 已投影
  且 history/artifact/usage 的 outbox 均已消费或可幂等恢复
    -> Memoh control 表写 RunFinalized 标记（含 run_id + materialization watermark）
    -> 此后才允许 archive/GC 该 run 的 agent_run_transition 日志
```

ToolStep 关闭时按 ModelResult 原始 Call 顺序写 assistant tool-call/tool-result 历史（spec §8.2 已有要求）；并行 Call 的 transition 提交顺序不影响历史顺序。

## 5. queue 集成（复用 Memoh 现有 queue）

两处，均不改 queue 本身：

1. **boundary 消费**：queue-safe boundary（ModelStep 无 tool calls 完成 / ToolStep 关闭，从 `RunEnded`/`ToolStepClosed` 事件得知）仲裁 queue，选中 item 转成 `run.AgentInput{InputID, payload}`，用 `run.DeriveInputCommandID` 提交 `AcceptInput`；queue claim 标记 applied 与该 Commit 同事务（或经幂等 outbox）。
2. **Prepare gate**（spec §10.2）：处理 `PrepareModelRequest` 的事务内检查是否存在 eligible steer item；存在则不接受该次 Prepare，先应用对应 `AcceptInput`（revision 前移，Prepare 因 hard-CAS 返回 stale），Loop 重新 Load 后由 `PlanningHint.Inputs` 带着输入重新规划。

## 6. recovery scanner

lease 过期后（含 backend-loss grace）：

```text
扫描 agent_run_lease 中 expires_at 过期的行:
  写 agent_run_recovery 记录（record_ref 稳定）
  target 是 model step -> 提交 RecoverModelExecution
  target 是 tool call  -> 检查该 call 是否已有结果 transition；
                          无结果 -> 提交 SubmitToolFailure{Outcome: Unknown}
  两者都用 run.DeriveSystemCommandID(runID, stepID, callID, record_ref)，
  经正常 Commit + EvaluateCommit(recoveryValid=true) 提交；
  同一 recovery 记录重放复用同一 CommandID（幂等）
```

scanner 不直接改写任何状态行。

## 7. 验收清单

| # | 项 | 判据 |
|---|---|---|
| 1 | conformance | `runtimetest.RunConformance(t, memohFactory)` 11 项全绿 |
| 2 | 幂等横切 | 同一 CommandID 重放：不重复 history 行、不重复 outbox、不重发 grant |
| 3 | 并发 CAS | 两 worker 并发提交同 revision：一个成功、一个经 rebase 或 stale |
| 4 | crash 恢复 | start 后 kill worker：lease 过期 → scanner 收束（model→Prepared 重试；tool→Unknown 终止）|
| 5 | rebuild | truncate agent_run_state 后从 header+log 重建，watermark 不变；尾部截断日志 → `ErrLogTruncated` halt |
| 6 | Prepare gate | eligible steer 存在时 Prepare 被拒、AcceptInput 先行、重新规划带输入 |
| 7 | materialization | RunEnded 后 history/usage 恰好一次；RunFinalized 前日志不可 GC |
| 8 | 迟到结果 | Run terminal 后的完成提交返回 terminal，结果进审计视图（产品可见）|
| 9 | 集成矩阵 | spec §14.4 全部条目 |

## 8. 非目标

- 不实现 `agent/session`、`agent/queue` 包（spec §2.4/§2.5 实施状态）。
- 不做工具效果分级（spec §17：Unknown 只覆盖崩溃与 lease 失效；计划内停机用排空）。
- 不在线转换旧 deferred/approval 记录（排空或 `runtime_upgrade_required` 终态审计，spec §11 兼容原则 5）。
