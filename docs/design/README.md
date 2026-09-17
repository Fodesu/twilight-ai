# Agent Core 设计文档

这些文档共同描述 Twilight Agent Core 的 v1 架构。它们不是产品 API 文档，也不把
某个具体部署（CLI、HTTP、数据库或 provider）提升为 Core 协议。

## 权威边界

```text
agent-session.md              Session kernel：stream、ownership、append、read
agent-session-extension.md    Writer、module registry、projection、claim admission
agent-artifact.md             Artifact binding、content reference、retention claim
agent-session-chatlog.md      对话内容与 context projection
agent-run.md                  Run machine、Runtime、Loop、Executor contract
agent-turn.md                 Turn、attempt、input routing、结算投影
agent-decision.md             PromptBuilder 与决策组件目录
agent-runtime.md              authority 组装与 Session 所有权、driver、spawn、app 策略
agent-run-exe-revision.md     RUN-EXE 修订稿（未实现）：ExecutionRef、Backend 契约、backend 一次选择
agent-workspace.md            可选 Workspace/Runtime/TargetRef domain
```

依赖方向是：

```text
agent/session (kernel)       agent/artifact (independent core)
          \                  /
           agent/session/extension
                    ↓
        chatlog / session-run / turn
                    ↓
             decision / host
                    ↓
       application / transport / provider adapter
```

`agent/run` 与 `agent/turn` 的协议核心保持独立；它们的 Session adapter 才依赖
Module Framework。Workspace 同样是可选 application domain，不是 Core 的依赖。

Workspace 是可选的 application domain。Agent Core 只携带 opaque `TargetRef`，不
解释 Workspace、Runtime 或 provider 的生命周期。

## 术语规则

- `RunStatus`、`TurnStatus`、`ExecutionStatus`、`AttachmentState` 属于不同状态域，
  不互换枚举。
- `AttachmentState=orphaned` 是 Executor 的观察结果；它在 recovery control
  plane 中映射为 `RecoveryDisposition=deferred`。
- `recovery_required` 是应用/API view，表示仍需控制面动作，不表示执行结果为
  `Unknown`。
- Artifact 的“孤儿 claim”只表示 retention claim 没有对应 owner fact，与
  Executor 的 `orphaned` execution 无关。
- Event stream 是唯一事实权威；projection、snapshot、HTTP view 和 CLI 输出都
  是派生数据。

## 阅读与维护规则

1. 协议不变量只在拥有该概念的文档中定义一次；其他文档只引用条款。
2. 应用文档只描述组装、输入 admission、transport 和验证场景，不复制 Core 的
   state machine 或 wire contract。
3. 每个公共名称必须同时与 Go API、测试和文档一致；更名时更新所有三者。
4. 每个恢复场景必须注明：故障窗口、保持的 identity、允许的新事实、禁止的
   side effect 和验证 oracle。
5. 未实现的部署能力不得写成已经存在的 Core 能力；应放在 reference application
   或 adapter 的实施计划中。

`test-cloud-agent.md` 是 reference application 的 E2E 规范。进程级 harness 与演示
入口已于 2026-09-17 移除，待 authority / app 分层稳定后在 app 层重建；harness 验证上述
合同，但不重新定义合同。
