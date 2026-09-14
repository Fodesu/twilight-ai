# Test Cloud Agent

状态：重构前设计草案，供确认与实施。本文定义 `cmd/twilight-agent` 的目标应用；当前目录中的 CLI/HTTP 原型作为待替换实现。协议语义沿用 [Host](agent-host.md)、[Run](agent-run.md)、[Turn](agent-turn.md) 与 [Session](agent-session.md)。

## 1. 交付目标

交付一个单用户、长期在线的远程目录问答 Agent。用户从终端提交问题，worker 上的模型通过工具阅读指定目录，完成多步推理，回复保存在 Session。客户端重新连接后可以读取原请求的进度、历史与结果。

首个部署固定为一个 authority 进程、一个 worker 进程、一个 Session、一个 AgentPreset。Session 一次接受一个未完成输入。这个应用级 admission 约束使每个 InputID 对应一个 Turn，工具执行沿用 preset 的并行策略。

验收包含两条路径：真实 provider 完成目录问答；确定性测试模型验证进程退出与恢复。实现复用现有 core 协议、Host、Worker 和 HTTP executor。

## 2. 进程与代码边界

```text
chat                        serve                         worker
HTTP 客户端                 Session authority             模型与工具执行
    |                           |                              |
    +-- 提交 / 查询 ----------->+                              |
    |                           +-- Assignment -------------->+
    +<-- 历史 / 状态 / 回复 -----+<-- Outcome / Attach ---------+
                                |                              |
                         Session log + CAS              Execution records
```

| 进程 | 拥有的工作 | 生命周期 |
| --- | --- | --- |
| `serve` | 配置与 preset、Host、输入接收、后台驱动、恢复、HTTP 查询 | 服务进程 |
| `worker` | provider 客户端、工具、LocalExecutor、Worker、执行记录 | 独立 worker 进程 |
| `chat` | 稳定请求 ID、提交、轮询、终端显示 | 客户端连接 |

authority 的组合直接使用现有 `host.New`。应用需要 `SubmitInput`、`Writers`、`Coordinator` 和投影读取，这些能力已经由 Host 提供。worker 组合 `host.NewCatalog`、`host.NewLocalExecutor`、`executor.NewWorker` 与 `executor/http.Server`。

代码按具体应用职责组织：

```text
cmd/twilight-agent/main.go      参数解析、子命令分发、退出码
internal/cloudagent/
  config.go                   参数校验、持久配置、目录锁
  server.go                   authority 组装、HTTP、服务生命周期
  service.go                  admission、后台驱动与恢复调度
  views.go                    请求状态、历史、回复的读取
  worker.go                   provider、工具与 Worker 组装
  tools.go                    固定工具定义及实现
  client.go                   HTTP 客户端与终端交互
```

这些文件属于同一个具体应用包。业务状态由 Session 投影读取；进程内保存 admission mutex、唤醒 channel、连接健康信息和 goroutine 生命周期。测试通过现有 core 端口注入模型、工具与传输。

## 3. 用户操作

以下命令展示目标入口，实际参数随实现一并验证：

```sh
twilight-agent worker --root /srv/twilight/worker --workspace /srv/agent-docs --model MODEL_ID --listen 127.0.0.1:8089
twilight-agent serve --root /srv/twilight/authority --session default --model MODEL_ID --executor http://127.0.0.1:8089 --listen 127.0.0.1:8088
twilight-agent chat --server http://127.0.0.1:8088
twilight-agent chat --server http://127.0.0.1:8088 --input-id request-123 --text "总结这个目录中的项目"
twilight-agent chat --server http://127.0.0.1:8088 --input-id request-123
```

worker 从 `TWILIGHT_API_KEY` 读取 provider 凭据，`--base-url` 配置 OpenAI-compatible endpoint。authority 保存公开的模型引用、system prompt 与工具定义。离线验收通过 `--mock` 选择测试模型，authority 与 worker 使用匹配的模型引用。

交互客户端启动后读取已有历史和当前请求。每次发送前生成并显示 InputID，同一次发送的网络重试复用该 ID 和正文。指定 `--input-id` 且省略正文时进入已有请求的观察流程。交互命令提供 `/status`、`/stop`、`/quit`；单次模式在完成、失败、停止或需要恢复操作时返回明确退出码。

关闭客户端结束观察连接。服务端继续处理已经接受的输入。

## 4. 配置与本地存储

authority 与 worker 使用不同的绝对数据目录，目录权限为当前运行用户私有。首个部署支持 Linux/macOS 本地文件系统，各进程在打开存储前获取该目录的 OS 排他文件锁，并持有到进程退出。持有目录锁的 authority 以 `Ownership.Takeover` 打开 Session，接替已退出的旧进程。

| 目录 | 持久内容 |
| --- | --- |
| authority root | 应用配置、完整 AgentPreset 与 PresetRef、Session log、冻结请求 CAS |
| worker root | 公开执行配置、执行记录与已完成 Outcome |
| workspace | 操作员明确提供给 Agent 阅读的资料 |

应用配置采用 JSON，首次初始化通过临时文件、同步和原子替换保存，再开放服务。后续启动从已保存配置重建同一 Session、preset 与模型/工具引用，并校验摘要。启动参数与保存的语义配置冲突时返回 `configuration_conflict`。worker 同样校验模型引用、provider endpoint、工具定义与 workspace 路径；凭据通过环境更新。

恢复时完整 PresetRef 必须能解析。首次应用重构使用新数据目录，已有目录保留原状供旧入口读取与审计。后续重启使用这个应用创建的同一目录与配置。

HTTP 两端绑定 loopback，远程访问通过 SSH 转发；同机部署直接使用 loopback。worker 以受限用户运行，workspace 使用专门准备的资料目录，数据目录与凭据位于 workspace 之外。

## 5. 输入接收与幂等

主写接口为 `PUT /inputs/{inputId}`，正文为 `{"text":"..."}`。InputID 在当前 Session 内唯一；正文按接受时的原始文本比较。

一个应用级 mutex 串行化以下 admission 流程：

1. 从 Chatlog 查找 InputID。已有 ID 且正文相同，返回当前请求；正文不同，返回 `409 input_conflict`。
2. 新 ID 检查 submitted backlog、active Turn 和尚待结算的 failed Turn。存在任一项时返回 `409 session_busy`，响应给出当前请求身份。
3. 使用 `Host.SubmitInput` 持久化输入，成功后通知后台驱动，返回 `202` 和 InputID。

精确重放优先于 busy 检查，因此原请求在执行中仍可安全重发。首次持久接受后的响应表示输入已经进入 Session；TurnID 在投递后可查询。

应用先读取已提交输入再决定是否调用 `SubmitInput`。现有 helper 每次生成提交时间，应用的 HTTP 重放路径复用已保存事实。提交响应未知时关闭新输入 admission 并暂停调度，通过 `Host.Close` 停止 recovery listener、关闭并清除缓存 Writer，然后重新 OpenSession，按原 InputID 回读。恢复期间查询失败返回 `503 state_unavailable`。确认存储中尚无该 ID 后才允许以原 ID 重试提交。

唤醒 channel 只提示后台重新检查 Session。它可以合并通知，进程重启后通过 submitted backlog 找回已接受工作。

## 6. 后台驱动与恢复

`serve` 持有一个后台调度 goroutine，按 RunID 管理独立的阻塞驱动 goroutine。HTTP handler 负责提交与查询；模型、工具和 Host 驱动使用服务持有的工作 context。调度在启动驱动后继续处理通知与控制请求，驱动完成通过 channel 回报。

启动顺序：获取目录锁、加载配置、构建 Host，启动查询 HTTP 与后台调度。调度通过 OpenSession 完成 Session 打开与 recovery Attach，成功后开放新输入 admission。存储可读而 executor 暂时不可达时，查询接口报告服务恢复中，调度退避重试；Session 尚未可读时返回 `503 state_unavailable`。

调度在启动、输入提交、提交观察通知和连接恢复时检查权威状态：

| 当前事实 | 应用动作 |
| --- | --- |
| active Turn 有可推进工作 | 独立 goroutine 调用 `Host.Drive`，驱动到下一个静止点 |
| Executing 需要重新连接 | `Host.Open` 安装已有执行的 recovery listener |
| `TurnAttemptFailed` | `Coordinator.Settle` 保存失败结算，保留 RunEnd 详情 |
| 空闲且存在一个 submitted 输入 | 独立 goroutine 调用 `Session.Drain`，投递并驱动 |
| 既无 active Turn 也无 submitted 输入 | 等待下一次唤醒 |

网络故障后重新读取 Outcome 或重新 Attach 原 Assignment。调度对连接恢复采用有上限的退避，已有 recovery listener 继续承担结果读取；重新 Open 由明确的连接失败/监听失效触发。成功重连后恢复同一 Run/Step/Call/Claim。

authority 为每个 executor HTTP 请求配置有界时限，包含 Outcome 长轮询与 Cancel。单次读取超时触发重新读取/Attach，工作 context 保持有效。已停止 Run 的阻塞驱动随网络请求结束退出，其收尾与后继 Run 的调度独立。

后台定期检查提交 head 与调度状态，使丢失的内存通知能够收敛。应用 goroutine 的单次驱动和 Host 已有的 Run owner guard 共同协调主动驱动与 recovery callback。驱动回报附带 RunID；已终态 Run 的迟到回报只完成自身清理。

failed Turn 的结算完成后允许新 InputID。用户再次提出问题会创建新的 Turn。存储错误保留已提交事实并暴露基础设施错误；模型与工具的业务失败沿用 core 的失败语义。

启动检查也覆盖“输入已持久、Turn 尚未创建”和“Run 已失败、Turn 尚未结算”的窗口。检测到超出单输入约束的旧 backlog 时返回 `incompatible_application_state`，保留原存储。

## 7. 查询、回复与停止

| HTTP 入口 | 作用 |
| --- | --- |
| `GET /health` | 进程存活、存储可用、executor 连通性、admission 状态及当前 InputID |
| `PUT /inputs/{id}` | 持久接受输入或重放查询 |
| `GET /inputs/{id}` | 请求身份、已提交进展、所属 Turn/Run、最终回复、错误 |
| `GET /history?after=SEQ` | 按提交顺序读取用户消息、assistant 与工具结果 |
| `GET /events?from=SEQ` | 分页读取 canonical Session events，供恢复验证与诊断 |
| `POST /turns/{id}/stop` | 操作员明确停止指定 Turn |

请求查询返回 `inputId`、`turnId`、`runId`、`status`、`reply`、`error` 与 `observedThrough`。投递前 Turn/Run 为空。状态由 Input 与 Turn 投影导出：`accepted`、`running`、`completed`、`failed`、`stopped`。Run 的 waiting/recovery 详情单独返回。

executor 的当前连接状态放在独立的 `execution` 字段，包含 `observedAt`、`unavailable` 或 `recovery_required` 等应用层观察值。`recovery_required` 表示 control plane 尚需采取恢复、对账或处置动作，不是 Executor 的 `AttachmentState`，也不等同于 Run/Turn 的终态。持久 Run 状态与当前连通性各自保留来源。读取失败返回结构化错误；确认 InputID 缺失时返回 404。

组合投影读取使用同一 committed head。可复用 Writer 的只读 View，或校验各投影返回的 Head 一致后组装响应。最终回复按该 Input 的 TurnID 读取最后一条 assistant 文本；历史保留工具调用及结果。`completed` 响应包含同一提交视图中的回复，空文本结果允许成立。

事件读取的 `from` 为包含式 Seq，默认从首行开始；响应包含 `events`、`next`、`head`、`hasMore`。分页沿用 Store 的完整 commit group 边界。客户端按 Seq 去重并持久回读；历史与最终结果在重新连接后仍然可得。

错误统一为 `{"error":{"code":"...","message":"..."}}`。日志附带 SessionID、InputID、TurnID、RunID 和适用的执行 key，provider 凭据从日志中脱敏。

停止请求先取得已知执行 key，调用 `Coordinator.Stop` 提交停止，再取消应用启动的该 Run 驱动、向已知执行发送有界时限的取消请求。响应区分已提交的 Turn 停止与外部取消的观察结果，外部取消未确认时明确返回该状态。终态 Turn 的重复停止返回已有终态。

Host 安装的 recovery listener 由 Host 生命周期管理。当前应用只有一个 Session；停止提交后，调度通过 `Host.Open` 替换这组监听，按当前未完成执行安装后续监听，随后开放新输入 admission。旧 Run 的本地驱动独立收尾，迟到结果按 core 提交规则处理。

## 8. 模型与工具

真实模式使用现有 OpenAI-compatible provider。authority 冻结模型请求，worker 执行一次 provider 调用，Outcome 经既有协议返回。

首批固定工具用于远程资料问答：

| 工具 | 输入 | 输出 |
| --- | --- | --- |
| `list_files` | workspace 相对目录、分页位置 | 排序后的条目、类型、下一页位置 |
| `read_file` | workspace 相对路径、偏移、长度 | 文本片段、实际偏移、是否截断 |

目录读取每页至多 200 项，单次文件读取至多 64 KiB。文件访问使用 Go `os.Root` 限定在配置目录内，校验相对路径与文件类型；文本读取支持普通文件，二进制文件返回结构化工具错误。分页顺序稳定于一次目录观察，后续调用读取当时的文件内容。

这些只读工具采用现有 `DirectExecution`，定义与参数校验由同一应用代码提供。authority 注册公开定义，worker 注册实现。工具读取错误成为带原 CallID 的结果进入下一次模型请求。

确定性验收模型能够产生两个并行工具调用，其中一个调用可由测试门闩延迟。测试 fixture 记录实际调用次数，验证已完成调用与仍在 worker 中执行的调用各自恢复。真实工具保持自然执行行为。

## 9. 进程退出的合同

| 场景 | 保存的事实与后续行为 |
| --- | --- |
| chat 断开 | accepted 输入继续执行；重新连接按 InputID 读取 |
| authority 在输入提交后退出 | 启动扫描 submitted 输入，继续投递 |
| authority 在工具执行中被终止，worker 存活 | 新 authority Attach 原 Assignment，等待同一次执行的 Outcome |
| worker 已保存 Outcome 后重启 | 从 execution store 返回原结果 |
| worker 在普通模型/工具调用中被终止 | Executor 观察为 `orphaned`，Recovery disposition 为 `deferred`；查询暴露 `recovery_required`，操作员必须先显式 reconcile/takeover/dispose，或停止该 Turn 后发起新请求 |
| worker 暂时不可达 | 保留当前 Run，连接恢复后重新 Attach/读取 |

服务的信号 context 与工作 context 分开管理。`serve` 收到第一次终止信号后进入 draining：停止新输入 admission，查询服务与现有工作继续，等待已接受请求结算。进入需人工处理的等待状态时输出请求身份并保持可查询。操作员可显式停止 Turn；第二次终止信号触发进程强制退出，后续按上表的 crash 路径恢复。

`worker` 的有序停机先要求 authority 完成 draining，再关闭执行服务。强制终止 worker 的验收覆盖 orphaned 分支。

Session 与 Execution records 保留已提交事实。恢复同一 Assignment 的输入与已保存结果具有稳定身份；尚未保存的 provider/tool 计算受实际执行环境的存活与恢复能力约束。上述合同以进程崩溃和进程重启为验证范围。

## 10. 验收与实施顺序

| 验收 | 通过条件 |
| --- | --- |
| 真实目录问答 | 实际 provider 使用工具读取 fixture 目录，回复与工具结果写入 Session |
| 输入幂等 | 丢失 PUT 响应后同 ID 重发，仅有一次 input 提交和一个 Turn；不同正文返回冲突 |
| 并发 admission | 两个新 ID 同时提交，接受一个；另一个明确 busy；原 ID 重放仍可用 |
| 客户端退出 | chat 退出后任务完成，重新启动 chat 能读取原回复 |
| 提交窗口恢复 | 在 input commit 后、Turn 创建前强制退出，重启后该输入完成 |
| authority 崩溃 | 真实子进程 SIGKILL 后重开；worker 存活；Run/Step/Call/Claim 相同，测试工具调用一次 |
| 并行工具恢复 | A 已完成、B 在 worker 等待时 authority 崩溃；重启后保留 A 并接回 B |
| Outcome 恢复 | worker 已持久化结果后退出，重启读取原结果，工具调用计数保持不变 |
| worker 崩溃 | 若执行记录存在但无法关联 backend，观察为 `orphaned → deferred` 并显示 `recovery_required`；若记录缺失才自动进入 `missing` 处置。停止请求可以结算 Turn，随后提交新任务 |
| 短暂断网 | 恢复连接后原请求继续，事件序列与回复可完整回读 |
| 失败收尾 | attempt_failed 被结算并保存错误，新的输入可被接受 |
| 服务管理 | draining、目录排他锁、配置冲突、存储不可用均有确定响应 |
| 文件范围 | 路径越界、越界符号链接、特殊文件被拒绝，输出遵守大小上限 |

实施按以下顺序推进，每一步保留可运行测试：

1. 建立具体应用包、持久配置、三个子命令与目录锁，整理 worker 组装。
2. 实现稳定 InputID admission、单个后台调度、持久查询与停止流程。
3. 接入只读目录工具与 HTTP chat，完成真实 provider 问答。
4. 迁移现有 listener-free executor 测试，补充真实子进程 crash/restart 测试与运行说明。
5. 删除被替换的旧入口和重复组装，运行相关 race 测试与仓库编译检查。

当前已有的 `remote_test.go` 覆盖 HTTP 协议下的断连、authority 重开和持久 Outcome 回读。真实子进程故障、完整输入生命周期和真实 provider 问答列入本次重构的新增验收，完成后记录实际运行结果。
