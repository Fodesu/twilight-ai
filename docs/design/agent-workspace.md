# Workspace / Runtime boundary

状态：v1 设计规范。Workspace 是 Agent Core 之外的可选 application domain；本文只定义
它如何通过 opaque target 接入执行。

Workspace is an optional application domain for logical work environments.

- **Workspace** is a durable logical mutable-world identity. Its identity is
  stable while the physical runtime may be replaced.
- **Checkpoint** is an immutable durable state anchor. A checkpoint may be
  restored or used as the source of a fork. Its `StateRef` identifies durable
  provider state independently of the source environment's lifetime.
- **Runtime** is a physical materialization of a Workspace. Local processes,
  containers, VMs, and cloud sandboxes implement the same provider-neutral
  lifecycle (`Create`, `Restore`, `Attach`).
- **RuntimeBinding** belongs to the Workspace domain and records the provider,
  environment reference, and generation of the current materialization.
- **ExecutionBinding** belongs to one Effect Assignment and records only the
  provider-side execution reference.

`Provider.Attach(EnvironmentRef)` adopts an existing environment.
`Provider.Restore(RestoreSpec{State, Destination})` materializes checkpoint state
in a destination described by `Spec`. The destination may retain the Workspace
identity for recovery or use a new identity for a fork. Checkpoint production
and durable-state retention are owned by the provider/application adapter.

Agent Core carries an opaque `run.TargetRef{Kind, ID}` on an Assignment. An
application-provided `loop.TargetResolver` supplies the target for a Run; its
mapping must be durable if a Run can outlive the process that started it.

The provider adapter resolves the target to a RuntimeBinding and creates or
adopts an ExecutionBinding before dispatch. The durable execution record keeps
that execution binding before the external dispatch crosses its uncertainty
barrier. Attach, status, outcome, and cancel use the same binding, preventing a
Worker takeover from accidentally addressing a newly created provider job.

`AgentPreset` is decision identity (model, tools, prompt policy, and scheduling);
the same preset can be
used against different logical workspaces.
