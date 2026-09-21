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
- **ExecutionRef** belongs to one execution record of the Executor and records
  only the provider and its opaque execution handle (RUN-EXE-9).

`Provider.Attach(EnvironmentRef)` adopts an existing environment.
`Provider.Restore(RestoreSpec{State, Destination})` materializes checkpoint state
in a destination described by `Spec`. The destination may retain the Workspace
identity for recovery or use a new identity for a fork. Checkpoint production
and durable-state retention are owned by the provider/application adapter.

Session fork (SES section 8, OWN-FRK-2) does not call Restore. A fork copies
the Session's committed facts only: the child Session inherits the
conversation history and Turn settlements, not the workspace state the parent
had at the fork point, and the parent's later tool calls keep their effects on
the parent's workspace. The child's Runs execute against whatever its target
binding resolves to. Regenerate and edit (TRN-DUR-3) are therefore
history-only operations. Resolving the fork point to a Turn-boundary
workspace checkpoint and restoring it into a new workspace bound to the child
Session is future design; it needs a checkpoint reference recorded at Turn
boundaries and is carried out by the application's fork policy (below).

Agent Core carries an opaque `run.TargetRef{Kind, ID}` on an Assignment. An
application-provided `loop.TargetResolver` supplies the target per effect
(RUN-LOP-9): the Loop asks it once for every model or tool effect it is about
to start, with the effect's coordinates (Session, RunID, StepID, CallID,
EffectID, kind and tool ref), and copies the answer into that effect's
Assignment only. Different effects of one Run may resolve to different
targets. The mapping must be durable if a Run can outlive the process that
started it; the target itself is not a Run fact.

The seam and its implementation live in different layers (APP-TGT-1).
Agent Core owns `loop.EffectContext`, `run.TargetRef` and the
`loop.TargetResolver` interface only; it keeps no target fact and has no
default resolver: a nil resolver gives every effect no target. The
application (cloud agent / Memoh) owns the resource registry, the workspace
manager and the `TargetResolver` implementation, and guarantees the
durability of its Session → workspace mapping.

Conversation lineage and resource lineage are separate lineages. A Session
fork (OWN-FRK-2) copies committed facts and carries no resource binding to
the child; the application's fork policy decides the child's binding and
establishes a new resource binding for it:

- share the parent's existing workspace;
- clone the workspace;
- restore a checkpoint into a new workspace;
- allocate a fresh workspace.

Until the application binds one, the child's tool effects have no target.

The provider adapter is a Backend of the Worker: it resolves the target to a
RuntimeBinding and, in Prepare, allocates or derives the ExecutionRef the
Worker persists before Start crosses the external uncertainty barrier. Attach,
status, outcome, and cancel address that Ref, preventing a Worker takeover from
accidentally addressing a newly created provider job.

`AgentPreset` is decision identity (model, tools, prompt policy, and scheduling);
the same preset can be
used against different logical workspaces.
