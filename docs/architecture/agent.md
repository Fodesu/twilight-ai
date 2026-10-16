# Agent

The agent layer composes Core protocols into ownership management, reliable
command delivery, work scheduling, and disposable execution. Its central
property is durable continuation: progress does not depend on keeping a
Session's computing instance alive.

## Decision

A Session has three independent lifetimes:

| Concept | Authority | Lifetime |
| --- | --- | --- |
| Session / Run | Ledger and folded domain facts | Durable interaction |
| Ownership | Session root's current fenced capability | Temporary write interval |
| Activation | Agent process interpreting current work | Disposable computation |

Execution Authority is independent of all three. A Turn may be active and a
model execution running while the Session has no owner or Activation. A Turn
may span many epochs, Activations, and processes.

Activation state is not another semantic event stream. Its continuity comes
from durable obligations, not suspended stacks, callbacks, or an owner table.
This is a breaking API change; no old ownership or blocking Driver adapters
are retained.

## Responsibilities

- `agentcore/session`: atomic ordered commits, CommitID identity, projections,
  and the storage protocol for current-epoch ownership and fenced append.
- `agentcore/session/writer`: `Factory.Bind` constructs local write state over
  an acquired handle. `Writer.Close` closes local state only.
- `agentcore/run`: deterministic decisions, effects, Advance/Deliver, and finite
  reconciliation. Unknown external execution is never proof of absence.
- `agentcore/executor`: durable execution acceptance, independent execution
  leases, backend recovery, terminal Outcomes, and safe acknowledgement.
- `agentcore/runtime` and `driver`: finite Session/Turn interpretation and work
  inspection. No detached owner lifetimes, responder goroutines, or timers.
- `agent/ownership`: acquire, renew (including while binding), close local
  Writer, and release the exact acquired epoch. No execution-side Open.
- `agent/inbox`: durable application command delivery and atomic Session
  receipts. Applications define command kinds and handlers.
- `agent/scheduling`: durable, versioned work claims. A claim is not Session
  ownership; losing it cannot grant or revoke Session write authority.
- `agent/activation`: scan independent authorities, run bounded computations,
  and hand off all remaining work before releasing ownership.

The store atomically checks current ownership/epoch and expected segment head
with each append. Heartbeat placement never changes that safety boundary.
Session lease expiry currently permits takeover; expiry alone does not reject
an append or renewal while the same epoch remains current. Explicit Release
revokes that epoch's write access, and a later Acquire advances the epoch.

## Work discovery

`runtime.Inspect` reads semantic obligations without taking ownership:

| Durable condition | Next action |
| --- | --- |
| Submitted inputs with no active Run | Route the next Turn |
| Executable Run state | Advance |
| Started effect without confirmed execution | Reconcile original assignment key |
| Running execution | Observe later; recover only under execution protocol |
| Terminal, unacknowledged Outcome | Deliver or prove existing disposition, then ACK |
| External response wait | Human command or durable response job |
| Pending compaction | Advance/Deliver maintenance before normal preparation |
| Settled compaction not acknowledged | Retry cleanup |
| Time-slice exhaustion | Persist scheduling continuation |

`Discovery.Sweep` enumerates all Session pages, Inbox pending entries, and
Executor outstanding pages. Notifications are optional latency optimizations.
A durable source write followed by a failed scheduling signal is repaired by
a later sweep. Concurrent new keys behind a cursor are found next sweep.
A failing Session does not prevent visiting the other Session pages.

The execution management interface is `effect.OutstandingPort`, separate from
the per-assignment data plane. It includes never-claimed acceptances, unsettled
executions and terminal unacknowledged Outcomes, excluding aborted and
acknowledged keys. Lease expiry schedules observation, not blind replay.
Missing discovery capability is an error, never an empty successful scan.

A sweep uses stable source revisions, not pending counts or process clocks.
Repeated observations of the same revision must not override backoff or
manufacture a new activation. Separate projection reads use the minimum
observed Session head as their conservative discovery watermark; no mixed-head
read is used to authorize a semantic change.

Running effects get bounded reconciliation probes rather than immediate
repeated activation. A human response wait alone does not create a polling
loop. An optional `ResponseScheduler.Ensure` must durably and idempotently
accept a job by Session/ResponseID; it gets no Writer and returns results as
commands. A goroutine that may disappear is not an implementation of Ensure.

Absolute time-based obligations must themselves be durable: for example a
higher-version scheduling Signal with NotBefore, or an application timer
source that reconstructs that Signal. An in-memory timer is not recovery truth.

## Linearization and handoff boundaries

There is no single transaction spanning Session, Executor, Inbox and queue.
Each authority has its own linearization point, with recoverable gaps:

| Operation | Durable boundary | Recovery after a crash |
| --- | --- | --- |
| Acquire | Current epoch installed | Append rejects superseded epochs |
| Apply command | Domain facts and receipt appended atomically | Replay receipt; then Resolve Inbox |
| Accept effect intent | Effect-naming Run fact / compaction request | Scan and reconcile original key |
| Dispatch | Executor acceptance before external Start | Attach original key; never infer absence from timeout |
| Produce Outcome | Execution terminal record | Outstanding scan rediscovers settlement |
| Settle Outcome | Effect-naming Session disposition | Prove disposition and repair Executor ACK |
| Signal work | Source high-watermark and generation advance | Re-signal from durable sources if signal was never committed |
| Claim work | Fresh token and generation snapshot | Expired claim can be taken over |
| Release | Root no longer owned by this epoch | Stale Release cannot revoke a successor |
| Finish claim | Conditional token/generation update | Uncovered new work survives old Finish |

Effect disposition proof reads the owning Run's durable stream, not only its
active projection or a guessed command-index hit. CancelRun dispositions are
included. A dropped Deliver or terminal Run by itself is not ACK proof.
Proof is combined with Executor terminal/Abort checks; an active/orphaned
execution is not collected merely because semantic recovery recorded a fact.
Outcomes and required bodies must remain readable until safe acknowledgement.

Inbox receipts include a command digest and immutable applied/rejected result.
Their CommitID is scoped to Session and command identity, since CommitID
lookup includes inherited history. A fork must decide its own commands rather
than reuse its parent's receipts. Infrastructure failures leave commands
pending; no handler may perform external effects or recursively commit while
holding its transactional View.

## Yield protocol

`Runner.RunOne` performs:

1. Claim work and maintain its reservation throughout acquisition and work.
   This does not grant Session ownership.
2. Acquire a fresh Session handle and bind a Writer.
3. Apply a bounded number of Inbox commands and reconcile outstanding effects.
4. Execute a bounded number of finite conversation Steps.
5. Reinspect remaining semantic and delivery obligations.
6. Stop/join Session renewal, close local Writer state and Release that epoch.
7. Stop/join claim renewal and Finish only the covered generation. Retain
   retry/probe/continuation when work remains; cleanup failures retain a retry.
   Concurrent future probe signals merge with an earlier continuation, not
   postpone it.

Step, command handling and reconciliation retain no Writer callback. Thus
method completion is the local computation barrier; remote execution need not
finish. The Activation deadline includes acquisition and binding (30s by
default). Start RPCs have bounded cancellation/deadline semantics distinct
from the external execution's lifetime. Losing a job claim is reported but
does not itself revoke the Session capability or throw away every slow
Activation before its first Step; bounded progress remains fenced, and a
stale claim cannot Finish a successor's work. Request cancellation detaches computation and never means domain Stop.
Even `Loop.Run`, the optional synchronous Advance/Deliver convenience, detaches
without cancelling an accepted execution when its context is cancelled.

Input arriving between final inspection, Release and Finish is independent
durable work. If signalled concurrently its newer generation survives Finish;
if its signal is lost, a sweep reconstructs it. ErrOwned reschedules the job,
notifies no in-memory owner as a substitute for delivery, and does not drain it.
A process crash can skip this entire clean-yield sequence: persisted intents,
source scans and expired claims still suffice to recover.

`Service.Run` is a process-level loop combining periodic fair sweeps with
finite activations. It does not keep Sessions open while idle. Separate worker
replicas may share its durable ports. Storage availability, fair scheduling and
sufficient capacity are explicit liveness assumptions, not CAP guarantees.

## Composition and adapter requirements

Build a Kernel with the application `inbox.Module` when using Inbox receipts.
Create `ownership.Manager{Writers: kernel.Writers, Options: ...}`, an Execution,
a `scheduling.Store`, Inbox Consumer, Discovery and Runner over the same
Session/Execution authorities. Configure the default preset for new Turns.
Cloud ownership must use a finite lease duration for crash takeover. A
non-expiring Session hold requires explicit Release or operator Takeover;
periodic work scans cannot revoke it. Lease/claim durations must allow the
configured renewal operations to complete.
Serve Discovery even when no Session is owned. Stream provisional progress
from `effect.ProgressPort` independently of Session ownership; it is not durable
semantic history and is not an activation trigger.

The repository provides the ports and explicit test-only reference adapters
for Inbox, scheduling and execution stores. These maps do not constitute a
production durable deployment. Cloud adapters must implement the documented
atomicity, clock, pagination, claim fencing and retention contracts. Session
filesystem storage is the existing local adapter; no MQ or distributed
transaction dependency is introduced by this change.

## Failure verification

Tests exercise real Session filesystem storage with test Execution/Inbox/queue
authorities whose state survives replacement of the computing instances:

- Session is unowned while a model runs; another epoch completes the same Turn.
- Every settlement notification is absent; source sweeps still make progress.
- Durable Inbox enqueue without Signal is rediscovered.
- A new command between Release and Finish survives the old generation ACK.
- ErrOwned and time-slice exhaustion retain their scheduling obligations.
- A lost Dispatch response does not repeat an accepted model execution.
- Session settlement followed by failed Executor ACK is repaired from proof.
- Worker replacement attaches the original execution after its lease expires.
- Context cancellation and ownership loss do not cancel successor-owned effects.
- Compaction can become dormant, while ordinary preparation remains gated.
- Receipt/domain commit, Resolve failure, rejection, stale Writer, and fork
  boundaries preserve command identity and immutable results.

This protocol guarantees ordered semantic commits and policy-respecting effect
recovery, not exactly-once execution against arbitrary external systems.
