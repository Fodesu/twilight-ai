# Agent Core

Agent Core defines Twilight's durable execution model. It owns session history,
run state, effect lifecycle, persistence, ownership, and recovery. Hosts and
providers supply model access, tool execution, transports, and external
resources.

## Semantic flow

A client submits an Input to a Session. The chatlog commits the input, and a
Turn associates it with a preset and a Run.

The Run loop reads the current Run state, derives the next command, and commits
its protocol transition. When the transition requires external work, the
execution boundary handles that work and returns an Outcome. The loop commits
the outcome and continues the Run.

Session projections expose the resulting state to application code.

## CQRS and event sourcing

Agent Core uses event sourcing for durable semantic state and CQRS to separate
state changes from state reads.

### Event sourcing

Commands produce committed facts. The ledger stores those facts in order, and
module projections fold them into current state. The facts provide the durable
history; projections provide derived read models.

```text
Command → validate current state → append facts → fold projections
                                      │
                                      └── durable history
```

A restart rebuilds state from the ledger and retained content. A projection
cache improves read performance while the ledger remains the source of truth.

### CQRS

The write path uses a Writer. It owns the session lease, validates commands,
appends commits atomically, and applies the corresponding projection updates.

The read path uses projections and query surfaces. Reads can inspect session,
turn, run, and workspace state without acquiring session ownership.

CQRS is a logical separation in Agent Core. The write and read paths may share
an implementation while retaining independent responsibilities.

## Durable state

Session state is stored as an append-only sequence of committed facts. A commit
contains an ordered event group and a commit identity. Projections fold facts
into queryable state.

The ledger enforces:

- commit ordering;
- idempotent commit identities;
- legal state transitions;
- writer-epoch fencing.

A process reconstructs session state by reopening the store and folding the
recorded facts. Projection caches accelerate this operation and do not replace
the ledger.

Transient observations, such as executor progress, remain outside semantic
history unless their protocol commits them as facts.

## Core concepts

### Session

A Session is the durable boundary of one interaction. Its history contains the
inputs, turns, runs, workspace bindings, and other module-owned facts associated
with that interaction.

Opening a Session creates an ownership lease. Reads use projections and do not
require ownership. A Writer commits changes under its ownership epoch.

### Input

An Input is a durable entry in the session chatlog. A Turn references the input
IDs used to construct its request.

### Turn

A Turn is one user-visible unit of progress. It associates inputs with a
preset and a Run. The Turn surface combines Turn facts with the state of its
Run and settles as completed, failed, stopped, or superseded.

The Turn module owns Turn lifecycle facts. The Run module owns Run lifecycle
facts.

### Run

A Run is the logical execution history of a Turn. It is a versioned state
machine with stable identities and deterministic transitions.

A Run records:

- the active logical step;
- model and tool effects;
- effect requests and settlements;
- recovery state;
- the terminal Run state.

Run identity and canonicalization rules are bound to the schema version that
created the Run. Replays use those rules.

### Step

A Step represents logical progress inside a Run. The current implementation
uses model steps and tool steps.

A Step may produce one or more physical execution attempts when its effect is
recovered and replay is permitted.

### Effect

An Effect is a request for external work. Model calls and tool calls are
represented as effects. The owning protocol assigns each effect a stable
identity and records its lifecycle: a Run for conversation steps, or a Session
compaction operation for a context-maintenance summary.

### Execution

An Execution is one physical attempt for an Effect. The executor persists the
selected provider and its opaque backend reference.

The Core addresses the execution through the effect assignment key. Backend
handle formats remain inside the executor and provider layers.

### Outcome

An Outcome is the terminal result observed from an Execution. The executor
reports success, failure, cancellation, or an unavailable result. The owning
protocol commits the outcome and performs its next transition.

An uncertain dispatch remains recoverable until the backend provides a reliable
observation. An interrupted request therefore keeps its settlement unresolved.

### Wait

A Wait records an external response required by a tool call. The Run remains
active while waiting. The response is committed before the Run continues.

### Artifact

Facts reference large or immutable bodies through artifact identities. The
content store retains those bodies, and the binding and retention stores keep
the references valid across restarts and forks.

## Run loop

`agentcore/run/loop` is the Run decision interpreter. The RunStore remains the
source of authority. Each operation loads the Run, derives the next plan,
commits protocol transitions, and reloads state when another actor has advanced
it.

The loop exposes three execution modes:

- `Advance` interprets the Run until it dispatches one or more assignments or
  reaches a waiting or finished state. It prepares and commits protocol
  transitions, then returns assignment keys before outcome retrieval.
- `Deliver` accepts one available Outcome, verifies that the named effect is
  still executing, commits the settlement, and reports the next action. A
  stale outcome is dropped.
- `Run` is the blocking host convenience. It calls `Advance`, watches every
  dispatched key, waits for outcomes, calls `Deliver`, and repeats.

The blocking form still uses asynchronous effect execution. A tool step can
dispatch several assignments and keep them in a pending set. Tool scheduling
supports parallel execution, sequential execution, and a maximum parallel
count. The watcher receives settlement notices or reads outcomes when they
become available. Progress is forwarded independently of outcome delivery.

The Loop serializes operations for one Run and permits different Runs to
advance concurrently. Its EventSink carries committed facts and provisional
model or tool observations. The ledger remains authoritative for committed
state.

Cancellation cancels in-flight assignments, drains their reported outcomes,
and settles them under a detached control context before the loop returns.
Ownership loss cancels the current drive and leaves the durable execution
records for the next owner to recover.

## Context preflight and compaction

Every model invocation can inspect its complete candidate after `Builder.Build`
and before `PrepareModelRequest` is committed. This includes later invocations
within the same Turn, not only the first request. `CheckCandidate` either accepts
it, asks for a bounded rebuild with a fresh Run snapshot, or yields
`context_maintenance` to the Session driver. The hook never waits for a summary
model. `BeforePrepare` remains a pre-build extension point, not the place to
measure a request that does not exist yet. `PromptToken` is an opaque freshness
identifier, not a token usage count.

The application's `ContextPolicy` decides thresholds, retention preferences,
summary model and summary request content. `BudgetPolicy` supplies the default
comparison over an application-provided token estimator and model limits. It
accounts for output reservation, the request's explicit output cap, a safety
margin and an optional lower input limit. Estimation must cover the entire
candidate, including system messages, tool definitions and modality overhead.
It does not add history and current-Turn messages twice. Provider accounting may
still differ from an estimate; unknown model limits are not an unlimited budget.
The summary builder is also responsible for fitting its own model's capacity.

`chatlog.PlanCompaction` selects a tool-pair-closed retained suffix and pins the
ordered context digest and covered position before summary generation. Pending
tool calls keep their issuing assistant. The suffix may therefore exceed the
requested entry count; when nothing can be replaced, compaction cannot make
progress. A direct `Chatlog.Compact` must receive this captured plan rather than
reconstructing its base after generating a summary.

Compaction is a small Session protocol, not a normal Run ModelStep:

```text
requested (plan + frozen summary request + stable effect identity)
    → dispatch / attach / recover through the Executor
    → summary Outcome
    → atomically commit summary + chatlog compaction + operation finished
    → acknowledge Outcome, then record acknowledgement
```

The requested fact claims the frozen request through the normal artifact
admission and retention mechanism. The executor's durable request, execution and
Outcome services are reused; a shared Watcher is only a wake-up/read mechanism,
not the record of pending work. A summary effect's transport RunID is a separate
namespace and creates no Run or ordinary Assistant history.

At most one operation is pending in a Session. Its start checks the plan and Run
quiescence; pending maintenance blocks `PrepareModelRequest` inside the same
Writer critical section as that command. Successful settlement rechecks both
quiescence and the captured base. A changed base finishes the operation as stale
without replacing any context; the driver rebuilds and reevaluates policy. The
old summary is never relabelled with the new context's digest. Model failure
finishes the operation as failed and stops the current drive rather than sending
an unchecked oversized request.

A successful automatic compaction is allowed once for the same preparation
(Run, source step and input cohort); its rebuilt request must fit policy. Failed
or stale operations are also bounded by `MaxCompactionAttempts` (default four).
This bound is derived from recorded operations and survives owner replacement.
An application can change its policy or explicitly plan maintenance, but a
permanently oversized retained suffix must not cause an infinite summary loop.

`Execution.Open` discovers pending operations even without an active Run or a
currently configured automatic policy, and resumes them under the Session's
recovery lifetime. A reattached normal effect also continues through the Session
driver, so maintenance first requested after Open is not left pending. Outcome
callbacks yield the Watcher before driving further effects; Stop cancels and
joins these continuations. Failure notifications run outside operation locks
and joined work, allowing the host's callback to close the Session.
Cancellation leaves accepted work for the next owner. A
completed but unacknowledged operation is also recoverable: ACK happens only
after the Session commit, and repeating settlement or ACK writes no duplicate
summary. The guarantee is idempotent result adoption, not exactly-once provider
invocation. Uncertain dispatch keeps the same intent and effect identity.

Operation state follows own-segment inheritance: a fork inherits conversation
summaries, not its parent's pending execution or ACK responsibility. Hosts use
`Compactions.NeedsRecovery` when deciding whether a Session has work or can be
released as idle. Discovery of an unowned Session remains the host's existing
activation/lease mechanism; an explicitly closed Session still needs activation.
No new physical queue or SQL migration is required.

## Execution boundary

The executor uses the following lifecycle:

```text
Loop.Advance
     │
     ├── Validate → Prepare → accepted commit → Start
     │                                      │
     │                                      ▼
     │                              asynchronous backend work
     │                                      │
     └── assignment key ◄────────── Worker / Backend record
                                            │
                              settlement notice or outcome read
                                            │
                                            ▼
                                      Loop.Deliver
```

`Validate` checks backend capability before an effect starts.

`Prepare` deterministically derives an execution reference. It performs no
external allocation. The Worker persists the reference before `Start`.

`Start` crosses the external effect boundary. The backend reports whether the
execution is active, terminal, absent, or uncertain.

`Attach` reconnects to an existing execution. A missing execution can be
replayed when the assignment's replay policy permits it. An uncertain or
orphaned execution remains under recovery control.

## Ownership and recovery

A Session Writer commits under an ownership epoch. A later owner fences an older
Writer at the commit boundary.

Execution recovery proceeds as follows:

1. The Worker persists the execution reference.
2. The Worker starts or attaches to the backend execution.
3. A replacement Worker reads the durable execution record.
4. It queries the backend for the execution state.
5. It attaches to an active execution, commits a terminal outcome, or applies
   the assignment's replay policy.

The Worker owns execution leases and watcher goroutines. The owner and Driver
decide when records require recovery; the Worker provides the recovery
operations.

Semantic ownership and physical execution records remain separate so that a
Session can change owners while an external execution is being recovered.

## Session Kernel and Runtime

The Session Kernel composes the durable semantic services:

- session stores and Writers;
- the ledger and commit observers;
- the module registry;
- projections and projection caches;
- the Run adapter;
- Turn coordination;
- chatlog commands;
- artifacts, frozen content, and session history.

The Runtime drives active work around the Kernel. It contains the Run loop,
model and tool ports, executor integration, progress delivery, responders, and
recovery handling.

The Driver calls the blocking form of the Loop. The Loop waits for in-flight
outcomes internally, then returns when the Run is complete, waiting for an
external response, requires execution recovery, or reaches an error. The Driver
uses Responders for external waits and Recovery for executions left by a
previous owner.

## Workspace boundary

A logical Workspace has a durable binding to a physical Environment. The
Environment provider supplies filesystem and execution capabilities.

Workspace provider policy covers path validation, command execution, lifecycle,
and isolation. Agent Core uses the provider-neutral contract.
