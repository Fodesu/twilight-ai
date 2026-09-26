// Package sandbox is the workspace backend of the Worker (CLD-TOL-1): an
// executor.ExecutionBackend that runs workspace-placed tools inside the
// Environment a Session's Workspace is materialized in. It composes the
// kernel's in-process executor with an environment manager: every tool
// call's Assignment carries a workspace target (RUN-LOP-9), the manager
// resolves it to an attached Environment through the workspace Store and
// the environment Provider, and the tool runs there. Routing by the tool's
// Placement declaration sends only workspace-placed calls here.
package sandbox

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/felinics/twilight/agent/environment"
	"github.com/felinics/twilight/agent/tools"
	"github.com/felinics/twilight/agent/workspace"
	"github.com/felinics/twilight/agentcore/executor"
	"github.com/felinics/twilight/agentcore/executor/notice"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/agentcore/run/model/sdkconv"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/sdk"
)

// Provider is the backend's name in a Worker's Route table and in records.
const Provider = "sandbox"

// Options compose a Backend.
type Options struct {
	// Workspaces holds the Workspace records and their RuntimeBindings.
	Workspaces workspace.Store
	// Provider materializes and attaches environments.
	Provider environment.Provider
	// Backend is the Provider's identity written into RuntimeBindings.
	Backend environment.Backend
	// Tools are the workspace-placed tools this backend serves.
	Tools []tools.Tool
	// Progress receives the tools' progress frames; nil discards them.
	Progress effect.ProgressSink
}

// Backend is the workspace backend.
type Backend struct {
	inner *loop.LocalExecutor
	envs  *manager
}

var (
	_ executor.ExecutionBackend = (*Backend)(nil)
	_ notice.Source             = (*Backend)(nil)
)

// New composes a Backend.
func New(opts Options) (*Backend, error) {
	if opts.Workspaces == nil || opts.Provider == nil {
		return nil, errors.New("sandbox: a workspace store and an environment provider are required")
	}
	if opts.Backend == "" {
		return nil, errors.New("sandbox: the provider's backend identity is required")
	}
	envs := &manager{store: opts.Workspaces, provider: opts.Provider, backend: opts.Backend, attached: make(map[workspace.ID]environment.Environment)}
	catalog := toolCatalog{}
	for _, t := range opts.Tools {
		if t == nil || t.Ref() == "" {
			return nil, errors.New("sandbox: tools require a ref")
		}
		if _, dup := catalog[t.Ref()]; dup {
			return nil, fmt.Errorf("sandbox: duplicate tool %s", t.Ref())
		}
		catalog[t.Ref()] = &adapter{tool: t, envs: envs}
	}
	inner, err := loop.NewLocalExecutor(noModels{}, catalog, opts.Progress, false)
	if err != nil {
		return nil, err
	}
	return &Backend{inner: inner, envs: envs}, nil
}

// Route is the Worker route that sends workspace-placed tool calls here.
func Route(b *Backend) executor.Route {
	return executor.Route{Provider: Provider, Backend: b, Match: func(a effect.Assignment) bool {
		t, ok := a.Tool()
		return ok && t.Placement == run.PlacementWorkspace
	}}
}

// PublicTools are the preset entries of workspace tools: frozen definition,
// policies and the workspace placement.
func PublicTools(ts []tools.Tool) ([]turn.PublicTool, error) {
	out := make([]turn.PublicTool, 0, len(ts))
	for _, t := range ts {
		def, err := sdkconv.FreezeToolDefinition(t.Definition())
		if err != nil {
			return nil, err
		}
		out = append(out, turn.PublicTool{Ref: t.Ref(), Definition: def, Policy: t.ResponsePolicy(), Replay: t.Replay(), Placement: run.PlacementWorkspace})
	}
	return out, nil
}

// Validate refuses what this backend cannot serve before any effect: a model
// call, a process-placed tool (a routing error), a workspace tool whose
// Assignment carries no workspace target (the Session is bound to none);
// the in-process executor then checks the tool's declarations.
func (b *Backend) Validate(ctx context.Context, a effect.Assignment) (*run.ToolFailure, error) {
	t, ok := a.Tool()
	if !ok {
		return &run.ToolFailure{Class: run.FailureProvider, Message: "the workspace backend serves tool calls only"}, nil
	}
	if t.Placement != run.PlacementWorkspace {
		return &run.ToolFailure{Class: run.FailureInvalidInput, Message: fmt.Sprintf("tool %s is placed in the process, not in a workspace", t.ToolRef)}, nil
	}
	if a.Target == nil || a.Target.Kind != workspace.TargetKind || a.Target.ID == "" {
		return &run.ToolFailure{Class: run.FailureInvalidInput, Message: fmt.Sprintf("tool %s runs in a workspace but the session is bound to none", t.ToolRef)}, nil
	}
	return b.inner.Validate(ctx, a)
}

func (b *Backend) Prepare(ctx context.Context, a effect.Assignment) (string, error) {
	return b.inner.Prepare(ctx, a)
}

func (b *Backend) Start(ctx context.Context, ref string, a effect.Assignment) error {
	if failure, err := b.Validate(ctx, a); err != nil {
		return err
	} else if failure != nil {
		return fmt.Errorf("%w: %s: %s", loop.ErrExecutorRejected, failure.Class, failure.Message)
	}
	return b.inner.Start(ctx, ref, a)
}

func (b *Backend) Restart(ctx context.Context, previous string, a effect.Assignment) (string, error) {
	return b.inner.Restart(ctx, previous, a)
}
func (b *Backend) Attach(ctx context.Context, ref string) (effect.Attachment, error) {
	return b.inner.Attach(ctx, ref)
}
func (b *Backend) Status(ctx context.Context, ref string) (effect.ExecutionStatus, error) {
	return b.inner.Status(ctx, ref)
}
func (b *Backend) Outcome(ctx context.Context, ref string) (effect.Outcome, error) {
	return b.inner.Outcome(ctx, ref)
}
func (b *Backend) Cancel(ctx context.Context, ref string) error { return b.inner.Cancel(ctx, ref) }

// Settled is notice.Source: the in-process executor's settlement notices.
func (b *Backend) Settled(ctx context.Context, epoch string, after uint64, fn func(notice.Ref) bool) error {
	return b.inner.Settled(ctx, epoch, after, fn)
}

// Close detaches every environment this process attached; the workspaces
// keep their RuntimeBindings.
func (b *Backend) Close(ctx context.Context) error { return b.envs.close(ctx) }

// --- tool adapter -------------------------------------------------------------

type noModels struct{}

func (noModels) ResolveModel(ref run.ModelRef) (loop.ModelInvoker, error) {
	return nil, fmt.Errorf("sandbox: no model %s: the workspace backend serves tool calls only", ref)
}

type toolCatalog map[run.ToolRef]loop.ExecutableTool

func (c toolCatalog) ResolveTool(ref run.ToolRef) (loop.ExecutableTool, error) {
	t, ok := c[ref]
	if !ok {
		return nil, fmt.Errorf("sandbox: unknown workspace tool %s", ref)
	}
	return t, nil
}

// adapter presents a workspace tool to the in-process executor and resolves
// the environment when the call runs.
type adapter struct {
	tool tools.Tool
	envs *manager
}

var _ loop.ExecutableTool = (*adapter)(nil)

func (a *adapter) Ref() run.ToolRef                            { return a.tool.Ref() }
func (a *adapter) Definition() sdk.ToolDefinition              { return a.tool.Definition() }
func (a *adapter) ResponsePolicy() run.ResponsePolicy          { return a.tool.ResponsePolicy() }
func (a *adapter) Replay() run.ReplayPolicy                    { return a.tool.Replay() }
func (a *adapter) Placement() run.ToolPlacement                { return run.PlacementWorkspace }
func (a *adapter) ValidateArguments(v run.CanonicalJSON) error { return a.tool.ValidateArguments(v) }

func (a *adapter) Execute(ctx context.Context, req loop.ToolExecutionRequest) loop.ToolExecutionOutcome { //nolint:gocritic // hugeParam: loop.ExecutableTool.Execute takes the request by value
	if req.Target == nil || req.Target.Kind != workspace.TargetKind || req.Target.ID == "" {
		return loop.ToolExecutionFailed{Failure: run.ToolFailure{Class: run.FailureInvalidInput, Message: "workspace tool call without a workspace target"}, Retry: run.RetryNever}
	}
	env, err := a.envs.attach(ctx, workspace.ID(req.Target.ID))
	if err != nil {
		switch {
		case errors.Is(err, workspace.ErrNotFound):
			return loop.ToolExecutionFailed{Failure: run.ToolFailure{Class: run.FailureNotFound, Message: err.Error()}, Retry: run.RetryNever}
		case errors.Is(err, environment.ErrUnsupported):
			return loop.ToolExecutionFailed{Failure: run.ToolFailure{Class: run.FailureUnavailable, Message: err.Error()}, Retry: run.RetryNever}
		default:
			return loop.ToolExecutionFailed{Failure: run.ToolFailure{Class: run.FailureUnavailable, Message: err.Error()}, Retry: run.RetryAllowed}
		}
	}
	return a.tool.Run(ctx, env, &req)
}

// --- environment manager ------------------------------------------------------

// manager resolves a Workspace to an attached Environment: the one the
// Workspace's RuntimeBinding names, or a new materialization recorded with a
// conditional write on the binding generation, so two replicas that
// materialize the same workspace at once agree on one environment.
type manager struct {
	store    workspace.Store
	provider environment.Provider
	backend  environment.Backend

	mu       sync.Mutex
	attached map[workspace.ID]environment.Environment
}

func (m *manager) attach(ctx context.Context, id workspace.ID) (environment.Environment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if env, ok := m.attached[id]; ok {
		return env, nil
	}
	env, err := m.resolve(ctx, id)
	if err != nil {
		return nil, err
	}
	m.attached[id] = env
	return env, nil
}

func (m *manager) resolve(ctx context.Context, id workspace.ID) (environment.Environment, error) {
	ws, err := m.store.Get(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("workspace %s: %w", id, err)
	}
	var expected uint64
	if ws.Runtime != nil {
		expected = ws.Runtime.Generation
		if ws.Runtime.Backend == m.backend {
			env, err := m.provider.Attach(ctx, ws.Runtime.EnvironmentRef)
			if err == nil {
				return env, nil
			}
			if !errors.Is(err, environment.ErrNotFound) {
				return nil, fmt.Errorf("workspace %s: attach %s: %w", id, ws.Runtime.EnvironmentRef, err)
			}
		}
		// The recorded environment is gone or belongs to another provider:
		// the workspace is materialized again under the next generation.
	}
	return m.materialize(ctx, &ws, expected)
}

// materialize creates the workspace's next environment and records it;
// when another replica recorded a binding first, its environment is
// attached instead and the one created here is closed.
func (m *manager) materialize(ctx context.Context, ws *workspace.Workspace, expected uint64) (environment.Environment, error) {
	var env environment.Environment
	var err error
	if ws.Snapshot != nil {
		snap, serr := m.store.GetSnapshot(ctx, *ws.Snapshot)
		if serr != nil {
			return nil, fmt.Errorf("workspace %s: snapshot %s: %w", ws.ID, *ws.Snapshot, serr)
		}
		env, err = m.provider.Restore(ctx, environment.RestoreSpec{State: snap.StateRef, Destination: environment.Spec{Subject: string(ws.ID), Base: string(ws.Base)}})
	} else {
		env, err = m.provider.Create(ctx, environment.Spec{Subject: string(ws.ID), Base: string(ws.Base)})
	}
	if err != nil {
		return nil, fmt.Errorf("workspace %s: materialize: %w", ws.ID, err)
	}
	binding := environment.Binding{Backend: m.backend, EnvironmentRef: env.Ref(), Generation: expected + 1}
	if err := m.store.UpdateRuntime(ctx, ws.ID, expected, binding); err != nil {
		_ = env.Close(ctx)
		if !errors.Is(err, workspace.ErrGenerationConflict) {
			return nil, fmt.Errorf("workspace %s: record runtime: %w", ws.ID, err)
		}
		current, gerr := m.store.Get(ctx, ws.ID)
		if gerr != nil || current.Runtime == nil || current.Runtime.Backend != m.backend {
			return nil, fmt.Errorf("workspace %s: another replica materialized it under a binding this backend cannot attach", ws.ID)
		}
		return m.provider.Attach(ctx, current.Runtime.EnvironmentRef)
	}
	return env, nil
}

func (m *manager) close(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var first error
	for id, env := range m.attached {
		if err := env.Close(ctx); err != nil && first == nil {
			first = err
		}
		delete(m.attached, id)
	}
	return first
}
