package relay_test

import (
	"context"
	"errors"
	"testing"

	"github.com/felinics/twilight/agentcore/checkpoint"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/process"
	"github.com/felinics/twilight/agentcore/process/relay"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/schema"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/extension"
	runmod "github.com/felinics/twilight/agentcore/session/run"
	"github.com/felinics/twilight/agentcore/store/sqlite/sqlitetest"
)

const sid session.SessionID = "s1"

// history is a Session ledger of run facts, one commit per call to add.
type history struct {
	registry *extension.Registry
	commits  []session.Commit
}

func (h *history) add(t *testing.T, runID run.RunID, facts ...run.Fact) {
	t.Helper()
	batch := session.StreamBatch{Stream: runmod.Stream(runID)}
	for _, f := range facts {
		typ := runmod.EventType(schema.Wire, f)
		payload, err := h.registry.Encode(typ, runmod.Event{RunID: runID, Fact: f})
		if err != nil {
			t.Fatal(err)
		}
		batch.Events = append(batch.Events, session.Event{Type: typ, RecordedAtUnixMilli: 1, Payload: payload})
	}
	h.commits = append(h.commits, session.Commit{Seq: session.CommitSeq(len(h.commits)), Batches: []session.StreamBatch{batch}})
}

func (h *history) ReadCommits(_ context.Context, req session.CommitReadRequest) (session.CommitPage, error) {
	from := int(req.From)
	if from > len(h.commits) {
		from = len(h.commits)
	}
	return session.CommitPage{Commits: h.commits[from:], Head: session.Head{Next: session.CommitSeq(len(h.commits))}}, nil
}

// executor answers Attach from a table and counts Dispatches routed to it.
type executor struct {
	effect.ExecutionPort
	held       map[effect.AssignmentKey]bool
	redispatch map[effect.AssignmentKey]int
	refuse     error
}

func (e *executor) Attach(_ context.Context, key effect.AssignmentKey) (effect.Attachment, error) {
	if e.held[key] {
		return effect.Attachment{State: effect.AttachmentActive}, nil
	}
	return effect.Attachment{State: effect.AttachmentMissing}, nil
}

func (e *executor) Redispatch(_ context.Context, _ session.SessionID, key effect.AssignmentKey) error {
	e.redispatch[key]++
	if e.refuse != nil {
		return e.refuse
	}
	e.held[key] = true
	return nil
}

type fixture struct {
	relay *relay.Relay
	hist  *history
	exec  *executor
	store process.Store
	cps   checkpoint.Store
}

func newFixture(t *testing.T, maxAttempts int) *fixture {
	t.Helper()
	registry, err := extension.BuildRegistry(runmod.Module)
	if err != nil {
		t.Fatal(err)
	}
	db := sqlitetest.Open(t)
	f := &fixture{hist: &history{registry: registry}, exec: &executor{held: map[effect.AssignmentKey]bool{}, redispatch: map[effect.AssignmentKey]int{}}, store: db.Processes(), cps: db.Checkpoints()}
	f.relay, err = relay.New(relay.Ports{History: f.hist, Registry: registry, Ledger: f.store, Checkpoints: f.cps, Executions: f.exec, Redispatch: f.exec.Redispatch, MaxAttempts: maxAttempts, Now: func() int64 { return 1 }})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *fixture) phase(t *testing.T, key effect.AssignmentKey) process.Phase {
	t.Helper()
	state, _, ok, err := f.store.Load(context.Background(), key)
	if err != nil || !ok {
		t.Fatalf("load %v = ok:%v %v", key, ok, err)
	}
	return state.Phase
}

func TestRelaySync(t *testing.T) {
	ctx := context.Background()
	scope := run.Scope(sid)
	model := effect.AssignmentKey{Session: scope, RunID: "r1", Effect: "sha256:m1"}
	tool := effect.AssignmentKey{Session: scope, RunID: "r1", Effect: "sha256:t1"}
	t.Run("a started effect the executor never received is dispatched; settlement closes the process", func(t *testing.T) {
		f := newFixture(t, 3)
		f.exec.held[model] = true // the Loop's own Dispatch landed
		f.hist.add(t, "r1", run.ModelStepStarted{StepID: "st1", Effect: model.Effect})
		f.hist.add(t, "r1", run.ToolCallStarted{StepID: "st2", CallID: "c1", Effect: tool.Effect})
		rep, err := f.relay.Sync(ctx, sid, 1)
		if err != nil || rep.Requested != 2 || rep.Dispatched != 2 || rep.Next != 2 {
			t.Fatalf("sync = %+v %v", rep, err)
		}
		if f.exec.redispatch[model] != 0 || f.exec.redispatch[tool] != 1 {
			t.Fatalf("redispatches = %v, want only the tool effect", f.exec.redispatch)
		}
		if f.phase(t, model) != process.PhaseDispatched || f.phase(t, tool) != process.PhaseDispatched {
			t.Fatalf("phases = %s %s", f.phase(t, model), f.phase(t, tool))
		}
		// Replaying the same page changes nothing.
		if err := f.cps.Save(ctx, relay.Consumer, "session/"+string(sid), 0); !errors.Is(err, checkpoint.ErrRewind) {
			t.Fatalf("checkpoint rewind = %v", err)
		}
		f.hist.add(t, "r1", run.ModelStepCompleted{StepID: "st1", ResultDigest: "sha256:res"})
		f.hist.add(t, "r1", run.ToolCallFailed{StepID: "st2", CallID: "c1"})
		rep, err = f.relay.Sync(ctx, sid, 1)
		if err != nil || rep.Requested != 0 || rep.Settled != 2 || rep.Next != 4 {
			t.Fatalf("sync after settlement = %+v %v", rep, err)
		}
		if f.phase(t, model) != process.PhaseAcknowledged || f.phase(t, tool) != process.PhaseAcknowledged {
			t.Fatalf("phases = %s %s, want acknowledged", f.phase(t, model), f.phase(t, tool))
		}
		open, err := f.store.Open(ctx, scope)
		if err != nil || len(open) != 0 {
			t.Fatalf("open = %v %v, want none", open, err)
		}
		// Sync from the head is a no-op.
		if rep, err := f.relay.Sync(ctx, sid, 1); err != nil || rep.Requested+rep.Dispatched+rep.Settled != 0 || rep.Next != 4 {
			t.Fatalf("idle sync = %+v %v", rep, err)
		}
	})
	t.Run("a refused dispatch is retried across syncs and given up at the budget", func(t *testing.T) {
		f := newFixture(t, 2)
		f.exec.refuse = effect.ErrDispatchRetryable
		f.hist.add(t, "r1", run.ModelStepStarted{StepID: "st1", Effect: model.Effect})
		rep, err := f.relay.Sync(ctx, sid, 1)
		if err != nil || rep.Failed != 1 || f.phase(t, model) != process.PhaseRequested {
			t.Fatalf("first sync = %+v %v phase=%s", rep, err, f.phase(t, model))
		}
		// The checkpoint moved on; the open process is revisited anyway.
		if rep, err := f.relay.Sync(ctx, sid, 1); err != nil || rep.GivenUp != 1 || rep.Failed != 0 {
			t.Fatalf("second sync = %+v %v", rep, err)
		}
		if f.phase(t, model) != process.PhaseGivenUp || f.exec.redispatch[model] != 2 {
			t.Fatalf("phase=%s redispatches=%d", f.phase(t, model), f.exec.redispatch[model])
		}
	})
	t.Run("a lost dispatch response decides nothing", func(t *testing.T) {
		f := newFixture(t, 3)
		f.exec.refuse = effect.ErrDispatchUnknown
		f.hist.add(t, "r1", run.ToolCallStarted{StepID: "st2", CallID: "c1", Effect: tool.Effect})
		if rep, err := f.relay.Sync(ctx, sid, 1); err != nil || rep.Requested != 1 || rep.Dispatched+rep.Failed+rep.GivenUp != 0 {
			t.Fatalf("sync = %+v %v", rep, err)
		}
		f.exec.refuse = nil
		f.exec.held[tool] = true
		if rep, err := f.relay.Sync(ctx, sid, 1); err != nil || rep.Dispatched != 1 || f.phase(t, tool) != process.PhaseDispatched {
			t.Fatalf("second sync = %+v %v phase=%s", rep, err, f.phase(t, tool))
		}
	})
	t.Run("a stale owner's epoch is fenced", func(t *testing.T) {
		f := newFixture(t, 3)
		f.hist.add(t, "r1", run.ModelStepStarted{StepID: "st1", Effect: model.Effect})
		if _, err := f.relay.Sync(ctx, sid, 2); err != nil {
			t.Fatal(err)
		}
		f.hist.add(t, "r1", run.ModelStepCompleted{StepID: "st1", ResultDigest: "sha256:res"})
		if _, err := f.relay.Sync(ctx, sid, 1); !errors.Is(err, ledger.ErrFenced) {
			t.Fatalf("sync under an earlier epoch = %v, want fenced", err)
		}
	})
}
