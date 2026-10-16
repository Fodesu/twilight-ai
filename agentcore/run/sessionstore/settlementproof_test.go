package sessionstore

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/schema"
	"github.com/felinics/twilight/agentcore/run/store"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/filestore/filestoretest"
	"github.com/felinics/twilight/agentcore/session/writer"
)

// Proof uses no Writer projection, command evaluation, or ownership operation.
// Only the bound scope is needed, and any other Writer access would panic.
type proofScope struct {
	writer.Writer
	sid session.SessionID
}

func (w proofScope) SessionID() session.SessionID { return w.sid }

func appendProofFacts(t *testing.T, h session.Handle, reg *module.Registry, runID run.RunID, id ledger.CommitID, facts ...run.Fact) ledger.Commit {
	t.Helper()
	events := make([]ledger.Event, len(facts))
	for i, fact := range facts {
		payload, err := reg.Encode(EventType(fact), Event{RunID: runID, Fact: fact})
		if err != nil {
			t.Fatal(err)
		}
		events[i] = ledger.Event{Type: EventType(fact), Payload: payload}
	}
	c, err := h.Append(context.Background(), ledger.Proposal{CommitID: id, Batches: []ledger.EventBatch{{Domain: Stream(runID), Events: events}}})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestEffectProofSurvivesTerminalRunAndFindsCancelDisposition(t *testing.T) {
	ctx := context.Background()
	durable := filestoretest.Store(t)
	reg, err := module.BuildRegistry(Module)
	if err != nil {
		t.Fatal(err)
	}
	const sid session.SessionID = "effect-proof"
	if _, err := durable.Create(ctx, session.CreateRequest{SessionID: sid}); err != nil {
		t.Fatal(err)
	}
	h, err := durable.Open(ctx, sid, session.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(ctx)
	const runID run.RunID = "run"
	const effectID run.EffectID = "model-effect"
	prefix := []run.Fact{run.RunCreated{RunID: runID},
		run.ModelStepPrepared{StepID: "step", Model: "m", RequestDigest: "sha256:req"},
		run.ModelStepStarted{StepID: "step", Effect: effectID}}
	appendProofFacts(t, h, reg, runID, "prefix", prefix...)
	// Cross a stream page boundary before the disposition.
	for i := 0; i < 130; i++ {
		f := run.InputAccepted{Input: run.AgentInput{ID: run.InputID(fmt.Sprintf("input-%d", i)), Digest: "sha256:input"}}
		appendProofFacts(t, h, reg, runID, ledger.CommitID(fmt.Sprintf("input-%d", i)), f)
		prefix = append(prefix, f)
	}
	state := run.MachineState{RunID: runID, Status: run.RunActive, Current: run.ModelStep{RefValue: run.StepRef{RunID: runID, ID: "step"}, Status: run.ModelExecuting, Effect: effectID}}
	closing, err := schema.Machine().Decide(state, run.CancelRun{})
	if err != nil {
		t.Fatal(err)
	}
	cancelCommit := appendProofFacts(t, h, reg, runID, "cancel-run-not-an-effect-command", closing...)
	// The actual machine projection removes this terminal Run. That is not
	// absence of a durable disposition proof.
	machine := newMachine()
	for _, fact := range append(prefix, closing...) {
		machine, err = machine.Apply(module.DecodedEvent{Domain: Stream(runID), Value: Event{RunID: runID, Fact: fact}})
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, active := machine.Active[runID]; active {
		t.Fatal("terminal Run remained in projection")
	}
	// A fresh adapter has neither the command result nor a machine cache.
	adapter := &SessionRunStore{cfg: Config{Store: durable, Registry: reg}}
	proof := adapter.Bind(proofScope{sid: sid}).(store.EffectDispositionReader)
	facts, found, err := proof.EffectDisposition(ctx, runID, effectID)
	if err != nil || !found || len(facts) != 1 || store.DispositionEffect(facts[0]) != effectID {
		t.Fatalf("cancel proof=%+v found=%v err=%v", facts, found, err)
	}
	if _, ok := facts[0].(run.ModelStepFailed); !ok {
		t.Fatalf("cancel proof=%T", facts[0])
	}
	for _, query := range []struct {
		run    run.RunID
		effect run.EffectID
	}{{runID, "other"}, {"other-run", effectID}} {
		if facts, found, err := proof.EffectDisposition(ctx, query.run, query.effect); err != nil || found || len(facts) != 0 {
			t.Fatalf("foreign query=%+v proof=%+v found=%v err=%v", query, facts, found, err)
		}
	}
	// Run is module.Own: an inherited parent settlement cannot authorize ACK
	// of a child's different AssignmentKey, even with identical IDs.
	const child session.SessionID = "child"
	if _, err := durable.Create(ctx, session.CreateRequest{SessionID: child, Fork: &session.ForkOrigin{Session: sid, Seq: cancelCommit.Seq}}); err != nil {
		t.Fatal(err)
	}
	childProof := adapter.Bind(proofScope{sid: child}).(store.EffectDispositionReader)
	if facts, found, err := childProof.EffectDisposition(ctx, runID, effectID); err != nil || found || len(facts) != 0 {
		t.Fatalf("inherited effect was accepted as local proof: %+v found=%v err=%v", facts, found, err)
	}
}

func TestEffectProofFindsEveryToolInSingleCancelCommit(t *testing.T) {
	ctx := context.Background()
	durable := filestoretest.Store(t)
	reg, err := module.BuildRegistry(Module)
	if err != nil {
		t.Fatal(err)
	}
	const sid session.SessionID = "tools-proof"
	if _, err := durable.Create(ctx, session.CreateRequest{SessionID: sid}); err != nil {
		t.Fatal(err)
	}
	h, err := durable.Open(ctx, sid, session.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(ctx)
	state := run.MachineState{RunID: "run", Status: run.RunActive, Current: run.ToolStep{RefValue: run.StepRef{RunID: "run", ID: "tools"}, Calls: []run.ToolCallState{
		{CallID: "call-1", Status: run.ToolExecuting, Effect: "tool-1"},
		{CallID: "call-2", Status: run.ToolExecuting, Effect: "tool-2"},
	}}}
	facts, err := schema.Machine().Decide(state, run.CancelRun{})
	if err != nil {
		t.Fatal(err)
	}
	appendProofFacts(t, h, reg, "run", "cancel-both-tools", facts...)
	adapter := &SessionRunStore{cfg: Config{Store: durable, Registry: reg}}
	proof := adapter.Bind(proofScope{sid: sid}).(store.EffectDispositionReader)
	for _, id := range []run.EffectID{"tool-1", "tool-2"} {
		facts, found, err := proof.EffectDisposition(ctx, "run", id)
		if err != nil || !found || len(facts) != 1 || store.DispositionEffect(facts[0]) != id {
			t.Fatalf("%s proof=%+v found=%v err=%v", id, facts, found, err)
		}
	}
}

type failedProofStore struct {
	session.Store
	err   error
	stall bool
}

func (s failedProofStore) ReadStream(context.Context, session.StreamReadRequest) (session.StreamPage, error) {
	return session.StreamPage{HasMore: s.stall}, s.err
}

func TestEffectProofReadFailureIsNotAbsence(t *testing.T) {
	boom := errors.New("ledger unavailable")
	for _, s := range []failedProofStore{{err: boom}, {stall: true}} {
		adapter := &SessionRunStore{cfg: Config{Store: s}}
		proof := adapter.Bind(proofScope{sid: "session"}).(store.EffectDispositionReader)
		_, found, err := proof.EffectDisposition(context.Background(), "run", "effect")
		if err == nil || found || (s.err != nil && !errors.Is(err, boom)) {
			t.Fatalf("proof read found=%v err=%v", found, err)
		}
	}
}
