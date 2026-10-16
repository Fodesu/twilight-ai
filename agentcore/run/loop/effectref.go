package loop

import (
	"context"
	"errors"
	"fmt"

	run "github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/schema"
	"github.com/felinics/twilight/agentcore/run/store"
)

// effectRef is one effect of a Run as this Loop addresses it: the derived
// EffectID the start fact records, with the schema whose Identity derives the
// start, settlement and recovery CommandIDs from it. It holds
// nothing of the physical execution -- worker, lease and backend handle stay
// in the executor -- so any owner that reads the same state derives the same
// identities and a repeated command replays instead of granting twice.
type effectRef struct {
	runID run.RunID
	id    run.EffectID
}

// modelEffect is the next model effect of a Prepared ModelStep: its sequence
// is the count of results the step rejected so far, so a step started again
// after a rejection requests a new effect.
func modelEffect(runID run.RunID, step *run.ModelStep) effectRef {
	return effectRef{runID: runID, id: schema.Identity().DeriveEffectID(runID, step.RefValue.ID, "", step.Rejects)}
}

// toolEffect is the one tool effect of a call: a call starts at most once.
func toolEffect(runID run.RunID, stepID run.StepID, callID run.CallID) effectRef {
	return effectRef{runID: runID, id: schema.Identity().DeriveEffectID(runID, stepID, callID, 0)}
}

func (e *effectRef) startID() run.CommandID { return schema.Identity().DeriveStartCommandID(e.id) }
func (e *effectRef) settlementID() run.CommandID {
	return schema.Identity().DeriveSettlementCommandID(e.id)
}
func (e *effectRef) recoveryID() run.CommandID {
	return schema.Identity().DeriveRecoveryCommandID(e.id)
}

// settle commits the owner settlement of an effect under its derived
// CommandID. A sentinel rejection means the effect is over (another actor
// moved the target); ownership loss is returned as is.
// When the accepted settlement terminates the Run, the terminal RunResult is
// returned: the RunStore already handed back the folded state, so the Loop
// finishes from it instead of reloading a Run the projection no longer holds.
func (l *Loop) settle(ctx context.Context, rt store.RunStore, events EventSink, e *effectRef, base run.RunPosition, cmd run.AgentCommand) (*run.RunResult, error) {
	id := e.settlementID()
	_, recovering := cmd.(run.RecoverModelExecution)
	if recovering {
		id = e.recoveryID()
	}
	res, err := l.commit(ctx, rt, e.runID, id, base, cmd)
	if err != nil {
		if retriable(err) {
			return nil, nil
		}
		return nil, err
	}
	l.emitCommitted(ctx, events, rt.Scope(), e.runID, res.Facts)
	// These accepted facts are durable proof, not merely a dropped delivery
	// or a Run that left the projection. ACK is best effort; a crash/error
	// here is repaired by AcknowledgeSettled from the durable effect ledger.
	// Recovery can also precede dispatch: close a missing key, and never
	// collect an accepted execution while it is still nonterminal.
	key := AssignmentKey{Session: rt.Scope(), RunID: e.runID, Effect: e.id}
	if effectDisposition(res.Facts, e.id) {
		_, _ = l.acknowledgeProven(ctx, key)
	}
	if res.Snapshot.State.Status.Terminal() {
		return res.Snapshot.State.Result, nil
	}
	return nil, nil
}

// AcknowledgeSettled repairs the window between durable owner settlement and
// executor ACK. It is safe after a dropped Deliver, but LoopDropped itself is
// never proof. The optional EffectDispositionReader must resolve the effect's
// closing facts from durable storage, including CancelRun dispositions and
// Runs no longer held in an active projection. No command is replayed or decided.
//
// Only facts naming this effect's disposition qualify. Even a recovery fact
// is not proof execution ended: Abort must first prove a terminal execution
// or fence a missing key with a tombstone. Active/orphaned executions are
// left readable and untouched. Unknown external effects are never replayed.
// true means ACK succeeded (including an idempotent repeat); false means no
// proof, no Acknowledger, an aborted key, or execution still nonterminal.
func (l *Loop) AcknowledgeSettled(ctx context.Context, st store.RunStore, key AssignmentKey) (bool, error) {
	if err := l.checkArgs(ctx, st, key.RunID); err != nil {
		return false, err
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if key.Session != st.Scope() || key.Effect == "" {
		return false, fmt.Errorf("agent: loop: ACK key does not belong to the store or has no effect")
	}
	proof, ok := st.(store.EffectDispositionReader)
	if !ok {
		return false, store.ErrEffectProofUnsupported
	}
	facts, found, err := proof.EffectDisposition(ctx, key.RunID, key.Effect)
	if err != nil {
		return false, err
	}
	if !found || !effectDisposition(facts, key.Effect) {
		return false, nil
	}
	return l.acknowledgeProven(ctx, key)
}

// effectDisposition requires an effect-naming closing fact, not merely a
// commit-index hit, a start, RunEnded, or a settlement of another effect.
func effectDisposition(facts []run.Fact, id run.EffectID) bool {
	if id == "" {
		return false
	}
	for _, fact := range facts {
		if store.DispositionEffect(fact) == id {
			return true
		}
	}
	return false
}

// acknowledgeProven is called only with durable disposition proof in hand.
// Abort never disposes an accepted execution. It fences a missing key against
// late Dispatch, or reports the accepted ledger's state. Lease expiry and
// missing backend handles cannot pass this terminal gate.
func (l *Loop) acknowledgeProven(ctx context.Context, key AssignmentKey) (bool, error) {
	a, ok := l.Executor.(Acknowledger)
	if !ok {
		return false, nil
	}
	att, err := l.Executor.Abort(ctx, key)
	if err != nil {
		return false, err
	}
	if att.State != AttachmentTerminal || !att.Execution.Terminal() || att.Execution == ExecutionAborted {
		return false, nil
	}
	if err := a.Acknowledge(ctx, key); err != nil {
		return false, err
	}
	return true, nil
}

// ownershipLost reports the terminal ownership error.
func ownershipLost(err error) bool { return errors.Is(err, store.ErrOwnershipLost) }
