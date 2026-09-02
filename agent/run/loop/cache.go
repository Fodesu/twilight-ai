package loop

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"

	run "github.com/memohai/twilight/agent/run"
)

// ClaimStore is the host-injected record of this Loop's live execution
// claims. A claim is the only local state a Loop needs to reclaim an accepted
// start after its own process died: with the claim, the start CommandID, the
// settlement CommandID and the recovery CommandID all derive (RUN-WIR-3).
//
// The in-process default (memoryClaims) gives response-loss recovery within
// one process. A durable ClaimStore lets a replacement process replay the
// settlement of a tool call the dead process had finished but not reported,
// instead of waiting for lease expiry.
//
// Implementations must be safe for concurrent use. Put replaces any existing
// claim for the key; Delete of a missing key is a no-op.
type ClaimStore interface {
	Put(ctx context.Context, runID run.RunID, stepID run.StepID, callID run.CallID, claim run.ExecutionClaim) error
	Get(ctx context.Context, runID run.RunID, stepID run.StepID, callID run.CallID) (run.ExecutionClaim, bool, error)
	Delete(ctx context.Context, runID run.RunID, stepID run.StepID, callID run.CallID) error
	// DeleteRun forgets every claim of a finished Run.
	DeleteRun(ctx context.Context, runID run.RunID) error
}

type claimKey struct {
	runID  run.RunID
	stepID run.StepID
	callID run.CallID
}

// memoryClaims is the default in-process ClaimStore.
type memoryClaims struct {
	mu     sync.Mutex
	claims map[claimKey]run.ExecutionClaim
}

func newMemoryClaims() *memoryClaims {
	return &memoryClaims{claims: make(map[claimKey]run.ExecutionClaim)}
}

func (m *memoryClaims) Put(_ context.Context, runID run.RunID, stepID run.StepID, callID run.CallID, claim run.ExecutionClaim) error {
	m.mu.Lock()
	m.claims[claimKey{runID, stepID, callID}] = claim
	m.mu.Unlock()
	return nil
}

func (m *memoryClaims) Get(_ context.Context, runID run.RunID, stepID run.StepID, callID run.CallID) (run.ExecutionClaim, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.claims[claimKey{runID, stepID, callID}]
	return c, ok, nil
}

func (m *memoryClaims) Delete(_ context.Context, runID run.RunID, stepID run.StepID, callID run.CallID) error {
	m.mu.Lock()
	delete(m.claims, claimKey{runID, stepID, callID})
	m.mu.Unlock()
	return nil
}

func (m *memoryClaims) DeleteRun(_ context.Context, runID run.RunID) error {
	m.mu.Lock()
	for k := range m.claims {
		if k.runID == runID {
			delete(m.claims, k)
		}
	}
	m.mu.Unlock()
	return nil
}

// attempt is one execution attempt this Loop owns or is trying to own. Every
// command identity of the attempt derives from the claim.
type attempt struct {
	runID  run.RunID
	stepID run.StepID
	callID run.CallID
	claim  run.ExecutionClaim
}

func (a attempt) startID() run.CommandID {
	return run.DeriveStartCommandID(a.runID, a.stepID, a.callID, a.claim)
}

func (a attempt) settlementID() run.CommandID {
	return run.DeriveSettlementCommandID(a.runID, a.stepID, a.callID, a.claim)
}

func (a attempt) recoveryID() run.CommandID {
	return run.DeriveModelRecoveryCommandID(a.runID, a.stepID, a.claim)
}

// claimFor returns the attempt for key, reusing a stored claim when the Loop
// (or a predecessor process sharing the ClaimStore) already started it, and
// minting and storing a fresh claim otherwise.
func (l *Loop) claimFor(ctx context.Context, runID run.RunID, stepID run.StepID, callID run.CallID) (attempt, error) {
	claim, ok, err := l.Claims.Get(ctx, runID, stepID, callID)
	if err != nil {
		return attempt{}, fmt.Errorf("agent: loop: claim store: %w", err)
	}
	if !ok {
		claim = freshExecutionClaim()
		if err := l.Claims.Put(ctx, runID, stepID, callID, claim); err != nil {
			return attempt{}, fmt.Errorf("agent: loop: claim store: %w", err)
		}
	}
	return attempt{runID: runID, stepID: stepID, callID: callID, claim: claim}, nil
}

// hasClaim reports whether a claim for the target is already stored.
func (l *Loop) hasClaim(ctx context.Context, runID run.RunID, stepID run.StepID, callID run.CallID) (bool, error) {
	_, ok, err := l.Claims.Get(ctx, runID, stepID, callID)
	if err != nil {
		return false, fmt.Errorf("agent: loop: claim store: %w", err)
	}
	return ok, nil
}

func (l *Loop) forgetClaim(ctx context.Context, a attempt) {
	_ = l.Claims.Delete(context.WithoutCancel(ctx), a.runID, a.stepID, a.callID)
}

func (l *Loop) forgetRunClaims(ctx context.Context, runID run.RunID) {
	_ = l.Claims.DeleteRun(context.WithoutCancel(ctx), runID)
}

// settle commits the owner settlement of an attempt under its derived
// CommandID. On success or on a sentinel rejection the claim is released:
// the attempt is over either way. A transport failure keeps the claim so the
// next Run (in this or a replacement process) replays the same settlement.
func (l *Loop) settle(ctx context.Context, runtime run.Runtime, events EventSink, a attempt, base uint64, grant run.ExecutionGrant, cmd run.AgentCommand, proto run.Protocol) error {
	id := a.settlementID()
	if _, recovering := cmd.(run.RecoverModelExecution); recovering {
		id = a.recoveryID()
	}
	res, err := l.commit(context.WithoutCancel(ctx), runtime, a.runID, id, base, grant, cmd, proto)
	if err != nil {
		if retriable(err) {
			l.forgetClaim(ctx, a)
			return nil
		}
		return err
	}
	l.forgetClaim(ctx, a)
	l.emitCommitted(ctx, events, a.runID, res.Events)
	return nil
}

// resumeOwnedStarts re-enters every Executing target this Loop holds a claim
// for. With a durable ClaimStore this is how a replacement process finishes
// what its predecessor started: the derived start ID replays and returns the
// live grant, then the effect runs (or re-runs) and settles.
func (l *Loop) resumeOwnedStarts(ctx context.Context, runtime run.Runtime, events EventSink, snapshot *run.RuntimeSnapshot) (bool, error) {
	runID := snapshot.State.RunID
	switch current := snapshot.State.Current.(type) {
	case run.ModelStep:
		if current.Status != run.ModelExecuting {
			return false, nil
		}
		if ok, err := l.hasClaim(ctx, runID, current.RefValue.ID, ""); err != nil || !ok {
			return false, err
		}
		return true, l.runModelStep(ctx, runtime, events, snapshot, current.RefValue.ID)
	case run.ToolStep:
		var ids []run.CallID
		for _, call := range current.Calls {
			if call.Status != run.ToolExecuting {
				continue
			}
			if ok, err := l.hasClaim(ctx, runID, current.RefValue.ID, call.CallID); err != nil {
				return false, err
			} else if ok {
				ids = append(ids, call.CallID)
			}
		}
		if len(ids) == 0 {
			return false, nil
		}
		return true, l.runToolCalls(ctx, runtime, events, snapshot, run.StartToolCalls{StepID: current.RefValue.ID, CallIDs: ids})
	default:
		return false, nil
	}
}

var errNoClaimStore = errors.New("agent: loop: nil claim store")

func freshExecutionClaim() run.ExecutionClaim {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("agent: loop: %v", err))
	}
	return run.ExecutionClaim(hex.EncodeToString(b[:]))
}
