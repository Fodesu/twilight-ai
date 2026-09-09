package loop

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"

	run "github.com/felinics/twilight/agent/run"
)

// attempt is one execution attempt this Loop owns. Every command identity of
// the attempt derives from its claim, which lives only in the worker's memory
// (RUN-MCH-3): a crash hands the target to the next owner's takeover.
type attempt struct {
	runID  run.RunID
	stepID run.StepID
	callID run.CallID
	claim  run.ExecutionClaim
}

func newAttempt(runID run.RunID, stepID run.StepID, callID run.CallID) attempt {
	return attempt{runID: runID, stepID: stepID, callID: callID, claim: freshExecutionClaim()}
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

// settle commits the owner settlement of an attempt under its derived
// CommandID. A sentinel rejection means the attempt is over (another actor
// moved the target); ownership loss is returned as is.
//
// When the accepted settlement terminates the Run, the terminal RunResult is
// returned: the Runtime already handed back the folded state, so the Loop
// finishes from it instead of reloading a Run the projection no longer holds.
func (l *Loop) settle(ctx context.Context, runtime boundRuntime, events EventSink, a attempt, base run.RunPosition, cmd run.AgentCommand, proto run.Protocol) (*run.RunResult, error) {
	id := a.settlementID()
	if _, recovering := cmd.(run.RecoverModelExecution); recovering {
		id = a.recoveryID()
	}
	res, err := l.commit(context.WithoutCancel(ctx), runtime, a.runID, id, base, cmd, proto)
	if err != nil {
		if retriable(err) {
			return nil, nil
		}
		return nil, err
	}
	l.emitCommitted(ctx, events, runtime.sid, a.runID, res.Events)
	if res.Snapshot.State.Status.Terminal() {
		return res.Snapshot.State.Result, nil
	}
	return nil, nil
}

func freshExecutionClaim() run.ExecutionClaim {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("agent: loop: %v", err))
	}
	return run.ExecutionClaim(hex.EncodeToString(b[:]))
}

// ownershipLost reports the terminal ownership error (RUN-LOP-5).
func ownershipLost(err error) bool { return errors.Is(err, run.ErrOwnershipLost) }
