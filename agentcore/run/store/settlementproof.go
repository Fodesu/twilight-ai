package store

import (
	"context"
	"errors"

	"github.com/felinics/twilight/agentcore/run"
)

// ErrEffectProofUnsupported means a RunStore cannot prove durable effect
// disposition. It must not be interpreted as absence of a settlement.
var ErrEffectProofUnsupported = errors.New("agent: run store: durable effect proof unsupported")

// EffectDispositionReader is an optional read-only capability next to
// RunStore. It speaks only Run types. EffectDisposition returns committed
// closing facts naming exactly this Run and EffectID, from the durable Run
// ledger or an index of it, never from an active-state projection. found is
// false only for proven absence; read failures are errors, not absence. Facts
// are detached and proof survives a terminal Run leaving the projection.
//
// Proof is by effect fact, not only a derived settlement CommandID: CancelRun
// can close several effects in one differently named commit. A recovery or
// cancellation disposition does not prove execution terminated, so callers
// must also close/probe the execution before allowing Outcome collection.
type EffectDispositionReader interface {
	EffectDisposition(context.Context, run.RunID, run.EffectID) (facts []run.Fact, found bool, err error)
}

// DispositionEffect names the effect a closing fact settles or recovers.
// Starts, RunEnded, and facts about effects that never started return empty.
func DispositionEffect(fact run.Fact) run.EffectID {
	switch f := fact.(type) {
	case run.ModelStepCompleted:
		return f.Effect
	case run.ModelStepFailed:
		return f.Effect
	case run.ModelStepRejected:
		return f.Effect
	case run.ModelStepRecovered:
		return f.Effect
	case run.ToolCallCompleted:
		return f.Effect
	case run.ToolCallFailed:
		return f.Effect
	default:
		return ""
	}
}
