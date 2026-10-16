package sessionstore

import (
	"context"
	"errors"

	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/store"
	"github.com/felinics/twilight/agentcore/session"
)

var _ store.EffectDispositionReader = (*bound)(nil)

// EffectDisposition reads effect-naming closing facts from the durable Run
// stream. It also finds dispositions in CancelRun commits, whose CommandID is
// not an effect-derived settlement ID. Active projections and retained local
// CommitResults are never consulted; terminal Runs remain provable.
func (b *bound) EffectDisposition(ctx context.Context, runID run.RunID, effectID run.EffectID) ([]run.Fact, bool, error) {
	if err := store.CheckContext(ctx); err != nil {
		return nil, false, err
	}
	if runID == "" || effectID == "" {
		return nil, false, errors.New("sessionstore: effect proof requires RunID and EffectID")
	}
	var from ledger.StreamSeq
	for {
		// Run streams are module.Own: inherited ancestor dispositions are
		// not proof about an execution addressed by this Session's scope.
		page, err := b.s.cfg.Store.ReadStream(ctx, session.StreamReadRequest{SessionID: b.w.SessionID(), Domain: Stream(runID), Inheritance: module.Own, From: from, Limit: 128})
		if err != nil {
			return nil, false, err
		}
		for i := range page.Events {
			decoded, err := b.s.cfg.Registry.Decode(page.Events[i])
			if err != nil {
				return nil, false, err
			}
			ev, ok := decoded.Value.(Event)
			if !ok || ev.RunID != runID {
				return nil, false, errors.New("sessionstore: effect proof event does not belong to the Run")
			}
			if store.DispositionEffect(ev.Fact) == effectID {
				return []run.Fact{ev.Fact}, true, nil
			}
		}
		if !page.HasMore {
			return nil, false, nil
		}
		if len(page.Events) == 0 {
			return nil, false, errors.New("sessionstore: effect proof stream page did not advance")
		}
		from += ledger.StreamSeq(len(page.Events))
	}
}
