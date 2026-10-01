package execution

import (
	"context"

	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/loop"
)

// forwardProgress subscribes to key's frames on port and emits each as a
// provisional Event on sink: this is how deltas produced wherever the
// effect runs reach the host's observation stream. It ends with the
// stream, the context, or a port that has no progress for the key.
func forwardProgress(ctx context.Context, port effect.ProgressPort, key effect.AssignmentKey, sink loop.EventSink) {
	_ = port.Progress(ctx, key, 0, func(f effect.ProgressFrame) bool {
		kind, ok := progressEventKind(f.Kind)
		if !ok {
			return true
		}
		_ = sink.Emit(ctx, loop.Event{Session: key.Session, RunID: key.RunID, Effect: key.Effect,
			Generation: f.Generation, Sequence: f.Sequence, Kind: kind, Durability: loop.EventProvisional, Payload: f.Payload})
		return true
	})
}

func progressEventKind(k effect.ProgressKind) (loop.EventKind, bool) {
	switch k {
	case effect.ProgressTextDelta:
		return loop.EventModelTextDelta, true
	case effect.ProgressReasoningDelta:
		return loop.EventModelReasoningDelta, true
	case effect.ProgressToolProgress:
		return loop.EventToolProgress, true
	case effect.ProgressReset:
		return loop.EventProgressReset, true
	default:
		return "", false
	}
}
