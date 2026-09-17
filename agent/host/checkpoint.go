package host

import (
	"context"
	"fmt"

	"github.com/felinics/twilight/agent/context/compaction"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/chatlog"
	"github.com/felinics/twilight/agent/session/writer"
	"github.com/felinics/twilight/agent/turn"
)

// Checkpoint commits a summary and its checkpoint in one group (CHT-EVT-3);
// it is the chatlog Service's command with the Host's quiescence rule as the
// guard. A Turn must not be active.
func (h *Host) Checkpoint(ctx context.Context, sid session.SessionID, summaryText string, retain []chatlog.EntryDigestPair) (chatlog.CheckpointID, error) {
	guard := func(v writer.View) error {
		state, err := v.Projection(turn.SurfaceProjectionID, turn.SurfaceProjection.Version)
		if err != nil {
			return err
		}
		tsurf := state.(turn.TurnSurface)
		if active, ok := tsurf.Active(); ok {
			return fmt.Errorf("%w: checkpoint while turn %s is active", turn.ErrConflict, active.TurnID)
		}
		return nil
	}
	return h.chatlog.Checkpoint(ctx, sid, summaryText, retain, guard)
}

// Compact summarizes the context with the preset's model and commits a
// checkpoint retaining a pair-closed suffix; ok is false when the context is
// already within the retain window (HST-CKP-1).
func (s *Session) Compact(ctx context.Context) (chatlog.CheckpointID, bool, error) {
	policy := compaction.Policy{AfterEntries: s.opts.CompactAfterEntries, RetainEntries: s.opts.CompactRetainEntries}
	state, _, err := s.h.Projection(ctx, s.sid, chatlog.ContextProjectionID, chatlog.ContextProjection.Version)
	if err != nil {
		return "", false, err
	}
	entries := state.(chatlog.Context).Entries
	retain, withinWindow := policy.Retain(entries)
	if withinWindow {
		return "", false, nil
	}
	materialized, err := chatlog.NewMaterializer(s.h.content).Entries(ctx, entries)
	if err != nil {
		return "", false, err
	}
	summary, err := compaction.Summarizer{
		ResolvePreset: s.h.Presets.Resolve,
		Content:       s.h.frozen,
		Executor:      s.h.Executor,
	}.Summarize(ctx, s.sid, s.opts.Preset, materialized)
	if err != nil {
		return "", false, err
	}
	id, err := s.h.Checkpoint(ctx, s.sid, summary, retain)
	if err != nil {
		return "", false, err
	}
	return id, true, nil
}

// maybeCompact runs the automatic policy after a settlement; failures reach
// the caller through CompactWarn and never change the settled results.
func (s *Session) maybeCompact(ctx context.Context) {
	if s.opts.CompactAfterEntries <= 0 {
		return
	}
	state, _, err := s.h.Projection(ctx, s.sid, chatlog.ContextProjectionID, chatlog.ContextProjection.Version)
	if err == nil && len(state.(chatlog.Context).Entries) <= s.opts.CompactAfterEntries {
		return
	}
	if err == nil {
		_, _, err = s.Compact(ctx)
	}
	if err != nil && s.opts.CompactWarn != nil {
		s.opts.CompactWarn(err)
	}
}
