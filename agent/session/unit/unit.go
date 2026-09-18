// Package unit is the one place a cross-module commit of a Session is
// assembled (SES-ATM). A Work names a CommitID and the Parts that write under
// it; every Part prepares its own module's events against the same
// transactional View, and the Writer appends them as one commit or nothing.
// No module builds another module's events: the Turn does not encode Run
// facts, the Run does not carry chatlog events. A Part that has to refuse
// (a precondition of its module fails) refuses the whole unit.
package unit

import (
	"context"
	"errors"

	"github.com/felinics/twilight/agent/es"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/writer"
)

// Part is one module's contribution to a unit of work.
type Part interface {
	// Prepare runs inside the Writer's critical section against the View
	// every other Part of the unit sees. It returns the batches its module
	// writes for this commit, possibly none, or an error, which refuses the
	// unit and writes nothing. now is the commit's RecordedAtUnixMilli.
	Prepare(ctx context.Context, view writer.View, now int64) ([]writer.TypedBatch, error)
}

// PartFunc adapts a function to Part.
type PartFunc func(ctx context.Context, view writer.View, now int64) ([]writer.TypedBatch, error)

func (f PartFunc) Prepare(ctx context.Context, view writer.View, now int64) ([]writer.TypedBatch, error) {
	return f(ctx, view, now)
}

// Work is one atomic commit: its identity and the Parts that write under it,
// in the order their batches are laid out.
type Work struct {
	CommitID session.CommitID
	// Intent is the digest of the operation this unit realizes: the command
	// envelope, the request, whatever decides the events. It is required and
	// sealed into the commit; a later unit with the same CommitID is judged by
	// it: the same intent is already applied, a different one is a conflict,
	// even after the state moved so far that the Parts could not rebuild the
	// group (EXT-WRT-2). Intent() computes it from any canonical value.
	Intent es.Digest
	Parts  []Part
}

// Intent digests the canonical JSON of v as a unit's Intent.
func Intent(v any) (es.Digest, error) { return es.DigestCanonical(v) }

// Commit appends the unit through w. A CommitID the log already holds is
// judged without preparing any Part: the same Intent is CommitAlreadyApplied
// with the sealed commit; a different Intent, or a commit that was sealed
// without one and so cannot be compared, is CommitConflict. Otherwise
// every Part prepares against one View and their batches are merged by
// stream in Part order, so the commit holds at most one batch per stream. A
// Part error is returned as is with nothing written; the Writer's own
// verdicts (CommitConflict, CommitInvalid) come back in the result.
func Commit(ctx context.Context, w writer.Writer, now int64, work Work) (writer.CommitResult, error) {
	if w == nil {
		return writer.CommitResult{}, errors.New("unit: nil writer")
	}
	if work.CommitID == "" {
		return writer.CommitResult{}, errors.New("unit: empty CommitID")
	}
	if work.Intent == "" {
		return writer.CommitResult{}, errors.New("unit: a unit of work declares its Intent")
	}
	var replay *session.Commit
	res, err := w.Commit(ctx, func(view writer.View) (*writer.SemanticGroup, error) {
		if existing, found, err := view.LookupCommit(work.CommitID); err != nil {
			return nil, err
		} else if found {
			replay = &existing
			return nil, nil
		}
		group := &writer.SemanticGroup{CommitID: work.CommitID, Intent: work.Intent}
		index := map[session.StreamRef]int{}
		for _, p := range work.Parts {
			batches, err := p.Prepare(ctx, view, now)
			if err != nil {
				return nil, err
			}
			for _, b := range batches {
				if len(b.Events) == 0 {
					continue
				}
				if i, ok := index[b.Stream]; ok {
					group.Batches[i].Events = append(group.Batches[i].Events, b.Events...)
					continue
				}
				index[b.Stream] = len(group.Batches)
				group.Batches = append(group.Batches, writer.TypedBatch{Stream: b.Stream, Events: append([]writer.TypedEvent(nil), b.Events...)})
			}
		}
		if len(group.Batches) == 0 {
			return nil, errors.New("unit: no part wrote an event")
		}
		return group, nil
	})
	if err != nil {
		return writer.CommitResult{}, err
	}
	if replay != nil {
		switch {
		case replay.Intent == "":
			return writer.CommitResult{Outcome: writer.CommitConflict, Detail: "CommitID was committed without an intent; the replay cannot be verified"}, nil
		case replay.Intent != work.Intent:
			return writer.CommitResult{Outcome: writer.CommitConflict, Detail: "same CommitID, different intent"}, nil
		}
		return writer.CommitResult{Outcome: writer.CommitAlreadyApplied, Commit: *replay}, nil
	}
	return res, nil
}

// Events returns the events of a commit in batch order.
func Events(c session.Commit) []session.Event {
	var out []session.Event
	for _, b := range c.Batches {
		out = append(out, b.Events...)
	}
	return out
}
