package chatlog

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/felinics/twilight/agent/es"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/writer"
)

// Commands are the chatlog's canonical commands -- submitting and
// withdrawing inputs, committing checkpoints -- each through the Writer the
// caller owns (CHT-EVT, AUTH-OWN-2). They are the only writer of chatlog
// facts: callers go through these methods instead of building chatlog
// TypedEvents by hand.
type Commands struct {
	Now func() time.Time
}

// Guard is a caller-supplied precondition evaluated inside a command's
// commit critical section, on the same View the command reads. Checkpoint
// uses it for the "no Turn may be active" rule (APP-CKP-1), which is turn
// domain policy this package cannot import.
type Guard func(v writer.View) error

// ErrNotSubmitted reports a withdraw of an input that is not in the
// submitted state (CHT-EVT-2).
var ErrNotSubmitted = errors.New("chatlog: input is not submitted")

// TextContent builds the v1 user input body (DEC-INP-1): the canonical JSON
// {"text":"..."} an Input.Content carries.
func TextContent(text string) run.CanonicalJSON {
	raw, err := es.MarshalCanonical(text)
	if err != nil {
		panic(err) // a string always marshals
	}
	return run.MustParseCanonicalJSON(`{"text":` + string(raw) + `}`)
}

// Submit records one user input as submitted (CHT-EVT-1); idempotency
// rides on the CommitID, so a retried submission replays.
func (s *Commands) Submit(ctx context.Context, w writer.Writer, id run.InputID, text string) (run.AgentInput, error) {
	content := TextContent(text)
	res, err := w.Commit(ctx, func(writer.View) (*writer.SemanticGroup, error) {
		return &writer.SemanticGroup{CommitID: session.CommitID("input-submitted/" + string(id)),
			Batches: []writer.TypedBatch{{Stream: session.StreamRef{Kind: session.StreamKindSession}, Events: []writer.TypedEvent{{
				Type: TypeInputSubmitted, RecordedAtUnixMilli: s.Now().UnixMilli(),
				Value: InputSubmittedPayload{InputID: InputID(id), Content: content, SubmittedAtUnixMilli: s.Now().UnixMilli()},
			}}}}}, nil
	})
	if err != nil {
		return run.AgentInput{}, err
	}
	switch res.Outcome {
	case writer.CommitApplied, writer.CommitAlreadyApplied:
		return run.AgentInput{ID: id, Payload: content}, nil
	default:
		return run.AgentInput{}, fmt.Errorf("chatlog: submit input: %s: %s", res.Outcome, res.Detail)
	}
}

// Withdraw marks a submitted, undelivered input as withdrawn
// (CHT-EVT-2), for example the original input of a Turn the caller forked
// before in order to edit it (AUTH-FRK-2).
func (s *Commands) Withdraw(ctx context.Context, w writer.Writer, id run.InputID, reason string) error {
	res, err := w.Commit(ctx, func(v writer.View) (*writer.SemanticGroup, error) {
		state, err := v.Projection(SurfaceProjectionID, SurfaceProjection.Version)
		if err != nil {
			return nil, err
		}
		view, ok := state.(Surface).Inputs.Get(InputID(id))
		if !ok || view.Status != InputSubmitted {
			return nil, fmt.Errorf("%w: input %s is not a submitted input", ErrNotSubmitted, id)
		}
		return &writer.SemanticGroup{CommitID: session.CommitID("input-withdrawn/" + string(id)),
			Batches: []writer.TypedBatch{{Stream: session.StreamRef{Kind: session.StreamKindSession}, Events: []writer.TypedEvent{{
				Type: TypeInputWithdrawn, RecordedAtUnixMilli: s.Now().UnixMilli(),
				Value: InputWithdrawnPayload{InputID: InputID(id), Reason: reason},
			}}}}}, nil
	})
	if err != nil {
		return err
	}
	switch res.Outcome {
	case writer.CommitApplied, writer.CommitAlreadyApplied:
		return nil
	default:
		return fmt.Errorf("chatlog: withdraw input: %s: %s", res.Outcome, res.Detail)
	}
}

// Checkpoint commits a summary and its checkpoint in one group (CHT-EVT-3).
// The base is read inside the commit's critical section, so the digest pins
// exactly the context being replaced. guard (when non-nil) runs in the same
// critical section first; callers pass the active-Turn rule there.
// retain names entries of the current context (compaction.RetainLast builds
// a pair-closed suffix, which CheckRetainClosure verifies).
func (s *Commands) Checkpoint(ctx context.Context, w writer.Writer, summaryText string, retain []EntryDigestPair, guard Guard) (CheckpointID, error) {
	if strings.TrimSpace(summaryText) == "" {
		return "", errors.New("chatlog: checkpoint requires a summary text")
	}
	var checkpointID CheckpointID
	res, err := w.Commit(ctx, func(v writer.View) (*writer.SemanticGroup, error) {
		if guard != nil {
			if err := guard(v); err != nil {
				return nil, err
			}
		}
		cstate, err := v.Projection(ContextProjectionID, ContextProjection.Version)
		if err != nil {
			return nil, err
		}
		entries := cstate.(Context).Entries
		if len(entries) == 0 {
			return nil, errors.New("chatlog: checkpoint over an empty context")
		}
		if err := CheckRetainClosure(entries, retain); err != nil {
			return nil, err
		}
		pairs := make([]EntryDigestPair, len(entries))
		for i := range entries {
			pairs[i] = entries[i].Pair()
		}
		baseDigest, err := DigestBaseContext(pairs)
		if err != nil {
			return nil, err
		}
		var summaryID SummaryID
		if checkpointID, summaryID, err = checkpointIDs(w.SessionID(), baseDigest, summaryText); err != nil {
			return nil, err
		}
		summary := Summary{ID: summaryID, Parts: Parts{TextPart{Text: summaryText}}}
		if summary.Digest, err = DigestSummary(&summary); err != nil {
			return nil, err
		}
		payload := CheckpointCreatedPayload{
			CheckpointID: checkpointID, CoveredThrough: entries[len(entries)-1].Seq,
			BaseContextDigest: baseDigest, SummaryID: summaryID, SummaryDigest: summary.Digest,
			Retained: retain,
		}
		if payload.Digest, err = DigestCheckpoint(&payload); err != nil {
			return nil, err
		}
		now := s.Now().UnixMilli()
		return &writer.SemanticGroup{CommitID: session.CommitID("checkpoint/" + string(checkpointID)),
			Batches: []writer.TypedBatch{{Stream: session.StreamRef{Kind: session.StreamKindSession}, Events: []writer.TypedEvent{
				{Type: TypeSummary, RecordedAtUnixMilli: now, Value: SummaryPayload{Summary: summary}},
				{Type: TypeCheckpointCreated, RecordedAtUnixMilli: now, Value: payload},
			}}}}, nil
	})
	if err != nil {
		return "", err
	}
	switch res.Outcome {
	case writer.CommitApplied, writer.CommitAlreadyApplied:
		return checkpointID, nil
	default:
		return "", fmt.Errorf("chatlog: checkpoint: %s: %s", res.Outcome, res.Detail)
	}
}

// checkpointIDs derives the checkpoint and summary identifiers from what the
// checkpoint replaces: the Session, the base context digest and the summary
// text. A Checkpoint retried over the same base therefore carries the same
// CommitID and is answered as already applied instead of writing a second
// checkpoint (APP-CKP-1).
func checkpointIDs(sid session.SessionID, base es.Digest, summaryText string) (CheckpointID, SummaryID, error) {
	raw, err := es.EncodeTypedPayload(uint16(session.ProtocolVersion1), "twilight/chatlog/checkpoint", struct {
		SessionID session.SessionID `json:"sessionId"`
		Base      es.Digest         `json:"base"`
		Summary   string            `json:"summary"`
	}{sid, base, summaryText})
	if err != nil {
		return "", "", err
	}
	// The digest is "sha256:<hex>"; the IDs carry the first 16 hex digits.
	d := string(es.DigestBytes(raw))
	const prefix = "sha256:"
	if len(d) > len(prefix) && d[:len(prefix)] == prefix {
		d = d[len(prefix):]
	}
	if len(d) > 16 {
		d = d[:16]
	}
	return CheckpointID("ckpt-" + d), SummaryID("sum-" + d), nil
}

// CheckRetainClosure requires retained tool results and their issuing
// assistants to travel together, so the compacted context stays valid
// provider input (APP-CKP-2). Subset and order are the fold's job.
func CheckRetainClosure(entries []Entry, retain []EntryDigestPair) error {
	kept := make(map[EntryDigestPair]bool, len(retain))
	for _, p := range retain {
		kept[p] = true
	}
	owner := map[CallID]*Entry{}
	for i := range entries {
		e := &entries[i]
		if e.Kind != EntryAssistant || e.Assistant == nil {
			continue
		}
		for _, call := range e.Assistant.CallIDs {
			owner[call] = e
		}
	}
	results := map[CallID]*Entry{}
	for i := range entries {
		e := &entries[i]
		if e.Kind == EntryToolResult && e.ToolResult != nil {
			results[e.ToolResult.CallID] = e
		}
	}
	for i := range entries {
		e := &entries[i]
		if !kept[e.Pair()] {
			continue
		}
		switch e.Kind {
		case EntryToolResult:
			if a := owner[e.ToolResult.CallID]; a != nil && !kept[a.Pair()] {
				return fmt.Errorf("chatlog: retained tool_result %s without its assistant", e.ID)
			}
		case EntryAssistant:
			for _, call := range e.Assistant.CallIDs {
				if r := results[call]; r != nil && !kept[r.Pair()] {
					return fmt.Errorf("chatlog: retained assistant %s without the result of call %s", e.ID, call)
				}
			}
		}
	}
	return nil
}
