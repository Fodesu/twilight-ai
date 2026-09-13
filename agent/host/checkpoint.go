package host

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/felinics/twilight/agent/decision"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/loop"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/chatlog"
	"github.com/felinics/twilight/agent/session/writer"
	"github.com/felinics/twilight/agent/turn"
	"github.com/felinics/twilight/sdk"
)

// CompactorSystemPrompt asks the profile's model for the checkpoint summary.
const CompactorSystemPrompt = "You are the conversation compactor. Reply with a concise summary of the conversation transcript that preserves facts, decisions, names and open tasks. Reply with the summary text only."

// RetainLast selects a pair-closed suffix of at most n entries: a retained
// tool result pulls in the assistant that issued its call, so the retained
// set stays valid provider input (HST-CKP-2).
func RetainLast(entries []chatlog.Entry, n int) []chatlog.EntryDigestPair {
	if n <= 0 || len(entries) == 0 {
		return nil
	}
	owner := map[chatlog.CallID]int{} // CallID -> index of the issuing assistant
	for i, e := range entries {
		if e.Kind != chatlog.EntryAssistant || e.Assistant == nil {
			continue
		}
		for _, part := range e.Assistant.Parts {
			if call, ok := part.(chatlog.ToolCallPart); ok {
				owner[call.CallID] = i
			}
		}
	}
	start := len(entries) - n
	if start < 0 {
		start = 0
	}
	for changed := true; changed; {
		changed = false
		for i := start; i < len(entries); i++ {
			e := &entries[i]
			if e.Kind != chatlog.EntryToolResult || e.ToolResult == nil {
				continue
			}
			if at, ok := owner[e.ToolResult.CallID]; ok && at < start {
				start = at
				changed = true
			}
		}
	}
	out := make([]chatlog.EntryDigestPair, 0, len(entries)-start)
	for i := start; i < len(entries); i++ {
		out = append(out, entries[i].Pair())
	}
	return out
}

// Checkpoint commits a summary and its checkpoint in one group (CHT-EVT-3).
// The base is read inside the commit's critical section, so the digest pins
// exactly the context being replaced; a Turn must not be active. retain names
// entries of the current context (RetainLast builds a pair-closed suffix).
func (h *Host) Checkpoint(ctx context.Context, sid session.SessionID, summaryText string, retain []chatlog.EntryDigestPair) (chatlog.CheckpointID, error) {
	if strings.TrimSpace(summaryText) == "" {
		return "", errors.New("host: checkpoint requires a summary text")
	}
	w, err := h.Writers.Writer(ctx, sid)
	if err != nil {
		return "", err
	}
	checkpointID := chatlog.CheckpointID("ckpt-" + randomHex(8))
	summaryID := chatlog.SummaryID("sum-" + randomHex(8))
	res, err := w.Commit(ctx, func(v writer.View) (*writer.SemanticGroup, error) {
		state, err := v.Projection(turn.SurfaceProjectionID, turn.SurfaceProjection.Version)
		if err != nil {
			return nil, err
		}
		tsurf := state.(turn.TurnSurface)
		if active, ok := tsurf.Active(); ok {
			return nil, fmt.Errorf("%w: checkpoint while turn %s is active", turn.ErrConflict, active.TurnID)
		}
		cstate, err := v.Projection(chatlog.ContextProjectionID, chatlog.ContextProjection.Version)
		if err != nil {
			return nil, err
		}
		entries := cstate.(chatlog.Context).Entries
		if len(entries) == 0 {
			return nil, errors.New("host: checkpoint over an empty context")
		}
		if err := checkRetainClosure(entries, retain); err != nil {
			return nil, err
		}
		pairs := make([]chatlog.EntryDigestPair, len(entries))
		for i := range entries {
			pairs[i] = entries[i].Pair()
		}
		baseDigest, err := chatlog.DigestBaseContext(pairs)
		if err != nil {
			return nil, err
		}
		summary := chatlog.Summary{ID: summaryID, Parts: chatlog.Parts{chatlog.TextPart{Text: summaryText}}}
		if summary.Digest, err = chatlog.DigestSummary(&summary); err != nil {
			return nil, err
		}
		payload := chatlog.CheckpointCreatedPayload{
			CheckpointID: checkpointID, CoveredThrough: v.Head().Next - 1,
			BaseContextDigest: baseDigest, SummaryID: summaryID, SummaryDigest: summary.Digest,
			Retained: retain,
		}
		if payload.Digest, err = chatlog.DigestCheckpoint(&payload); err != nil {
			return nil, err
		}
		now := h.now().UnixMilli()
		return &writer.SemanticGroup{CommitID: session.CommitID("checkpoint/" + string(checkpointID)), Events: []writer.TypedEvent{
			{Type: chatlog.TypeSummary, RecordedAtUnixMilli: now, Value: chatlog.SummaryPayload{Summary: summary}},
			{Type: chatlog.TypeCheckpointCreated, RecordedAtUnixMilli: now, Value: payload},
		}}, nil
	})
	if err != nil {
		return "", err
	}
	switch res.Outcome {
	case writer.CommitApplied, writer.CommitAlreadyApplied:
		return checkpointID, nil
	default:
		return "", fmt.Errorf("host: checkpoint: %s: %s", res.Outcome, res.Detail)
	}
}

// checkRetainClosure requires retained tool results and their issuing
// assistants to travel together, so the compacted context stays valid
// provider input (HST-CKP-2). Subset and order are the fold's job.
func checkRetainClosure(entries []chatlog.Entry, retain []chatlog.EntryDigestPair) error {
	kept := make(map[chatlog.EntryDigestPair]bool, len(retain))
	for _, p := range retain {
		kept[p] = true
	}
	owner := map[chatlog.CallID]*chatlog.Entry{}
	for i := range entries {
		e := &entries[i]
		if e.Kind != chatlog.EntryAssistant || e.Assistant == nil {
			continue
		}
		for _, part := range e.Assistant.Parts {
			if call, ok := part.(chatlog.ToolCallPart); ok {
				owner[call.CallID] = e
			}
		}
	}
	results := map[chatlog.CallID]*chatlog.Entry{}
	for i := range entries {
		e := &entries[i]
		if e.Kind == chatlog.EntryToolResult && e.ToolResult != nil {
			results[e.ToolResult.CallID] = e
		}
	}
	for i := range entries {
		e := &entries[i]
		if !kept[e.Pair()] {
			continue
		}
		switch e.Kind {
		case chatlog.EntryToolResult:
			if a := owner[e.ToolResult.CallID]; a != nil && !kept[a.Pair()] {
				return fmt.Errorf("host: retained tool_result %s without its assistant", e.ID)
			}
		case chatlog.EntryAssistant:
			for _, part := range e.Assistant.Parts {
				call, ok := part.(chatlog.ToolCallPart)
				if !ok {
					continue
				}
				if r := results[call.CallID]; r != nil && !kept[r.Pair()] {
					return fmt.Errorf("host: retained assistant %s without the result of call %s", e.ID, call.CallID)
				}
			}
		}
	}
	return nil
}

// Compact summarizes the context with the profile's model and commits a
// checkpoint retaining a pair-closed suffix; ok is false when the context is
// already within the retain window (HST-CKP-1).
func (s *Session) Compact(ctx context.Context) (chatlog.CheckpointID, bool, error) {
	retainN := s.opts.CompactRetainEntries
	if retainN <= 0 {
		retainN = defaultCompactRetain
	}
	state, _, err := s.h.Projection(ctx, s.sid, chatlog.ContextProjectionID, chatlog.ContextProjection.Version)
	if err != nil {
		return "", false, err
	}
	entries := state.(chatlog.Context).Entries
	retain := RetainLast(entries, retainN)
	if len(entries) == 0 || len(retain) >= len(entries) {
		return "", false, nil
	}
	summary, err := s.summarize(ctx, entries)
	if err != nil {
		return "", false, err
	}
	id, err := s.h.Checkpoint(ctx, s.sid, summary, retain)
	if err != nil {
		return "", false, err
	}
	return id, true, nil
}

const defaultCompactRetain = 4

// summarize is the compactor's model call. It is an effect like any other and
// goes through the Executor port (HST-CKP-1): the request is frozen and
// dispatched as a model Assignment outside any Run, so the authority holds no
// model client and a remote executor serves it the same way. A crash while
// it generates writes nothing.
func (s *Session) summarize(ctx context.Context, entries []chatlog.Entry) (string, error) {
	profile, err := s.h.Profiles.Resolve(s.opts.Profile)
	if err != nil {
		return "", err
	}
	frozen, err := run.FreezeModelRequest(sdk.Request{Model: string(profile.Model), Messages: []sdk.Message{
		sdk.SystemMessage(CompactorSystemPrompt),
		sdk.UserMessage(renderTranscript(entries)),
	}})
	if err != nil {
		return "", err
	}
	digest, err := run.ProtocolV1().DigestRequest(frozen)
	if err != nil {
		return "", err
	}
	raw, err := run.EncodeFrozenRequest(&frozen, digest)
	if err != nil {
		return "", err
	}
	if err := s.h.frozen.Put(ctx, digest, raw); err != nil {
		return "", err
	}
	a := loop.Assignment{Session: s.sid, RunID: run.RunID("compact-" + randomHex(8)), StepID: "summary",
		Claim: run.ExecutionClaim(randomHex(16)), Schema: run.SchemaVersion1, Kind: loop.AssignmentModel,
		Model: &loop.ModelAssignment{Model: profile.Model, RequestDigest: digest}}
	outcomes := make(chan loop.Outcome, 1)
	if err := s.h.Executor.Dispatch(ctx, a, func(out loop.Outcome) { outcomes <- out }); err != nil {
		return "", err
	}
	var out loop.Outcome
	select {
	case out = <-outcomes:
	case <-ctx.Done():
		_ = s.h.Executor.Cancel(context.WithoutCancel(ctx), a.RunID)
		return "", ctx.Err()
	}
	switch {
	case out.Err != nil:
		return "", out.Err
	case out.Cancelled:
		return "", errors.New("host: compactor call was cancelled")
	case out.Model == nil || strings.TrimSpace(out.Model.Text) == "":
		return "", errors.New("host: compactor returned an empty summary")
	}
	return out.Model.Text, nil
}

// renderTranscript flattens entries into the compactor's input.
func renderTranscript(entries []chatlog.Entry) string {
	var b strings.Builder
	for i := range entries {
		e := &entries[i]
		switch e.Kind {
		case chatlog.EntryInput:
			text, err := decision.InputText(e.Input.Content)
			if err != nil {
				text = e.Input.Content.String()
			}
			fmt.Fprintf(&b, "user: %s\n", text)
		case chatlog.EntryAssistant:
			for _, part := range e.Assistant.Parts {
				switch v := part.(type) {
				case chatlog.TextPart:
					fmt.Fprintf(&b, "assistant: %s\n", v.Text)
				case chatlog.ToolCallPart:
					fmt.Fprintf(&b, "assistant: [calls %s %s]\n", v.Name, v.Input.String())
				}
			}
		case chatlog.EntryToolResult:
			fmt.Fprintf(&b, "tool (%s): %s\n", e.ToolResult.Status, decision.PartsText(e.ToolResult.Parts))
		case chatlog.EntrySummary:
			fmt.Fprintf(&b, "summary: %s\n", decision.PartsText(e.Summary.Parts))
		}
	}
	return b.String()
}

// maybeCompact runs the automatic policy after a settlement; failures reach
// the host through CompactWarn and never change the settled results.
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
