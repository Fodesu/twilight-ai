package ref

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strings"

	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/chatlog"
	"github.com/felinics/twilight/agent/turn"
)

// NewTurnID mints a collision-free TurnID.
func NewTurnID() turn.TurnID { return turn.TurnID("turn-" + randomHex(8)) }

// NewInputID mints a collision-free InputID; chatlog requires session-global
// uniqueness across restarts.
func NewInputID() run.InputID { return run.InputID("in-" + randomHex(8)) }

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic("ref: rand: " + err.Error())
	}
	return hex.EncodeToString(b)
}

// SessionOptions tunes OpenSession.
type SessionOptions struct {
	// Profile is the agent configuration new Turns run under (required).
	Profile turn.ProfileRef
	// Companion defaults to turn.CompanionV1Version.
	Companion turn.CompanionVersion
	// ResumeActive resumes a still-active Turn synchronously inside
	// OpenSession. Interactive hosts leave it false and call Resume themselves.
	ResumeActive bool
	// CompactAfterEntries triggers automatic compaction when the context
	// grows past this many entries after a settlement; zero disables it.
	CompactAfterEntries int
	// CompactRetainEntries is the pair-closed suffix a compaction keeps
	// verbatim; zero selects the default.
	CompactRetainEntries int
	// CompactWarn receives automatic-compaction failures; they never change
	// the settled results. Nil discards them.
	CompactWarn func(error)
}

// Result is the conversation-level outcome of one settled (or steered) Turn.
type Result struct {
	TurnID      turn.TurnID
	Status      turn.TurnStatus
	Disposition turn.ResumeDisposition
	// Reply is the settled Turn's last assistant text; empty while the Turn
	// still runs (already_driving) or when the attempt produced no text.
	Reply string
}

// SessionStatus reports what a host may need to act on after opening.
type SessionStatus struct {
	Active turn.TurnID
	Failed []turn.TurnID
}

// Session is the host-facing object over one open Session: it submits text,
// routes it (Deliver into the running Turn, or Start), drains the backlog of
// queued inputs after settlement, and reads replies from the chatlog.
// Concurrent Send calls are safe: writes serialize in the Session Writer, and
// a Send that lands in a running Turn returns already_driving.
type Session struct {
	// Recovered is the takeover disposition count from opening (RUN-CMT-7).
	Recovered int

	m      *Memory
	sid    session.SessionID
	driver *SessionDriver
	opts   SessionOptions
}

// OpenSession ensures the stream exists, takes ownership per the assembly's
// Ownership options, runs the takeover disposition and returns the host
// object.
func (m *Memory) OpenSession(ctx context.Context, sid session.SessionID, opts SessionOptions) (*Session, error) {
	if opts.Profile.ID == "" || opts.Profile.Digest == "" {
		return nil, errors.New("ref: open session requires a profile ref")
	}
	companion := opts.Companion
	if companion == "" {
		companion = turn.CompanionV1Version
	}
	if err := m.EnsureSession(ctx, sid); err != nil {
		return nil, err
	}
	recovered, err := m.Open(ctx, sid)
	if err != nil {
		return nil, err
	}
	s := &Session{Recovered: recovered, m: m, sid: sid, opts: opts,
		driver: &SessionDriver{Coordinator: m.Coordinator, Memory: m, Profile: opts.Profile, Companion: companion}}
	if opts.ResumeActive {
		if _, _, err := s.Resume(ctx); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// Status reports the active Turn and the Turns awaiting Retry or Settle.
func (s *Session) Status(ctx context.Context) (SessionStatus, error) {
	surface, err := s.m.TurnSurface(ctx, s.sid)
	if err != nil {
		return SessionStatus{}, err
	}
	var out SessionStatus
	if v, ok := surface.Active(); ok {
		out.Active = v.TurnID
	}
	for _, id := range surface.Order {
		if surface.Turns[id].Status == turn.TurnAttemptFailed {
			out.Failed = append(out.Failed, id)
		}
	}
	return out, nil
}

// Send submits text and blocks until it is settled or absorbed: the first
// Result is the Turn the input landed in, further Results are backlog Turns
// this call drained after settlement. Concurrent Sends race on routing
// (Deliver or Start); a lost race re-routes, and an input another driver
// already took returns as already_driving.
func (s *Session) Send(ctx context.Context, text string) ([]Result, error) {
	in, err := s.m.SubmitText(ctx, s.sid, text)
	if err != nil {
		return nil, err
	}
	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		resp, err := s.driver.Send(ctx, s.sid, []run.AgentInput{in})
		if err == nil {
			return s.settled(ctx, resp)
		}
		if !errors.Is(err, turn.ErrConflict) {
			return nil, err
		}
		lastErr = err
		if r, taken := s.absorbed(ctx, in); taken {
			return []Result{r}, nil
		}
	}
	return nil, lastErr
}

// absorbed reports whether another driver already delivered the input; the
// Turn that took it settles and reports there.
func (s *Session) absorbed(ctx context.Context, in run.AgentInput) (Result, bool) {
	chat, err := s.m.ChatlogSurface(ctx, s.sid)
	if err != nil {
		return Result{}, false
	}
	v, ok := chat.Inputs[chatlog.InputID(in.ID)]
	if !ok || v.Status == chatlog.InputSubmitted {
		return Result{}, false
	}
	r := Result{TurnID: turn.TurnID(v.Input.TurnID), Disposition: ResumeAlreadyDriving}
	if surface, serr := s.m.TurnSurface(ctx, s.sid); serr == nil {
		r.Status = surface.Turns[r.TurnID].Status
	}
	return r, true
}

// Resume drives a still-active Turn (after a restart) to settlement; ok is
// false when no Turn is active.
func (s *Session) Resume(ctx context.Context) ([]Result, bool, error) {
	status, err := s.Status(ctx)
	if err != nil {
		return nil, false, err
	}
	if status.Active == "" {
		return nil, false, nil
	}
	resp, err := s.m.Drive(ctx, turn.TurnRef{SessionID: s.sid, TurnID: status.Active})
	if err != nil {
		return nil, false, err
	}
	out, err := s.settled(ctx, resp)
	return out, true, err
}

// Retry retries the first Turn awaiting Retry; ok is false when none is.
func (s *Session) Retry(ctx context.Context) ([]Result, bool, error) {
	status, err := s.Status(ctx)
	if err != nil {
		return nil, false, err
	}
	if len(status.Failed) == 0 {
		return nil, false, nil
	}
	ref := turn.TurnRef{SessionID: s.sid, TurnID: status.Failed[0]}
	if _, err := s.m.Coordinator.Retry(ctx, turn.RetryRequest{Ref: ref, Reason: "host retry"}); err != nil {
		return nil, false, err
	}
	resp, err := s.m.Drive(ctx, ref)
	if err != nil {
		return nil, false, err
	}
	out, err := s.settled(ctx, resp)
	return out, true, err
}

// Close releases this Session's Writer; other Sessions of the assembly stay
// open.
func (s *Session) Close(ctx context.Context) error {
	w, err := s.m.Writers.Writer(ctx, s.sid)
	if err != nil {
		return err
	}
	return w.Close(ctx)
}

// settled turns a TurnResponse into Results and drains the backlog: while a
// settlement leaves submitted, undelivered inputs, the next Turn starts from
// them (REF-DRV-3).
func (s *Session) settled(ctx context.Context, resp turn.TurnResponse) ([]Result, error) {
	out := []Result{s.result(ctx, resp)}
	if resp.Disposition == ResumeAlreadyDriving {
		// The running driver settles the Turn and drains in its own call.
		return out, nil
	}
	for range [64]struct{}{} {
		next, ok, err := s.driver.OnTurnSettled(ctx, s.sid)
		if err != nil {
			if errors.Is(err, turn.ErrConflict) {
				// A concurrent Send or drain took the backlog; it reports there.
				return out, nil
			}
			return out, err
		}
		if !ok {
			// The backlog is drained and no Turn is active: the automatic
			// compaction policy runs here (REF-CKP-1).
			s.maybeCompact(ctx)
			return out, nil
		}
		out = append(out, s.result(ctx, next))
		if next.Disposition == ResumeAlreadyDriving {
			return out, nil
		}
	}
	return out, errors.New("ref: drain did not converge")
}

func (s *Session) result(ctx context.Context, resp turn.TurnResponse) Result {
	r := Result{TurnID: resp.Ref.TurnID, Status: resp.Status, Disposition: resp.Disposition}
	if resp.Disposition == turn.ResumeFinished {
		if chat, err := s.m.ChatlogSurface(ctx, s.sid); err == nil {
			r.Reply = lastAssistantText(&chat, chatlog.TurnID(resp.Ref.TurnID))
		}
	}
	return r
}

// lastAssistantText is the text of the Turn's last assistant entry.
func lastAssistantText(chat *chatlog.Surface, turnID chatlog.TurnID) string {
	for i := len(chat.EntryOrder) - 1; i >= 0; i-- {
		e := chat.EntryOrder[i]
		if e.Kind != chatlog.EntryAssistant {
			continue
		}
		a, ok := chat.Assistants.Get(chatlog.AssistantID(e.ID))
		if !ok || a.TurnID != turnID {
			continue
		}
		var b strings.Builder
		for _, part := range a.Parts {
			if t, isText := part.(chatlog.TextPart); isText {
				b.WriteString(t.Text)
			}
		}
		return b.String()
	}
	return ""
}
