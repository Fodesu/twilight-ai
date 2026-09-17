package host

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/chatlog"
	"github.com/felinics/twilight/agent/session/writer"
	"github.com/felinics/twilight/agent/turn"
)

// SessionOptions tunes OpenSession.
type SessionOptions struct {
	// AgentPreset is the decision identity new Turns run under (required).
	Preset turn.PresetRef
	// NewTurnID mints TurnIDs; nil selects the random default.
	NewTurnID func() turn.TurnID
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

// Session is the facade over one open Session (HST-SES): it submits text,
// routes it (Deliver into the running Turn, or Start), drives the Turn,
// drains the backlog of queued inputs after settlement, and reads replies
// from the chatlog. Submit routes and returns at once, driving in the
// background and reporting through Events; Send is the blocking form over
// the same route-and-drive path. Concurrent calls are safe: writes serialize
// in the Session Writer, and a call whose input lands in a running Turn
// reports already_driving.
type Session struct {
	// Recovered is the takeover disposition count from opening (RUN-CMT-7).
	Recovered int

	h         *Host
	sid       session.SessionID
	opts      SessionOptions
	newTurnID func() turn.TurnID

	// bg bounds the background drives Submit starts; Close cancels it and
	// waits for them (HST-SES-4). bgN counts the drives in flight and bgIdle
	// is closed when the count returns to zero, so Wait can observe quiescence
	// while later Submits are still allowed.
	bg     context.Context
	cancel context.CancelFunc
	bgMu   sync.Mutex
	bgN    int
	bgIdle chan struct{}
}

func (s *Session) bgStart() {
	s.bgMu.Lock()
	if s.bgN == 0 {
		s.bgIdle = make(chan struct{})
	}
	s.bgN++
	s.bgMu.Unlock()
}

func (s *Session) bgDone() {
	s.bgMu.Lock()
	s.bgN--
	if s.bgN == 0 {
		close(s.bgIdle)
	}
	s.bgMu.Unlock()
}

// Wait blocks until every background drive Submit has started so far has
// finished, or ctx ends. It does not cancel anything; Close does. A caller
// that wants a synchronous view after Submit -- a test, a shutdown sequence,
// a Send that must not race the backlog drain -- waits here first.
func (s *Session) Wait(ctx context.Context) error {
	s.bgMu.Lock()
	idle, n := s.bgIdle, s.bgN
	s.bgMu.Unlock()
	if n == 0 {
		return nil
	}
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// OpenSession ensures the stream exists, takes ownership per the Host's
// Ownership port, runs the takeover disposition and returns the facade
// (HST-SES-1).
func (h *Host) OpenSession(ctx context.Context, sid session.SessionID, opts SessionOptions) (*Session, error) {
	if opts.Preset.ID == "" || opts.Preset.Digest == "" {
		return nil, errors.New("host: open session requires a preset ref")
	}
	if _, err := h.Presets.Resolve(opts.Preset); err != nil {
		return nil, err
	}
	newTurnID := opts.NewTurnID
	if newTurnID == nil {
		newTurnID = NewTurnID
	}
	if err := h.EnsureSession(ctx, sid); err != nil {
		return nil, err
	}
	recovered, err := h.Open(ctx, sid)
	if err != nil {
		return nil, err
	}
	s := &Session{Recovered: recovered, h: h, sid: sid, opts: opts, newTurnID: newTurnID}
	s.bg, s.cancel = context.WithCancel(context.Background())
	if opts.ResumeActive {
		if _, _, err := s.Resume(ctx); err != nil {
			_ = s.Close(context.WithoutCancel(ctx))
			return nil, err
		}
	}
	return s, nil
}

// Status reports the active Turn and the Turns awaiting Retry or Settle.
func (s *Session) Status(ctx context.Context) (SessionStatus, error) {
	surface, err := s.h.TurnSurface(ctx, s.sid)
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
// this call drained after settlement (HST-SES-2). It is Submit's route
// followed by a synchronous drive. Concurrent Sends race on routing (Deliver
// or Start); a lost race re-routes, and an input another driver already took
// returns as already_driving.
func (s *Session) Send(ctx context.Context, text string) ([]Result, error) {
	in, err := s.h.SubmitText(ctx, s.sid, text)
	if err != nil {
		return nil, err
	}
	ref, absorbed, err := s.routeInput(ctx, in)
	if err != nil {
		return nil, err
	}
	if absorbed != nil {
		return []Result{*absorbed}, nil
	}
	resp, err := s.h.Drive(ctx, ref)
	if err != nil {
		return nil, err
	}
	return s.settled(ctx, resp)
}

// Submit submits text, commits its route and returns the Turn it landed in
// without waiting (HST-SES-4). The Turn is driven to settlement -- and the
// backlog drained -- in the background; progress and the reply arrive on
// Events, failures on Events and Ports.Warn. Close cancels the background
// drive; a cancelled Turn stays active and resumes on the next open.
func (s *Session) Submit(ctx context.Context, text string) (turn.TurnRef, error) {
	in, err := s.h.SubmitText(ctx, s.sid, text)
	if err != nil {
		return turn.TurnRef{}, err
	}
	ref, absorbed, err := s.routeInput(ctx, in)
	if err != nil {
		return turn.TurnRef{}, err
	}
	if absorbed != nil {
		// A running driver carries the input; nothing to drive here.
		return turn.TurnRef{SessionID: s.sid, TurnID: absorbed.TurnID}, nil
	}
	s.bgStart()
	go func() {
		defer s.bgDone()
		resp, err := s.h.Drive(s.bg, ref)
		if err != nil {
			s.h.fail(s.sid, fmt.Errorf("host: driving turn %s: %w", ref.TurnID, err))
			return
		}
		if _, err := s.settled(s.bg, resp); err != nil {
			s.h.fail(s.sid, fmt.Errorf("host: settling turn %s: %w", ref.TurnID, err))
		}
	}()
	return ref, nil
}

// Events is this Session's event stream from now on (HST-EVT-1).
func (s *Session) Events(ctx context.Context) <-chan Event { return s.h.Events(ctx, s.sid) }

// routeInput commits one input's route with the conflict retry of HST-SES-3.
// It returns the Turn to drive, or the already_driving Result when another
// driver took the input first.
func (s *Session) routeInput(ctx context.Context, in run.AgentInput) (turn.TurnRef, *Result, error) {
	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		ref, err := s.commitRoute(ctx, []run.AgentInput{in})
		if err == nil {
			return ref, nil, nil
		}
		if !errors.Is(err, turn.ErrConflict) {
			return turn.TurnRef{}, nil, err
		}
		lastErr = err
		if r, taken := s.absorbed(ctx, in); taken {
			return turn.TurnRef{}, &r, nil
		}
	}
	return turn.TurnRef{}, nil, lastErr
}

// Route is HST-DRV-3: commit the inputs' route -- Deliver into the active
// Turn, or Start a new one -- then drive the Turn to its next quiescent point.
// A Turn awaiting Retry or Settle is a conflict: those are host decisions.
func (s *Session) Route(ctx context.Context, inputs []run.AgentInput) (turn.TurnResponse, error) {
	ref, err := s.commitRoute(ctx, inputs)
	if err != nil {
		return turn.TurnResponse{}, err
	}
	return s.h.Drive(ctx, ref)
}

// commitRoute is the commit half of Route: Deliver into the active Turn or
// Start a new one, returning the Turn the inputs landed in.
func (s *Session) commitRoute(ctx context.Context, inputs []run.AgentInput) (turn.TurnRef, error) {
	surface, err := s.h.TurnSurface(ctx, s.sid)
	if err != nil {
		return turn.TurnRef{}, err
	}
	if active, ok := surface.Active(); ok {
		ref := turn.TurnRef{SessionID: s.sid, TurnID: active.TurnID}
		if _, err := s.h.Coordinator.Deliver(ctx, turn.DeliverRequest{Ref: ref, Inputs: inputs}); err != nil {
			return turn.TurnRef{}, err
		}
		return ref, nil
	}
	for _, v := range surface.Turns {
		if v.Status == turn.TurnAttemptFailed {
			return turn.TurnRef{}, fmt.Errorf("%w: turn %s awaits Retry or Settle", turn.ErrConflict, v.TurnID)
		}
	}
	ref := turn.TurnRef{SessionID: s.sid, TurnID: s.newTurnID()}
	if _, err := s.h.Coordinator.Start(ctx, turn.StartRequest{Ref: ref, Inputs: inputs, Preset: s.opts.Preset}); err != nil {
		return turn.TurnRef{}, err
	}
	return ref, nil
}

// Drain is HST-DRV-4: start the next Turn from the backlog of
// submitted, undelivered inputs; ok is false when there is none.
func (s *Session) Drain(ctx context.Context) (turn.TurnResponse, bool, error) {
	surface, err := s.h.ChatlogSurface(ctx, s.sid)
	if err != nil {
		return turn.TurnResponse{}, false, err
	}
	pending := surface.SubmittedInputs()
	if len(pending) == 0 {
		return turn.TurnResponse{}, false, nil
	}
	inputs := make([]run.AgentInput, len(pending))
	for i, in := range pending {
		inputs[i] = run.AgentInput{ID: run.InputID(in.ID), Payload: in.Content}
	}
	resp, err := s.Route(ctx, inputs)
	return resp, err == nil, err
}

// absorbed reports whether another driver already delivered the input; the
// Turn that took it settles and reports there.
func (s *Session) absorbed(ctx context.Context, in run.AgentInput) (Result, bool) {
	chat, err := s.h.ChatlogSurface(ctx, s.sid)
	if err != nil {
		return Result{}, false
	}
	v, ok := chat.Inputs.Get(chatlog.InputID(in.ID))
	if !ok || v.Status == chatlog.InputSubmitted {
		return Result{}, false
	}
	r := Result{TurnID: turn.TurnID(v.Input.TurnID), Disposition: ResumeAlreadyDriving}
	if surface, serr := s.h.TurnSurface(ctx, s.sid); serr == nil {
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
	resp, err := s.h.Drive(ctx, turn.TurnRef{SessionID: s.sid, TurnID: status.Active})
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
	previous, err := s.h.Coordinator.Status(ctx, ref)
	if err != nil {
		return nil, false, err
	}
	if _, err := s.h.Coordinator.Retry(ctx, turn.RetryRequest{Ref: ref, PreviousRunID: previous.RunID, Reason: "host retry"}); err != nil {
		return nil, false, err
	}
	resp, err := s.h.Drive(ctx, ref)
	if err != nil {
		return nil, false, err
	}
	out, err := s.settled(ctx, resp)
	return out, true, err
}

// Close cancels the background drives Submit started, waits for them to
// return, then releases this Session's Writer; other Sessions of the Host
// stay open. A Turn a cancelled drive left active resumes on the next open.
func (s *Session) Close(ctx context.Context) error {
	s.h.stopRecovery(s.sid)
	if s.cancel != nil {
		s.cancel()
	}
	_ = s.Wait(context.Background()) // drives observe the cancelled bg ctx and return
	// Closing and forgetting go together: a Writer that failed (EXT-WRT-4)
	// does not block the close, and the next OpenSession reopens from the log.
	return writer.CloseWriter(ctx, s.h.Writers, s.sid)
}

// settled turns a TurnResponse into Results and drains the backlog: while a
// settlement leaves submitted, undelivered inputs, the next Turn starts from
// them (HST-DRV-4).
func (s *Session) settled(ctx context.Context, resp turn.TurnResponse) ([]Result, error) {
	out := []Result{s.result(ctx, resp)}
	if resp.Disposition == ResumeAlreadyDriving {
		// The running driver settles the Turn and drains in its own call.
		return out, nil
	}
	for range [64]struct{}{} {
		next, ok, err := s.Drain(ctx)
		if err != nil {
			if errors.Is(err, turn.ErrConflict) {
				// A concurrent Send or drain took the backlog; it reports there.
				return out, nil
			}
			return out, err
		}
		if !ok {
			// The backlog is drained and no Turn is active: the automatic
			// compaction policy runs here (HST-CKP-1).
			s.maybeCompact(ctx)
			return out, nil
		}
		out = append(out, s.result(ctx, next))
		if next.Disposition == ResumeAlreadyDriving {
			return out, nil
		}
	}
	return out, errors.New("host: drain did not converge")
}

func (s *Session) result(ctx context.Context, resp turn.TurnResponse) Result {
	r := Result{TurnID: resp.Ref.TurnID, Status: resp.Status, Disposition: resp.Disposition}
	if resp.Disposition == turn.ResumeFinished {
		if chat, err := s.h.ChatlogSurface(ctx, s.sid); err == nil {
			r.Reply = s.lastAssistantText(ctx, &chat, chatlog.TurnID(resp.Ref.TurnID))
		}
	}
	return r
}

// lastAssistantText materializes the text of the Turn's last assistant
// entry: the Surface names the frozen ModelResult by digest, the Host's
// content store holds it (CHT-MAT-1).
func (s *Session) lastAssistantText(ctx context.Context, chat *chatlog.Surface, turnID chatlog.TurnID) string {
	for i := len(chat.EntryOrder) - 1; i >= 0; i-- {
		e := chat.EntryOrder[i]
		if e.Kind != chatlog.EntryAssistant {
			continue
		}
		a, ok := chat.Assistants.Get(chatlog.AssistantID(e.ID))
		if !ok || a.TurnID != turnID {
			continue
		}
		entry := chatlog.Entry{Kind: chatlog.EntryAssistant, ID: e.ID, Digest: a.Digest, Assistant: &a}
		m, err := chatlog.NewMaterializer(s.h.content).Entry(ctx, &entry)
		if err != nil {
			s.h.warn(fmt.Errorf("host: materialize reply of turn %s: %w", turnID, err))
			return ""
		}
		return m.Text()
	}
	return ""
}
