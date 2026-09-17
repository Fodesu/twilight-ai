package host

import (
	"context"
	"errors"
	"fmt"

	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/chatlog"
	"github.com/felinics/twilight/agent/session/writer"
	"github.com/felinics/twilight/agent/turn"
)

// ForkRequest forks a Session at one commit of its ledger (HST-FRK-1): the
// child inherits every commit of Parent up to and including At and continues
// from there under its own identity.
type ForkRequest struct {
	Parent session.SessionID
	At     session.CommitSeq
	Child  session.SessionID
}

// Fork creates the child Session (SES-FRK-1) and claims the artifacts its
// inherited prefix references (EXT-WRT-8). The child is not opened: open it
// with OpenSession like any Session. Executing targets the prefix leaves
// behind belong to the parent's executions; the child's takeover disposition
// treats them as missing and replans or records Unknown (RUN-CMT-7).
func (h *Host) Fork(ctx context.Context, req ForkRequest) (session.SegmentHeader, error) {
	if req.Parent == "" || req.Child == "" {
		return session.SegmentHeader{}, errors.New("host: fork requires parent and child session ids")
	}
	if req.Parent == req.Child {
		return session.SegmentHeader{}, errors.New("host: a session cannot fork itself")
	}
	return writer.Fork(ctx, h.Store, h.registry, h.admission, writer.ForkRequest{
		Parent: req.Parent, At: req.At, Child: req.Child, CreatedAtUnixMilli: h.now().UnixMilli(),
	})
}

// ForkBeforeTurn forks Parent at the commit just before turnID started
// (HST-FRK-2): the child holds the conversation as it was when that Turn's
// inputs were still submitted and undelivered, so the same inputs can be
// regenerated (Route delivers them to a new Turn) or withdrawn and replaced
// (WithdrawInput, then Submit). A Turn started by the first commit of the
// ledger leaves no prefix to fork; create a new Session instead.
func (h *Host) ForkBeforeTurn(ctx context.Context, parent session.SessionID, turnID turn.TurnID, child session.SessionID) (session.SegmentHeader, error) {
	seq, err := h.history.StartCommit(ctx, parent, turnID)
	if err != nil {
		return session.SegmentHeader{}, err
	}
	if seq == 0 {
		return session.SegmentHeader{}, &session.Error{Code: session.ErrInvalid, Operation: "fork", SessionID: child,
			Detail: fmt.Sprintf("turn %s started in the first commit of %s; there is no prefix to fork", turnID, parent)}
	}
	return h.Fork(ctx, ForkRequest{Parent: parent, At: seq - 1, Child: child})
}

// DeleteSession drops a Session's root and releases its claims (HST-FRK-3,
// SES-GC-1): the Session is gone, but every commit a live fork inherits stays
// until Collect finds it unreachable. A Session this Host holds open is
// closed first; one owned by another process is ErrOwned.
func (h *Host) DeleteSession(ctx context.Context, sid session.SessionID) error {
	h.stopRecovery(sid)
	if err := writer.CloseWriter(ctx, h.Writers, sid); err != nil {
		return err
	}
	return writer.Delete(ctx, h.Store, h.admission, sid)
}

// Collect reclaims the storage of deleted Sessions no live Session reaches
// (SES-GC-2).
func (h *Host) Collect(ctx context.Context) (session.CollectReport, error) {
	return writer.Collect(ctx, h.Store)
}

// WithdrawInput marks a submitted, undelivered input as withdrawn
// (CHT-EVT-2): the Application's decision that an input is not to be
// delivered, for example the original input of a Turn the caller forked
// before in order to edit it (HST-FRK-2).
func (h *Host) WithdrawInput(ctx context.Context, sid session.SessionID, id run.InputID, reason string) error {
	err := h.chatlog.WithdrawInput(ctx, sid, id, reason)
	if errors.Is(err, chatlog.ErrNotSubmitted) {
		return fmt.Errorf("%w: input %s is not a submitted input", turn.ErrConflict, id)
	}
	return err
}
