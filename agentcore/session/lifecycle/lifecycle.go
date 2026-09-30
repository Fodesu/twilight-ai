// Package lifecycle creates, forks and reclaims Sessions over one Store.
package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/turn"
)

// Lifecycle creates, forks and reclaims Sessions over one Store. Every
// method changes the set of Sessions or their storage; none opens one.
type Lifecycle struct {
	Store     session.Stores
	Registry  *module.Registry
	Admission writer.Admission
	History   History
	Clock     func() time.Time
}

func (l Lifecycle) now() int64 {
	if l.Clock != nil {
		return l.Clock().UnixMilli()
	}
	return time.Now().UnixMilli()
}

// Create creates the Session; ext are the segment's module extension slots
// (nil for none), carried opaquely (SES-WIR-5).
func (l Lifecycle) Create(ctx context.Context, sid session.SessionID, ext module.Extensions) error {
	_, err := l.Store.Create(ctx, session.CreateRequest{SessionID: sid, CreatedAtUnixMilli: l.now(), Ext: ext})
	return err
}

// Ensure makes sure the Session exists, whatever record created it: a root
// made here, a fork or a spawned child all count. Create alone would refuse
// a Session whose segment fields differ (SES-CRT-1), so existence is probed
// first.
func (l Lifecycle) Ensure(ctx context.Context, sid session.SessionID) error {
	if _, err := l.Store.Header(ctx, sid); err == nil {
		return nil
	} else if !session.IsCode(err, session.ErrNotFound) {
		return err
	}
	if err := l.Create(ctx, sid, nil); err != nil {
		// A concurrent creator winning the race is still "exists".
		if _, herr := l.Store.Header(ctx, sid); herr == nil {
			return nil
		}
		return err
	}
	return nil
}

// ForkRequest forks a Session at one commit of its ledger (OWN-FRK-1): the
// child inherits every commit of Parent up to and including At and continues
// from there under its own identity.
type ForkRequest struct {
	Parent session.SessionID
	At     ledger.CommitSeq
	Child  session.SessionID
	// Ext are the child segment's module extension slots (SES-WIR-5).
	Ext module.Extensions
}

// Fork creates the child Session (SES-FRK-1) and claims the artifacts its
// inherited prefix references (EXT-WRT-8). The child is not opened.
func (l Lifecycle) Fork(ctx context.Context, req ForkRequest) (session.SegmentHeader, error) {
	if req.Parent == "" || req.Child == "" {
		return session.SegmentHeader{}, errors.New("lifecycle: fork requires parent and child session ids")
	}
	if req.Parent == req.Child {
		return session.SegmentHeader{}, errors.New("lifecycle: a session cannot fork itself")
	}
	// A fork point inside a Turn would hand the child a Turn whose Run is
	// the parent's execution (SES-FRK-5): semantic history branches only at
	// quiescent points (OWN-FRK-1).
	if active, ok, err := l.History.ActiveAt(ctx, req.Parent, req.At); err != nil {
		return session.SegmentHeader{}, err
	} else if ok {
		return session.SegmentHeader{}, &session.Error{Code: session.ErrInvalid, Operation: "fork", SessionID: req.Child,
			Detail: fmt.Sprintf("turn %s of %s is active at commit %d; fork at a quiescent point", active, req.Parent, req.At)}
	}
	return writer.Fork(ctx, l.Store, l.Registry, writer.ForkRequest{
		Parent: req.Parent, At: req.At, Child: req.Child, CreatedAtUnixMilli: l.now(), Ext: req.Ext,
	})
}

// ForkBeforeTurn forks parent at the commit just before turnID started
// (OWN-FRK-2): the child holds the conversation as it was when that Turn's
// inputs were still submitted and undelivered.
func (l Lifecycle) ForkBeforeTurn(ctx context.Context, parent session.SessionID, turnID turn.TurnID, child session.SessionID) (session.SegmentHeader, error) {
	seq, err := l.History.StartCommit(ctx, parent, turnID)
	if err != nil {
		return session.SegmentHeader{}, err
	}
	if seq == 0 {
		return session.SegmentHeader{}, &session.Error{Code: session.ErrInvalid, Operation: "fork", SessionID: child,
			Detail: fmt.Sprintf("turn %s started in the first commit of %s; there is no prefix to fork", turnID, parent)}
	}
	return l.Fork(ctx, ForkRequest{Parent: parent, At: seq - 1, Child: child})
}

// ForkBeforeInputs forks parent at the last commit before turnID and its
// inputs: the conversation as it stood before that Turn was asked, under the
// same quiescence guard every fork passes. The child carries ext.
func (l Lifecycle) ForkBeforeInputs(ctx context.Context, parent session.SessionID, turnID turn.TurnID, child session.SessionID, ext module.Extensions) (session.SegmentHeader, error) {
	at, err := l.History.PrefixCommit(ctx, parent, turnID)
	if err != nil {
		return session.SegmentHeader{}, err
	}
	return l.Fork(ctx, ForkRequest{Parent: parent, At: at, Child: child, Ext: ext})
}

// Delete tombstones the Session and reclaims along its path, releasing the
// retention claims of what it reclaimed (SES-GC-1/2/3). The Session must not
// be open in this process.
func (l Lifecycle) Delete(ctx context.Context, sid session.SessionID) error {
	return writer.Delete(ctx, l.Store, l.Admission, sid)
}

// Collect reclaims the storage of deleted Sessions no live Session reaches
// (SES-GC-2) and releases the claims of the commits it reclaimed (SES-GC-3).
func (l Lifecycle) Collect(ctx context.Context) (session.CollectReport, error) {
	return writer.Collect(ctx, l.Store, l.Admission)
}
