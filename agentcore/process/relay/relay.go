// Package relay is the effect process manager: it reads the Session ledger
// and records, in the process ledger, how each started effect reaches the
// Executor and how its settlement comes back (RUN-EXE-15).
package relay

import (
	"context"
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/checkpoint"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/process"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/extension"
	runmod "github.com/felinics/twilight/agentcore/session/run"
)

// Consumer names the relay's checkpoints.
const Consumer = "twilight/process/relay"

// DefaultMaxAttempts bounds the dispatch attempts of one effect before the
// relay gives up on it.
const DefaultMaxAttempts = 3

// History reads a Session's committed history: the process.Store, or anything that
// serves its ReadCommits.
type History interface {
	ReadCommits(context.Context, session.CommitReadRequest) (session.CommitPage, error)
}

// Ports are what a Relay is built from. Nothing here knows whether the
// Executor is a Worker in this process or a remote one behind HTTP, or
// whether the stores are a file or a shared database: a local agent and a
// cloud agent run the same Relay over different ports.
type Ports struct {
	// History is the Session ledger the relay consumes.
	History History
	// Registry decodes the Session's events; the run module's facts are
	// what the relay reacts to.
	Registry *extension.Registry
	// Ledger is the process authority.
	Ledger process.Store
	// Checkpoints holds the relay's position in each Session ledger.
	Checkpoints checkpoint.Store
	// Executions is the Executor's port: Attach tells whether an effect
	// reached it.
	Executions effect.ExecutionPort
	// Redispatch hands the Assignment of an Executing effect to the
	// Executor again (loop.Redispatch, on the Session's Writer).
	Redispatch func(ctx context.Context, sid session.SessionID, key effect.AssignmentKey) error
	// MaxAttempts bounds dispatch attempts; zero selects DefaultMaxAttempts.
	MaxAttempts int
	// Now stamps the process facts; nil selects time.Now.
	Now func() int64
}

// Relay is the process manager of effects: it reads the Session ledger from
// its checkpoint, records what each fact means for the effect's process, and
// makes the Executor whole for every effect the Session started. It routes
// and records; the Run decides what an effect is and how it settles, the
// Executor decides how an execution is recovered.
type Relay struct {
	Ports
}

// Report is what one Sync did.
type Report struct {
	Requested  int
	Dispatched int
	Failed     int
	GivenUp    int
	Settled    int
	// Next is the checkpoint after this Sync.
	Next session.CommitSeq

	touched map[effect.AssignmentKey]bool
}

// New returns a Relay over ports.
func New(ports Ports) (*Relay, error) {
	switch {
	case ports.History == nil, ports.Registry == nil, ports.Ledger == nil, ports.Checkpoints == nil, ports.Executions == nil, ports.Redispatch == nil:
		return nil, errors.New("process: relay requires History, Registry, Ledger, Checkpoints, Executions and Redispatch")
	}
	if ports.MaxAttempts <= 0 {
		ports.MaxAttempts = DefaultMaxAttempts
	}
	return &Relay{Ports: ports}, nil
}

func ledgerName(sid session.SessionID) string { return "session/" + string(sid) }

func (r *Relay) now() int64 {
	if r.Now != nil {
		return r.Now()
	}
	return nowUnixMilli()
}

// Sync reads the Session's ledger from the relay's checkpoint under the
// owner's epoch and brings every effect process up to date: a start fact
// opens a process and is made whole with the Executor; a settlement fact
// closes it; a process still waiting on the Executor from an earlier Sync
// is asked about again. The checkpoint moves to the head once the page is
// processed, so a crash replays the page and every record is idempotent by
// process.CommitID.
func (r *Relay) Sync(ctx context.Context, sid session.SessionID, epoch ledger.Epoch) (Report, error) {
	rep := Report{touched: map[effect.AssignmentKey]bool{}}
	next, _, err := r.Checkpoints.Load(ctx, Consumer, ledgerName(sid))
	if err != nil {
		return rep, err
	}
	page, err := r.History.ReadCommits(ctx, session.CommitReadRequest{SessionID: sid, From: session.CommitSeq(next)})
	if err != nil {
		return rep, err
	}
	for i := range page.Commits {
		c := &page.Commits[i]
		for _, batch := range c.Batches {
			if batch.Stream.Domain != runmod.StreamDomain {
				continue
			}
			runID := run.RunID(batch.Stream.ID)
			for _, row := range batch.Events {
				decoded, err := r.Registry.Decode(row)
				if err != nil || decoded.Unknown {
					continue
				}
				ev, ok := decoded.Value.(runmod.Event)
				if !ok {
					continue
				}
				if err := r.react(ctx, sid, epoch, runID, ev.Fact, &rep); err != nil {
					return rep, err
				}
			}
		}
	}
	rep.Next = page.Head.Next
	if err := r.Checkpoints.Save(ctx, Consumer, ledgerName(sid), uint64(page.Head.Next)); err != nil && !errors.Is(err, checkpoint.ErrRewind) {
		return rep, err
	}
	// Processes the page did not touch and the Executor has not confirmed:
	// a refused or lost dispatch from an earlier Sync is tried again.
	open, err := r.Ledger.Open(ctx, run.Scope(sid))
	if err != nil {
		return rep, err
	}
	for _, key := range open {
		if rep.touched[key] {
			continue
		}
		state, head, ok, err := r.Ledger.Load(ctx, key)
		if err != nil {
			return rep, err
		}
		if !ok || state.Phase != process.PhaseRequested {
			continue
		}
		if err := r.ensureDispatched(ctx, sid, epoch, key, &state, head, &rep); err != nil {
			return rep, err
		}
	}
	rep.touched = nil
	return rep, nil
}

func (r *Relay) react(ctx context.Context, sid session.SessionID, epoch ledger.Epoch, runID run.RunID, fact run.Fact, rep *Report) error {
	switch f := fact.(type) {
	case run.ModelStepStarted:
		return r.requested(ctx, sid, epoch, process.Requested{RunID: runID, StepID: f.StepID, Effect: f.Effect, Kind: process.KindModel}, rep)
	case run.ToolCallStarted:
		return r.requested(ctx, sid, epoch, process.Requested{RunID: runID, StepID: f.StepID, CallID: f.CallID, Effect: f.Effect, Kind: process.KindTool}, rep)
	case run.ModelStepCompleted:
		return r.settled(ctx, sid, epoch, runID, f.StepID, "", rep)
	case run.ModelStepRecovered:
		return r.settled(ctx, sid, epoch, runID, f.StepID, "", rep)
	case run.ToolCallCompleted:
		return r.settled(ctx, sid, epoch, runID, f.StepID, f.CallID, rep)
	case run.ToolCallFailed:
		return r.settled(ctx, sid, epoch, runID, f.StepID, f.CallID, rep)
	case run.ToolCallAnswered:
		return r.settled(ctx, sid, epoch, runID, f.StepID, f.CallID, rep)
	}
	return nil
}

// requested opens the effect's process and makes the Executor whole for it.
func (r *Relay) requested(ctx context.Context, sid session.SessionID, epoch ledger.Epoch, req process.Requested, rep *Report) error {
	key := effect.AssignmentKey{Session: run.Scope(sid), RunID: req.RunID, Effect: req.Effect}
	state, head, ok, err := r.Ledger.Load(ctx, key)
	if err != nil {
		return err
	}
	if !ok {
		if err := r.append(ctx, epoch, key, head, process.RequestCommitID(key), process.EventDispatchRequested, req); err != nil {
			return err
		}
		rep.Requested++
		if state, head, ok, err = r.Ledger.Load(ctx, key); err != nil || !ok {
			return fmt.Errorf("process: %v after request: ok=%v %w", key, ok, err)
		}
	}
	rep.touched[key] = true
	if state.Phase != process.PhaseRequested {
		return nil
	}
	return r.ensureDispatched(ctx, sid, epoch, key, &state, head, rep)
}

// ensureDispatched asks the Executor whether it holds the effect and, when
// it does not, hands the Assignment over again; every decision is a fact.
func (r *Relay) ensureDispatched(ctx context.Context, sid session.SessionID, epoch ledger.Epoch, key effect.AssignmentKey, state *process.State, head process.Head, rep *Report) error {
	attachment, err := r.Executions.Attach(ctx, key)
	if err != nil {
		return err
	}
	if attachment.State != effect.AttachmentMissing {
		if err := r.append(ctx, epoch, key, head, process.DispatchedCommitID(key), process.EventDispatched, nil); err != nil {
			return err
		}
		rep.Dispatched++
		return nil
	}
	err = r.Redispatch(ctx, sid, key)
	switch {
	case err == nil:
		if err := r.append(ctx, epoch, key, head, process.DispatchedCommitID(key), process.EventDispatched, nil); err != nil {
			return err
		}
		rep.Dispatched++
	case errors.Is(err, effect.ErrDispatchUnknown):
		// The request may have crossed the boundary: the Executor's ledger
		// answers on the next Sync; nothing is decided here.
	case errors.Is(err, effect.ErrDispatchRetryable):
		attempt := state.Attempts + 1
		if attempt >= r.MaxAttempts {
			if err := r.append(ctx, epoch, key, head, process.GivenUpCommitID(key), process.EventGivenUp, process.GivenUp{Reason: fmt.Sprintf("dispatch refused %d times: %v", attempt, err)}); err != nil {
				return err
			}
			rep.GivenUp++
			return nil
		}
		if err := r.append(ctx, epoch, key, head, process.FailedCommitID(key, attempt), process.EventDispatchFailed, process.Failed{Attempt: attempt, Reason: err.Error()}); err != nil {
			return err
		}
		rep.Failed++
	default:
		// A definite rejection, or an effect the Run no longer executes
		// (the Loop settled it): the process ends with the reason.
		if err := r.append(ctx, epoch, key, head, process.GivenUpCommitID(key), process.EventGivenUp, process.GivenUp{Reason: err.Error()}); err != nil {
			return err
		}
		rep.GivenUp++
	}
	return nil
}

// settled closes the process of the effect a settlement fact names. The
// fact carries the step and call, not the effect, so the open processes of
// the Session are matched on those.
func (r *Relay) settled(ctx context.Context, sid session.SessionID, epoch ledger.Epoch, runID run.RunID, stepID run.StepID, callID run.CallID, rep *Report) error {
	keys, err := r.Ledger.Open(ctx, run.Scope(sid))
	if err != nil {
		return err
	}
	for _, key := range keys {
		state, head, ok, err := r.Ledger.Load(ctx, key)
		if err != nil {
			return err
		}
		if !ok || state.Requested.RunID != runID || state.Requested.StepID != stepID || state.Requested.CallID != callID {
			continue
		}
		if state.Phase == process.PhaseRequested || state.Phase == process.PhaseDispatched {
			if err := r.append(ctx, epoch, key, head, process.DeliveredCommitID(key), process.EventOutcomeDelivered, nil); err != nil {
				return err
			}
			head.Next++
			state.Phase = process.PhaseDelivered
		}
		if state.Phase == process.PhaseDelivered {
			// The Loop acknowledged the Executor when it settled (RUN-EXE-13);
			// the process records that the settlement is a Session fact.
			if err := r.append(ctx, epoch, key, head, process.AcknowledgedCommitID(key), process.EventAcknowledged, nil); err != nil {
				return err
			}
			rep.Settled++
		}
	}
	return nil
}

// append commits one event at head under the owner's epoch; a commit the
// ledger already holds is success.
func (r *Relay) append(ctx context.Context, epoch ledger.Epoch, key effect.AssignmentKey, head process.Head, id process.CommitID, typ process.EventType, payload any) error {
	ev, err := ledger.NewEvent(typ, r.now(), payload)
	if err != nil {
		return err
	}
	err = r.Ledger.Append(ctx, epoch, key, process.Commit{Seq: head.Next, CommitID: id, Events: []process.Event{ev}})
	if err == nil || errors.Is(err, ledger.ErrAlreadyApplied) {
		return nil
	}
	return err
}
