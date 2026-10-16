// Package activation hosts disposable Session computations. Work discovery
// is level-triggered: durable Session, Inbox and Execution state is truth;
// scheduling signals are a reconstructible index, not semantic authority.
package activation

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/felinics/twilight/agent/inbox"
	"github.com/felinics/twilight/agent/scheduling"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	runtime "github.com/felinics/twilight/agentcore/runtime"
	"github.com/felinics/twilight/agentcore/session"
)

// ResponseScheduler accepts an idempotent, durable job identified by the
// Session and ResponseID. It receives no Writer. Results return as commands
// through the agent's Inbox. Nil leaves response waits to external callers.
// A callback that merely launches a goroutine does not satisfy this contract.
type ResponseScheduler interface {
	Ensure(context.Context, session.SessionID, run.ResponseRequest) error
}

// Discovery must run independently of per-Session Activations. Each sweep
// visits all pages, including idle/unowned Sessions and orphan executions.
// Permanent network partitions cannot guarantee progress: liveness assumes
// eventual authority availability, fair sweeps and available worker capacity.
type Discovery struct {
	Sessions      session.Directory
	Projections   session.ProjectionReader
	Executions    effect.OutstandingPort
	Inbox         inbox.Store
	Responses     ResponseScheduler
	Queue         scheduling.Store
	PageSize      int
	ProbeInterval time.Duration
	Now           func() time.Time
}

func (d *Discovery) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}
	return time.Now()
}

func (d *Discovery) pageSize() int {
	if d.PageSize > 0 {
		return d.PageSize
	}
	return 128
}

func probeDuration(interval time.Duration) time.Duration {
	if interval <= 0 {
		return 15 * time.Second
	}
	return interval
}

func executionSource(key effect.AssignmentKey) string {
	// Tuple framing avoids collisions between identifiers containing slashes.
	return fmt.Sprintf("execution:%q:%q:%q", key.Session, key.RunID, key.Effect)
}

// Sweep repairs every source -> scheduling-index write gap. A corrupt Session
// does not starve later Sessions: independent errors are accumulated while the
// remaining pages are visited. A directory failure is returned, not treated
// as an empty page or proof that obligations disappeared.
func (d *Discovery) Sweep(ctx context.Context) error {
	if d.Sessions == nil || d.Projections == nil || d.Executions == nil || d.Queue == nil {
		return errors.New("activation: durable session and execution discovery and scheduling are required")
	}
	var failures []error
	var after session.SessionID
	for {
		page, err := d.Sessions.ListSessions(ctx, after, d.pageSize())
		if err != nil {
			return errors.Join(append(failures, err)...)
		}
		for _, record := range page.Items {
			work, err := runtime.Inspect(ctx, d.Projections, record.ID)
			if session.IsCode(err, session.ErrNotFound) || session.IsCode(err, session.ErrDeleted) {
				continue // concurrently deleted; never infer an execution ACK
			}
			if err != nil {
				failures = append(failures, fmt.Errorf("activation: inspect %s: %w", record.ID, err))
				continue
			}
			if work.Ready || work.Pending {
				var due int64
				if !work.Ready {
					due = d.now().Add(probeDuration(d.ProbeInterval)).UnixMilli()
				}
				if err := d.Queue.Signal(ctx, scheduling.Signal{Session: record.ID, Source: "session", Revision: work.Revision, NotBeforeUnixMilli: due}); err != nil {
					failures = append(failures, err)
				}
			}
			if d.Responses != nil {
				for _, response := range work.Responses {
					if err := d.Responses.Ensure(ctx, record.ID, response); err != nil {
						failures = append(failures, err)
					}
				}
			}
		}
		if !page.More {
			break
		}
		if page.Next <= after {
			return errors.Join(append(failures, errors.New("activation: session directory did not advance"))...)
		}
		after = page.Next
	}
	if d.Inbox != nil {
		sessions, err := d.Inbox.Sessions(ctx, 0)
		if err != nil {
			failures = append(failures, err)
		} else {
			for _, sid := range sessions {
				pending, err := d.Inbox.Pending(ctx, sid)
				if err != nil {
					failures = append(failures, err)
					continue
				}
				if len(pending) == 0 {
					continue
				}
				// Inbox Seq starts at zero. The increasing enqueue sequence,
				// not a changing pending count, identifies new input work.
				last := pending[len(pending)-1].Seq
				if last == ^uint64(0) {
					failures = append(failures, errors.New("activation: inbox sequence exhausted"))
					continue
				}
				if err := d.Queue.Signal(ctx, scheduling.Signal{Session: sid, Source: "inbox", Revision: last + 1}); err != nil {
					failures = append(failures, err)
				}
			}
		}
	}
	var executionAfter effect.AssignmentKey
	for {
		page, err := d.Executions.ListOutstanding(ctx, executionAfter, d.pageSize())
		if err != nil {
			return errors.Join(append(failures, err)...)
		}
		for _, execution := range page.Items {
			var due int64
			if !execution.Status.Terminal() && execution.Owner != "" {
				// Lease expiry schedules observation, never blind replay.
				due = max(d.now().UnixMilli(), execution.LeaseUntilUnixMilli)
			}
			if err := d.Queue.Signal(ctx, scheduling.Signal{Session: session.SessionID(execution.Key.Session),
				Source: executionSource(execution.Key), Revision: execution.Revision, NotBeforeUnixMilli: due}); err != nil {
				failures = append(failures, err)
			}
		}
		if !page.More {
			break
		}
		if page.Next == executionAfter || len(page.Items) == 0 {
			return errors.Join(append(failures, errors.New("activation: execution directory did not advance"))...)
		}
		executionAfter = page.Next
	}
	return errors.Join(failures...)
}
