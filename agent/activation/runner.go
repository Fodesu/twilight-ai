package activation

import (
	"context"
	"errors"
	"time"

	"github.com/felinics/twilight/agent/inbox"
	"github.com/felinics/twilight/agent/ownership"
	"github.com/felinics/twilight/agent/scheduling"
	"github.com/felinics/twilight/agentcore/driver"
	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	runtime "github.com/felinics/twilight/agentcore/runtime"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/sessionkernel"
)

// Runner performs one disposable Activation per claim. It keeps no Session
// table, recovery callback or per-Session watcher. Discovery must be served
// independently; a queue notification by itself is not a recovery mechanism.
type Runner struct {
	Queue         scheduling.Store
	Ownership     *ownership.Manager
	Kernel        *sessionkernel.Kernel
	Execution     *runtime.Execution
	Preset        preset.PresetRef
	Inbox         *inbox.Consumer
	Owner         string
	StepBudget    int
	CommandBudget int
	ClaimDuration time.Duration
	// MaxDuration bounds acquisition and all local work; zero selects 30s.
	// It does not bound the independently owned external execution.
	MaxDuration   time.Duration
	RetryInterval time.Duration
	ProbeInterval time.Duration
	Now           func() time.Time
}

func (r *Runner) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *Runner) retryAt() int64 {
	delay := r.RetryInterval
	if delay <= 0 {
		delay = time.Second
	}
	return max(1, r.now().Add(delay).UnixMilli())
}

// RunOne claims work, acquires a fresh Session capability, performs finite
// steps, then closes local computation and releases ownership before finishing
// the job. Errors and ErrOwned reschedule instead of swallowing the wakeup.
// A crash before Finish leaves an expiring claim; a crash before Signal is
// repaired by Discovery from the durable source. Neither requires a cross-
// authority transaction or an in-memory notification to survive.
func (r *Runner) RunOne(ctx context.Context) (worked bool, err error) {
	if r.Queue == nil || r.Ownership == nil || r.Kernel == nil || r.Execution == nil || r.Owner == "" {
		return false, errors.New("activation: scheduling, ownership, kernel, execution and owner are required")
	}
	directory, ok := r.Execution.Executor.(effect.OutstandingPort)
	if !ok {
		return false, effect.ErrOutstandingUnsupported
	}
	lease := r.ClaimDuration
	if lease <= 0 {
		lease = 30 * time.Second
	}
	claim, claimed, err := r.Queue.Claim(ctx, r.Owner, lease)
	if err != nil || !claimed {
		return false, err
	}
	duration := r.MaxDuration
	if duration <= 0 {
		duration = 30 * time.Second
	}
	workCtx, cancelWork := context.WithTimeout(ctx, duration)
	defer cancelWork()
	claimLife := maintainClaim(workCtx, r.Queue, claim, lease)
	nextAt := r.retryAt() // conservative until every source has been inspected
	var owned *ownership.Handle
	defer func() {
		// Request cancellation is not a semantic Stop. Cleanup is bounded and
		// detached, and never calls Executor.Cancel on that basis.
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if owned != nil {
			if closeErr := owned.Close(cleanup); closeErr != nil {
				err = errors.Join(err, closeErr)
				nextAt = r.retryAt()
			}
		}
		if claimErr := claimLife.close(); claimErr != nil {
			err = errors.Join(err, claimErr)
			nextAt = r.retryAt()
		}
		if finishErr := r.Queue.Finish(cleanup, claim, nextAt); finishErr != nil {
			err = errors.Join(err, finishErr)
		}
	}()
	owned, err = r.Ownership.Acquire(workCtx, claim.Session)
	if err != nil {
		return true, err
	}
	w := owned.Writer()
	commandBudget := r.CommandBudget
	if commandBudget <= 0 {
		commandBudget = 32
	}
	if r.Inbox != nil {
		if _, err := r.Inbox.Consume(workCtx, w, commandBudget); err != nil {
			return true, err
		}
	}
	executionsPending, err := r.reconcileExecutions(workCtx, w, directory)
	if err != nil {
		return true, err
	}
	conversation, err := runtime.New(runtime.Config{Writer: w, Driver: r.Execution.Driver,
		Turns: r.Kernel.Turns, Chatlog: r.Kernel.Chatlog, Preset: r.Preset})
	if err != nil {
		return true, err
	}
	budget := r.StepBudget
	if budget <= 0 {
		budget = 32
	}
	for range budget {
		if err := workCtx.Err(); err != nil {
			return true, err
		}
		result, err := conversation.Step(workCtx)
		if err != nil {
			return true, err
		}
		if result.Disposition == driver.Waiting {
			break
		}
		work, err := runtime.Inspect(workCtx, w.Projections(), claim.Session)
		if err != nil {
			return true, err
		}
		if !work.Ready {
			break
		}
	}
	// There are no local driver goroutines to join: Step and Consume retain
	// no Writer. Close fences local commits before the persistent Release.
	work, err := runtime.Inspect(workCtx, w.Projections(), claim.Session)
	if err != nil {
		return true, err
	}
	inboxPending := false
	if r.Inbox != nil {
		pending, err := r.Inbox.Store.Pending(workCtx, claim.Session)
		if err != nil {
			return true, err
		}
		inboxPending = len(pending) > 0
	}
	switch {
	case work.Ready || inboxPending:
		nextAt = max(1, r.now().UnixMilli()) // time-slice continuation
	case work.Pending || executionsPending:
		nextAt = max(1, r.now().Add(probeDuration(r.ProbeInterval)).UnixMilli())
	default:
		nextAt = 0
	}
	return true, nil
}

func (r *Runner) reconcileExecutions(ctx context.Context, w writer.Writer, directory effect.OutstandingPort) (bool, error) {
	// Key ordering allows seeking directly to this Session's range without
	// enumerating every other Session's executions for each Activation.
	after := effect.AssignmentKey{Session: run.Scope(w.SessionID())}
	pending := false
	for {
		page, err := directory.ListOutstanding(ctx, after, 128)
		if err != nil {
			return pending, err
		}
		for _, execution := range page.Items {
			if execution.Key.Session != run.Scope(w.SessionID()) {
				return pending, nil
			}
			if err := r.Execution.Driver.ReconcileExecution(ctx, w, execution.Key); err != nil {
				return pending, err
			}
			// The page can race our ACK. An extra bounded probe is harmless;
			// clearing an unseen settlement obligation would not be.
			pending = true
		}
		if !page.More {
			return pending, nil
		}
		if len(page.Items) == 0 || page.Next == after {
			return pending, errors.New("activation: execution directory did not advance")
		}
		after = page.Next
	}
}
