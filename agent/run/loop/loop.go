package loop

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"

	run "github.com/memohai/twilight/agent/run"
)

// Loop is the in-process interpreter of one Run (RUN-LOP-2). It holds no
// authoritative state; every iteration starts from Runtime.Load.
type Loop struct {
	Models    ModelCatalog
	Tools     ToolCatalog
	Planner   RequestPlanner
	Execution ExecutionPolicy
	Streaming bool

	// starts retains the identity of an in-flight start command across a
	// transient Run return. A commit may have succeeded while its response was
	// lost; reusing the same command and claim lets the next Run recover the
	// original grant without issuing a second start.
	startsMu sync.Mutex
	starts   map[startKey]startAttempt

	settlementsMu sync.Mutex
	settlements   map[startKey]settlementAttempt

	runsMu   sync.Mutex
	runs     map[run.RunID]struct{}
	eventsMu sync.Mutex
}

// New validates and normalizes the execution policy (RUN-LOP-1).
func New(models ModelCatalog, tools ToolCatalog, planner RequestPlanner, policy ExecutionPolicy, streaming bool) (*Loop, error) {
	if models == nil {
		return nil, errors.New("agent: loop: nil model catalog")
	}
	if tools == nil {
		return nil, errors.New("agent: loop: nil tool catalog")
	}
	if planner == nil {
		return nil, errors.New("agent: loop: nil request planner")
	}
	if policy.ToolExecution != "" && policy.ToolExecution != ToolExecutionParallel && policy.ToolExecution != ToolExecutionSequential {
		return nil, fmt.Errorf("agent: loop: unknown ToolExecution mode %q", policy.ToolExecution)
	}
	if policy.MaxParallel < 0 {
		return nil, errors.New("agent: loop: negative MaxParallel")
	}
	if policy.LeaseRenewInterval < 0 {
		return nil, errors.New("agent: loop: negative LeaseRenewInterval")
	}
	return &Loop{Models: models, Tools: tools, Planner: planner, Execution: policy, Streaming: streaming,
		starts: make(map[startKey]startAttempt), settlements: make(map[startKey]settlementAttempt), runs: make(map[run.RunID]struct{})}, nil
}

func (l *Loop) toolScheduling() run.ToolScheduling {
	mode := run.ToolScheduleMode(l.Execution.ToolExecution)
	if mode == "" {
		mode = run.ToolScheduleParallel
	}
	return run.ToolScheduling{Mode: mode, MaxParallel: l.Execution.MaxParallel}
}

func (l *Loop) acquireRun(runID run.RunID) error {
	l.runsMu.Lock()
	defer l.runsMu.Unlock()
	if _, ok := l.runs[runID]; ok {
		return ErrRunAlreadyRunning
	}
	l.runs[runID] = struct{}{}
	return nil
}

func (l *Loop) releaseRun(runID run.RunID) {
	l.runsMu.Lock()
	delete(l.runs, runID)
	l.runsMu.Unlock()
}

// Run drives the Run until it finishes, has no executable effect, or the
// context is cancelled (RUN-LOP-2). The caller context remains active for
// reads and normal control commits. Accepted effect settlements use a
// detached control context so worker cancellation cannot discard their outcome.
func (l *Loop) Run(ctx context.Context, runtime run.Runtime, runID run.RunID, events EventSink) (LoopResult, error) {
	if ctx == nil {
		return LoopResult{}, errors.New("agent: loop: nil context")
	}
	if runtime == nil {
		return LoopResult{}, errors.New("agent: loop: nil runtime")
	}
	if runID == "" {
		return LoopResult{}, errors.New("agent: loop: empty RunID")
	}
	if err := l.acquireRun(runID); err != nil {
		return LoopResult{}, err
	}
	defer l.releaseRun(runID)
	if events != nil {
		events = &serializedEventSink{sink: events, mu: &l.eventsMu}
	}

	for {
		if err := ctx.Err(); err != nil {
			// Workers started by this Loop have already been settled by the
			// branches below before we reach this check.
			return LoopResult{}, err
		}
		snapshot, err := runtime.Load(ctx, runID)
		if err != nil {
			return LoopResult{}, err
		}
		if snapshot.State.RunID != runID {
			return LoopResult{}, fmt.Errorf("agent: loop: runtime returned RunID %q for %q", snapshot.State.RunID, runID)
		}
		if snapshot.State.Status.Terminal() {
			l.forgetRunCaches(runID)
			if events != nil {
				_ = events.Emit(ctx, Event{
					RunID:      snapshot.State.RunID,
					Kind:       EventRunFinished,
					Durability: EventCommitted,
				})
			}
			return LoopResult{Disposition: LoopFinished, Result: snapshot.State.Result}, nil
		}

		if handled, err := l.resumeSettlement(ctx, runtime, events, &snapshot); err != nil {
			return LoopResult{}, err
		} else if handled {
			continue
		}
		if handled, err := l.resumeCachedStart(ctx, runtime, events, &snapshot); err != nil {
			return LoopResult{}, err
		} else if handled {
			continue
		}

		effect, err := run.Next(snapshot.State)
		if err != nil {
			return LoopResult{}, err
		}

		switch eff := effect.(type) {
		case run.NeedModelRequest:
			if err := l.planAndPrepare(ctx, runtime, events, &snapshot, eff.Hint); err != nil {
				return LoopResult{}, err
			}
		case run.StartModelCall:
			if err := l.runModelStep(ctx, runtime, events, &snapshot, eff.StepID); err != nil {
				return LoopResult{}, err
			}
		case run.StartToolCalls:
			if err := l.runToolCalls(ctx, runtime, events, &snapshot, eff); err != nil {
				return LoopResult{}, err
			}
		case run.Idle:
			recovery := run.NeedsRecovery(snapshot.State)
			reason := WaitReason("")
			if recovery {
				reason = ExecutionRecovery
			}
			return LoopResult{Disposition: LoopWaiting, Reason: reason, ExecutionRecovery: recovery}, nil
		default:
			return LoopResult{}, fmt.Errorf("agent: loop: unknown effect %T", effect)
		}
	}
}

// commit builds the envelope via the sanctioned constructor and submits it.
// A non-sentinel commit failure is replayed once with the same CommandID and
// digest (RUN-LOP-5): if the first attempt actually
// committed and only the response was lost, the replay returns AlreadyApplied
// instead of abandoning a live grant or re-executing an expensive step.
func (l *Loop) commit(ctx context.Context, runtime run.Runtime, runID run.RunID, id run.CommandID, base uint64, grant run.ExecutionGrant, cmd run.AgentCommand, proto run.Protocol) (run.CommitResult, error) {
	if proto.Version() == 0 {
		return run.CommitResult{}, errors.New("agent: loop: uninitialized protocol")
	}
	env, err := proto.BuildEnvelope(runID, id, cmd)
	if err != nil {
		return run.CommitResult{}, err
	}
	req := run.CommitRequest{BaseRevision: base, Grant: grant, Command: env}
	res, err := runtime.Commit(ctx, req)
	if err != nil && !retriable(err) {
		res, err = runtime.Commit(ctx, req)
	}
	return res, err
}

// retriable reports the commit errors that mean "reload and rederive".
func retriable(err error) bool {
	return errors.Is(err, run.ErrStaleRuntime) || errors.Is(err, run.ErrRunTerminal) || errors.Is(err, run.ErrCommandConflict)
}

func freshCommandID() run.CommandID {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("agent: loop: %v", err))
	}
	return run.CommandID(hex.EncodeToString(b[:]))
}

func freshExecutionClaim() run.ExecutionClaim {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("agent: loop: %v", err))
	}
	return run.ExecutionClaim(hex.EncodeToString(b[:]))
}
