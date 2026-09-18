package plan

import (
	"fmt"

	"github.com/felinics/twilight/agent/run"
)

// Effect is the at-most-one pending action Machine.Next derives from the
// current state (RUN-MCH-4). Effects are never persisted; the Loop re-derives
// them after every Load.
type Effect interface{ effect() }

type NeedModelRequest struct {
	Hint PromptInput
}

func (NeedModelRequest) effect() {}

type StartModelCall struct {
	StepID run.StepID
}

func (StartModelCall) effect() {}

// WithdrawPrepared asks the Loop to commit WithdrawPreparedStep: inputs were
// accepted after this step was Prepared, so its frozen request is incomplete
// and the Run should replan.
type WithdrawPrepared struct {
	StepID run.StepID
}

func (WithdrawPrepared) effect() {}

type StartToolCalls struct {
	StepID  run.StepID
	CallIDs []run.CallID
}

func (StartToolCalls) effect() {}

// Idle means the Run is still active and Next has no executable effect.
// Application inspects MachineState with WaitingCalls, ExecutingCalls, and
// NeedsRecovery. Loop does not interpret those queries.
type Idle struct{}

func (Idle) effect() {}

// WaitingCalls returns the outstanding ResponseRequests on the current ToolStep.
// Application uses this after Loop returns LoopWaiting. The result is detached.
func WaitingCalls(s run.MachineState) []run.ResponseRequest { //nolint:gocritic // hugeParam: read-only query over a detached state value
	ts, ok := s.Current.(run.ToolStep)
	if !ok {
		return nil
	}
	var out []run.ResponseRequest
	for i := range ts.Calls {
		c := &ts.Calls[i]
		if c.Status != run.ToolWaiting || c.Waiting == nil {
			continue
		}
		cloned := run.CloneResponseRequest(c.Waiting)
		if cloned != nil {
			out = append(out, *cloned)
		}
	}
	return out
}

// ExecutingCalls returns CallIDs still Executing on the current ToolStep.
func ExecutingCalls(s run.MachineState) []run.CallID { //nolint:gocritic // hugeParam: read-only query over a detached state value
	ts, ok := s.Current.(run.ToolStep)
	if !ok {
		return nil
	}
	var out []run.CallID
	for i := range ts.Calls {
		c := &ts.Calls[i]
		if c.Status == run.ToolExecuting {
			out = append(out, c.CallID)
		}
	}
	return out
}

// NeedsRecovery reports that an execution is in flight and this process has
// no Start effect for it: a ModelStep is Executing, or a ToolStep has
// Executing calls and no Pending calls.
func NeedsRecovery(s run.MachineState) bool { //nolint:gocritic // hugeParam: read-only query over a detached state value
	switch cur := s.Current.(type) {
	case run.ModelStep:
		return cur.Status == run.ModelExecuting
	case run.ToolStep:
		pending := false
		executing := false
		for i := range cur.Calls {
			c := &cur.Calls[i]
			switch c.Status {
			case run.ToolPending:
				pending = true
			case run.ToolExecuting:
				executing = true
			}
		}
		return executing && !pending
	default:
		return false
	}
}

// PromptInput is what the Loop hands the application PromptBuilder: the Run
// boundary facts only. Conversation content (previous assistant output, tool
// results) is read from the Session by the prompt builder itself.
type PromptInput struct {
	Scope      run.Scope // filled by the Loop; Next does not know it
	Owner      run.OwnerID
	RunID      run.RunID
	SourceStep run.StepID
	Inputs     []run.AgentInput
}

// Next derives the pending effect from the current state (RUN-MCH-4).
// Terminal states return ErrRunTerminal; callers check Status first.
//
//nolint:gocritic // hugeParam: Next is a pure value-state interpreter and must not mutate MachineState.
func Next(s run.MachineState) (Effect, error) {
	if s.Status.Terminal() {
		return nil, run.ErrRunTerminal
	}
	switch cur := s.Current.(type) {
	case run.Open:
		var source run.StepID
		if s.LastToolStep != nil {
			source = s.LastToolStep.RefValue.ID
		}
		return NeedModelRequest{Hint: PromptInput{
			Owner:      s.Owner,
			RunID:      s.RunID,
			SourceStep: source,
			Inputs:     append([]run.AgentInput(nil), s.PendingInputs...),
		}}, nil
	case run.ModelStep:
		if cur.Status == run.ModelPrepared {
			if len(s.PendingInputs) > 0 {
				return WithdrawPrepared{StepID: cur.RefValue.ID}, nil
			}
			return StartModelCall{StepID: cur.RefValue.ID}, nil
		}
		return Idle{}, nil
	case run.ToolStep:
		var pending []run.CallID
		live := false
		for _, c := range cur.Calls {
			switch c.Status {
			case run.ToolPending:
				pending = append(pending, c.CallID)
				live = true
			case run.ToolWaiting, run.ToolExecuting:
				live = true
			}
		}
		if len(pending) > 0 {
			return StartToolCalls{StepID: cur.RefValue.ID, CallIDs: pending}, nil
		}
		if live {
			return Idle{}, nil
		}
		return nil, fmt.Errorf("agent: next: tool step %q has no live calls but was not closed", cur.RefValue.ID)
	default:
		return nil, fmt.Errorf("agent: next: unknown current variant %T", s.Current)
	}
}
