package run

// Effect is the at-most-one pending action Machine.Next derives from the
// current state (RUN-MCH-4). Effects are never persisted; the Loop re-derives
// them after every Load.
type Effect interface{ effect() }

type NeedModelRequest struct {
	Hint PlanningHint
}

func (NeedModelRequest) effect() {}

type StartModelCall struct {
	StepID StepID
}

func (StartModelCall) effect() {}

type StartToolCalls struct {
	StepID  StepID
	CallIDs []CallID
}

func (StartToolCalls) effect() {}

// Idle means the Run is still active and Next has no executable effect.
// The usual case is a ToolStep whose remaining live calls are Waiting.
// Application reads those calls from MachineState; Loop does not interpret them.
type Idle struct{}

func (Idle) effect() {}

type WaitForExecutionRecovery struct{}

func (WaitForExecutionRecovery) effect() {}

// WaitingCalls returns the outstanding ResponseRequests on the current ToolStep.
// Application uses this after Loop returns LoopWaiting. The result is detached.
func WaitingCalls(s MachineState) []ResponseRequest {
	ts, ok := s.Current.(ToolStep)
	if !ok {
		return nil
	}
	var out []ResponseRequest
	for _, c := range ts.Calls {
		if c.Status != ToolWaiting || c.Waiting == nil {
			continue
		}
		cloned := cloneResponseRequest(c.Waiting)
		if cloned != nil {
			out = append(out, *cloned)
		}
	}
	return out
}

// PlanningHint is what the Loop hands the application RequestPlanner.
type PlanningHint struct {
	RunID      RunID
	SourceStep StepID
	Inputs     []AgentInput
	// LastToolStep contains the committed results of the preceding tool
	// boundary, allowing the planner to construct the next model request.
	LastToolStep    *ToolStep
	LastModelResult *ModelResult
}

// Next derives the pending effect from the current state (RUN-MCH-4).
// Terminal states yield no effect; callers check Status first.
//
//nolint:gocritic // hugeParam: Next is a pure value-state interpreter and must not mutate MachineState.
func Next(s MachineState) (Effect, error) {
	if s.Status.Terminal() {
		return nil, ErrRunTerminal
	}
	switch cur := s.Current.(type) {
	case nil:
		return NeedModelRequest{Hint: PlanningHint{
			RunID:           s.RunID,
			SourceStep:      s.LastClosedStep,
			Inputs:          append([]AgentInput(nil), s.PendingInputs...),
			LastToolStep:    cloneToolStepPtr(s.LastToolStep),
			LastModelResult: cloneModelResult(s.LastModelResult),
		}}, nil
	case ModelStep:
		if cur.Status == ModelPrepared {
			return StartModelCall{StepID: cur.RefValue.ID}, nil
		}
		return WaitForExecutionRecovery{}, nil
	case ToolStep:
		var pending []CallID
		waiting := false
		executing := false
		for _, c := range cur.Calls {
			switch c.Status {
			case ToolPending:
				pending = append(pending, c.CallID)
			case ToolWaiting:
				waiting = true
			case ToolExecuting:
				executing = true
			}
		}
		if len(pending) > 0 {
			return StartToolCalls{StepID: cur.RefValue.ID, CallIDs: pending}, nil
		}
		if executing {
			return WaitForExecutionRecovery{}, nil
		}
		if waiting {
			return Idle{}, nil
		}
		return nil, rejectionf("next: tool step %q has no live calls but was not closed", cur.RefValue.ID)
	default:
		return nil, rejectionf("next: unknown step variant %T", s.Current)
	}
}
