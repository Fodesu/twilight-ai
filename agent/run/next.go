package run

// Effect is the at-most-one pending action Machine.Next derives from the
// current state (spec §3.6). Effects are never persisted; the Loop re-derives
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

type WaitForResponse struct {
	Requests []ResponseRequest
}

func (WaitForResponse) effect() {}

type WaitForExecutionRecovery struct{}

func (WaitForExecutionRecovery) effect() {}

// PlanningHint is what the Loop hands the application RequestPlanner.
type PlanningHint struct {
	RunID      RunID
	SourceStep StepID
	Inputs     []AgentInput
}

// Next derives the pending effect from the current state (spec §3.7.3).
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
			RunID:      s.RunID,
			SourceStep: s.LastClosedStep,
			Inputs:     append([]AgentInput(nil), s.PendingInputs...),
		}}, nil
	case ModelStep:
		if cur.Status == ModelPrepared {
			return StartModelCall{StepID: cur.RefValue.ID}, nil
		}
		return WaitForExecutionRecovery{}, nil
	case ToolStep:
		var pending []CallID
		var waiting []ResponseRequest
		executing := false
		for _, c := range cur.Calls {
			switch c.Status {
			case ToolPending:
				pending = append(pending, c.CallID)
			case ToolWaiting:
				if c.Waiting != nil {
					waiting = append(waiting, *c.Waiting)
				}
			case ToolExecuting:
				executing = true
			}
		}
		if len(pending) > 0 {
			return StartToolCalls{StepID: cur.RefValue.ID, CallIDs: pending}, nil
		}
		if len(waiting) > 0 {
			// Even with an Executing call alongside, the Loop waits for a
			// response or an execution wake (spec §3.7.3).
			return WaitForResponse{Requests: waiting}, nil
		}
		if executing {
			return WaitForExecutionRecovery{}, nil
		}
		return nil, rejectionf("next: tool step %q has no live calls but was not closed", cur.RefValue.ID)
	default:
		return nil, rejectionf("next: unknown step variant %T", s.Current)
	}
}
