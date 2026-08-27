package agent

import "fmt"

// Evolve folds one fact into the state (spec §3.7.2). It is mechanical: no
// RunConfig reads, no policy branches, total over every fact Decide produces.
// Its folding semantics, together with the canonical encoding, form the
// permanent compatibility contract of a published SchemaVersion; replay
// depends only on Evolve.
func Evolve(s MachineState, f Fact) (MachineState, error) {
	switch fact := f.(type) {
	case ModelStepPrepared:
		binding, err := DigestModelStepBinding(fact.Model, fact.RequestDigest, fact.ToolsDigest)
		if err != nil {
			return s, err
		}
		s.Current = ModelStep{
			RefValue:      StepRef{RunID: s.RunID, ID: fact.StepID, Digest: binding},
			Request:       fact.Request,
			RequestDigest: fact.RequestDigest,
			Model:         fact.Model,
			Tools:         fact.Tools,
			ToolsDigest:   fact.ToolsDigest,
			Status:        ModelPrepared,
		}
		s.ModelSteps++
		s.PendingInputs = removeInputs(s.PendingInputs, fact.InputIDs)
		return s, nil

	case ModelStepStarted:
		ms, err := evolveModelStep(s, fact.StepID)
		if err != nil {
			return s, err
		}
		ms.Status = ModelExecuting
		s.Current = *ms
		return s, nil

	case ModelStepRecovered:
		ms, err := evolveModelStep(s, fact.StepID)
		if err != nil {
			return s, err
		}
		ms.Status = ModelPrepared
		s.Current = *ms
		return s, nil

	case ModelStepRejected:
		ms, err := evolveModelStep(s, fact.StepID)
		if err != nil {
			return s, err
		}
		ms.Rejects++
		ms.Status = ModelPrepared
		s.Current = *ms
		s.Usage = addUsage(s.Usage, fact.Usage)
		return s, nil

	case ModelStepCompleted:
		if _, err := evolveModelStep(s, fact.StepID); err != nil {
			return s, err
		}
		result := fact.Result
		s.LastModelResult = &result
		s.Usage = addUsage(s.Usage, fact.Result.Usage)
		s.Current = nil
		return s, nil

	case ToolStepOpened:
		if s.Current != nil {
			return s, fmt.Errorf("agent: evolve: tool step opened while a step is current")
		}
		setDigest, err := digestBindingSet(fact.Calls)
		if err != nil {
			return s, err
		}
		calls := make([]ToolCallState, len(fact.Calls))
		for i, b := range fact.Calls {
			status := ToolPending
			var waiting *ResponseRequest
			if b.Response != nil {
				status = ToolWaiting
				w := *b.Response
				waiting = &w
			}
			calls[i] = ToolCallState{
				CallID:           b.CallID,
				ToolRef:          b.ToolRef,
				DefinitionDigest: b.DefinitionDigest,
				BindingDigest:    b.BindingDigest,
				Arguments:        b.Arguments,
				Policy:           b.Policy,
				Status:           status,
				Waiting:          waiting,
			}
		}
		s.Current = ToolStep{
			RefValue: StepRef{RunID: s.RunID, ID: fact.StepID, Digest: setDigest},
			Source:   fact.Source,
			Calls:    calls,
		}
		return s, nil

	case ToolCallStarted:
		return evolveCall(s, fact.StepID, fact.CallID, func(c *ToolCallState) {
			c.Status = ToolExecuting
		})

	case ToolCallApproved:
		return evolveCall(s, fact.StepID, fact.CallID, func(c *ToolCallState) {
			c.Status = ToolPending
			c.Waiting = nil
		})

	case ToolCallCompleted:
		return evolveCall(s, fact.StepID, fact.CallID, func(c *ToolCallState) {
			c.Status = ToolCompleted
			r := fact.Result
			c.Result = &r
			c.Waiting = nil
		})

	case ToolCallAnswered:
		return evolveCall(s, fact.StepID, fact.CallID, func(c *ToolCallState) {
			c.Status = ToolCompleted
			c.Result = &ToolExecutionResult{Output: fact.Payload}
			c.Waiting = nil
		})

	case ToolCallFailed:
		return evolveCall(s, fact.StepID, fact.CallID, func(c *ToolCallState) {
			c.Status = ToolFailed
			c.Failure = &ToolCallFailure{Failure: fact.Failure, Outcome: fact.Outcome}
			c.Waiting = nil
		})

	case ToolStepClosed:
		ts, ok := s.Current.(ToolStep)
		if !ok || ts.RefValue.ID != fact.StepID {
			return s, fmt.Errorf("agent: evolve: tool step %q is not current", fact.StepID)
		}
		s.Current = nil
		return s, nil

	case InputAccepted:
		for _, in := range s.PendingInputs {
			if in.ID == fact.Input.ID {
				return s, nil // idempotent append per InputID
			}
		}
		s.PendingInputs = append(append([]AgentInput(nil), s.PendingInputs...), fact.Input)
		return s, nil

	case RunEnded:
		s.Status = fact.Status
		s.Current = nil
		s.Result = &RunResult{
			Status:  fact.Status,
			Reason:  fact.Reason,
			Failure: fact.Failure,
			Model:   s.LastModelResult,
			Usage:   s.Usage,
		}
		return s, nil

	default:
		return s, fmt.Errorf("agent: evolve: unknown fact variant %T", f)
	}
}

func evolveModelStep(s MachineState, step StepID) (*ModelStep, error) {
	ms, ok := s.Current.(ModelStep)
	if !ok || ms.RefValue.ID != step {
		return nil, fmt.Errorf("agent: evolve: model step %q is not current", step)
	}
	return &ms, nil
}

func evolveCall(s MachineState, step StepID, call CallID, apply func(*ToolCallState)) (MachineState, error) {
	ts, ok := s.Current.(ToolStep)
	if !ok || ts.RefValue.ID != step {
		return s, fmt.Errorf("agent: evolve: tool step %q is not current", step)
	}
	i := ts.callIndex(call)
	if i < 0 {
		return s, fmt.Errorf("agent: evolve: unknown call %q", call)
	}
	calls := append([]ToolCallState(nil), ts.Calls...)
	apply(&calls[i])
	ts.Calls = calls
	s.Current = ts
	return s, nil
}

func removeInputs(inputs []AgentInput, ids []InputID) []AgentInput {
	if len(ids) == 0 {
		return inputs
	}
	drop := make(map[InputID]bool, len(ids))
	for _, id := range ids {
		drop[id] = true
	}
	var kept []AgentInput
	for _, in := range inputs {
		if !drop[in.ID] {
			kept = append(kept, in)
		}
	}
	return kept
}
