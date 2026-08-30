package run

import "fmt"

// Evolve folds one fact with the current schema version. Persisted replay uses
// EvolveVersion so future schema versions can keep their historical folding
// semantics while this convenience API remains value-based.
//
//nolint:gocritic // hugeParam: public fold boundary must stay value-based: Evolve(state, fact) -> new state.
func Evolve(s MachineState, f Fact) (MachineState, error) {
	return EvolveVersion(currentSchemaVersion, s, f)
}

// EvolveVersion folds one persisted fact using the Evolve semantics for its
// SchemaVersion. It is mechanical: no IO, no policy decisions, and no Decide.
//
//nolint:gocritic // hugeParam: public replay boundary must stay value-based: EvolveVersion(version, state, fact) -> new state.
func EvolveVersion(schemaVersion uint16, s MachineState, f Fact) (MachineState, error) {
	switch schemaVersion {
	case SchemaVersion1:
		return evolveV1(s, f)
	default:
		return s, fmt.Errorf("agent: evolve: unsupported schema version %d", schemaVersion)
	}
}

// evolveV1 is the frozen fold semantics for SchemaVersion1.
//
//nolint:gocritic // hugeParam: v1 fold body intentionally preserves value-state semantics.
func evolveV1(s MachineState, f Fact) (MachineState, error) {
	switch fact := f.(type) {
	case ModelStepPrepared:
		if s.Current != nil {
			return s, fmt.Errorf("agent: evolve: model step prepared while a step is current")
		}
		// v1 preparation is the atomic consumption boundary for pending inputs.
		// A persisted fact must name every pending input exactly once, in queue
		// order; accepting a subset or an invented ID would make replay diverge
		// from the command that created this frozen request.
		if len(fact.InputIDs) != len(s.PendingInputs) {
			return s, fmt.Errorf("agent: evolve: model step prepared input IDs do not completely consume pending inputs: got %d, want %d", len(fact.InputIDs), len(s.PendingInputs))
		}
		for i, input := range s.PendingInputs {
			if fact.InputIDs[i] != input.ID {
				return s, fmt.Errorf("agent: evolve: model step prepared input ID at position %d = %q, want pending input %q", i, fact.InputIDs[i], input.ID)
			}
		}
		s.Current = ModelStep{
			RefValue:      StepRef{RunID: s.RunID, ID: fact.StepID, Digest: fact.BindingDigest},
			Request:       fact.Request,
			RequestDigest: fact.RequestDigest,
			Model:         fact.Model,
			Tools:         fact.Tools,
			ToolsDigest:   fact.ToolsDigest,
			Status:        ModelPrepared,
		}
		s.ModelSteps++
		s.PendingInputs = nil
		return s, nil

	case ModelStepStarted:
		ms, err := evolveModelStep(&s, fact.StepID)
		if err != nil {
			return s, err
		}
		ms.Status = ModelExecuting
		s.Current = *ms
		return s, nil

	case ModelStepRecovered:
		ms, err := evolveModelStep(&s, fact.StepID)
		if err != nil {
			return s, err
		}
		ms.Status = ModelPrepared
		s.Current = *ms
		return s, nil

	case ModelStepRejected:
		ms, err := evolveModelStep(&s, fact.StepID)
		if err != nil {
			return s, err
		}
		ms.Rejects++
		ms.Status = ModelPrepared
		s.Current = *ms
		s.Usage = s.Usage.Add(fact.Usage)
		return s, nil

	case ModelStepCompleted:
		if _, err := evolveModelStep(&s, fact.StepID); err != nil {
			return s, err
		}
		result := fact.Result
		s.LastModelResult = &result
		s.Usage = s.Usage.Add(fact.Result.Usage)
		s.Current = nil
		return s, nil

	case ToolStepOpened:
		if s.Current != nil {
			return s, fmt.Errorf("agent: evolve: tool step opened while a step is current")
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
			if err := ValidateToolCallState(calls[i]); err != nil {
				return s, err
			}
		}
		s.Current = ToolStep{
			RefValue: StepRef{RunID: s.RunID, ID: fact.StepID, Digest: fact.BindingSetDigest},
			Source:   fact.Source,
			Calls:    calls,
		}
		return s, nil

	case ToolCallStarted:
		return evolveCall(&s, fact.StepID, fact.CallID, func(c *ToolCallState) {
			c.Status = ToolExecuting
		})

	case ToolCallApproved:
		return evolveCall(&s, fact.StepID, fact.CallID, func(c *ToolCallState) {
			c.Status = ToolPending
			c.Waiting = nil
		})

	case ToolCallCompleted:
		return evolveCall(&s, fact.StepID, fact.CallID, func(c *ToolCallState) {
			c.Status = ToolCompleted
			r := fact.Result
			c.Result = &r
			c.Waiting = nil
		})

	case ToolCallAnswered:
		return evolveCall(&s, fact.StepID, fact.CallID, func(c *ToolCallState) {
			c.Status = ToolCompleted
			c.Result = &ToolExecutionResult{Output: fact.Payload}
			c.Waiting = nil
		})

	case ToolCallFailed:
		return evolveCall(&s, fact.StepID, fact.CallID, func(c *ToolCallState) {
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
		s.LastClosedStep = fact.StepID
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

func evolveModelStep(s *MachineState, step StepID) (*ModelStep, error) {
	ms, ok := s.Current.(ModelStep)
	if !ok || ms.RefValue.ID != step {
		return nil, fmt.Errorf("agent: evolve: model step %q is not current", step)
	}
	return &ms, nil
}

func evolveCall(s *MachineState, step StepID, call CallID, apply func(*ToolCallState)) (MachineState, error) {
	ts, ok := s.Current.(ToolStep)
	if !ok || ts.RefValue.ID != step {
		return *s, fmt.Errorf("agent: evolve: tool step %q is not current", step)
	}
	i := ts.callIndex(call)
	if i < 0 {
		return *s, fmt.Errorf("agent: evolve: unknown call %q", call)
	}
	calls := append([]ToolCallState(nil), ts.Calls...)
	apply(&calls[i])
	// Spec §4.2: Evolve must reject illegal field combinations, e.g. an
	// unknown-outcome failure whose class is not effect_unknown.
	if err := ValidateToolCallState(calls[i]); err != nil {
		return *s, err
	}
	ts.Calls = calls
	s.Current = ts
	return *s, nil
}
