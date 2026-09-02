package run

import (
	"errors"
	"fmt"
)

// machineStateWireV1 is the persisted snapshot shape of MachineState for
// SchemaVersion1. It flattens the interface-typed Current into a discriminator
// plus at most one step body so the snapshot round-trips through JSON. Its
// canonical bytes are the InitialStateDigest preimage (RUN-NEW-1), so field
// names and omission rules are frozen with the schema.
type machineStateWireV1 struct {
	RunID           RunID        `json:"runId"`
	Status          RunStatus    `json:"status"`
	ModelSteps      int          `json:"modelSteps"`
	Usage           Usage        `json:"usage"`
	PendingInputs   []AgentInput `json:"pendingInputs"`
	LastModelResult *ModelResult `json:"lastModelResult"`
	Result          *RunResult   `json:"result"`
	LastToolStep    *ToolStep    `json:"lastToolStep,omitempty"`
	// Current is "open", "model" or "tool" for an active Run and absent for a
	// terminal one.
	Current   string     `json:"current,omitempty"`
	ModelStep *ModelStep `json:"modelStep,omitempty"`
	ToolStep  *ToolStep  `json:"toolStep,omitempty"`
}

const (
	currentWireOpen  = "open"
	currentWireModel = "model"
	currentWireTool  = "tool"
)

func machineStateToWireV1(s *MachineState) (machineStateWireV1, error) {
	w := machineStateWireV1{
		RunID: s.RunID, Status: s.Status, ModelSteps: s.ModelSteps,
		Usage: s.Usage, PendingInputs: s.PendingInputs,
		LastModelResult: s.LastModelResult, Result: s.Result, LastToolStep: s.LastToolStep,
	}
	switch cur := s.Current.(type) {
	case nil:
	case Open:
		w.Current = currentWireOpen
	case ModelStep:
		w.Current = currentWireModel
		w.ModelStep = &cur
	case ToolStep:
		w.Current = currentWireTool
		w.ToolStep = &cur
	default:
		return machineStateWireV1{}, fmt.Errorf("agent: snapshot: unknown current variant %T", s.Current)
	}
	return w, nil
}

func machineStateFromWireV1(w *machineStateWireV1) (MachineState, error) {
	s := MachineState{
		RunID: w.RunID, Status: w.Status, ModelSteps: w.ModelSteps,
		Usage: w.Usage, PendingInputs: w.PendingInputs,
		LastModelResult: w.LastModelResult, Result: w.Result, LastToolStep: w.LastToolStep,
	}
	switch w.Current {
	case "":
		if w.ModelStep != nil || w.ToolStep != nil {
			return MachineState{}, errors.New("agent: snapshot: step body without current discriminator")
		}
	case currentWireOpen:
		if w.ModelStep != nil || w.ToolStep != nil {
			return MachineState{}, errors.New("agent: snapshot: open state carries a step body")
		}
		s.Current = Open{}
	case currentWireModel:
		if w.ModelStep == nil || w.ToolStep != nil {
			return MachineState{}, errors.New("agent: snapshot: model current requires exactly a modelStep body")
		}
		s.Current = *w.ModelStep
	case currentWireTool:
		if w.ToolStep == nil || w.ModelStep != nil {
			return MachineState{}, errors.New("agent: snapshot: tool current requires exactly a toolStep body")
		}
		s.Current = *w.ToolStep
	default:
		return MachineState{}, fmt.Errorf("agent: snapshot: unknown current %q", w.Current)
	}
	return s, nil
}

// encodeMachineStateV1 renders the canonical v1 snapshot bytes.
func encodeMachineStateV1(s *MachineState) ([]byte, error) {
	if s == nil {
		return nil, errors.New("agent: snapshot: nil state")
	}
	w, err := machineStateToWireV1(s)
	if err != nil {
		return nil, err
	}
	return marshalCanonical(w)
}

// decodeMachineStateV1 parses v1 snapshot bytes, rejecting unknown fields,
// trailing data, and non-canonical-equivalent wire, then validates the
// structural invariants of the restored state.
func decodeMachineStateV1(raw []byte) (MachineState, error) {
	var w machineStateWireV1
	if err := decodeStrictJSON(raw, &w); err != nil {
		return MachineState{}, fmt.Errorf("agent: snapshot: %w", err)
	}
	s, err := machineStateFromWireV1(&w)
	if err != nil {
		return MachineState{}, err
	}
	if err := requireCanonicalEquivalent(raw, w); err != nil {
		return MachineState{}, fmt.Errorf("agent: snapshot: %w", err)
	}
	if err := ValidateMachineState(&s); err != nil {
		return MachineState{}, err
	}
	return s, nil
}
