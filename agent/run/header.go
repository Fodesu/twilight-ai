package run

import (
	"errors"
	"fmt"

	"github.com/memohai/twilight/agent/es"
)

// snapshotSchemaVersion versions the MachineState wire shape used inside
// RunHeader.InitialState. It is independent of the event SchemaVersion: event
// encoding is frozen forever, while the snapshot shape may evolve with a
// version bump (spec §8.2).
const snapshotSchemaVersion uint16 = 1

// encodeMachineStateWire renders the canonical bytes of a MachineState for
// header digests. The interface-typed Current step is flattened the same way
// state equivalence comparison does.
func encodeMachineStateWire(s *MachineState) ([]byte, error) {
	return marshalCanonical(stateComparable(s))
}

// RunHeader is the formal persisted Revision-0 protocol record (spec §5.1.1).
// The complete execution authority is RunHeader + TransitionRecord log; every
// fold starts from the header's initial state. It is immutable after
// creation; ordinary Runtime.Commit never touches it.
type RunHeader struct {
	SchemaVersion       uint16         `json:"schemaVersion"`
	RunID               RunID          `json:"runId"`
	InitialStateVersion uint16         `json:"initialStateVersion"`
	InitialState        MachineState   `json:"initialState"`
	InitialStateDigest  Digest         `json:"initialStateDigest"`
	// CausationID links the Run to the creating session/queue/application
	// operation. Opaque to run; the application defines its interpretation.
	CausationID  es.CausationID `json:"causationId,omitempty"`
	HeaderDigest Digest         `json:"headerDigest"`
}

type runHeaderDigestBody struct {
	SchemaVersion       uint16         `json:"schemaVersion"`
	RunID               RunID          `json:"runId"`
	InitialStateVersion uint16         `json:"initialStateVersion"`
	InitialStateDigest  Digest         `json:"initialStateDigest"`
	CausationID         es.CausationID `json:"causationId,omitempty"`
}

// BuildRunHeader creates the immutable header for a new Run. The initial
// state is the minimal InitializeRun state: no seed input, no model policy,
// no limits — the first input arrives as the Revision-1 AcceptInput
// transition (spec §5.1.1 rules 2-3).
func BuildRunHeader(runID RunID, causationID es.CausationID) (RunHeader, error) {
	initial, err := InitializeRun(runID)
	if err != nil {
		return RunHeader{}, err
	}
	stateBytes, err := encodeMachineStateWire(&initial)
	if err != nil {
		return RunHeader{}, err
	}
	stateDigest := sha256Digest(stateBytes)
	header := RunHeader{
		SchemaVersion:       currentSchemaVersion,
		RunID:               runID,
		InitialStateVersion: snapshotSchemaVersion,
		InitialState:        initial,
		InitialStateDigest:  stateDigest,
		CausationID:         causationID,
	}
	headerDigest, err := digestRunHeader(&header)
	if err != nil {
		return RunHeader{}, err
	}
	header.HeaderDigest = headerDigest
	return header, nil
}

func digestRunHeader(h *RunHeader) (Digest, error) {
	body, err := encodeEnvelopeBody(h.SchemaVersion, "run_header", runHeaderDigestBody{
		SchemaVersion:       h.SchemaVersion,
		RunID:               h.RunID,
		InitialStateVersion: h.InitialStateVersion,
		InitialStateDigest:  h.InitialStateDigest,
		CausationID:         h.CausationID,
	})
	if err != nil {
		return "", err
	}
	return sha256Digest(body), nil
}

// ValidateRunHeader verifies header integrity: state digest, header digest,
// and that the initial state is a legal Revision-0 state for this RunID.
// Imported/uploaded runs must pass this before their log is folded
// (spec §5.1.1 rule 6).
func ValidateRunHeader(h *RunHeader) error {
	if h.RunID == "" {
		return errors.New("agent: run header: empty RunID")
	}
	if !isSupportedSchemaVersion(h.SchemaVersion) {
		return fmt.Errorf("agent: run header: unsupported schema version %d", h.SchemaVersion)
	}
	if h.InitialStateVersion != snapshotSchemaVersion {
		return fmt.Errorf("agent: run header: unsupported initial state version %d", h.InitialStateVersion)
	}
	if h.InitialState.RunID != h.RunID {
		return errors.New("agent: run header: initial state RunID mismatch")
	}
	if h.InitialState.Status != RunActive || h.InitialState.Current != nil ||
		len(h.InitialState.PendingInputs) != 0 || h.InitialState.ModelSteps != 0 ||
		h.InitialState.Result != nil || h.InitialState.LastModelResult != nil ||
		h.InitialState.LastClosedStep != "" || h.InitialState.Usage != (Usage{}) {
		return errors.New("agent: run header: initial state is not a minimal Revision-0 state")
	}
	stateBytes, err := encodeMachineStateWire(&h.InitialState)
	if err != nil {
		return err
	}
	if sha256Digest(stateBytes) != h.InitialStateDigest {
		return errors.New("agent: run header: initial state digest mismatch")
	}
	want, err := digestRunHeader(h)
	if err != nil {
		return err
	}
	if h.HeaderDigest != want {
		return errors.New("agent: run header: header digest mismatch")
	}
	return nil
}

// FoldRun rebuilds the run state from its complete authority: header +
// transition log. It validates the header first, then folds the records
// (spec §9.1). This is the entry point durable adapters and import/migration
// paths use; trusting an uploaded MachineState snapshot is never legal.
func FoldRun(header *RunHeader, records []TransitionRecord) (MachineState, uint64, error) {
	if err := ValidateRunHeader(header); err != nil {
		return MachineState{}, 0, err
	}
	return FoldTransitions(header.InitialState, records)
}
