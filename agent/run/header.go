package run

import (
	"errors"

	"github.com/memohai/twilight/agent/es"
)

// snapshotSchemaVersion versions the MachineState wire shape used inside
// RunHeader.InitialState. It is independent of the event SchemaVersion. Both
// shapes remain pre-release until the protocol is published; after publication
// each can evolve only through its documented versioning rules (RUN-CMP-1).
const snapshotSchemaVersion uint16 = 1

func encodeMachineStateVersion(schemaVersion uint16, s *MachineState) ([]byte, error) {
	proto, err := ProtocolFor(schemaVersion)
	if err != nil {
		return nil, err
	}
	return proto.EncodeMachineState(s)
}

func validateHeaderInitialStateVersion(h *RunHeader) error {
	proto, err := ProtocolFor(h.SchemaVersion)
	if err != nil {
		return err
	}
	return proto.ValidateHeader(h)
}

// RunHeader is the formal persisted Revision-0 protocol record (RUN-NEW-1).
// RunHeader plus the TransitionRecord log is the canonical record used for
// audit, projections, and verified import/replay. Runtime atomically stores the
// header with Revision-0 state during Create and stores each next state with
// its transition during Commit. Every fold starts from the header's initial
// state. The header is immutable after creation.
type RunHeader struct {
	SchemaVersion       uint16       `json:"schemaVersion"`
	RunID               RunID        `json:"runId"`
	InitialStateVersion uint16       `json:"initialStateVersion"`
	InitialState        MachineState `json:"initialState"`
	InitialStateDigest  Digest       `json:"initialStateDigest"`
	// CausationID links the Run to its creating turn/application operation.
	// Opaque to run; the application defines its interpretation.
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
// transition (RUN-NEW-1).
func BuildRunHeader(runID RunID, causationID es.CausationID) (RunHeader, error) {
	initial, err := InitializeRun(runID)
	if err != nil {
		return RunHeader{}, err
	}
	stateBytes, err := ProtocolV1.EncodeMachineState(&initial)
	if err != nil {
		return RunHeader{}, err
	}
	stateDigest := sha256Digest(stateBytes)
	header := RunHeader{
		SchemaVersion:       ProtocolV1.Version(),
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
// (RUN-NEW-2).
func ValidateRunHeader(h *RunHeader) error {
	if h.RunID == "" {
		return errors.New("agent: run header: empty RunID")
	}
	if err := validateHeaderInitialStateVersion(h); err != nil {
		return err
	}
	if h.InitialState.RunID != h.RunID {
		return errors.New("agent: run header: initial state RunID mismatch")
	}
	if h.InitialState.Status != RunActive || !atOpen(h.InitialState.Current) ||
		len(h.InitialState.PendingInputs) != 0 || h.InitialState.ModelSteps != 0 ||
		h.InitialState.Result != nil || h.InitialState.LastModelResult != nil ||
		h.InitialState.LastClosedStep != "" || h.InitialState.LastToolStep != nil ||
		h.InitialState.Usage != (Usage{}) {
		return errors.New("agent: run header: initial state is not a minimal Revision-0 state")
	}
	stateBytes, err := encodeMachineStateVersion(h.SchemaVersion, &h.InitialState)
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

// FoldRun rebuilds the run state from the canonical header + transition log.
// It validates the header first, then folds the records (RUN-NEW-2). This is
// the entry point durable adapters and import/migration paths use; trusting an
// uploaded MachineState snapshot is never legal.
func FoldRun(header *RunHeader, records []TransitionRecord) (MachineState, uint64, error) {
	if err := ValidateRunHeader(header); err != nil {
		return MachineState{}, 0, err
	}
	return FoldTransitions(header.InitialState, records)
}
