package run

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/memohai/twilight/agent/es"
)

// NewRun is the immutable, versioned creation data for a Run. RunID is
// caller-supplied so retries retain a stable identity.
type NewRun struct {
	SchemaVersion uint16         `json:"schemaVersion"`
	RunID         RunID          `json:"runId"`
	CausationID   es.CausationID `json:"causationId,omitempty"`
}

// BuildNewRun constructs a current-version Run creation value.
func BuildNewRun(runID RunID, causationID es.CausationID) (NewRun, error) {
	run := NewRun{SchemaVersion: SchemaVersion1, RunID: runID, CausationID: causationID}
	if err := ValidateNewRun(run); err != nil {
		return NewRun{}, err
	}
	return run, nil
}

// ValidateNewRun verifies version support and textual identity encoding.
func ValidateNewRun(run NewRun) error {
	if run.RunID == "" {
		return errors.New("agent: new run: empty RunID")
	}
	if !utf8.ValidString(string(run.RunID)) {
		return errors.New("agent: new run: RunID is not valid UTF-8")
	}
	if !utf8.ValidString(string(run.CausationID)) {
		return errors.New("agent: new run: CausationID is not valid UTF-8")
	}
	if run.SchemaVersion != SchemaVersion1 {
		return fmt.Errorf("agent: new run: unsupported schema version %d", run.SchemaVersion)
	}
	return nil
}

// BuildRunHeaderFromNewRun constructs Revision 0 using the explicit v1 rule.
// This dispatch must not use currentSchemaVersion: later command schemas must
// never change the bytes admitted by a v1 NewRun.
func BuildRunHeaderFromNewRun(run NewRun) (RunHeader, error) {
	if err := ValidateNewRun(run); err != nil {
		return RunHeader{}, err
	}
	switch run.SchemaVersion {
	case SchemaVersion1:
		return buildRunHeaderV1(run)
	default:
		return RunHeader{}, fmt.Errorf("agent: new run: unsupported schema version %d", run.SchemaVersion)
	}
}

const newRunV1InitialStateVersion uint16 = 1

func buildRunHeaderV1(run NewRun) (RunHeader, error) {
	initial := MachineState{RunID: run.RunID, Status: RunActive}
	stateBytes, err := encodeMachineStateV1(&initial)
	if err != nil {
		return RunHeader{}, err
	}
	header := RunHeader{
		SchemaVersion:       SchemaVersion1,
		RunID:               run.RunID,
		InitialStateVersion: newRunV1InitialStateVersion,
		InitialState:        initial,
		InitialStateDigest:  sha256Digest(stateBytes),
		CausationID:         run.CausationID,
	}
	header.HeaderDigest, err = digestRunHeader(&header)
	if err != nil {
		return RunHeader{}, err
	}
	if err := ValidateRunHeader(&header); err != nil {
		return RunHeader{}, err
	}
	return header, nil
}

// CreateResult is the outcome of conditionally creating one Run.
type CreateResult struct {
	Header  RunHeader `json:"header"`
	Created bool      `json:"created"`
}

// RunRecord is one consistent, verified read of a Run.
type RunRecord struct {
	Header      RunHeader          `json:"header"`
	Snapshot    RuntimeSnapshot    `json:"snapshot"`
	Transitions []TransitionRecord `json:"transitions"`
}

var (
	// ErrCreateConflict reports that a RunID is already associated with a
	// different canonical Revision-0 header.
	ErrCreateConflict = errors.New("agent: run create conflict")
	// ErrRunNotFound reports an operation addressed a RunID not in this Runtime.
	ErrRunNotFound = errors.New("agent: run not found")
)

func cloneRuntimeSnapshot(snapshot RuntimeSnapshot) RuntimeSnapshot {
	return RuntimeSnapshot{State: cloneMachineState(&snapshot.State), Revision: snapshot.Revision, SchemaVersion: snapshot.SchemaVersion}
}

func cloneRunHeader(header RunHeader) RunHeader {
	header.InitialState = cloneMachineState(&header.InitialState)
	return header
}

func canonicalHeadersEqual(left, right RunHeader) (bool, error) {
	if err := ValidateRunHeader(&left); err != nil {
		return false, err
	}
	if err := ValidateRunHeader(&right); err != nil {
		return false, err
	}
	leftBytes, err := marshalCanonical(left)
	if err != nil {
		return false, err
	}
	rightBytes, err := marshalCanonical(right)
	if err != nil {
		return false, err
	}
	return bytes.Equal(leftBytes, rightBytes), nil
}

// checkContext avoids locking when cancellation already makes an operation
// inapplicable. Context is intentionally not retained by the Runtime.
func checkContext(ctx context.Context) error {
	if ctx == nil {
		return errors.New("agent: runtime: nil context")
	}
	return ctx.Err()
}
