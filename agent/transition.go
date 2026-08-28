package agent

import (
	"errors"
	"fmt"
)

// TransitionRecord is the atomic authority record for one accepted
// Runtime.Commit. A transition owns the complete ordered AgentEvent group for
// one Revision; runtimes should persist it as the unit of log completeness and
// expose its Events as the public committed event stream.
type TransitionRecord struct {
	SchemaVersion    uint16       `json:"schemaVersion"`
	RunID            RunID        `json:"runId"`
	Revision         uint64       `json:"revision"`
	CommandID        CommandID    `json:"commandId"`
	CommandDigest    Digest       `json:"commandDigest"`
	Events           []AgentEvent `json:"events"`
	TransitionDigest Digest       `json:"transitionDigest"`
}

type transitionRecordDigestBody struct {
	SchemaVersion uint16       `json:"schemaVersion"`
	RunID         RunID        `json:"runId"`
	Revision      uint64       `json:"revision"`
	CommandID     CommandID    `json:"commandId"`
	CommandDigest Digest       `json:"commandDigest"`
	Events        []AgentEvent `json:"events"`
}

func transitionRecordBody(record *TransitionRecord) transitionRecordDigestBody {
	return transitionRecordDigestBody{
		SchemaVersion: record.SchemaVersion,
		RunID:         record.RunID,
		Revision:      record.Revision,
		CommandID:     record.CommandID,
		CommandDigest: record.CommandDigest,
		Events:        record.Events,
	}
}

// DigestTransitionRecord computes the digest for one transition aggregate. The
// digest binds the transition identity and the complete ordered event group;
// TransitionDigest itself is excluded from the digest input.
func DigestTransitionRecord(record *TransitionRecord) (Digest, error) {
	if record == nil {
		return "", errors.New("agent: transition: nil record")
	}
	body, err := encodeEnvelopeBody(record.SchemaVersion, "transition_record", transitionRecordBody(record))
	if err != nil {
		return "", err
	}
	return sha256Digest(body), nil
}

// BuildTransitionRecord freezes and validates the complete event group of one
// accepted command as an atomic transition record.
func BuildTransitionRecord(events []AgentEvent) (TransitionRecord, error) {
	if len(events) == 0 {
		return TransitionRecord{}, errors.New("agent: transition: empty event group")
	}
	if len(events) > int(^uint16(0))+1 {
		return TransitionRecord{}, fmt.Errorf("agent: transition: event group too large: %d", len(events))
	}
	frozen := cloneEvents(events)
	first := frozen[0]
	record := TransitionRecord{
		SchemaVersion: first.SchemaVersion,
		RunID:         first.RunID,
		Revision:      first.Revision,
		CommandID:     first.CommandID,
		CommandDigest: first.CommandDigest,
		Events:        frozen,
	}
	digest, err := DigestTransitionRecord(&record)
	if err != nil {
		return TransitionRecord{}, err
	}
	record.TransitionDigest = digest
	if err := ValidateTransitionRecord(&record); err != nil {
		return TransitionRecord{}, err
	}
	return record, nil
}

// ValidateTransitionRecord verifies that a transition is internally complete:
// every nested event belongs to the transition, indexes are contiguous, fact
// digests match, and the transition digest binds the whole aggregate.
func ValidateTransitionRecord(record *TransitionRecord) error {
	if record == nil {
		return errors.New("agent: transition: nil record")
	}
	if record.RunID == "" || record.Revision == 0 || record.CommandID == "" || record.CommandDigest == "" {
		return errors.New("agent: transition: missing identity")
	}
	if !isSupportedSchemaVersion(record.SchemaVersion) {
		return fmt.Errorf("agent: transition: unsupported schema version %d", record.SchemaVersion)
	}
	if len(record.Events) == 0 {
		return fmt.Errorf("agent: transition: revision %d has no events", record.Revision)
	}
	if len(record.Events) > int(^uint16(0))+1 {
		return fmt.Errorf("agent: transition: revision %d event group too large: %d", record.Revision, len(record.Events))
	}
	for i := range record.Events {
		e := record.Events[i]
		if e.SchemaVersion != record.SchemaVersion {
			return fmt.Errorf("agent: transition: event %d schema version %d does not match transition %d", i, e.SchemaVersion, record.SchemaVersion)
		}
		if e.RunID != record.RunID {
			return fmt.Errorf("agent: transition: event %d run %q does not match transition run %q", i, e.RunID, record.RunID)
		}
		if e.Revision != record.Revision {
			return fmt.Errorf("agent: transition: event %d revision %d does not match transition revision %d", i, e.Revision, record.Revision)
		}
		if e.Index != uint16(i) {
			return fmt.Errorf("agent: transition: revision %d index %d has event index %d", record.Revision, i, e.Index)
		}
		if e.CommandID != record.CommandID || e.CommandDigest != record.CommandDigest {
			return fmt.Errorf("agent: transition: revision %d command identity changed within transition", record.Revision)
		}
		typ := factType(e.Fact)
		if typ == "" || e.Type != typ {
			return fmt.Errorf("agent: transition: event type %q does not match fact variant %T", e.Type, e.Fact)
		}
		wantDigest, err := DigestFact(e.SchemaVersion, e.Type, e.Fact)
		if err != nil {
			return err
		}
		if e.Digest != wantDigest {
			return fmt.Errorf("agent: transition: fact digest mismatch at revision %d index %d", e.Revision, e.Index)
		}
	}
	if record.TransitionDigest == "" {
		return errors.New("agent: transition: missing digest")
	}
	wantDigest, err := DigestTransitionRecord(record)
	if err != nil {
		return err
	}
	if record.TransitionDigest != wantDigest {
		return fmt.Errorf("agent: transition: digest mismatch at revision %d", record.Revision)
	}
	return nil
}
