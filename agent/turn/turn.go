// Package turn is the first-party Turn module (docs/design/agent-turn.md):
// the logical turn, its Run attempts, mid-turn input delivery and
// settlement. An attempt's end is a Turn fact of its own, attempt_ended,
// recorded on the session stream in the same commit as the Run's run_ended
// (TRN-EVT-1, RUN-CMT-9): the surface folds the session stream only.
package turn

import (
	"errors"
	"fmt"

	"github.com/felinics/twilight/agent/es"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/chatlog"
	"github.com/felinics/twilight/agent/session/extension"
	"github.com/felinics/twilight/agent/session/writer"
)

const ModuleID extension.ModuleID = "turn"

type (
	TurnID   string
	PresetID string
)

type TurnRef struct {
	SessionID session.SessionID
	TurnID    TurnID
}

type PresetRef struct {
	ID     PresetID  `json:"id"`
	Digest es.Digest `json:"digest"`
}

type Settlement string

const (
	SettlementCompleted Settlement = "completed"
	SettlementFailed    Settlement = "failed"
	SettlementStopped   Settlement = "stopped"
)

// EventTypes (TRN-EVT-1): the Turn domain's facts.
const (
	TypeStarted    session.EventType = "twilight/turn/started"
	TypeFailed     session.EventType = "twilight/turn/failed"
	TypeSuperseded session.EventType = "twilight/turn/superseded"
	// TypeAttemptStarted registers one Run attempt of a Turn on the session
	// stream: the Turn's decision to run, carrying the identities the
	// Coordinator needs without the machine projection (TRN-SCP-1).
	TypeAttemptStarted session.EventType = "twilight/turn/attempt_started"
	// TypeAttemptEnded records the end of one attempt on the session stream,
	// in the same commit as the Run's run_ended (RUN-CMT-9). run_ended is the
	// execution aggregate's fact; attempt_ended is the conversation's.
	TypeAttemptEnded session.EventType = "twilight/turn/attempt_ended"
)

type StartedPayload struct {
	TurnID   TurnID            `json:"turnId"`
	InputIDs []chatlog.InputID `json:"inputIds,omitempty"`
	Preset   PresetRef         `json:"preset"`
}

// AttemptStartedPayload registers one attempt on the session stream.
type AttemptStartedPayload struct {
	TurnID        TurnID    `json:"turnId"`
	RunID         run.RunID `json:"runId"`
	Attempt       uint32    `json:"attempt"`
	SchemaVersion uint16    `json:"schemaVersion"`
}

// AttemptEndedPayload settles one attempt: the Run's terminal result as the
// Turn records it. AttemptEnder writes it from the Run's facts.
type AttemptEndedPayload struct {
	TurnID  TurnID       `json:"turnId"`
	RunID   run.RunID    `json:"runId"`
	Attempt uint32       `json:"attempt"`
	End     run.RunEnded `json:"end"`
}

type FailedPayload struct {
	TurnID       TurnID     `json:"turnId"`
	RunID        run.RunID  `json:"runId"`
	Settlement   Settlement `json:"settlement"`
	FailureClass string     `json:"failureClass,omitempty"`
}

type SupersededPayload struct {
	TurnID            TurnID `json:"turnId"`
	ReplacementTurnID TurnID `json:"replacementTurnId"`
}

// --- identity derivations (TRN-ID) ----------------------------------------------

func digestOf(domain string, parts ...string) es.Digest {
	raw, _ := es.EncodeTypedPayload(1, domain, parts)
	return es.DigestBytes(raw)
}

// PlanDigest is TRN-ID-2.
func PlanDigest(turnID TurnID, preset es.Digest, inputs []chatlog.InputID) es.Digest {
	parts := []string{string(turnID), string(preset)}
	for _, id := range inputs {
		parts = append(parts, string(id))
	}
	return digestOf("twilight/turn/plan", parts...)
}

// StartOperationDigest is TRN-ID-3; it is the Start commit's CommitID.
func StartOperationDigest(sid session.SessionID, turnID TurnID, plan es.Digest) es.Digest {
	return digestOf("twilight/turn/start-operation", string(sid), string(turnID), string(plan))
}

// DeriveRunID is TRN-ID-4.
func DeriveRunID(sid session.SessionID, turnID TurnID, attempt uint32) run.RunID {
	return run.RunID(digestOf("twilight/turn/run", string(sid), string(turnID), fmt.Sprintf("%d", attempt)))
}

func RetryCommitID(sid session.SessionID, turnID TurnID, attempt uint32) session.CommitID {
	return session.CommitID(digestOf("twilight/turn/retry", string(sid), string(turnID), fmt.Sprintf("%d", attempt)))
}

func CancelCommandID(sid session.SessionID, turnID TurnID, runID run.RunID) run.CommandID {
	return run.CommandID(digestOf("twilight/turn/cancel-run", string(sid), string(turnID), string(runID), string(run.ReasonCancelled)))
}

func SettleCommitID(sid session.SessionID, turnID TurnID, runID run.RunID) session.CommitID {
	return session.CommitID(digestOf("twilight/turn/settle", string(sid), string(turnID), string(runID)))
}

// --- module -----------------------------------------------------------------------

func def[T any](typ session.EventType, check func(*T) error) extension.EventDefinition {
	return extension.EventDefinition{Type: typ, Current: 1, Stream: extension.SessionStream,
		Codecs: map[extension.PayloadVersion]extension.PayloadCodec{1: extension.JSONCodec[T]{Check: check}}}
}

// Module declares the turn events, the surface projection and the Requires of
// TRN-SCP-1: chatlog (input_delivered v1). The surface reads no run-stream
// event; the attempt's end reaches it as attempt_ended.
var Module = extension.ModuleDescriptor{
	Source: extension.SourceTwilight,
	ID:     ModuleID,
	Requires: []extension.ModuleRequirement{
		{Source: extension.SourceTwilight, Module: chatlog.ModuleID, Events: map[session.EventType][]extension.PayloadVersion{
			chatlog.TypeInputDelivered: {1},
		}},
	},
	Events: []extension.EventDefinition{
		def[AttemptEndedPayload](TypeAttemptEnded, func(p *AttemptEndedPayload) error {
			if p.TurnID == "" || p.RunID == "" || p.Attempt == 0 || p.End.End == nil {
				return errors.New("attempt_ended requires turnId, runId, attempt and end")
			}
			return nil
		}),
		def[StartedPayload](TypeStarted, func(p *StartedPayload) error {
			if p.TurnID == "" || p.Preset.ID == "" || p.Preset.Digest == "" {
				return errors.New("started requires turnId and preset")
			}
			return nil
		}),
		def[FailedPayload](TypeFailed, func(p *FailedPayload) error {
			if p.TurnID == "" || p.RunID == "" || (p.Settlement != SettlementFailed && p.Settlement != SettlementStopped) {
				return errors.New("failed requires turnId, runId and settlement failed|stopped")
			}
			return nil
		}),
		def[SupersededPayload](TypeSuperseded, func(p *SupersededPayload) error {
			if p.TurnID == "" || p.ReplacementTurnID == "" {
				return errors.New("superseded requires turnId and replacementTurnId")
			}
			return nil
		}),
		def[AttemptStartedPayload](TypeAttemptStarted, func(p *AttemptStartedPayload) error {
			if p.TurnID == "" || p.RunID == "" || p.Attempt == 0 || p.SchemaVersion == 0 {
				return errors.New("attempt_started requires turnId, runId, attempt and schemaVersion")
			}
			return nil
		}),
	},
	Projections: []extension.ProjectionDefinition{SurfaceProjection},
}

// AttemptEnder is the runmod.Attacher of this module (RUN-CMT-9): when a
// commit records run_ended for a Run some Turn of the Session owns, it adds
// attempt_ended for that attempt to the same group, from the Writer's view of
// the surface. A Run no Turn owns adds nothing.
type AttemptEnder struct{}

func (AttemptEnder) Attach(view writer.View, runID run.RunID, facts []run.Fact) ([]run.ModuleEvent, error) {
	var ended *run.RunEnded
	for _, f := range facts {
		if e, ok := f.(run.RunEnded); ok {
			ended = &e
		}
	}
	if ended == nil {
		return nil, nil
	}
	state, err := view.Projection(SurfaceProjectionID, SurfaceProjection.Version)
	if err != nil {
		return nil, err
	}
	s := state.(TurnSurface)
	turnID, ok := s.RunOwner[runID]
	if !ok {
		return nil, nil
	}
	for _, a := range s.Turns[turnID].Attempts {
		if a.RunID == runID {
			return []run.ModuleEvent{{Type: TypeAttemptEnded, Value: AttemptEndedPayload{TurnID: turnID, RunID: runID, Attempt: a.Attempt, End: *ended}}}, nil
		}
	}
	return nil, fmt.Errorf("turn: run %s is owned by turn %s but has no attempt", runID, turnID)
}
