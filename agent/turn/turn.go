// Package turn is the first-party Turn module (docs/design/agent-turn.md):
// the logical turn, its Run attempts, mid-turn input delivery and
// settlement. Attempt outcomes are projected from the Run's own run_ended
// fact; the module writes no derived copy of them.
package turn

import (
	"errors"
	"fmt"

	"github.com/felinics/twilight/agent/es"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/chatlog"
	"github.com/felinics/twilight/agent/session/extension"
	runmod "github.com/felinics/twilight/agent/session/run"
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

// EventTypes (TRN-EVT-1): the Turn domain's own decisions. An attempt's end
// is not among them: the surface folds it from twilight/run/run_ended.
const (
	TypeStarted    session.EventType = "twilight/turn/started"
	TypeFailed     session.EventType = "twilight/turn/failed"
	TypeSuperseded session.EventType = "twilight/turn/superseded"
	// TypeAttemptStarted registers one Run attempt of a Turn on the session
	// stream: the Turn's decision to run, carrying the identities the
	// Coordinator needs without the machine projection (TRN-SCP-1).
	TypeAttemptStarted session.EventType = "twilight/turn/attempt_started"
)

type StartedPayload struct {
	TurnID   TurnID            `json:"turnId"`
	InputIDs []chatlog.InputID `json:"inputIds,omitempty"`
	Preset   PresetRef         `json:"preset"`
}

// AttemptStartedPayload registers one attempt on the session stream.
type AttemptStartedPayload struct {
	TurnID  TurnID    `json:"turnId"`
	RunID   run.RunID `json:"runId"`
	Attempt uint32    `json:"attempt"`
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
	parts := make([]string, 0, 2+len(inputs))
	parts = append(parts, string(turnID), string(preset))
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
	return extension.EventDefinition{Type: typ, Stream: extension.SessionStream,
		Codecs: map[extension.SchemaVersion]extension.PayloadCodec{1: extension.JSONCodec[T]{Check: check}}}
}

// Module declares the turn events, the surface projection and the Requires of
// TRN-SCP-1: run (run_ended v1, which settles attempts) and chatlog
// (input_delivered v1).
var Module = extension.ModuleDescriptor{
	Source: extension.SourceTwilight,
	ID:     ModuleID,
	Requires: []extension.ModuleRequirement{
		{Source: extension.SourceTwilight, Module: runmod.ModuleID, Events: map[session.EventType][]extension.SchemaVersion{
			runmod.Prefix + "run_ended": {1},
		}},
		{Source: extension.SourceTwilight, Module: chatlog.ModuleID, Events: map[session.EventType][]extension.SchemaVersion{
			chatlog.TypeInputDelivered: {1},
		}},
	},
	Events: []extension.EventDefinition{
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
			if p.TurnID == "" || p.RunID == "" || p.Attempt == 0 {
				return errors.New("attempt_started requires turnId, runId and attempt")
			}
			return nil
		}),
	},
	Projections: []extension.ProjectionDefinition{SurfaceProjection},
}
