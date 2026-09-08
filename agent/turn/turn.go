// Package turn is the first-party Turn module (docs/design/agent-turn.md):
// the logical turn, its Run attempts, mid-turn input delivery, settlement,
// and the companion that turns Run facts into conversation content.
package turn

import (
	"errors"
	"fmt"

	"github.com/memohai/twilight/agent/es"
	"github.com/memohai/twilight/agent/run"
	"github.com/memohai/twilight/agent/session"
	"github.com/memohai/twilight/agent/session/chatlog"
	"github.com/memohai/twilight/agent/session/extension"
	runmod "github.com/memohai/twilight/agent/session/run"
)

const ModuleID extension.ModuleID = "turn"

type (
	TurnID             string
	ExecutionBindingID string
	CompanionVersion   string
)

type TurnRef struct {
	SessionID session.SessionID
	TurnID    TurnID
}

type ExecutionBindingRef struct {
	ID     ExecutionBindingID `json:"id"`
	Digest es.Digest          `json:"digest"`
}

type Settlement string

const (
	SettlementCompleted Settlement = "completed"
	SettlementFailed    Settlement = "failed"
	SettlementStopped   Settlement = "stopped"
)

const (
	TypeStarted    session.EventType = "twilight/turn/started"
	TypeCompleted  session.EventType = "twilight/turn/completed"
	TypeFailed     session.EventType = "twilight/turn/failed"
	TypeSuperseded session.EventType = "twilight/turn/superseded"
)

type StartedPayload struct {
	TurnID           TurnID              `json:"turnId"`
	InputIDs         []chatlog.InputID   `json:"inputIds,omitempty"`
	ExecutionBinding ExecutionBindingRef `json:"executionBinding"`
	Companion        CompanionVersion    `json:"companion"`
}

type CompletedPayload struct {
	TurnID TurnID    `json:"turnId"`
	RunID  run.RunID `json:"runId"`
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
func PlanDigest(turnID TurnID, binding es.Digest, companion CompanionVersion, inputs []chatlog.InputID) es.Digest {
	parts := []string{string(turnID), string(binding), string(companion)}
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
	return extension.EventDefinition{Type: typ, Current: 1,
		Codecs: map[extension.PayloadVersion]extension.PayloadCodec{1: extension.JSONCodec[T]{Check: check}}}
}

// Module declares the turn events, the surface projection and the Requires of
// TRN-SCP-1: run (created, input_accepted, ended v1) and chatlog (present).
var Module = extension.ModuleDescriptor{
	ID: ModuleID,
	Requires: []extension.ModuleRequirement{
		{Module: runmod.ModuleID, Events: map[session.EventType][]extension.PayloadVersion{
			runmod.Prefix + "run_created":    {1},
			runmod.Prefix + "input_accepted": {1},
			runmod.Prefix + "run_ended":      {1},
		}},
		{Module: chatlog.ModuleID},
	},
	Events: []extension.EventDefinition{
		def[StartedPayload](TypeStarted, func(p *StartedPayload) error {
			if p.TurnID == "" || p.ExecutionBinding.ID == "" || p.ExecutionBinding.Digest == "" || p.Companion == "" {
				return errors.New("started requires turnId, binding and companion")
			}
			return nil
		}),
		def[CompletedPayload](TypeCompleted, func(p *CompletedPayload) error {
			if p.TurnID == "" || p.RunID == "" {
				return errors.New("completed requires turnId and runId")
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
	},
	Projections: []extension.ProjectionDefinition{SurfaceProjection},
}
