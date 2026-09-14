// Package protocol defines the wire/application protocol for the process-
// independent effect port in agent/run/effect. It owns message encoding, but
// never mutates a Session or Run; settlement remains the authority's Loop concern.
package protocol

import (
	"context"
	"errors"
	"fmt"

	"github.com/felinics/twilight/agent/es"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/effect"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/sdk"
)

const ProtocolVersion uint16 = 1

// AssignmentEnvelope is the serializable dispatch message. AssignmentDigest
// binds the complete effect payload to AssignmentKey.
type AssignmentEnvelope struct {
	ProtocolVersion  uint16            `json:"protocolVersion"`
	Session          session.SessionID `json:"session"`
	Assignment       effect.Assignment `json:"assignment"`
	AssignmentDigest run.Digest        `json:"assignmentDigest"`
}

// ToolOutcomeEnvelope is the wire representation of loop's sealed outcome.
type ToolOutcomeEnvelope struct {
	Kind    string                   `json:"kind"`
	Result  *run.ToolExecutionResult `json:"result,omitempty"`
	Failure *run.ToolFailure         `json:"failure,omitempty"`
}

type WireError struct {
	Code    string `json:"code"`
	Message string `json:"message,omitempty"`
}

// OutcomeEnvelope is stable and JSON-safe. In particular it does not contain
// a Go error or a sealed interface.
type OutcomeEnvelope struct {
	ProtocolVersion  uint16               `json:"protocolVersion"`
	Key              effect.AssignmentKey `json:"key"`
	AssignmentDigest run.Digest           `json:"assignmentDigest,omitempty"`
	Model            *sdk.ModelResult     `json:"model,omitempty"`
	Tool             *ToolOutcomeEnvelope `json:"tool,omitempty"`
	Error            *WireError           `json:"error,omitempty"`
	Cancelled        bool                 `json:"cancelled,omitempty"`
	Unknown          bool                 `json:"unknown,omitempty"`
}

func (a AssignmentEnvelope) Digest() (run.Digest, error) {
	return es.DigestCanonical(a.Assignment)
}

func (o OutcomeEnvelope) Digest() (run.Digest, error) {
	return es.DigestCanonical(o)
}

func EncodeOutcome(out effect.Outcome, assignmentDigest run.Digest) OutcomeEnvelope {
	w := OutcomeEnvelope{ProtocolVersion: ProtocolVersion, Key: out.Key, AssignmentDigest: assignmentDigest,
		Model: out.Model, Cancelled: out.Cancelled, Unknown: out.Unknown}
	if out.Err != nil {
		code := "executor_error"
		switch {
		case errors.Is(out.Err, run.ErrFrozenValueMissing):
			code = "frozen_value_missing"
		case errors.Is(out.Err, context.Canceled):
			code = "cancelled"
		case errors.Is(out.Err, context.DeadlineExceeded):
			code = "deadline_exceeded"
		}
		w.Error = &WireError{Code: code, Message: out.Err.Error()}
	}
	switch t := out.Tool.(type) {
	case effect.ToolExecutionSucceeded:
		r := t.Result
		w.Tool = &ToolOutcomeEnvelope{Kind: "succeeded", Result: &r}
	case effect.ToolExecutionFailed:
		f := t.Failure
		w.Tool = &ToolOutcomeEnvelope{Kind: "failed", Failure: &f}
	case effect.ToolExecutionUnknown:
		f := t.Failure
		w.Tool = &ToolOutcomeEnvelope{Kind: "unknown", Failure: &f}
	}
	return w
}

func DecodeOutcome(w OutcomeEnvelope) effect.Outcome {
	out := effect.Outcome{Key: w.Key, Model: w.Model, Cancelled: w.Cancelled, Unknown: w.Unknown}
	if w.Error != nil {
		switch w.Error.Code {
		case "frozen_value_missing":
			out.Err = fmt.Errorf("%w: %s", run.ErrFrozenValueMissing, w.Error.Message)
		case "cancelled":
			out.Err = context.Canceled
			out.Cancelled = true
		case "deadline_exceeded":
			out.Err = context.DeadlineExceeded
		default:
			out.Err = errors.New(w.Error.Message)
		}
	}
	if w.Tool != nil {
		switch w.Tool.Kind {
		case "succeeded":
			var r run.ToolExecutionResult
			if w.Tool.Result != nil {
				r = *w.Tool.Result
			}
			out.Tool = effect.ToolExecutionSucceeded{Result: r}
		case "failed":
			var f run.ToolFailure
			if w.Tool.Failure != nil {
				f = *w.Tool.Failure
			}
			out.Tool = effect.ToolExecutionFailed{Failure: f}
		case "unknown":
			var f run.ToolFailure
			if w.Tool.Failure != nil {
				f = *w.Tool.Failure
			}
			out.Tool = effect.ToolExecutionUnknown{Failure: f}
		default:
			out.Err = fmt.Errorf("run/protocol: unknown tool outcome %q", w.Tool.Kind)
		}
	}
	return out
}

func StatusTerminal(s effect.ExecutionStatus) bool {
	switch s {
	case effect.ExecutionCompleted, effect.ExecutionFailed, effect.ExecutionCancelled, effect.ExecutionUnknown:
		return true
	default:
		return false
	}
}

func StatusForOutcome(out effect.Outcome) effect.ExecutionStatus {
	if out.Unknown {
		return effect.ExecutionUnknown
	}
	if out.Cancelled {
		return effect.ExecutionCancelled
	}
	if _, ok := out.Tool.(effect.ToolExecutionUnknown); ok {
		return effect.ExecutionUnknown
	}
	if out.Err != nil {
		return effect.ExecutionFailed
	}
	return effect.ExecutionCompleted
}
