// Package protocol defines the wire/application protocol for the process-
// independent effect port in agent/run/effect. It owns message encoding, but
// never mutates a Session or Run; settlement remains the authority's Loop concern.
package protocol

import (
	"fmt"

	"github.com/felinics/twilight/agent/es"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/effect"
	"github.com/felinics/twilight/sdk"
)

const ProtocolVersion uint16 = 1

// AssignmentEnvelope is the serializable dispatch message. AssignmentDigest
// binds the complete effect payload to AssignmentKey.
type AssignmentEnvelope struct {
	ProtocolVersion  uint16            `json:"protocolVersion"`
	Session          run.Scope         `json:"session"`
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

// EncodeOutcome renders a sealed Outcome as the wire envelope: a model
// success in Model, a tool result in Tool, a failure in Error with its code,
// a cancellation or an unknown end as the flags.
func EncodeOutcome(out effect.Outcome, assignmentDigest run.Digest) OutcomeEnvelope {
	w := OutcomeEnvelope{ProtocolVersion: ProtocolVersion, Key: out.Key, AssignmentDigest: assignmentDigest}
	switch r := out.Result.(type) {
	case effect.ModelSucceeded:
		res := r.Result
		w.Model = &res
	case effect.ModelFailed:
		code := string(r.Code)
		if code == "" {
			code = string(effect.FailureExecutor)
		}
		w.Error = &WireError{Code: code, Message: r.Message}
	case effect.ToolExecutionSucceeded:
		res := r.Result
		w.Tool = &ToolOutcomeEnvelope{Kind: "succeeded", Result: &res}
	case effect.ToolExecutionFailed:
		f := r.Failure
		w.Tool = &ToolOutcomeEnvelope{Kind: "failed", Failure: &f}
	case effect.ToolExecutionUnknown:
		f := r.Failure
		w.Tool = &ToolOutcomeEnvelope{Kind: "unknown", Failure: &f}
	case effect.Cancelled:
		w.Cancelled = true
		if r.Message != "" {
			w.Error = &WireError{Code: "cancelled", Message: r.Message}
		}
	case effect.Unknown:
		w.Unknown = true
		if r.Message != "" {
			w.Error = &WireError{Code: string(effect.FailureExecutor), Message: r.Message}
		}
	}
	return w
}

// DecodeOutcome restores the sealed Outcome. The flags win over a body:
// an envelope marked unknown or cancelled is that, whatever else it carries;
// an envelope with neither a body nor a flag is Unknown.
func DecodeOutcome(w OutcomeEnvelope) effect.Outcome {
	out := effect.Outcome{Key: w.Key}
	message := ""
	if w.Error != nil {
		message = w.Error.Message
	}
	switch {
	case w.Unknown:
		out.Result = effect.Unknown{Message: message}
	case w.Cancelled:
		out.Result = effect.Cancelled{Message: message}
	case w.Tool != nil:
		switch w.Tool.Kind {
		case "succeeded":
			var r run.ToolExecutionResult
			if w.Tool.Result != nil {
				r = *w.Tool.Result
			}
			out.Result = effect.ToolExecutionSucceeded{Result: r}
		case "failed":
			var f run.ToolFailure
			if w.Tool.Failure != nil {
				f = *w.Tool.Failure
			}
			out.Result = effect.ToolExecutionFailed{Failure: f}
		case "unknown":
			var f run.ToolFailure
			if w.Tool.Failure != nil {
				f = *w.Tool.Failure
			}
			out.Result = effect.ToolExecutionUnknown{Failure: f}
		default:
			out.Result = effect.Unknown{Message: fmt.Sprintf("run/protocol: unknown tool outcome %q", w.Tool.Kind)}
		}
	case w.Error != nil:
		out.Result = effect.ModelFailed{Code: effect.FailureCode(w.Error.Code), Message: w.Error.Message}
	case w.Model != nil:
		out.Result = effect.ModelSucceeded{Result: *w.Model}
	default:
		out.Result = effect.Unknown{Message: "run/protocol: outcome envelope carries no result"}
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

// StatusForOutcome is the ExecutionStatus a terminal Outcome corresponds to.
func StatusForOutcome(out effect.Outcome) effect.ExecutionStatus { return out.Status() }
