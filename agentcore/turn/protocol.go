package turn

import (
	"context"
	"errors"

	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/session/writer"
)

// ErrConflict reports a Turn in a state that does not admit the operation.
var ErrConflict = errors.New("turn: conflict")

// StartRequest opens a new Turn under Preset with Inputs delivered into its
// Run.
type StartRequest struct {
	Ref    TurnRef
	Inputs []run.AgentInput
	Preset preset.PresetRef
}

// DeliverRequest carries Inputs into an active Turn's Run.
type DeliverRequest struct {
	Ref    TurnRef
	Inputs []run.AgentInput
}

// StopRequest settles the active Turn as stopped with Reason.
type StopRequest struct {
	Ref    TurnRef
	Reason string
}

// ResumeDisposition is where a still-active Turn stands when a caller looks
// at it again.
type ResumeDisposition string

const (
	ResumeWaitingForResponse ResumeDisposition = "waiting_for_response"
	ResumeWaitingForRecovery ResumeDisposition = "waiting_for_recovery"
	ResumeFinished           ResumeDisposition = "finished"
)

// TurnResponse is the Turn protocol's answer: the Turn's status, and where
// it stands when it is still active.
type TurnResponse struct {
	Ref         TurnRef
	RunID       run.RunID
	Status      TurnStatus
	Disposition ResumeDisposition
	End         run.RunEnd
	Waiting     []run.ResponseRequest
}

// Commands are the Turn protocol commits. Each takes the Writer of the
// Session it commits to: the caller's ownership capability, so every command
// lands on the same Writer, epoch and projection view as the other domains'
// commands, and a stale owner is fenced by the Writer itself. Driving a Run
// is not among them: every method returns as soon as its commit landed.
type Commands interface {
	Start(context.Context, writer.Writer, StartRequest) (TurnResponse, error)
	Deliver(context.Context, writer.Writer, DeliverRequest) (TurnResponse, error)
	Stop(context.Context, writer.Writer, StopRequest) (TurnResponse, error)
}

// Reader is the Turn status read; it needs no ownership.
type Reader interface {
	Status(context.Context, TurnRef) (TurnResponse, error)
}
