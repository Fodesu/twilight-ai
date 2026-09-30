package runtime

import (
	"github.com/felinics/twilight/agentcore/sessionkernel"
)

// The Turn protocol is the session kernel's: the Coordinator commits its
// commands against the fact layer. These aliases keep the vocabulary the
// SessionRuntime and the hosts speak in the runtime package.

// Commands are the Turn protocol commits; Reader reads a Turn's status; both
// with the Requests and Results, are the session kernel's vocabulary (see
// the Turns interface in runtime.go).

type (
	// Commands are the Turn protocol commits; Reader reads a Turn's status.
	Commands       = sessionkernel.Commands
	Reader         = sessionkernel.Reader
	StartRequest   = sessionkernel.StartRequest
	DeliverRequest   = sessionkernel.DeliverRequest
	StopRequest      = sessionkernel.StopRequest
	TurnResult       = sessionkernel.TurnResult
	ResumeDisposition = sessionkernel.ResumeDisposition
)

const (
	ResumeWaitingForResponse = sessionkernel.ResumeWaitingForResponse
	ResumeWaitingForRecovery = sessionkernel.ResumeWaitingForRecovery
	ResumeFinished           = sessionkernel.ResumeFinished
)

// RequireNoActiveTurn and RequireQuiescentRun are the quiescence
// preconditions, evaluated inside the Writer's critical section, that the
// session kernel's Turn protocol guards other domains' commits with.
var (
	RequireNoActiveTurn = sessionkernel.RequireNoActiveTurn
	RequireQuiescentRun = sessionkernel.RequireQuiescentRun
)

// Compile-time check that the Coordinator the session kernel assembles
// satisfies the Turns the SessionRuntime routes by.
var _ Turns = (*sessionkernel.Coordinator)(nil)
