package run

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/memohai/twilight/agent/es"
)

type RunID string
type StepID string
type CallID string
type CommandID string
type ResponseID string
type InputID string
type ToolRef string
type ModelRef string

// Digest is "sha256:<64 lowercase hex>" over canonical protocol bytes.
// It remains an alias while Run protocol types live in this package.
type Digest = es.Digest

// PlanningToken is opaque to agent; the application uses it to identify the
// context revision from which a RequestPlan was built.
type PlanningToken string

// ExecutionClaim is an opaque identity chosen by the execution loop for one
// start command. It lets a caller replay the same start request without
// accidentally acquiring a second execution grant.
type ExecutionClaim string

// ExecutionGrant is an opaque capability minted by the Runtime for one
// accepted start command. Callers only pass it back; its representation is
// implementation-defined.
type ExecutionGrant string

func sha256Digest(data []byte) Digest { return es.DigestBytes(data) }

// namespacedHash derives a stable identifier from a namespace and ordered
// parts. Parts are length-prefixed so no two distinct part lists collide.
func namespacedHash(namespace string, parts ...string) string {
	h := sha256.New()
	fmt.Fprintf(h, "%d:%s", len(namespace), namespace)
	for _, p := range parts {
		fmt.Fprintf(h, "%d:%s", len(p), p)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// DeriveModelRequestCommandID derives the CommandID for PrepareModelRequest
// from the Run and the Revision the planner loaded (RUN-WIR-3): concurrent
// planners on the same Revision converge on one command identity.
func DeriveModelRequestCommandID(run RunID, revision uint64) CommandID {
	return CommandID(namespacedHash("twilight/model-request", string(run), fmt.Sprintf("%d", revision)))
}

// DeriveModelStepID derives the frozen ModelStep identity from the Run, the
// preparing command, and the model-step binding digest (model + request +
// tools).
func DeriveModelStepID(run RunID, cmd CommandID, binding Digest) StepID {
	return StepID(namespacedHash("twilight/model-step", string(run), string(cmd), string(binding)))
}

// DeriveToolStepID derives the ToolStep identity from its source ModelStep
// and the binding-set digest over the full ordered call set.
func DeriveToolStepID(source StepID, bindingSet Digest) StepID {
	return StepID(namespacedHash("twilight/tool-step", string(source), string(bindingSet)))
}

// DeriveResponseID derives the stable ResponseID the Machine assigns when it
// creates a Waiting request. One call has at most one outstanding request, so
// (run, step, call, kind) identifies it.
func DeriveResponseID(run RunID, step StepID, call CallID, kind ResponseKind) ResponseID {
	return ResponseID(namespacedHash("twilight/response", string(run), string(step), string(call), string(kind)))
}

// DeriveResponseCommandID derives the CommandID for approval/rejection/answer
// commands: independent ingress processes converge on one command identity
// without coordination.
func DeriveResponseCommandID(run RunID, step StepID, call CallID, resp ResponseID) CommandID {
	return CommandID(namespacedHash("twilight/response-command", string(run), string(step), string(call), string(resp)))
}

// DeriveInputCommandID derives the CommandID for AcceptInput from the Run and
// the InputID. Queue-claim references stay private to the host.
func DeriveInputCommandID(run RunID, input InputID) CommandID {
	return CommandID(namespacedHash("twilight/input-command", string(run), string(input)))
}

// DeriveSystemCommandID derives the CommandID a recovery scanner uses for a
// system-issued command about one invalidated execution. The recovery record
// reference must be stable per record so replays reuse the same identity.
func DeriveSystemCommandID(run RunID, step StepID, call CallID, recoveryRecord string) CommandID {
	return CommandID(namespacedHash("twilight/system-command", string(run), string(step), string(call), recoveryRecord))
}

// DeriveModelRecoveryCommandID derives the stable command identity for
// recovering one model execution attempt. The claim is part of the identity:
// a model step may be started, recovered, and started again, and each attempt
// must have its own recovery record.
func DeriveModelRecoveryCommandID(run RunID, step StepID, claim ExecutionClaim) CommandID {
	return CommandID(namespacedHash("twilight/model-recovery", string(run), string(step), string(claim)))
}
