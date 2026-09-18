package canonical

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/felinics/twilight/agent/run"
)

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

// IdentityV1 is the SchemaVersion1 identity derivation: every RunID-scoped
// identity (steps, calls, responses, command ids) is a namespaced hash of its
// preimage. A later schema that changes any derivation gets its own
// implementation; replay of a v1 Run keeps using this one.
type IdentityV1 struct{}

// DeriveModelRequestCommandID derives the CommandID for PrepareModelRequest
// from the Run and the RunPosition the prompt builder loaded (RUN-WIR-3): concurrent
// prompt builders on the same position converge on one command identity.
func (IdentityV1) DeriveModelRequestCommandID(runID run.RunID, position run.RunPosition) run.CommandID {
	return run.CommandID(namespacedHash("twilight/model-request", string(runID), fmt.Sprintf("%d", position)))
}

// DeriveTakeoverClaim is the ExecutionClaim a new owner of a Scope uses for
// its takeover dispositions (RUN-CMT-7): epoch is the owner's generation over
// the store, so the same owner repeats idempotently and distinct owners issue
// distinct commands. It is deliberately not part of Schema.Identity: the
// claim belongs to the owner, not to any one Run, and one takeover may span
// Runs of different schema versions.
func DeriveTakeoverClaim(scope run.Scope, epoch uint64) run.ExecutionClaim {
	return run.ExecutionClaim(namespacedHash("twilight/run/takeover", string(scope), fmt.Sprintf("%d", epoch)))
}

// DeriveModelStepID derives the frozen ModelStep identity from the Run, the
// preparing command, and the model-step binding digest (model + request +
// tools).
func (IdentityV1) DeriveModelStepID(runID run.RunID, cmd run.CommandID, binding run.Digest) run.StepID {
	return run.StepID(namespacedHash("twilight/model-step", string(runID), string(cmd), string(binding)))
}

// DeriveCallID derives the Run-owned identity of one tool call from the
// ModelStep that produced it and the call's position in that step's result.
// The provider's own tool_call_id is kept beside it as ProviderCallID for the
// request round trip; it is not trusted to be unique or non-empty.
func (IdentityV1) DeriveCallID(source run.StepID, index int) run.CallID {
	return run.CallID(namespacedHash("twilight/tool-call", string(source), fmt.Sprintf("%d", index)))
}

// DeriveToolStepID derives the ToolStep identity from its source ModelStep
// and the binding-set digest over the full ordered call set.
func (IdentityV1) DeriveToolStepID(source run.StepID, bindingSet run.Digest) run.StepID {
	return run.StepID(namespacedHash("twilight/tool-step", string(source), string(bindingSet)))
}

// DeriveResponseID derives the stable ResponseID the Machine assigns when it
// creates a Waiting request. One call has at most one outstanding request, so
// (run, step, call, kind) identifies it.
func (IdentityV1) DeriveResponseID(runID run.RunID, step run.StepID, call run.CallID, kind run.ResponseKind) run.ResponseID {
	return run.ResponseID(namespacedHash("twilight/response", string(runID), string(step), string(call), string(kind)))
}

// DeriveResponseCommandID derives the CommandID for approval/rejection/answer
// commands: independent ingress processes converge on one command identity
// without coordination.
func (IdentityV1) DeriveResponseCommandID(runID run.RunID, step run.StepID, call run.CallID, resp run.ResponseID) run.CommandID {
	return run.CommandID(namespacedHash("twilight/response-command", string(runID), string(step), string(call), string(resp)))
}

// DeriveInputCommandID derives the CommandID for AcceptInput from the Run and
// the ordered InputIDs of the batch: the same inputs in the same order replay
// to the same command, a different batch is a different command. Queue-claim
// references stay private to the host.
func (IdentityV1) DeriveInputCommandID(runID run.RunID, inputs ...run.InputID) run.CommandID {
	parts := make([]string, 0, len(inputs)+1)
	parts = append(parts, string(runID))
	for _, in := range inputs {
		parts = append(parts, string(in))
	}
	return run.CommandID(namespacedHash("twilight/input-command", parts...))
}

// DeriveWithdrawCommandID derives the CommandID of WithdrawPreparedStep: one
// Prepared step is withdrawn at most once, so the identity needs no content.
func (IdentityV1) DeriveWithdrawCommandID(runID run.RunID, step run.StepID) run.CommandID {
	return run.CommandID(namespacedHash("twilight/withdraw-command", string(runID), string(step)))
}

// DeriveStartCommandID derives the CommandID of StartModelExecution (empty
// call) or StartToolCall from the target and the attempt's ExecutionClaim.
// Commit enforces this derivation so a caller-minted ID cannot bypass the
// idempotency index (RUN-WIR-3).
func (IdentityV1) DeriveStartCommandID(runID run.RunID, step run.StepID, call run.CallID, claim run.ExecutionClaim) run.CommandID {
	return run.CommandID(namespacedHash("twilight/start-command", string(runID), string(step), string(call), string(claim)))
}

// DeriveSettlementCommandID derives the CommandID of the owner's settlement
// of one execution attempt (model result/failure/reject, tool result/failure).
// One attempt settles once, so the identity needs no content: a replay with
// the same outcome is idempotent, a different outcome is a conflict.
func (IdentityV1) DeriveSettlementCommandID(runID run.RunID, step run.StepID, call run.CallID, claim run.ExecutionClaim) run.CommandID {
	return run.CommandID(namespacedHash("twilight/settlement-command", string(runID), string(step), string(call), string(claim)))
}

// DeriveModelRecoveryCommandID derives the stable command identity for
// recovering one model execution attempt. The claim is part of the identity:
// a model step may be started, recovered, and started again, and each attempt
// must have its own recovery record.
func (IdentityV1) DeriveModelRecoveryCommandID(runID run.RunID, step run.StepID, claim run.ExecutionClaim) run.CommandID {
	return run.CommandID(namespacedHash("twilight/model-recovery", string(runID), string(step), string(claim)))
}

// DeriveToolRecoveryCommandID derives the identity of the Unknown settlement
// a takeover commits for one abandoned tool attempt (RUN-CMT-7).
func (IdentityV1) DeriveToolRecoveryCommandID(runID run.RunID, step run.StepID, call run.CallID, claim run.ExecutionClaim) run.CommandID {
	return run.CommandID(namespacedHash("twilight/tool-recovery", string(runID), string(step), string(call), string(claim)))
}
