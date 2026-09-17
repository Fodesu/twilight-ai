package run

import (
	"errors"
	"fmt"
)

// canonicalV1 is the SchemaVersion1 digest rules. Replay of a v1 Run must
// keep using them after later versions exist.
type canonicalV1 struct{}

func (canonicalV1) DigestRequest(req ModelRequest) (Digest, error) { //nolint:gocritic // hugeParam: digest covers the complete immutable ModelRequest value.
	body, err := encodeEnvelopeBody(SchemaVersion1, "model_request", req)
	if err != nil {
		return "", err
	}
	return sha256Digest(body), nil
}

func (canonicalV1) DigestToolDefinition(def ToolDefinition) (Digest, error) {
	body, err := encodeEnvelopeBody(SchemaVersion1, "tool_definition", def)
	if err != nil {
		return "", err
	}
	return sha256Digest(body), nil
}

func (canonicalV1) DigestToolSpec(spec ToolSpec) (Digest, error) { //nolint:gocritic // hugeParam: digest covers the complete immutable ToolSpec value.
	body, err := encodeEnvelopeBody(SchemaVersion1, "tool_spec", spec)
	if err != nil {
		return "", err
	}
	return sha256Digest(body), nil
}

func (canonicalV1) DigestToolSpecs(specs []ToolSpec) (Digest, error) {
	// The fact wire drops an empty tool list (omitempty), so a decoded fact
	// carries nil where the command carried []. The preimage must not
	// distinguish them: a zero-tool step would otherwise fail its own
	// digest guard after one codec round trip.
	if len(specs) == 0 {
		specs = nil
	}
	body, err := encodeEnvelopeBody(SchemaVersion1, "tool_specs", specs)
	if err != nil {
		return "", err
	}
	return sha256Digest(body), nil
}

func (canonicalV1) DigestModelStepBinding(model ModelRef, requestDigest, toolsDigest Digest) (Digest, error) {
	if model == "" || requestDigest == "" || toolsDigest == "" {
		return "", errors.New("agent: model step binding requires model, request digest and tools digest")
	}
	return sha256Digest([]byte(namespacedHash("twilight/model-step-binding",
		string(model), string(requestDigest), string(toolsDigest)))), nil
}

type toolResponseDecisionDigestBody struct {
	Kind     ResponseKind     `json:"kind"`
	Decision ResponseDecision `json:"decision"`
	Reason   string           `json:"reason,omitempty"`
}

type toolResponsePayloadDigestBody struct {
	Payload CanonicalJSON `json:"payload"`
}

type toolOutputDigestBody struct {
	Output CanonicalJSON `json:"output"`
}

func (canonicalV1) DigestToolResponseDecision(kind ResponseKind, decision ResponseDecision, reason string) (Digest, error) {
	if kind != ResponseApproval && kind != ResponseExternal {
		return "", fmt.Errorf("agent: response decision: unsupported kind %q", kind)
	}
	if decision != ResponseDecisionApproved && decision != ResponseDecisionRejected {
		return "", fmt.Errorf("agent: response decision: unsupported decision %q", decision)
	}
	body, err := encodeEnvelopeBody(SchemaVersion1, "tool_response_decision", toolResponseDecisionDigestBody{
		Kind: kind, Decision: decision, Reason: reason,
	})
	if err != nil {
		return "", err
	}
	return sha256Digest(body), nil
}

func (canonicalV1) DigestToolResponsePayload(payload CanonicalJSON) (Digest, error) {
	body, err := encodeEnvelopeBody(SchemaVersion1, "tool_response_payload", toolResponsePayloadDigestBody{Payload: payload})
	if err != nil {
		return "", err
	}
	return sha256Digest(body), nil
}

// DigestModelResult names a frozen model result; ModelStepCompleted carries
// this digest and the FrozenValueStore holds the body (RUN-WIR-4).
func (canonicalV1) DigestModelResult(result ModelResult) (Digest, error) { //nolint:gocritic // hugeParam: digest covers the complete immutable ModelResult value.
	body, err := encodeEnvelopeBody(SchemaVersion1, "model_result", result)
	if err != nil {
		return "", err
	}
	return sha256Digest(body), nil
}

// DigestToolOutput names one tool output; ToolCallCompleted carries it.
func (canonicalV1) DigestToolOutput(output CanonicalJSON) (Digest, error) {
	if output.IsZero() {
		return "", errors.New("agent: tool output: empty output")
	}
	body, err := encodeEnvelopeBody(SchemaVersion1, "tool_output", toolOutputDigestBody{Output: output})
	if err != nil {
		return "", err
	}
	return sha256Digest(body), nil
}

// digestBindingSet covers the full ordered pre-Response call set of one
// ToolStep; it feeds DeriveToolStepID and is carried inside ToolStepOpened.
// It is pinned to SchemaVersion1: the value is persisted in v1 facts, so a
// future schema bump must not change how replayed v1 state folds.
func digestBindingSet(bindings []ToolCallBinding) (Digest, error) {
	body, err := encodeEnvelopeBody(SchemaVersion1, "tool_call_bindings", bindings)
	if err != nil {
		return "", err
	}
	return sha256Digest(body), nil
}

// DigestToolCallBinding covers one binding: definition, policy and canonical
// arguments plus the CallID (RUN-MCH-2). Runtime conformance suites use this
// helper to construct the same frozen binding identities as the Loop.
func DigestToolCallBinding(callID CallID, definitionDigest Digest, policy ResponsePolicy, arguments CanonicalJSON) (Digest, error) {
	return sha256Digest([]byte(namespacedHash("twilight/tool-call-binding",
		string(callID), string(definitionDigest), fmt.Sprintf("%d", policy), arguments.String()))), nil
}

func digestToolCallBinding(callID CallID, definitionDigest Digest, policy ResponsePolicy, arguments CanonicalJSON) (Digest, error) {
	return DigestToolCallBinding(callID, definitionDigest, policy, arguments)
}

// buildCreateGroupV1 produces the facts that establish a Run and queue its
// initial inputs (RUN-NEW-1). It is pure: the owning module places these
// facts in its creation commit, the RunStore never sees a Create command.
func buildCreateGroupV1(run NewRun, inputs []AgentInput) ([]Fact, error) {
	if err := ValidateNewRun(run); err != nil {
		return nil, err
	}
	facts := make([]Fact, 0, 1+len(inputs))
	facts = append(facts, RunCreated{SchemaVersion: run.SchemaVersion, RunID: run.RunID, Owner: run.Owner, Attempt: run.Attempt, CausationID: run.CausationID})
	seen := make(map[InputID]struct{}, len(inputs))
	for _, in := range inputs {
		if in.ID == "" {
			return nil, errors.New("agent: create group: input with empty InputID")
		}
		if _, dup := seen[in.ID]; dup {
			return nil, fmt.Errorf("agent: create group: duplicate InputID %q", in.ID)
		}
		seen[in.ID] = struct{}{}
		facts = append(facts, InputAccepted{Input: cloneAgentInput(in)})
	}
	return facts, nil
}
