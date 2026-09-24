package canonical

import (
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/es"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/model"
)

// Envelope types of the bodies facts name by digest only (RUN-WIR-4). The
// frozen store (agent/run/frozen) keeps each body as the typed envelope its
// digest was computed from, so it encodes and decodes under the same names.
const (
	RequestType      = "model_request"
	ModelResultType  = "model_result"
	ToolOutputType   = "tool_output"
	ToolResponseType = "tool_response_payload"
)

// preimageVersion is the version every digest preimage of this package
// carries in its domain separator. It is part of the persisted digests and
// never changes; a new digest rule would be a new domain, not a new number.
const preimageVersion uint16 = 1

// Digests is the digest rules of every body a fact names.
type Digests struct{}

func (Digests) DigestRequest(req model.ModelRequest) (run.Digest, error) { //nolint:gocritic // hugeParam: digest covers the complete immutable ModelRequest value.
	body, err := es.EncodeTypedPayload(preimageVersion, RequestType, req)
	if err != nil {
		return "", err
	}
	return es.DigestBytes(body), nil
}

func (Digests) DigestToolDefinition(def model.ToolDefinition) (run.Digest, error) {
	body, err := es.EncodeTypedPayload(preimageVersion, "tool_definition", def)
	if err != nil {
		return "", err
	}
	return es.DigestBytes(body), nil
}

func (Digests) DigestToolSpec(spec run.ToolSpec) (run.Digest, error) { //nolint:gocritic // hugeParam: digest covers the complete immutable ToolSpec value.
	body, err := es.EncodeTypedPayload(preimageVersion, "tool_spec", spec)
	if err != nil {
		return "", err
	}
	return es.DigestBytes(body), nil
}

func (Digests) DigestToolSpecs(specs []run.ToolSpec) (run.Digest, error) {
	// The fact wire drops an empty tool list (omitempty), so a decoded fact
	// carries nil where the command carried []. The preimage must not
	// distinguish them: a zero-tool step would otherwise fail its own
	// digest guard after one codec round trip.
	if len(specs) == 0 {
		specs = nil
	}
	body, err := es.EncodeTypedPayload(preimageVersion, "tool_specs", specs)
	if err != nil {
		return "", err
	}
	return es.DigestBytes(body), nil
}

func (Digests) DigestModelStepBinding(modelRef run.ModelRef, requestDigest, toolsDigest run.Digest) (run.Digest, error) {
	if modelRef == "" || requestDigest == "" || toolsDigest == "" {
		return "", errors.New("agent: model step binding requires model, request digest and tools digest")
	}
	return es.DigestBytes([]byte(namespacedHash("twilight/model-step-binding",
		string(modelRef), string(requestDigest), string(toolsDigest)))), nil
}

type toolResponseDecisionDigestBody struct {
	Kind     run.ResponseKind     `json:"kind"`
	Decision run.ResponseDecision `json:"decision"`
	Reason   string               `json:"reason,omitempty"`
}

type ToolResponsePayloadBody struct {
	Payload run.CanonicalJSON `json:"payload"`
}

type ToolOutputBody struct {
	Output run.CanonicalJSON `json:"output"`
}

func (Digests) DigestToolResponseDecision(kind run.ResponseKind, decision run.ResponseDecision, reason string) (run.Digest, error) {
	if kind != run.ResponseApproval && kind != run.ResponseExternal {
		return "", fmt.Errorf("agent: response decision: unsupported kind %q", kind)
	}
	if decision != run.ResponseDecisionApproved && decision != run.ResponseDecisionRejected {
		return "", fmt.Errorf("agent: response decision: unsupported decision %q", decision)
	}
	body, err := es.EncodeTypedPayload(preimageVersion, "tool_response_decision", toolResponseDecisionDigestBody{
		Kind: kind, Decision: decision, Reason: reason,
	})
	if err != nil {
		return "", err
	}
	return es.DigestBytes(body), nil
}

func (Digests) DigestToolResponsePayload(payload run.CanonicalJSON) (run.Digest, error) {
	body, err := es.EncodeTypedPayload(preimageVersion, ToolResponseType, ToolResponsePayloadBody{Payload: payload})
	if err != nil {
		return "", err
	}
	return es.DigestBytes(body), nil
}

// DigestModelResult names a frozen model result; ModelStepCompleted carries
// this digest and the frozen store holds the body (RUN-WIR-4).
func (Digests) DigestModelResult(result model.ModelResult) (run.Digest, error) { //nolint:gocritic // hugeParam: digest covers the complete immutable ModelResult value.
	body, err := es.EncodeTypedPayload(preimageVersion, ModelResultType, result)
	if err != nil {
		return "", err
	}
	return es.DigestBytes(body), nil
}

// DigestToolOutput names one tool output; ToolCallCompleted carries it.
func (Digests) DigestToolOutput(output run.CanonicalJSON) (run.Digest, error) {
	if output.IsZero() {
		return "", errors.New("agent: tool output: empty output")
	}
	body, err := es.EncodeTypedPayload(preimageVersion, ToolOutputType, ToolOutputBody{Output: output})
	if err != nil {
		return "", err
	}
	return es.DigestBytes(body), nil
}

// DigestToolCallBindingSet covers the full ordered pre-Response call set of
// one ToolStep; it feeds DeriveToolStepID and is carried inside
// ToolStepOpened. The value is persisted in facts, so its preimage never
// changes: replayed state must fold the same.
func (Digests) DigestToolCallBindingSet(bindings []run.ToolCallBinding) (run.Digest, error) {
	body, err := es.EncodeTypedPayload(preimageVersion, "tool_call_bindings", bindings)
	if err != nil {
		return "", err
	}
	return es.DigestBytes(body), nil
}

func (Digests) DigestToolCallBinding(callID run.CallID, definitionDigest run.Digest, policy run.ResponsePolicy, arguments run.CanonicalJSON) (run.Digest, error) {
	return es.DigestBytes([]byte(namespacedHash("twilight/tool-call-binding",
		string(callID), string(definitionDigest), fmt.Sprintf("%d", policy), arguments.String()))), nil
}
