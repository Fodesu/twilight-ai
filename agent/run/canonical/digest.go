package canonical

import (
	"errors"
	"fmt"

	"github.com/felinics/twilight/agent/es"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/model"
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

// V1 is the SchemaVersion1 digest rules. Replay of a v1 Run must keep using
// them after later versions exist.
type V1 struct{}

func (V1) DigestRequest(req model.ModelRequest) (run.Digest, error) { //nolint:gocritic // hugeParam: digest covers the complete immutable ModelRequest value.
	body, err := es.EncodeTypedPayload(run.SchemaVersion1, RequestType, req)
	if err != nil {
		return "", err
	}
	return es.DigestBytes(body), nil
}

func (V1) DigestToolDefinition(def model.ToolDefinition) (run.Digest, error) {
	body, err := es.EncodeTypedPayload(run.SchemaVersion1, "tool_definition", def)
	if err != nil {
		return "", err
	}
	return es.DigestBytes(body), nil
}

func (V1) DigestToolSpec(spec run.ToolSpec) (run.Digest, error) { //nolint:gocritic // hugeParam: digest covers the complete immutable ToolSpec value.
	body, err := es.EncodeTypedPayload(run.SchemaVersion1, "tool_spec", spec)
	if err != nil {
		return "", err
	}
	return es.DigestBytes(body), nil
}

func (V1) DigestToolSpecs(specs []run.ToolSpec) (run.Digest, error) {
	// The fact wire drops an empty tool list (omitempty), so a decoded fact
	// carries nil where the command carried []. The preimage must not
	// distinguish them: a zero-tool step would otherwise fail its own
	// digest guard after one codec round trip.
	if len(specs) == 0 {
		specs = nil
	}
	body, err := es.EncodeTypedPayload(run.SchemaVersion1, "tool_specs", specs)
	if err != nil {
		return "", err
	}
	return es.DigestBytes(body), nil
}

func (V1) DigestModelStepBinding(modelRef run.ModelRef, requestDigest, toolsDigest run.Digest) (run.Digest, error) {
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

func (V1) DigestToolResponseDecision(kind run.ResponseKind, decision run.ResponseDecision, reason string) (run.Digest, error) {
	if kind != run.ResponseApproval && kind != run.ResponseExternal {
		return "", fmt.Errorf("agent: response decision: unsupported kind %q", kind)
	}
	if decision != run.ResponseDecisionApproved && decision != run.ResponseDecisionRejected {
		return "", fmt.Errorf("agent: response decision: unsupported decision %q", decision)
	}
	body, err := es.EncodeTypedPayload(run.SchemaVersion1, "tool_response_decision", toolResponseDecisionDigestBody{
		Kind: kind, Decision: decision, Reason: reason,
	})
	if err != nil {
		return "", err
	}
	return es.DigestBytes(body), nil
}

func (V1) DigestToolResponsePayload(payload run.CanonicalJSON) (run.Digest, error) {
	body, err := es.EncodeTypedPayload(run.SchemaVersion1, ToolResponseType, ToolResponsePayloadBody{Payload: payload})
	if err != nil {
		return "", err
	}
	return es.DigestBytes(body), nil
}

// DigestModelResult names a frozen model result; ModelStepCompleted carries
// this digest and the frozen store holds the body (RUN-WIR-4).
func (V1) DigestModelResult(result model.ModelResult) (run.Digest, error) { //nolint:gocritic // hugeParam: digest covers the complete immutable ModelResult value.
	body, err := es.EncodeTypedPayload(run.SchemaVersion1, ModelResultType, result)
	if err != nil {
		return "", err
	}
	return es.DigestBytes(body), nil
}

// DigestToolOutput names one tool output; ToolCallCompleted carries it.
func (V1) DigestToolOutput(output run.CanonicalJSON) (run.Digest, error) {
	if output.IsZero() {
		return "", errors.New("agent: tool output: empty output")
	}
	body, err := es.EncodeTypedPayload(run.SchemaVersion1, ToolOutputType, ToolOutputBody{Output: output})
	if err != nil {
		return "", err
	}
	return es.DigestBytes(body), nil
}

// DigestToolCallBindingSet covers the full ordered pre-Response call set of
// one ToolStep; it feeds DeriveToolStepID and is carried inside
// ToolStepOpened. It is pinned to SchemaVersion1: the value is persisted in
// v1 facts, so a future schema bump must not change how replayed v1 state
// folds.
func (V1) DigestToolCallBindingSet(bindings []run.ToolCallBinding) (run.Digest, error) {
	body, err := es.EncodeTypedPayload(run.SchemaVersion1, "tool_call_bindings", bindings)
	if err != nil {
		return "", err
	}
	return es.DigestBytes(body), nil
}

func (V1) DigestToolCallBinding(callID run.CallID, definitionDigest run.Digest, policy run.ResponsePolicy, arguments run.CanonicalJSON) (run.Digest, error) {
	return es.DigestBytes([]byte(namespacedHash("twilight/tool-call-binding",
		string(callID), string(definitionDigest), fmt.Sprintf("%d", policy), arguments.String()))), nil
}
