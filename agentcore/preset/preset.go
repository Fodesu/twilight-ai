package preset

import (
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/model"
	"github.com/felinics/twilight/agentcore/run/model/sdkconv"
	"github.com/felinics/twilight/agentcore/run/schema"
	"github.com/felinics/twilight/sdk"
)

type (
	// PresetID names a preset under registration; the PresetRef a Session
	// records pairs it with the digest of the registered AgentPreset.
	PresetID string
	// PromptBuilderRef names the decision component that builds the model
	// prompt for a Turn. It is part of the preset digest.
	PromptBuilderRef string
)

// PresetRef is the digest-checked reference a Turn's started fact records:
// the decision identity the Turn runs under, addressable across processes.
type PresetRef struct {
	ID     PresetID          `json:"id"`
	Digest jsonstable.Digest `json:"digest"`
}

// PublicTool is one tool of an AgentPreset: its ref, frozen definition and
// response policy. ToolSpecs and the provider-facing tool list both derive
// from it.
type PublicTool struct {
	Ref        run.ToolRef          `json:"ref"`
	Definition model.ToolDefinition `json:"definition"`
	Policy     run.ResponsePolicy   `json:"policy"`
	// Replay is the tool's declared replay policy; it enters the preset
	// digest and the frozen ToolSpec. Omitted when unknown.
	Replay run.ReplayPolicy `json:"replay,omitempty"`
	// Placement is the tool's declared placement; it enters the preset
	// digest and the frozen ToolSpec. Omitted for process placement.
	Placement run.ToolPlacement `json:"placement,omitempty"`
}

// AgentPreset is the decision identity a Turn is started under: every
// input to the decision layer that must be the same when another process
// resumes the Turn. The Session records PresetRef{ID, Digest}; credentials,
// clients and tool implementations never enter it. Streaming is not part of
// the identity: it is an execution-side observation choice of the backend.
type AgentPreset struct {
	Model run.ModelRef `json:"model"`
	Tools []PublicTool `json:"tools,omitempty"`
	// PromptBuilder names the decision component resolved on the Owner side.
	PromptBuilder PromptBuilderRef `json:"promptBuilder"`
	// Scheduling is how the tool calls of one step run: parallel (default) or
	// sequential, with an optional bound on concurrent workers. It is frozen
	// onto each ToolStep.
	Scheduling run.ToolScheduling `json:"scheduling,omitempty"`
	// MalformedRetries is how many times one model step is retried after a
	// malformed result before the Run fails; zero fails on the first.
	MalformedRetries uint8 `json:"malformedRetries,omitempty"`
	// SystemPrompt is the conversation instruction frozen by the preset digest.
	SystemPrompt string `json:"systemPrompt,omitempty"`
}

// DigestDomain is the digest domain of DigestPreset. The string is frozen:
// it pins every PresetRef ever recorded.
const DigestDomain = "twilight/turn/preset"

// DigestPreset covers the fields that change what the decision layer does
// for a Turn: Model, Tools, Prompt, Scheduling, MalformedRetries and
// SystemPrompt.
func DigestPreset(p *AgentPreset) (jsonstable.Digest, error) {
	body := struct {
		Model            run.ModelRef       `json:"model"`
		Tools            []PublicTool       `json:"tools,omitempty"`
		PromptBuilder    PromptBuilderRef   `json:"promptBuilder"`
		Scheduling       run.ToolScheduling `json:"scheduling,omitempty"`
		MalformedRetries uint8              `json:"malformedRetries,omitempty"`
		SystemPrompt     string             `json:"systemPrompt,omitempty"`
	}{p.Model, p.Tools, p.PromptBuilder, p.Scheduling, p.MalformedRetries, p.SystemPrompt}
	raw, err := jsonstable.EncodeTypedPayload(1, DigestDomain, body)
	if err != nil {
		return "", err
	}
	return jsonstable.DigestBytes(raw), nil
}

// ValidatePreset checks the identity fields a registry must refuse to record
// without: model and prompt builder, plus a well-formed Scheduling.
func ValidatePreset(p *AgentPreset) error {
	switch {
	case p.Model == "":
		return errors.New("preset: preset requires a model")
	case p.PromptBuilder == "":
		return errors.New("preset: preset requires a prompt builder ref")
	}
	if m := p.Scheduling.Mode; m != "" && m != run.ToolScheduleParallel && m != run.ToolScheduleSequential {
		return fmt.Errorf("preset: preset scheduling mode %q is unknown", m)
	}
	if p.Scheduling.MaxParallel < 0 {
		return errors.New("preset: preset scheduling MaxParallel is negative")
	}
	return nil
}

// ToolSpecs derives the frozen ToolSpecs and provider definitions of p, in
// order.
func (p *AgentPreset) ToolSpecs() ([]run.ToolSpec, []sdk.ToolDefinition, error) {
	specs := make([]run.ToolSpec, 0, len(p.Tools))
	defs := make([]sdk.ToolDefinition, 0, len(p.Tools))
	for _, t := range p.Tools {
		d, err := schema.Canonical().DigestToolDefinition(t.Definition)
		if err != nil {
			return nil, nil, err
		}
		specs = append(specs, run.ToolSpec{Ref: t.Ref, Name: t.Definition.Name, DefinitionDigest: d, Policy: t.Policy, Replay: t.Replay, Placement: t.Placement})
		def_, err := sdkconv.ToolDefinition(t.Definition)
		if err != nil {
			return nil, nil, err
		}
		defs = append(defs, def_)
	}
	return specs, defs, nil
}
