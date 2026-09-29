package decision

import (
	"context"

	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/model/sdkconv"
	"github.com/felinics/twilight/agentcore/run/schema"

	"github.com/felinics/twilight/sdk"
)

// BuilderRef names a Builder in the catalog; it is part of the preset
// digest.
type BuilderRef = preset.PromptBuilderRef

// Builder assembles the next model input from the surrounding
// conversation, which it reads by the Input's Scope. The caller freezes
// the returned Prompt into an agent-owned ModelRequest before crossing
// the RunStore boundary.
type Builder interface {
	Build(context.Context, Input) (Prompt, error)
}

// Prompt is one built model input: the model to call, the provider request
// (messages and tool definitions), the inputs it consumed, the freshness
// token of the context it was built from, and the frozen tool specs.
type Prompt struct {
	Model    run.ModelRef
	Request  sdk.Request
	InputIDs []run.InputID
	Token    run.PromptToken
	Tools    []run.ToolSpec
}

// Input is what the execution hands the Builder: the Run boundary facts
// only. Conversation content (previous assistant output, tool results) is
// read from the Session by the Builder itself.
type Input struct {
	Scope      run.Scope // filled by the caller; planning does not know it
	RunID      run.RunID
	SourceStep run.StepID
	Inputs     []run.AgentInput
}

// ToolSpecs freezes the preset's tool contracts into what each side of the
// boundary needs: the run.ToolSpec the Run persists (definition digest,
// policy, replay and placement declarations) and the provider-facing
// definition of the model request, in contract order.
func ToolSpecs(tools []preset.ToolContract) ([]run.ToolSpec, []sdk.ToolDefinition, error) {
	specs := make([]run.ToolSpec, 0, len(tools))
	defs := make([]sdk.ToolDefinition, 0, len(tools))
	for _, t := range tools {
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
