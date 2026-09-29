package decision

import (
	"context"

	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/run"

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
