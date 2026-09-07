package run

import "encoding/json"

// Deep-copy helpers: Runtime return values must be read-only snapshots
// (RUN-CMT-6) — a caller mutating a returned slice or map must never reach
// authoritative storage or committed event bytes.
//
// The agent Runtime is an authority boundary. All persisted request/result
// shapes are agent-owned JSON-stable values, so cloning is mechanical: copy
// structs and copy slice/map containers. CanonicalJSON values are immutable.

func cloneRaw(v CanonicalJSON) CanonicalJSON { return v }

func clonePtr[T any](p *T) *T {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

func cloneProviderMetadata(meta ProviderMetadata) ProviderMetadata {
	if meta == nil {
		return nil
	}
	out := make(ProviderMetadata, len(meta))
	for k, v := range meta {
		out[k] = cloneRaw(v)
	}
	return out
}

func cloneCacheControl(c *CacheControl) *CacheControl {
	if c == nil {
		return nil
	}
	cc := *c
	return &cc
}

func cloneAgentInput(in AgentInput) AgentInput {
	in.Payload = cloneRaw(in.Payload)
	return in
}

func cloneAgentInputs(ins []AgentInput) []AgentInput {
	if ins == nil {
		return nil
	}
	out := make([]AgentInput, len(ins))
	for i, in := range ins {
		out[i] = cloneAgentInput(in)
	}
	return out
}

func cloneResponseRequest(r *ResponseRequest) *ResponseRequest {
	if r == nil {
		return nil
	}
	c := *r
	c.Payload = cloneRaw(c.Payload)
	return &c
}

func cloneToolCallFailure(f *ToolCallFailure) *ToolCallFailure {
	if f == nil {
		return nil
	}
	c := *f
	return &c
}

func cloneToolCallState(c *ToolCallState) ToolCallState {
	out := *c
	out.Arguments = cloneRaw(out.Arguments)
	out.Result = clonePtr(out.Result)
	out.Failure = cloneToolCallFailure(out.Failure)
	out.Waiting = cloneResponseRequest(out.Waiting)
	return out
}

func cloneToolCallBinding(b *ToolCallBinding) ToolCallBinding {
	out := *b
	out.Arguments = cloneRaw(out.Arguments)
	out.Response = cloneResponseRequest(out.Response)
	return out
}

func cloneToolCallBindings(bs []ToolCallBinding) []ToolCallBinding {
	if bs == nil {
		return nil
	}
	out := make([]ToolCallBinding, len(bs))
	for i := range bs {
		out[i] = cloneToolCallBinding(&bs[i])
	}
	return out
}

func cloneToolDefinition(d ToolDefinition) ToolDefinition {
	d.Parameters = cloneRaw(d.Parameters)
	d.CacheControl = cloneCacheControl(d.CacheControl)
	return d
}

func cloneToolSpecs(specs []ToolSpec) []ToolSpec {
	if specs == nil {
		return nil
	}
	return append([]ToolSpec(nil), specs...)
}

func cloneResponseFormat(f *ResponseFormat) *ResponseFormat {
	if f == nil {
		return nil
	}
	c := *f
	c.JSONSchema = cloneRaw(c.JSONSchema)
	return &c
}

func cloneMessagePart(p *MessagePart) MessagePart {
	out := *p
	out.Input = cloneRaw(out.Input)
	out.Result = cloneRaw(out.Result)
	out.CacheControl = cloneCacheControl(out.CacheControl)
	out.ProviderMetadata = cloneProviderMetadata(out.ProviderMetadata)
	return out
}

func cloneMessages(messages []Message) []Message {
	if messages == nil {
		return nil
	}
	out := make([]Message, len(messages))
	for i, m := range messages {
		if m.Content != nil {
			parts := make([]MessagePart, len(m.Content))
			for j := range m.Content {
				parts[j] = cloneMessagePart(&m.Content[j])
			}
			m.Content = parts
		}
		m.Usage = clonePtr(m.Usage)
		out[i] = m
	}
	return out
}

func cloneRequest(r *ModelRequest) ModelRequest {
	out := *r
	out.Messages = cloneMessages(out.Messages)
	out.Tools = cloneToolDefinitions(out.Tools)
	out.ResponseFormat = cloneResponseFormat(out.ResponseFormat)
	out.Temperature = clonePtr(out.Temperature)
	out.TopP = clonePtr(out.TopP)
	out.MaxTokens = clonePtr(out.MaxTokens)
	out.FrequencyPenalty = clonePtr(out.FrequencyPenalty)
	out.PresencePenalty = clonePtr(out.PresencePenalty)
	out.Seed = clonePtr(out.Seed)
	out.ReasoningEffort = clonePtr(out.ReasoningEffort)
	out.ReasoningSummary = clonePtr(out.ReasoningSummary)
	out.PromptCacheKey = clonePtr(out.PromptCacheKey)
	out.StopSequences = append([]string(nil), out.StopSequences...)
	if out.ProviderOptions != nil {
		opts := make(map[string]CanonicalJSON, len(out.ProviderOptions))
		for k, v := range out.ProviderOptions {
			opts[k] = cloneRaw(v)
		}
		out.ProviderOptions = opts
	}
	return out
}

func cloneToolDefinitions(defs []ToolDefinition) []ToolDefinition {
	if defs == nil {
		return nil
	}
	out := make([]ToolDefinition, len(defs))
	for i, d := range defs {
		out[i] = cloneToolDefinition(d)
	}
	return out
}

func cloneRunResult(r *RunResult) *RunResult {
	if r == nil {
		return nil
	}
	c := *r
	if c.Failure != nil {
		f := *c.Failure
		c.Failure = &f
	}
	c.UncertainCalls = append([]CallID(nil), c.UncertainCalls...)
	return &c
}

func cloneStep(s Step) Step {
	switch step := s.(type) {
	case ModelStep:
		step.Tools = cloneToolSpecs(step.Tools)
		return step
	case ToolStep:
		calls := make([]ToolCallState, len(step.Calls))
		for i := range step.Calls {
			calls[i] = cloneToolCallState(&step.Calls[i])
		}
		step.Calls = calls
		return step
	default:
		return s
	}
}

func cloneCurrent(c Current) Current {
	switch cur := c.(type) {
	case Open:
		return Open{}
	case ModelStep:
		return cloneStep(cur).(ModelStep)
	case ToolStep:
		return cloneStep(cur).(ToolStep)
	default:
		return c
	}
}

func cloneToolStepPtr(s *ToolStep) *ToolStep {
	if s == nil {
		return nil
	}
	step := cloneStep(*s).(ToolStep)
	return &step
}

func cloneMachineState(s *MachineState) MachineState {
	out := *s
	if out.Current != nil {
		out.Current = cloneCurrent(out.Current)
	}
	out.LastToolStep = cloneToolStepPtr(out.LastToolStep)
	out.PendingInputs = cloneAgentInputs(out.PendingInputs)
	out.Result = cloneRunResult(out.Result)
	return out
}

func snapshotJSONStable[T any](v T) (T, error) {
	var out T
	raw, err := marshalCanonical(v)
	if err != nil {
		return out, err
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, err
	}
	return out, nil
}

// snapshotFact detaches the caller-owned containers a fact may still share
// with its command (tool specs, bindings, input payloads). Digest-only facts
// carry no such containers and are copied by value.
func snapshotFact(f Fact) (Fact, error) {
	switch fact := f.(type) {
	case ModelStepPrepared:
		return snapshotJSONStable(fact)
	case ToolStepOpened:
		return snapshotJSONStable(fact)
	case InputAccepted:
		return snapshotJSONStable(fact)
	default:
		return cloneFact(f), nil
	}
}

func cloneFact(f Fact) Fact {
	switch fact := f.(type) {
	case ModelStepPrepared:
		fact.InputIDs = append([]InputID(nil), fact.InputIDs...)
		fact.Tools = cloneToolSpecs(fact.Tools)
		return fact
	case ToolStepOpened:
		fact.Calls = cloneToolCallBindings(fact.Calls)
		return fact
	case InputAccepted:
		fact.Input = cloneAgentInput(fact.Input)
		return fact
	case RunEnded:
		if stopped, ok := fact.End.(RunStoppedEnd); ok {
			stopped.UncertainCalls = append([]CallID(nil), stopped.UncertainCalls...)
			fact.End = stopped
		}
		return fact
	default:
		return f
	}
}
