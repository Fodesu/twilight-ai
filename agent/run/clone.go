package run

import "encoding/json"

// Deep-copy helpers: Runtime return values must be read-only snapshots (spec
// appendix A) — a caller mutating a returned slice or map must never reach
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

func cloneToolExecutionResult(r *ToolExecutionResult) *ToolExecutionResult {
	if r == nil {
		return nil
	}
	c := *r
	c.Output = cloneRaw(c.Output)
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
	out.Result = cloneToolExecutionResult(out.Result)
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
	out := make([]ToolSpec, len(specs))
	for i, s := range specs {
		s.Definition = cloneToolDefinition(s.Definition)
		out[i] = s
	}
	return out
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

func cloneReasoningParts(parts []ReasoningPart) []ReasoningPart {
	if parts == nil {
		return nil
	}
	out := make([]ReasoningPart, len(parts))
	for i, p := range parts {
		p.ProviderMetadata = cloneProviderMetadata(p.ProviderMetadata)
		out[i] = p
	}
	return out
}

func cloneSources(sources []Source) []Source {
	if sources == nil {
		return nil
	}
	out := make([]Source, len(sources))
	for i, s := range sources {
		s.ProviderMetadata = cloneProviderMetadata(s.ProviderMetadata)
		out[i] = s
	}
	return out
}

func cloneGeneratedFiles(files []GeneratedFile) []GeneratedFile {
	if files == nil {
		return nil
	}
	return append([]GeneratedFile(nil), files...)
}

func cloneModelToolCall(c ModelToolCall) ModelToolCall {
	c.Input = cloneRaw(c.Input)
	c.ProviderMetadata = cloneProviderMetadata(c.ProviderMetadata)
	return c
}

func cloneModelToolCalls(calls []ModelToolCall) []ModelToolCall {
	if calls == nil {
		return nil
	}
	out := make([]ModelToolCall, len(calls))
	for i, c := range calls {
		out[i] = cloneModelToolCall(c)
	}
	return out
}

func cloneResponseMetadata(r *ResponseMetadata) *ResponseMetadata {
	if r == nil {
		return nil
	}
	c := *r
	if r.Headers != nil {
		c.Headers = make(map[string]string, len(r.Headers))
		for k, v := range r.Headers {
			c.Headers[k] = v
		}
	}
	return &c
}

func cloneModelResult(r *ModelResult) *ModelResult {
	if r == nil {
		return nil
	}
	c := *r
	c.ReasoningParts = cloneReasoningParts(c.ReasoningParts)
	c.TextProviderMetadata = cloneProviderMetadata(c.TextProviderMetadata)
	c.Sources = cloneSources(c.Sources)
	c.Files = cloneGeneratedFiles(c.Files)
	c.ToolCalls = cloneModelToolCalls(c.ToolCalls)
	c.Response = cloneResponseMetadata(c.Response)
	return &c
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
	c.Model = cloneModelResult(c.Model)
	return &c
}

func cloneStep(s Step) Step {
	switch step := s.(type) {
	case ModelStep:
		step.Request = cloneRequest(&step.Request)
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
		out.Current = cloneStep(out.Current)
	}
	out.LastToolStep = cloneToolStepPtr(out.LastToolStep)
	out.PendingInputs = cloneAgentInputs(out.PendingInputs)
	out.LastModelResult = cloneModelResult(out.LastModelResult)
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

func snapshotFact(f Fact) (Fact, error) {
	switch fact := f.(type) {
	case ModelStepPrepared:
		return snapshotJSONStable(fact)
	case ModelStepCompleted:
		return snapshotJSONStable(fact)
	case ToolStepOpened:
		return snapshotJSONStable(fact)
	case ToolCallCompleted:
		return snapshotJSONStable(fact)
	case ToolCallAnswered:
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
		fact.Request = cloneRequest(&fact.Request)
		fact.InputIDs = append([]InputID(nil), fact.InputIDs...)
		fact.Tools = cloneToolSpecs(fact.Tools)
		return fact
	case ModelStepCompleted:
		if r := cloneModelResult(&fact.Result); r != nil {
			fact.Result = *r
		}
		return fact
	case ToolStepOpened:
		fact.Calls = cloneToolCallBindings(fact.Calls)
		return fact
	case ToolCallCompleted:
		fact.Result.Output = cloneRaw(fact.Result.Output)
		return fact
	case ToolCallAnswered:
		fact.Payload = cloneRaw(fact.Payload)
		return fact
	case InputAccepted:
		fact.Input = cloneAgentInput(fact.Input)
		return fact
	case RunEnded:
		return fact
	default:
		return f
	}
}

func cloneEvents(events []AgentEvent) []AgentEvent {
	if events == nil {
		return nil
	}
	out := make([]AgentEvent, len(events))
	for i, e := range events {
		e.Fact = cloneFact(e.Fact)
		out[i] = e
	}
	return out
}

func cloneTransitionRecord(record *TransitionRecord) TransitionRecord {
	out := *record
	out.Events = cloneEvents(out.Events)
	return out
}

func cloneTransitionRecords(records []TransitionRecord) []TransitionRecord {
	if records == nil {
		return nil
	}
	out := make([]TransitionRecord, len(records))
	for i := range records {
		out[i] = cloneTransitionRecord(&records[i])
	}
	return out
}

func flattenTransitionRecords(records []TransitionRecord) []AgentEvent {
	var total int
	for i := range records {
		total += len(records[i].Events)
	}
	if total == 0 {
		return nil
	}
	out := make([]AgentEvent, 0, total)
	for i := range records {
		out = append(out, cloneEvents(records[i].Events)...)
	}
	return out
}
