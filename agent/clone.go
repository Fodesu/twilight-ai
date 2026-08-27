package agent

import (
	"encoding/json"

	"github.com/memohai/twilight-ai/sdk"
)

// Deep-copy helpers: Runtime return values must be read-only snapshots (spec
// appendix A) — a caller mutating a returned slice, map, or json.RawMessage
// must never reach authoritative storage or committed event bytes.

func cloneRaw(m json.RawMessage) json.RawMessage {
	if m == nil {
		return nil
	}
	return append(json.RawMessage(nil), m...)
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

func cloneToolCallState(c ToolCallState) ToolCallState {
	c.Arguments = cloneRaw(c.Arguments)
	c.Result = cloneToolExecutionResult(c.Result)
	c.Failure = cloneToolCallFailure(c.Failure)
	c.Waiting = cloneResponseRequest(c.Waiting)
	return c
}

func cloneToolCallBinding(b ToolCallBinding) ToolCallBinding {
	b.Arguments = cloneRaw(b.Arguments)
	b.Response = cloneResponseRequest(b.Response)
	return b
}

func cloneToolCallBindings(bs []ToolCallBinding) []ToolCallBinding {
	if bs == nil {
		return nil
	}
	out := make([]ToolCallBinding, len(bs))
	for i, b := range bs {
		out[i] = cloneToolCallBinding(b)
	}
	return out
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

func cloneToolDefinition(d sdk.ToolDefinition) sdk.ToolDefinition {
	d.Parameters = cloneRaw(d.Parameters)
	if d.CacheControl != nil {
		cc := *d.CacheControl
		d.CacheControl = &cc
	}
	return d
}

func cloneRequest(r sdk.Request) sdk.Request {
	r.Messages = append([]sdk.Message(nil), r.Messages...)
	tools := make([]sdk.ToolDefinition, len(r.Tools))
	for i, t := range r.Tools {
		tools[i] = cloneToolDefinition(t)
	}
	if r.Tools != nil {
		r.Tools = tools
	}
	r.StopSequences = append([]string(nil), r.StopSequences...)
	if r.ProviderOptions != nil {
		opts := make(map[string]json.RawMessage, len(r.ProviderOptions))
		for k, v := range r.ProviderOptions {
			opts[k] = cloneRaw(v)
		}
		r.ProviderOptions = opts
	}
	return r
}

func cloneModelResult(r *sdk.ModelResult) *sdk.ModelResult {
	if r == nil {
		return nil
	}
	c := *r
	c.ReasoningParts = append([]sdk.ReasoningPart(nil), c.ReasoningParts...)
	c.Sources = append([]sdk.Source(nil), c.Sources...)
	c.Files = append([]sdk.GeneratedFile(nil), c.Files...)
	c.ToolCalls = append([]sdk.ToolCall(nil), c.ToolCalls...)
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
		step.Request = cloneRequest(step.Request)
		step.Tools = cloneToolSpecs(step.Tools)
		return step
	case ToolStep:
		calls := make([]ToolCallState, len(step.Calls))
		for i, c := range step.Calls {
			calls[i] = cloneToolCallState(c)
		}
		step.Calls = calls
		return step
	default:
		return s
	}
}

func cloneMachineState(s MachineState) MachineState {
	if s.Current != nil {
		s.Current = cloneStep(s.Current)
	}
	s.PendingInputs = cloneAgentInputs(s.PendingInputs)
	s.LastModelResult = cloneModelResult(s.LastModelResult)
	s.Result = cloneRunResult(s.Result)
	return s
}

func cloneFact(f Fact) Fact {
	switch fact := f.(type) {
	case ModelStepPrepared:
		fact.Request = cloneRequest(fact.Request)
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
		if fact.Failure != nil {
			f := *fact.Failure
			fact.Failure = &f
		}
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
