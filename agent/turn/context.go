package turn

import (
	"context"
	"fmt"

	"github.com/memohai/twilight/agent/run"
	"github.com/memohai/twilight/agent/run/loop"
	"github.com/memohai/twilight/sdk"
)

// EntryKind is the kind of one conversation entry in a folded Context.
type EntryKind string

const (
	EntryInput      EntryKind = "input"
	EntryAssistant  EntryKind = "assistant"
	EntryToolResult EntryKind = "tool_result"
)

// Entry is one element of the model-visible conversation, derived from
// committed chatlog events (CHT-CTX-1). Exactly one of the payload fields is
// set according to Kind.
type Entry struct {
	Kind       EntryKind
	Turn       TurnID
	Seq        uint64
	Input      *InputDeliveredPayload
	Assistant  *AssistantPayload
	ToolResult *ToolResultPayload
}

// ContextFold projects a session log into the ordered conversation the model
// should see: delivered inputs, assistant outputs and tool results across
// every Turn of the session, in commit order. It is a pure function of the
// events; turn lifecycle events contribute nothing (CHT-CTX-1, CHT-CTX-2).
//
// This is the minimal fold: no summary, checkpoint or supersession handling
// yet; those arrive with the Chatlog module.
func ContextFold(events []Event) ([]Entry, error) {
	entries := make([]Entry, 0, len(events))
	for i := range events {
		ev := &events[i]
		switch ev.Type {
		case EventInputDelivered:
			var p InputDeliveredPayload
			if err := ev.Payload.Decode(&p); err != nil {
				return nil, fmt.Errorf("agent: turn: fold input_delivered seq %d: %w", ev.Seq, err)
			}
			entries = append(entries, Entry{Kind: EntryInput, Turn: ev.Turn, Seq: ev.Seq, Input: &p})
		case EventAssistant:
			var p AssistantPayload
			if err := ev.Payload.Decode(&p); err != nil {
				return nil, fmt.Errorf("agent: turn: fold assistant seq %d: %w", ev.Seq, err)
			}
			entries = append(entries, Entry{Kind: EntryAssistant, Turn: ev.Turn, Seq: ev.Seq, Assistant: &p})
		case EventToolResult:
			var p ToolResultPayload
			if err := ev.Payload.Decode(&p); err != nil {
				return nil, fmt.Errorf("agent: turn: fold tool_result seq %d: %w", ev.Seq, err)
			}
			entries = append(entries, Entry{Kind: EntryToolResult, Turn: ev.Turn, Seq: ev.Seq, ToolResult: &p})
		}
	}
	return entries, nil
}

// ContextPlanner is the reference RequestPlanner (REF-PLN): it assembles the
// next sdk.Request from the session's folded chatlog plus the Run boundary
// facts in PlanningHint. sdk.Message is produced here and nowhere else; it is
// never stored.
//
// Within one Loop.Run the Coordinator has not yet materialized the tool step
// that just closed, so the planner appends hint.LastModelResult and
// hint.LastToolStep itself when the fold does not already contain that step
// (REF-PLN-3). Pending inputs named by hint.Inputs are consumed by this plan
// and are already in the fold as input_delivered events.
type ContextPlanner struct {
	Log     Log
	Session SessionID
	Model   run.ModelRef
	Tools   []run.ToolSpec
	System  string
	// InputText extracts the user-visible text of one input payload. nil
	// selects the v1 shape {"text": ...} (REF-INP-1).
	InputText func(run.CanonicalJSON) (string, error)
}

func (p *ContextPlanner) Plan(ctx context.Context, hint run.PlanningHint) (loop.RequestPlan, error) {
	if p.Log == nil || p.Session == "" || p.Model == "" {
		return loop.RequestPlan{}, fmt.Errorf("agent: turn: context planner requires Log, Session and Model")
	}
	events, err := p.Log.Replay(ctx, p.Session)
	if err != nil {
		return loop.RequestPlan{}, err
	}
	entries, err := ContextFold(events)
	if err != nil {
		return loop.RequestPlan{}, err
	}
	var msgs []sdk.Message
	if p.System != "" {
		msgs = append(msgs, sdk.SystemMessage(p.System))
	}
	inputText := p.InputText
	if inputText == nil {
		inputText = v1InputText
	}
	folded := make(map[run.StepID]bool)
	for i := range entries {
		e := &entries[i]
		switch e.Kind {
		case EntryInput:
			text, err := inputText(e.Input.Content)
			if err != nil {
				return loop.RequestPlan{}, err
			}
			msgs = append(msgs, sdk.UserMessage(text))
		case EntryAssistant:
			folded[e.Assistant.StepID] = true
			msg, err := assistantMessage(e.Assistant.Text, e.Assistant.ToolCalls)
			if err != nil {
				return loop.RequestPlan{}, err
			}
			msgs = append(msgs, msg)
		case EntryToolResult:
			part, err := toolResultPart(e.ToolResult)
			if err != nil {
				return loop.RequestPlan{}, err
			}
			msgs = append(msgs, sdk.ToolMessage(part))
		}
	}
	// The step that just closed inside this Loop.Run is committed on the Run
	// but not yet on the session log; add it from the boundary facts.
	if hint.LastToolStep != nil && hint.LastModelResult != nil && !folded[hint.LastToolStep.Source] {
		calls := make([]ToolCallPayload, 0, len(hint.LastModelResult.ToolCalls))
		for i, tc := range hint.LastModelResult.ToolCalls {
			calls = append(calls, ToolCallPayload{CallID: run.DeriveCallID(hint.LastToolStep.Source, i), ProviderCallID: tc.ToolCallID, Name: tc.ToolName, Input: tc.Input})
		}
		msg, err := assistantMessage(hint.LastModelResult.Text, calls)
		if err != nil {
			return loop.RequestPlan{}, err
		}
		msgs = append(msgs, msg)
		names := make(map[run.CallID]string, len(calls))
		for _, c := range calls {
			names[c.CallID] = c.Name
		}
		parts := make([]sdk.ToolResultPart, 0, len(hint.LastToolStep.Calls))
		for i := range hint.LastToolStep.Calls {
			c := &hint.LastToolStep.Calls[i]
			r := toolResultFromState(c)
			r.Name = names[c.CallID]
			part, err := toolResultPart(r)
			if err != nil {
				return loop.RequestPlan{}, err
			}
			parts = append(parts, part)
		}
		if len(parts) > 0 {
			msgs = append(msgs, sdk.ToolMessage(parts...))
		}
	}
	ids := make([]run.InputID, 0, len(hint.Inputs))
	for _, in := range hint.Inputs {
		ids = append(ids, in.ID)
	}
	defs := make([]sdk.ToolDefinition, 0, len(p.Tools))
	for _, spec := range p.Tools {
		defs = append(defs, spec.Definition.SDK())
	}
	return loop.RequestPlan{
		Model:    p.Model,
		Request:  sdk.Request{Model: string(p.Model), Messages: msgs, Tools: defs},
		InputIDs: ids,
		Tools:    p.Tools,
	}, nil
}

func v1InputText(content run.CanonicalJSON) (string, error) {
	var body struct {
		Text string `json:"text"`
	}
	if err := content.Decode(&body); err != nil {
		return "", fmt.Errorf("agent: turn: input payload: %w", err)
	}
	return body.Text, nil
}

func assistantMessage(text string, calls []ToolCallPayload) (sdk.Message, error) {
	var parts []sdk.MessagePart
	if text != "" {
		parts = append(parts, sdk.TextPart{Text: text})
	}
	for _, c := range calls {
		input, err := c.Input.Any()
		if err != nil {
			return sdk.Message{}, err
		}
		parts = append(parts, sdk.ToolCallPart{ToolCallID: c.ProviderCallID, ToolName: c.Name, Input: input})
	}
	return sdk.Message{Role: sdk.MessageRoleAssistant, Content: parts}, nil
}

func toolResultPart(r *ToolResultPayload) (sdk.ToolResultPart, error) {
	part := sdk.ToolResultPart{ToolCallID: r.ProviderCallID, ToolName: r.Name}
	switch r.Status {
	case ToolSuccess:
		out, err := r.Output.Any()
		if err != nil {
			return sdk.ToolResultPart{}, err
		}
		part.Result = out
	case ToolError:
		part.Result, part.IsError = r.Failure+": "+r.Message, true
	case ToolUnknown:
		part.Result, part.IsError = "tool outcome unknown: "+r.Message, true
	default:
		return sdk.ToolResultPart{}, fmt.Errorf("agent: turn: unknown tool result status %q", r.Status)
	}
	return part, nil
}

func toolResultFromState(c *run.ToolCallState) *ToolResultPayload {
	p := &ToolResultPayload{CallID: c.CallID, ProviderCallID: c.ProviderCallID}
	switch {
	case c.Result != nil:
		p.Status, p.Output = ToolSuccess, c.Result.Output
	case c.Failure != nil && c.Failure.Outcome == run.ToolOutcomeUnknown:
		p.Status, p.Failure, p.Message = ToolUnknown, c.Failure.Failure.Class, c.Failure.Failure.Message
	case c.Failure != nil:
		p.Status, p.Failure, p.Message = ToolError, c.Failure.Failure.Class, c.Failure.Failure.Message
	}
	return p
}
