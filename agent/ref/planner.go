package ref

import (
	"context"
	"errors"
	"fmt"

	"github.com/memohai/twilight/agent/es"
	"github.com/memohai/twilight/agent/run"
	"github.com/memohai/twilight/agent/run/loop"
	"github.com/memohai/twilight/agent/session/chatlog"
	"github.com/memohai/twilight/agent/session/extension"
	"github.com/memohai/twilight/sdk"
)

type (
	extensionProjectionID      = extension.ProjectionID
	extensionProjectionVersion = extension.ProjectionVersion
)

// ContextPlanner is the reference RequestPlanner (REF-PLN): it reads the
// chatlog context projection and assembles the next sdk.Request. Every
// assistant and tool_result of the Session is in the fold already, including
// those of earlier attempts of the same Turn (REF-PLN-6).
type ContextPlanner struct {
	Projections ProjectionSource
	Profile     Profile
	// InputText extracts the user text of one input payload; nil selects the
	// v1 shape {"text": ...} (REF-INP-1).
	InputText func(run.CanonicalJSON) (string, error)
}

func (p *ContextPlanner) Plan(ctx context.Context, hint run.PlanningHint) (loop.RequestPlan, error) {
	if p.Projections == nil || p.Profile.Model == "" {
		return loop.RequestPlan{}, errors.New("ref: planner requires projections and a model")
	}
	if hint.Session == "" {
		return loop.RequestPlan{}, errors.New("ref: planner hint has no session")
	}
	state, head, err := p.Projections.Load(ctx, hint.Session, chatlog.ContextProjectionID, chatlog.ContextProjection.Version)
	if err != nil {
		return loop.RequestPlan{}, err
	}
	entries := state.(chatlog.Context).Entries
	msgs, err := p.messages(entries)
	if err != nil {
		return loop.RequestPlan{}, err
	}
	specs, defs, err := p.Profile.ToolSpecs()
	if err != nil {
		return loop.RequestPlan{}, err
	}
	ids := make([]run.InputID, 0, len(hint.Inputs))
	for _, in := range hint.Inputs {
		ids = append(ids, in.ID)
	}
	return loop.RequestPlan{
		Model:         p.Profile.Model,
		Request:       sdk.Request{Model: string(p.Profile.Model), Messages: msgs, Tools: defs},
		InputIDs:      ids,
		PlanningToken: run.PlanningToken(fmt.Sprintf("%d:%s", head.Next, head.Digest)),
		Tools:         specs,
	}, nil
}

// messages is REF-PLN-2.
func (p *ContextPlanner) messages(entries []chatlog.Entry) ([]sdk.Message, error) {
	var msgs []sdk.Message
	if p.Profile.SystemPrompt != "" {
		msgs = append(msgs, sdk.SystemMessage(p.Profile.SystemPrompt))
	}
	inputText := p.InputText
	if inputText == nil {
		inputText = v1InputText
	}
	// ProviderCallID and tool name per CallID, from the assistant that issued
	// the call, for pairing tool results (REF-PLN-2 step 2).
	type callInfo struct{ provider, name string }
	calls := map[chatlog.CallID]callInfo{}
	// Inputs delivered mid-turn are committed while tool calls are still open
	// (TRN-DLV-2); providers require tool results to follow their assistant
	// message directly, so such inputs are held until the open calls resolve.
	open := map[chatlog.CallID]struct{}{}
	var deferred []sdk.Message
	flushDeferred := func() {
		if len(open) == 0 && len(deferred) > 0 {
			msgs = append(msgs, deferred...)
			deferred = nil
		}
	}
	for i := range entries {
		e := &entries[i]
		switch e.Kind {
		case chatlog.EntryInput:
			text, err := inputText(e.Input.Content)
			if err != nil {
				return nil, err
			}
			if len(open) > 0 {
				deferred = append(deferred, sdk.UserMessage(text))
			} else {
				msgs = append(msgs, sdk.UserMessage(text))
			}
		case chatlog.EntryAssistant:
			flushDeferred()
			var parts []sdk.MessagePart
			for _, part := range e.Assistant.Parts {
				switch v := part.(type) {
				case chatlog.TextPart:
					parts = append(parts, sdk.TextPart{Text: v.Text})
				case chatlog.ReasoningPart:
					parts = append(parts, sdk.ReasoningPart{Text: v.Text})
				case chatlog.ToolCallPart:
					calls[v.CallID] = callInfo{provider: v.ProviderCallID, name: v.Name}
					open[v.CallID] = struct{}{}
					input, err := v.Input.Any()
					if err != nil {
						return nil, err
					}
					parts = append(parts, sdk.ToolCallPart{ToolCallID: v.ProviderCallID, ToolName: v.Name, Input: input})
				case chatlog.ReferencePart:
					// v1 reference planner: no materializer; the reference is named.
					parts = append(parts, sdk.TextPart{Text: "[attachment " + v.Name + "]"})
				}
			}
			if len(parts) > 0 {
				msgs = append(msgs, sdk.Message{Role: sdk.MessageRoleAssistant, Content: parts})
			}
		case chatlog.EntryToolResult:
			r := e.ToolResult
			info := calls[r.CallID]
			part := sdk.ToolResultPart{ToolCallID: info.provider, ToolName: info.name}
			text := partsText(r.Parts)
			switch r.Status {
			case chatlog.ToolSuccess:
				part.Result = text
			case chatlog.ToolError:
				part.Result, part.IsError = text, true
			case chatlog.ToolUnknown:
				part.Result, part.IsError = "tool outcome unknown: "+text, true
			}
			msgs = append(msgs, sdk.ToolMessage(part))
			delete(open, r.CallID)
			flushDeferred()
		case chatlog.EntrySummary:
			flushDeferred()
			msgs = append(msgs, sdk.AssistantMessage(partsText(e.Summary.Parts)))
		}
	}
	// Calls left open (a stopped attempt) never resolve: release the inputs.
	msgs = append(msgs, deferred...)
	return msgs, nil
}

func partsText(parts chatlog.Parts) string {
	var out string
	for _, part := range parts {
		switch v := part.(type) {
		case chatlog.TextPart:
			out += v.Text
		case chatlog.ReferencePart:
			out += "[attachment " + v.Name + "]"
		}
	}
	return out
}

func v1InputText(content run.CanonicalJSON) (string, error) {
	var body struct {
		Text string `json:"text"`
	}
	if err := content.Decode(&body); err != nil {
		return "", fmt.Errorf("ref: input payload: %w", err)
	}
	return body.Text, nil
}

// InputContent is the v1 user body shape (REF-INP-1).
func InputContent(text string) run.CanonicalJSON {
	return run.MustParseCanonicalJSON(fmt.Sprintf(`{"text":%s}`, mustJSONString(text)))
}

func mustJSONString(s string) string {
	raw, _ := es.MarshalCanonical(s)
	return string(raw)
}
