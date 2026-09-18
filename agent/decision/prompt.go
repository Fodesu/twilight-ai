// Package decision is the decision layer of the agent core
// (docs/design/agent-decision.md): the components that turn committed state
// and an AgentPreset into the next effect request. Everything here is a
// deterministic function of projection state and the AgentPreset, runs on the
// authority side, and is named by a ref the AgentPreset digest covers, so the
// process that takes a Turn over rebuilds the same decision function.
package decision

import (
	"context"
	"errors"
	"fmt"

	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/loop"
	"github.com/felinics/twilight/agent/run/plan"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/chatlog"
	"github.com/felinics/twilight/agent/session/extension"
	"github.com/felinics/twilight/agent/turn"
	"github.com/felinics/twilight/sdk"
)

// PromptContextV1 names the context prompt builder: the chatlog context projection
// folded into one provider request (DEC-PMT-1).
const PromptContextV1 turn.PromptBuilderRef = "twilight/decision/prompt/context-v1"

// ProjectionSource is what a prompt builder reads state from: the owner
// process serves it from the Session Writer (writer.Projections), an observer
// from the Store.
type ProjectionSource = extension.ProjectionReader

// Sources are the two read ports of a prompt builder (DEC-PMT-1): the
// structural projections and the content resolver that materializes the
// frozen bodies the projections name by digest. Folding is pure; reading a
// body is I/O and happens only here.
type Sources struct {
	Projections ProjectionSource
	Content     chatlog.ContentResolver
}

// ContextPromptBuilder is the context-v1 PromptBuilder (DEC-PMT): it reads the
// chatlog context projection, materializes its entries and assembles the next
// sdk.Request. Every assistant and tool_result of the Session is in the fold
// already, including those of earlier attempts of the same Turn (DEC-PMT-6).
type ContextPromptBuilder struct {
	Sources Sources
	Preset  turn.AgentPreset
	// InputText extracts the user text of one input payload; nil selects the
	// v1 shape {"text": ...} (DEC-INP-1).
	InputText func(run.CanonicalJSON) (string, error)
}

// NewContextPromptBuilder is the PromptBuilderFactory of PromptContextV1.
func NewContextPromptBuilder(preset turn.AgentPreset, sources Sources) loop.PromptBuilder {
	return &ContextPromptBuilder{Sources: sources, Preset: preset}
}

func (p *ContextPromptBuilder) Build(ctx context.Context, hint plan.PromptInput) (loop.Prompt, error) {
	if p.Sources.Projections == nil || p.Preset.Model == "" {
		return loop.Prompt{}, errors.New("decision: builder requires projections and a model")
	}
	if hint.Scope == "" {
		return loop.Prompt{}, errors.New("decision: builder hint has no session")
	}
	state, head, err := p.Sources.Projections.Load(ctx, session.SessionID(hint.Scope), chatlog.ContextProjectionID, chatlog.ContextProjection.Version)
	if err != nil {
		return loop.Prompt{}, err
	}
	cctx, ok := state.(chatlog.Context)
	if !ok {
		return loop.Prompt{}, fmt.Errorf("decision: context projection is %T", state)
	}
	entries, err := chatlog.NewMaterializer(p.Sources.Content).Entries(ctx, cctx.Entries)
	if err != nil {
		return loop.Prompt{}, err
	}
	msgs, err := p.messages(entries)
	if err != nil {
		return loop.Prompt{}, err
	}
	specs, defs, err := p.Preset.ToolSpecs()
	if err != nil {
		return loop.Prompt{}, err
	}
	ids := make([]run.InputID, 0, len(hint.Inputs))
	for _, in := range hint.Inputs {
		ids = append(ids, in.ID)
	}
	return loop.Prompt{
		Model:    p.Preset.Model,
		Request:  sdk.Request{Model: string(p.Preset.Model), Messages: msgs, Tools: defs},
		InputIDs: ids,
		Token:    run.PromptToken(fmt.Sprintf("%d:%s", head.Next, head.Digest)),
		Tools:    specs,
	}, nil
}

// messages is DEC-PMT-2.
func (p *ContextPromptBuilder) messages(entries []chatlog.Materialized) ([]sdk.Message, error) {
	var msgs []sdk.Message
	if p.Preset.SystemPrompt != "" {
		msgs = append(msgs, sdk.SystemMessage(p.Preset.SystemPrompt))
	}
	inputText := p.InputText
	if inputText == nil {
		inputText = v1InputText
	}
	// ProviderCallID and tool name per CallID, from the assistant that issued
	// the call, for pairing tool results (DEC-PMT-2 step 2).
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
		m := &entries[i]
		e := m.Entry
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
			if len(open) > 0 {
				return nil, errors.New("decision: assistant follows unresolved tool calls")
			}
			flushDeferred()
			if m.Result == nil {
				return nil, fmt.Errorf("decision: assistant %s is not materialized", e.ID)
			}
			var parts []sdk.MessagePart
			for _, rp := range m.Result.ReasoningParts {
				if rp.Text != "" {
					parts = append(parts, sdk.ReasoningPart{Text: rp.Text})
				}
			}
			if m.Result.Text != "" {
				parts = append(parts, sdk.TextPart{Text: m.Result.Text})
			}
			for _, call := range m.Calls {
				calls[call.CallID] = callInfo{provider: call.ProviderCallID, name: call.Name}
				open[call.CallID] = struct{}{}
				input, err := call.Input.Any()
				if err != nil {
					return nil, err
				}
				parts = append(parts, sdk.ToolCallPart{ToolCallID: call.ProviderCallID, ToolName: call.Name, Input: input})
			}
			if len(parts) > 0 {
				msgs = append(msgs, sdk.Message{Role: sdk.MessageRoleAssistant, Content: parts})
			}
		case chatlog.EntryToolResult:
			r := e.ToolResult
			if _, ok := open[r.CallID]; !ok {
				return nil, fmt.Errorf("decision: tool result %s has no open call", r.CallID)
			}
			info := calls[r.CallID]
			part := sdk.ToolResultPart{ToolCallID: info.provider, ToolName: info.name}
			text := m.Text()
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
			if len(open) > 0 {
				return nil, errors.New("decision: summary follows unresolved tool calls")
			}
			flushDeferred()
			msgs = append(msgs, sdk.AssistantMessage(chatlog.PartsText(e.Summary.Parts)))
		}
	}
	if len(open) > 0 {
		return nil, errors.New("decision: context has unresolved tool calls")
	}
	return msgs, nil
}

func v1InputText(content run.CanonicalJSON) (string, error) {
	var body struct {
		Text string `json:"text"`
	}
	if err := content.Decode(&body); err != nil {
		return "", fmt.Errorf("decision: input payload: %w", err)
	}
	return body.Text, nil
}

// InputContent is the v1 user body shape (DEC-INP-1): the same canonical
// JSON is the chatlog Input content and the Run AgentInput payload. The
// constructor lives in chatlog, which owns the Input.Content wire shape.
func InputContent(text string) run.CanonicalJSON {
	return chatlog.TextContent(text)
}

// InputText is the v1 inverse of InputContent: the user text of an input
// payload (DEC-INP-1). Hosts use it to render transcripts.
func InputText(content run.CanonicalJSON) (string, error) { return v1InputText(content) }
