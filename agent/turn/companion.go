package turn

import (
	"fmt"

	"github.com/felinics/twilight/agent/es"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/session/chatlog"
)

// CompanionV1Version identifies the v1 mapping (TRN-CMP-2).
const CompanionV1Version CompanionVersion = "twilight/turn/companion/v1"

// CompanionV1 maps Run facts and the command's frozen content to chatlog and
// turn events of the same commit (TRN-CMP). It interprets Owner as TurnID.
type CompanionV1 struct{}

func (CompanionV1) Version() string { return string(CompanionV1Version) }

// AssistantID is TRN-MAP-2.
func AssistantID(turnID TurnID, step run.StepID) chatlog.AssistantID {
	return chatlog.AssistantID(digestOf("twilight/chatlog/assistant-id", string(turnID), string(step), string(CompanionV1Version)))
}

// ToolResultID is TRN-MAP-2.
func ToolResultID(turnID TurnID, call run.CallID) chatlog.ToolResultID {
	return chatlog.ToolResultID(digestOf("twilight/chatlog/tool-result-id", string(turnID), string(call), string(CompanionV1Version)))
}

// Map is a pure function of the request (TRN-CMP-1).
func (CompanionV1) Map(req run.CompanionRequest) ([]run.ModuleEvent, error) {
	turnID := TurnID(req.Owner)
	if turnID == "" {
		return nil, nil
	}
	var out []run.ModuleEvent
	for _, f := range req.Facts {
		switch fact := f.(type) {
		case run.ModelStepCompleted:
			cmd, ok := req.Command.(run.SubmitModelResult)
			if !ok {
				return nil, fmt.Errorf("turn: companion: ModelStepCompleted from %T", req.Command)
			}
			a, err := assistantFor(turnID, fact, &cmd.Result, callIDs(req.Facts))
			if err != nil {
				return nil, err
			}
			out = append(out, run.ModuleEvent{Type: chatlog.TypeAssistant, Value: chatlog.AssistantPayload{Assistant: a}})
		case run.ToolCallCompleted:
			cmd, ok := req.Command.(run.SubmitToolResult)
			if !ok {
				return nil, fmt.Errorf("turn: companion: ToolCallCompleted from %T", req.Command)
			}
			r, err := toolResultFor(turnID, fact.CallID, chatlog.ToolSuccess, cmd.Result.Output.String(), fact.OutputDigest)
			if err != nil {
				return nil, err
			}
			out = append(out, run.ModuleEvent{Type: chatlog.TypeToolResult, Value: chatlog.ToolResultPayload{ToolResult: r}})
		case run.ToolCallAnswered:
			cmd, ok := req.Command.(run.SubmitToolResponse)
			if !ok {
				return nil, fmt.Errorf("turn: companion: ToolCallAnswered from %T", req.Command)
			}
			r, err := toolResultFor(turnID, fact.CallID, chatlog.ToolSuccess, cmd.Payload.String(), fact.ResponseDigest)
			if err != nil {
				return nil, err
			}
			out = append(out, run.ModuleEvent{Type: chatlog.TypeToolResult, Value: chatlog.ToolResultPayload{ToolResult: r}})
		case run.ToolCallFailed:
			status := chatlog.ToolError
			if fact.Outcome == run.ToolOutcomeUnknown || fact.Failure.Class == run.FailureEffectUnknown {
				status = chatlog.ToolUnknown
			}
			text := fact.Failure.Class
			if fact.Failure.Message != "" {
				text += ": " + fact.Failure.Message
			}
			r, err := toolResultFor(turnID, fact.CallID, status, text, "")
			if err != nil {
				return nil, err
			}
			out = append(out, run.ModuleEvent{Type: chatlog.TypeToolResult, Value: chatlog.ToolResultPayload{ToolResult: r}})
		case run.RunEnded:
			if _, completed := fact.End.(run.RunCompletedEnd); completed {
				out = append(out, run.ModuleEvent{Type: TypeCompleted, Value: CompletedPayload{TurnID: turnID, RunID: req.RunID}})
			}
		}
	}
	return out, nil
}

// callIDs collects the derived CallIDs the ToolStepOpened of this commit
// assigned, by position in the model result.
func callIDs(facts []run.Fact) []run.CallID {
	for _, f := range facts {
		if opened, ok := f.(run.ToolStepOpened); ok {
			ids := make([]run.CallID, len(opened.Calls))
			for i, b := range opened.Calls {
				ids[i] = b.CallID
			}
			return ids
		}
	}
	return nil
}

func assistantFor(turnID TurnID, fact run.ModelStepCompleted, result *run.ModelResult, ids []run.CallID) (chatlog.Assistant, error) {
	a := chatlog.Assistant{ID: AssistantID(turnID, fact.StepID), TurnID: chatlog.TurnID(turnID), SourceDigest: fact.ResultDigest}
	for _, rp := range result.ReasoningParts {
		if rp.Text != "" {
			a.Parts = append(a.Parts, chatlog.ReasoningPart{Text: rp.Text})
		}
	}
	if result.Text != "" {
		a.Parts = append(a.Parts, chatlog.TextPart{Text: result.Text})
	}
	for i, tc := range result.ToolCalls {
		callID := run.DeriveCallID(fact.StepID, i)
		if i < len(ids) {
			callID = ids[i]
		}
		a.Parts = append(a.Parts, chatlog.ToolCallPart{CallID: chatlog.CallID(callID), ProviderCallID: tc.ToolCallID, Name: tc.ToolName, Input: tc.Input})
	}
	d, err := chatlog.DigestAssistant(&a)
	if err != nil {
		return chatlog.Assistant{}, err
	}
	a.Digest = d
	return a, nil
}

func toolResultFor(turnID TurnID, callID run.CallID, status chatlog.ToolResultStatus, text string, source es.Digest) (chatlog.ToolResult, error) {
	r := chatlog.ToolResult{ID: ToolResultID(turnID, callID), TurnID: chatlog.TurnID(turnID), CallID: chatlog.CallID(callID),
		Status: status, Parts: chatlog.Parts{chatlog.TextPart{Text: text}}, SourceDigest: source}
	d, err := chatlog.DigestToolResult(&r)
	if err != nil {
		return chatlog.ToolResult{}, err
	}
	r.Digest = d
	return r, nil
}

var _ run.Companion = CompanionV1{}
