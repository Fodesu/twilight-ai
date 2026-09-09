package sdk

import (
	"encoding/json"
	"testing"

	"github.com/google/jsonschema-go/jsonschema"
)

func TestRequestFromGenerateParams(t *testing.T) {
	temp := 0.7
	max := 128
	model := &Model{ID: "m-1"}
	meta := map[string]any{"p": map[string]any{"sig": "s1"}}
	params := GenerateParams{
		Model:       model,
		System:      "sys",
		Messages:    []Message{{Role: MessageRoleUser, Content: []MessagePart{TextPart{Text: "hi", ProviderMetadata: meta}}}},
		Temperature: &temp,
		MaxTokens:   &max,
		Tools: []Tool{{
			Name:         "search",
			Description:  "Search",
			Parameters:   map[string]any{"type": "object", "properties": map[string]any{"q": map[string]any{"type": "string"}}},
			Execute:      func(*ToolExecContext, any) (any, error) { return nil, nil },
			CacheControl: &CacheControl{Type: "ephemeral", TTL: "1h"},
		}},
		ToolChoice: map[string]any{"type": "function", "function": map[string]any{"name": "search"}},
	}

	req, err := RequestFromGenerateParams(params)
	if err != nil {
		t.Fatal(err)
	}
	if req.Model != "m-1" || req.System != "sys" {
		t.Fatalf("request identity = %+v", req)
	}
	if req.ToolChoice.Mode != ToolChoiceTool || req.ToolChoice.Tool != "search" {
		t.Fatalf("tool choice = %+v", req.ToolChoice)
	}
	if len(req.Tools) != 1 || req.Tools[0].Name != "search" || req.Tools[0].CacheControl.TTL != "1h" {
		t.Fatalf("tools = %+v", req.Tools)
	}
	if len(req.Tools[0].Parameters) == 0 || string(req.Tools[0].Parameters) == "null" {
		t.Fatalf("tool parameters not resolved: %s", req.Tools[0].Parameters)
	}

	// The adapter snapshots common mutable containers instead of returning the
	// original backing arrays/maps.
	params.Messages[0].Content[0].(TextPart).ProviderMetadata["p"] = "mutated"
	gotMeta := req.Messages[0].Content[0].(TextPart).ProviderMetadata["p"].(map[string]any)
	if gotMeta["sig"] != "s1" {
		t.Fatalf("request metadata aliased legacy params: %#v", gotMeta)
	}
}

func TestGenerateParamsFromRequest(t *testing.T) {
	topP := 0.5
	req := Request{
		Model:    "m-1",
		Messages: []Message{UserMessage("hi")},
		Tools: []ToolDefinition{{
			Name:         "lookup",
			Description:  "Lookup",
			Parameters:   json.RawMessage(`{"type":"object","properties":{"id":{"type":"string"}}}`),
			CacheControl: &CacheControl{Type: "ephemeral"},
		}},
		ToolChoice: ToolChoice{Mode: ToolChoiceTool, Tool: "lookup"},
		TopP:       &topP,
	}
	model := &Model{ID: "m-1"}
	params, err := GenerateParamsFromRequest(model, req)
	if err != nil {
		t.Fatal(err)
	}
	if params.Model != model || params.TopP == nil || *params.TopP != topP {
		t.Fatalf("params = %+v", params)
	}
	if len(params.Tools) != 1 || params.Tools[0].Execute != nil || params.Tools[0].RequireApproval {
		t.Fatalf("tool should contain definition only: %+v", params.Tools)
	}
	if _, ok := params.Tools[0].Parameters.(*jsonschema.Schema); !ok {
		t.Fatalf("tool parameters type = %T", params.Tools[0].Parameters)
	}
	choice, ok := params.ToolChoice.(map[string]any)
	if !ok || choice["type"] != "function" {
		t.Fatalf("legacy tool choice = %#v", params.ToolChoice)
	}

	if _, err := GenerateParamsFromRequest(&Model{ID: "other"}, req); err == nil {
		t.Fatal("expected model mismatch error")
	}

	req.ProviderOptions = map[string]json.RawMessage{"openai": json.RawMessage(`{"reasoning":{"effort":"low"}}`)}
	if _, err := GenerateParamsFromRequest(model, req); err == nil {
		t.Fatal("expected providerOptions to reject legacy adapter fallback")
	}
}

func TestToolChoiceFromLegacy(t *testing.T) {
	for _, mode := range []string{"auto", "none", "required"} {
		choice, err := ToolChoiceFromLegacy(mode)
		if err != nil {
			t.Fatalf("%s: %v", mode, err)
		}
		if choice.Mode != ToolChoiceMode(mode) || choice.Legacy() != mode {
			t.Fatalf("choice %s round trip = %+v / %#v", mode, choice, choice.Legacy())
		}
	}
	choice, err := ToolChoiceFromLegacy(map[string]any{"type": "function", "function": map[string]any{"name": "search"}})
	if err != nil {
		t.Fatal(err)
	}
	if choice.Mode != ToolChoiceTool || choice.Tool != "search" {
		t.Fatalf("tool choice = %+v", choice)
	}
	if _, err := ToolChoiceFromLegacy("bad"); err == nil {
		t.Fatal("expected unsupported string tool choice to fail")
	}
}

func TestModelResultAdapters(t *testing.T) {
	response := ResponseMetadata{ID: "resp-1", Headers: map[string]string{"h": "v"}}
	gen := &GenerateResult{
		Text:                 "ok",
		Reasoning:            "why",
		ReasoningParts:       []ReasoningPart{{ID: "r1", Text: "why", ProviderMetadata: map[string]any{"p": "v"}}},
		TextProviderMetadata: map[string]any{"t": "sig"},
		FinishReason:         FinishReasonStop,
		Usage:                Usage{TotalTokens: 3},
		Sources:              []Source{{ID: "src", URL: "https://example.test", ProviderMetadata: map[string]any{"s": "m"}}},
		ToolCalls:            []ToolCall{{ToolCallID: "c1", ToolName: "search", Input: map[string]any{"q": "go"}}},
		Response:             response,
		ToolResults:          []ToolResult{{ToolCallID: "c1"}},
		Steps:                []StepResult{{Text: "step"}},
		Messages:             []Message{AssistantMessage("step")},
	}

	model := ModelResultFromGenerateResult(gen)
	if model.Text != "ok" || model.Response == nil || model.Response.Headers["h"] != "v" {
		t.Fatalf("model result = %+v", model)
	}
	if len(model.ToolCalls) != 1 || len(model.Sources) != 1 || len(model.ReasoningParts) != 1 {
		t.Fatalf("missing single-call fields: %+v", model)
	}

	// Multi-step and tool execution fields intentionally do not round-trip
	// through ModelResult.
	back := GenerateResultFromModelResult(model)
	if len(back.ToolResults) != 0 || len(back.Steps) != 0 || len(back.Messages) != 0 {
		t.Fatalf("unexpected orchestration fields: %+v", back)
	}
	model.TextProviderMetadata["t"] = "mutated"
	if back.TextProviderMetadata["t"] != "sig" {
		t.Fatal("GenerateResult aliased ModelResult metadata")
	}
}
