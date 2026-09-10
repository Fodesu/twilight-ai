package sdk

import (
	"testing"
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

	req, err := requestFromGenerateParams(params)
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
		t.Fatalf("request metadata aliased the caller's params: %#v", gotMeta)
	}
}

func TestToolChoiceFromValue(t *testing.T) {
	for _, mode := range []string{"auto", "none", "required"} {
		choice, err := toolChoiceFromValue(mode)
		if err != nil {
			t.Fatalf("%s: %v", mode, err)
		}
		if choice.Mode != ToolChoiceMode(mode) {
			t.Fatalf("choice %s = %+v", mode, choice)
		}
	}
	choice, err := toolChoiceFromValue(map[string]any{"type": "function", "function": map[string]any{"name": "search"}})
	if err != nil {
		t.Fatal(err)
	}
	if choice.Mode != ToolChoiceTool || choice.Tool != "search" {
		t.Fatalf("tool choice = %+v", choice)
	}
	if _, err := toolChoiceFromValue("bad"); err == nil {
		t.Fatal("expected unsupported string tool choice to fail")
	}
}

func TestGenerateResultFromModelResult(t *testing.T) {
	response := &ResponseMetadata{ID: "resp-1", Headers: map[string]string{"h": "v"}}
	model := ModelResult{
		Text:                 "ok",
		Reasoning:            "why",
		ReasoningParts:       []ReasoningPart{{ID: "r1", Text: "why", ProviderMetadata: map[string]any{"p": "v"}}},
		TextProviderMetadata: map[string]any{"t": "sig"},
		FinishReason:         FinishReasonStop,
		Usage:                Usage{TotalTokens: 3},
		Sources:              []Source{{ID: "src", URL: "https://example.test", ProviderMetadata: map[string]any{"s": "m"}}},
		ToolCalls:            []ToolCall{{ToolCallID: "c1", ToolName: "search", Input: map[string]any{"q": "go"}}},
		Response:             response,
	}

	// The up-conversion is the client layer's one-way bridge: it may add the
	// orchestration fields a ModelResult never carries, but it must not alias
	// the boundary result it was given.
	gen := GenerateResultFromModelResult(model)
	if gen.Text != "ok" || gen.Response.Headers["h"] != "v" {
		t.Fatalf("generate result = %+v", gen)
	}
	if len(gen.ToolCalls) != 1 || len(gen.Sources) != 1 || len(gen.ReasoningParts) != 1 {
		t.Fatalf("missing single-call fields: %+v", gen)
	}
	if len(gen.ToolResults) != 0 || len(gen.Steps) != 0 || len(gen.Messages) != 0 {
		t.Fatalf("unexpected orchestration fields: %+v", gen)
	}
	model.TextProviderMetadata["t"] = "mutated"
	if gen.TextProviderMetadata["t"] != "sig" {
		t.Fatal("GenerateResult aliased ModelResult metadata")
	}
}
