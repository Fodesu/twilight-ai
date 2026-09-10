package sdk

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
)

type boundaryProvider struct {
	generate func(Request) (ModelResult, error)
	stream   func(Request) (<-chan StreamPart, error)
}

func (p boundaryProvider) Name() string                                { return "boundary" }
func (p boundaryProvider) ListModels(context.Context) ([]Model, error) { return nil, nil }
func (p boundaryProvider) Test(context.Context) *ProviderTestResult {
	return &ProviderTestResult{Status: ProviderStatusOK}
}
func (p boundaryProvider) TestModel(context.Context, string) (*ModelTestResult, error) {
	return &ModelTestResult{Supported: true}, nil
}
func (p boundaryProvider) DoGenerate(_ context.Context, req Request) (ModelResult, error) {
	return p.generate(req)
}
func (p boundaryProvider) DoStream(_ context.Context, req Request) (<-chan StreamPart, error) {
	return p.stream(req)
}

func TestModelGenerateUsesRequestBoundary(t *testing.T) {
	var captured Request
	provider := boundaryProvider{generate: func(req Request) (ModelResult, error) {
		captured = req
		return ModelResult{
			Text:         "ok",
			FinishReason: FinishReasonStop,
			Usage:        Usage{TotalTokens: 7},
			ToolCalls: []ToolCall{{
				ToolCallID: "c1",
				ToolName:   "lookup",
				Input:      map[string]any{"q": "go"},
			}},
		}, nil
	}}
	model := &Model{ID: "m-1", Provider: provider}
	result, err := model.Generate(context.Background(), Request{
		Model:    "m-1",
		Messages: []Message{UserMessage("hi")},
		Tools: []ToolDefinition{{
			Name:       "lookup",
			Parameters: json.RawMessage(`{"type":"object"}`),
		}},
		ToolChoice: ToolChoice{Mode: ToolChoiceTool, Tool: "lookup"},
	})
	if err != nil {
		t.Fatal(err)
	}
	// The provider sees the boundary type itself: there is no orchestration
	// state for it to see, and no way to attach a tool Execute closure to what
	// it receives.
	if captured.Model != "m-1" || len(captured.Messages) != 1 {
		t.Fatalf("captured request = %+v", captured)
	}
	if len(captured.Tools) != 1 || captured.Tools[0].Name != "lookup" || len(captured.Tools[0].Parameters) == 0 {
		t.Fatalf("captured tools = %+v", captured.Tools)
	}
	if captured.ToolChoice.Mode != ToolChoiceTool || captured.ToolChoice.Tool != "lookup" {
		t.Fatalf("captured tool choice = %+v", captured.ToolChoice)
	}
	if result.Text != "ok" || result.Usage.TotalTokens != 7 || len(result.ToolCalls) != 1 {
		t.Fatalf("model result = %+v", result)
	}

	if _, err := model.Generate(context.Background(), Request{Model: "other"}); err == nil {
		t.Fatal("expected model mismatch error")
	}
}

func TestModelGenerateAndStreamEquivalent(t *testing.T) {
	generated := ModelResult{
		Text:                 "hello",
		Reasoning:            "why",
		ReasoningParts:       []ReasoningPart{{ID: "r1", Text: "why", Format: ReasoningFormatOpenAIResponses, Model: "m-1", ProviderMetadata: map[string]any{"openai": map[string]any{"itemId": "rs_1"}}}},
		TextProviderMetadata: map[string]any{"google": map[string]any{"thoughtSignature": "txt-sig"}},
		FinishReason:         FinishReasonToolCalls,
		RawFinishReason:      "tool_calls",
		Usage:                Usage{TotalTokens: 5},
		Sources:              []Source{{SourceType: "url", ID: "src-1", URL: "https://example.test", ProviderMetadata: map[string]any{"p": "v"}}},
		Files:                []GeneratedFile{{Data: "abc", MediaType: "text/plain"}},
		ToolCalls:            []ToolCall{{ToolCallID: "c1", ToolName: "lookup", Input: map[string]any{"q": "go"}, ProviderMetadata: map[string]any{"tool": "meta"}}},
		Response:             &ResponseMetadata{ID: "resp-1"},
	}
	provider := boundaryProvider{
		generate: func(Request) (ModelResult, error) { return generated, nil },
		stream: func(Request) (<-chan StreamPart, error) {
			ch := make(chan StreamPart, 16)
			go func() {
				defer close(ch)
				ch <- &ReasoningStartPart{ID: "r1", Format: ReasoningFormatOpenAIResponses, Model: "m-1"}
				ch <- &ReasoningDeltaPart{ID: "r1", Text: "why"}
				ch <- &ReasoningEndPart{ID: "r1", ProviderMetadata: map[string]any{"openai": map[string]any{"itemId": "rs_1"}}}
				ch <- &TextDeltaPart{ID: "txt", Text: "hello"}
				ch <- &TextEndPart{ID: "txt", ProviderMetadata: map[string]any{"google": map[string]any{"thoughtSignature": "txt-sig"}}}
				ch <- &StreamSourcePart{Source: Source{SourceType: "url", ID: "src-1", URL: "https://example.test", ProviderMetadata: map[string]any{"p": "v"}}}
				ch <- &StreamFilePart{File: GeneratedFile{Data: "abc", MediaType: "text/plain"}}
				ch <- &StreamToolCallPart{ToolCallID: "c1", ToolName: "lookup", Input: map[string]any{"q": "go"}, ProviderMetadata: map[string]any{"tool": "meta"}}
				ch <- &FinishStepPart{FinishReason: FinishReasonToolCalls, RawFinishReason: "tool_calls", Usage: Usage{TotalTokens: 5}, Response: ResponseMetadata{ID: "resp-1"}}
				ch <- &FinishPart{FinishReason: FinishReasonToolCalls, RawFinishReason: "tool_calls", TotalUsage: Usage{TotalTokens: 5}}
			}()
			return ch, nil
		},
	}
	model := &Model{ID: "m-1", Provider: provider}
	generatedResult, err := model.Generate(context.Background(), Request{Model: "m-1"})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := model.Stream(context.Background(), Request{Model: "m-1"})
	if err != nil {
		t.Fatal(err)
	}
	for range stream.Parts {
	}
	streamed, err := stream.Result()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(generatedResult, *streamed) {
		t.Fatalf("Generate and Stream diverged:\n generate=%#v\n stream=%#v", generatedResult, *streamed)
	}
}

func TestModelStreamAssemblesSingleModelResult(t *testing.T) {
	provider := boundaryProvider{stream: func(req Request) (<-chan StreamPart, error) {
		if req.Model != "m-1" {
			t.Fatalf("request model = %q", req.Model)
		}
		ch := make(chan StreamPart, 8)
		go func() {
			defer close(ch)
			ch <- &StartPart{}
			ch <- &StartStepPart{}
			ch <- &ReasoningStartPart{ID: "r1", Format: ReasoningFormatOpenAIResponses, Model: "m-1"}
			ch <- &ReasoningDeltaPart{ID: "r1", Text: "why"}
			ch <- &ReasoningEndPart{ID: "r1", ProviderMetadata: map[string]any{"openai": map[string]any{"itemId": "rs_1"}}}
			ch <- &TextDeltaPart{ID: "txt", Text: "hello"}
			ch <- &StreamToolCallPart{ToolCallID: "c1", ToolName: "lookup", Input: map[string]any{"q": "go"}}
			ch <- &FinishStepPart{FinishReason: FinishReasonToolCalls, Usage: Usage{TotalTokens: 5}, Response: ResponseMetadata{ID: "resp-1"}}
			ch <- &FinishPart{FinishReason: FinishReasonToolCalls, TotalUsage: Usage{TotalTokens: 5}}
		}()
		return ch, nil
	}}
	model := &Model{ID: "m-1", Provider: provider}
	stream, err := model.Stream(context.Background(), Request{Model: "m-1"})
	if err != nil {
		t.Fatal(err)
	}
	var parts int
	for range stream.Parts {
		parts++
	}
	if parts == 0 {
		t.Fatal("no stream parts forwarded")
	}
	result, err := stream.Result()
	if err != nil {
		t.Fatal(err)
	}
	if result.Text != "hello" || result.FinishReason != FinishReasonToolCalls || result.Usage.TotalTokens != 5 {
		t.Fatalf("result = %+v", result)
	}
	if result.Reasoning != "why" || len(result.ReasoningParts) != 1 {
		t.Fatalf("reasoning = %q / %+v", result.Reasoning, result.ReasoningParts)
	}
	if len(result.ToolCalls) != 1 || result.ToolCalls[0].ToolName != "lookup" {
		t.Fatalf("tool calls = %+v", result.ToolCalls)
	}
	if result.Response == nil || result.Response.ID != "resp-1" {
		t.Fatalf("response = %+v", result.Response)
	}
}

// TestModelStreamStopsForwardingWhenCancelled covers the failure the old
// adapter had: it sent every part on an unbuffered path with no ctx escape, so
// a consumer that walked away mid-stream left the assembling goroutine blocked
// on a send that nobody would ever receive, forever.
func TestModelStreamStopsForwardingWhenCancelled(t *testing.T) {
	release := make(chan struct{})
	provider := boundaryProvider{stream: func(Request) (<-chan StreamPart, error) {
		ch := make(chan StreamPart)
		go func() {
			defer close(ch)
			// Keep producing until the test releases us, the way a provider
			// blocked on a slow upstream would.
			for {
				select {
				case ch <- &TextDeltaPart{ID: "txt", Text: "tick"}:
				case <-release:
					return
				}
			}
		}()
		return ch, nil
	}}
	model := &Model{ID: "m-1", Provider: provider}
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := model.Stream(ctx, Request{Model: "m-1"})
	if err != nil {
		t.Fatal(err)
	}
	// Take one part and abandon the stream.
	<-stream.Parts
	cancel()
	if _, err := stream.Result(); err == nil {
		t.Fatal("Result returned a partial result as success after cancellation")
	}
	close(release)
}

// TestModelStreamErrorsOnProviderErrorPart keeps a mid-stream provider failure
// from being reported as a successful, partial result.
func TestModelStreamErrorsOnProviderErrorPart(t *testing.T) {
	provider := boundaryProvider{stream: func(Request) (<-chan StreamPart, error) {
		ch := make(chan StreamPart, 4)
		ch <- &TextDeltaPart{ID: "txt", Text: "partial"}
		ch <- &ErrorPart{Error: context.DeadlineExceeded}
		close(ch)
		return ch, nil
	}}
	model := &Model{ID: "m-1", Provider: provider}
	stream, err := model.Stream(context.Background(), Request{Model: "m-1"})
	if err != nil {
		t.Fatal(err)
	}
	for range stream.Parts {
	}
	if _, err := stream.Result(); err == nil {
		t.Fatal("a provider ErrorPart did not surface as a Result error")
	}
}
