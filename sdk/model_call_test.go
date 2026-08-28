package sdk

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
)

type boundaryProvider struct {
	generate func(GenerateParams) (*GenerateResult, error)
	stream   func(GenerateParams) (*StreamResult, error)
}

func (p boundaryProvider) Name() string                                { return "boundary" }
func (p boundaryProvider) ListModels(context.Context) ([]Model, error) { return nil, nil }
func (p boundaryProvider) Test(context.Context) *ProviderTestResult {
	return &ProviderTestResult{Status: ProviderStatusOK}
}
func (p boundaryProvider) TestModel(context.Context, string) (*ModelTestResult, error) {
	return &ModelTestResult{Supported: true}, nil
}
func (p boundaryProvider) DoGenerate(_ context.Context, params GenerateParams) (*GenerateResult, error) {
	return p.generate(params)
}
func (p boundaryProvider) DoStream(_ context.Context, params GenerateParams) (*StreamResult, error) {
	return p.stream(params)
}

func TestModelGenerateUsesRequestBoundary(t *testing.T) {
	var captured GenerateParams
	provider := boundaryProvider{generate: func(params GenerateParams) (*GenerateResult, error) {
		captured = params
		return &GenerateResult{
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
	if captured.Model != model || len(captured.Messages) != 1 {
		t.Fatalf("captured params = %+v", captured)
	}
	if len(captured.Tools) != 1 || captured.Tools[0].Execute != nil || captured.Tools[0].Name != "lookup" {
		t.Fatalf("captured tools = %+v", captured.Tools)
	}
	choice, ok := captured.ToolChoice.(map[string]any)
	if !ok || choice["type"] != "function" {
		t.Fatalf("captured tool choice = %#v", captured.ToolChoice)
	}
	if result.Text != "ok" || result.Usage.TotalTokens != 7 || len(result.ToolCalls) != 1 {
		t.Fatalf("model result = %+v", result)
	}

	if _, err := model.Generate(context.Background(), Request{Model: "other"}); err == nil {
		t.Fatal("expected model mismatch error")
	}
}

type nativeBoundaryProvider struct {
	legacyGenerateCalled bool
	legacyStreamCalled   bool
	nativeGenerateReq    Request
	nativeStreamReq      Request
}

func (p *nativeBoundaryProvider) Name() string                                { return "native-boundary" }
func (p *nativeBoundaryProvider) ListModels(context.Context) ([]Model, error) { return nil, nil }
func (p *nativeBoundaryProvider) Test(context.Context) *ProviderTestResult {
	return &ProviderTestResult{Status: ProviderStatusOK}
}
func (p *nativeBoundaryProvider) TestModel(context.Context, string) (*ModelTestResult, error) {
	return &ModelTestResult{Supported: true}, nil
}
func (p *nativeBoundaryProvider) DoGenerate(context.Context, GenerateParams) (*GenerateResult, error) {
	p.legacyGenerateCalled = true
	return &GenerateResult{}, nil
}
func (p *nativeBoundaryProvider) DoStream(context.Context, GenerateParams) (*StreamResult, error) {
	p.legacyStreamCalled = true
	ch := make(chan StreamPart)
	close(ch)
	return &StreamResult{Stream: ch}, nil
}
func (p *nativeBoundaryProvider) Generate(_ context.Context, req Request) (ModelResult, error) {
	p.nativeGenerateReq = req
	return ModelResult{Text: "native", FinishReason: FinishReasonStop}, nil
}
func (p *nativeBoundaryProvider) Stream(_ context.Context, req Request) (ModelStream, error) {
	p.nativeStreamReq = req
	ch := make(chan StreamPart)
	close(ch)
	return ModelStream{Parts: ch, Result: func() (*ModelResult, error) {
		return &ModelResult{Text: "native-stream", FinishReason: FinishReasonStop}, nil
	}}, nil
}

func TestModelUsesNativeModelInvokerWhenAvailable(t *testing.T) {
	provider := &nativeBoundaryProvider{}
	model := &Model{ID: "m-1", Provider: provider}
	generated, err := model.Generate(context.Background(), Request{})
	if err != nil {
		t.Fatal(err)
	}
	if generated.Text != "native" || provider.nativeGenerateReq.Model != "m-1" || provider.legacyGenerateCalled {
		t.Fatalf("native generate not used: result=%+v provider=%+v", generated, provider)
	}
	stream, err := model.Stream(context.Background(), Request{})
	if err != nil {
		t.Fatal(err)
	}
	for range stream.Parts {
	}
	streamed, err := stream.Result()
	if err != nil {
		t.Fatal(err)
	}
	if streamed.Text != "native-stream" || provider.nativeStreamReq.Model != "m-1" || provider.legacyStreamCalled {
		t.Fatalf("native stream not used: result=%+v provider=%+v", streamed, provider)
	}
}

func TestModelGenerateAndStreamEquivalent(t *testing.T) {
	generateResult := &GenerateResult{
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
		Response:             ResponseMetadata{ID: "resp-1"},
	}
	provider := boundaryProvider{
		generate: func(GenerateParams) (*GenerateResult, error) { return generateResult, nil },
		stream: func(GenerateParams) (*StreamResult, error) {
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
			return &StreamResult{Stream: ch}, nil
		},
	}
	model := &Model{ID: "m-1", Provider: provider}
	generated, err := model.Generate(context.Background(), Request{Model: "m-1"})
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
	if !reflect.DeepEqual(generated, *streamed) {
		t.Fatalf("Generate and Stream diverged:\n generate=%#v\n stream=%#v", generated, *streamed)
	}
}

func TestModelStreamAssemblesSingleModelResult(t *testing.T) {
	provider := boundaryProvider{stream: func(params GenerateParams) (*StreamResult, error) {
		if params.Model == nil || params.Model.ID != "m-1" {
			t.Fatalf("params model = %+v", params.Model)
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
		return &StreamResult{Stream: ch}, nil
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
