package sdk_test

import (
	"context"
	"testing"

	"github.com/felinics/twilight/sdk"
	"github.com/google/jsonschema-go/jsonschema"
)

// TestClient_StreamText_BarePassthrough pins the fast path in StreamText: when
// MaxSteps == 0 and no OnStepCommitted barrier is set, the provider's
// StreamResult is returned as-is — no SDK loop, no step assembly, and the
// provider's own FinishPart arrives verbatim (not an SDK-synthesized one) —
// even when executable tools are configured.
func TestClient_StreamText_BarePassthrough(t *testing.T) {
	sentinelFinish := &sdk.FinishPart{
		FinishReason:    sdk.FinishReasonStop,
		RawFinishReason: "provider-raw-stop",
	}

	var providerResult *sdk.StreamResult
	mp := &mockProvider{streamHandler: func(call int, params sdk.GenerateParams) (*sdk.StreamResult, error) {
		if call != 1 {
			t.Fatalf("unexpected provider call %d", call)
		}
		ch := make(chan sdk.StreamPart, 8)
		ch <- &sdk.StartPart{}
		ch <- &sdk.TextDeltaPart{ID: "t", Text: "hi"}
		ch <- sentinelFinish
		close(ch)
		providerResult = &sdk.StreamResult{Stream: ch}
		return providerResult, nil
	}}

	executed := false
	sr, err := sdk.StreamText(context.Background(),
		sdk.WithModel(mockModel(mp)),
		sdk.WithMessages([]sdk.Message{sdk.UserMessage("hello")}),
		sdk.WithTools([]sdk.Tool{{
			Name:       "noop",
			Parameters: &jsonschema.Schema{Type: "object"},
			Execute: func(ctx *sdk.ToolExecContext, input any) (any, error) {
				executed = true
				return "done", nil
			},
		}}),
		// No WithMaxSteps, no WithOnStepCommitted: bare passthrough.
	)
	if err != nil {
		t.Fatalf("StreamText: %v", err)
	}
	if sr != providerResult {
		t.Fatal("StreamText must return the provider's StreamResult unchanged on the fast path")
	}

	var gotFinish *sdk.FinishPart
	for part := range sr.Stream {
		if fp, ok := part.(*sdk.FinishPart); ok {
			gotFinish = fp
		}
	}
	if gotFinish != sentinelFinish {
		t.Fatalf("FinishPart: got %#v, want the provider's own part verbatim", gotFinish)
	}
	if sr.Steps != nil {
		t.Fatalf("Steps must stay nil on the fast path, got %#v", sr.Steps)
	}
	if sr.Messages != nil {
		t.Fatalf("Messages must stay nil on the fast path, got %#v", sr.Messages)
	}
	if executed {
		t.Fatal("no tool must execute on the fast path")
	}
	if mp.calls != 1 {
		t.Fatalf("provider calls: got %d, want 1", mp.calls)
	}
}
