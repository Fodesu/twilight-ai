package sdk

import "testing"

func TestAddUsageAccumulatesCacheWriteTTLDetails(t *testing.T) {
	total := Usage{
		InputTokenDetails: InputTokenDetail{
			CacheWriteTokens:   100,
			CacheWrite5mTokens: 70,
			CacheWrite1hTokens: 30,
		},
	}
	step := Usage{
		InputTokenDetails: InputTokenDetail{
			CacheWriteTokens:   200,
			CacheWrite5mTokens: 120,
			CacheWrite1hTokens: 80,
		},
	}

	got := total.Add(step)

	if got.InputTokenDetails.CacheWriteTokens != 300 {
		t.Fatalf("CacheWriteTokens = %d, want 300", got.InputTokenDetails.CacheWriteTokens)
	}
	if got.InputTokenDetails.CacheWrite5mTokens != 190 {
		t.Fatalf("CacheWrite5mTokens = %d, want 190", got.InputTokenDetails.CacheWrite5mTokens)
	}
	if got.InputTokenDetails.CacheWrite1hTokens != 110 {
		t.Fatalf("CacheWrite1hTokens = %d, want 110", got.InputTokenDetails.CacheWrite1hTokens)
	}
}

func TestBuildStepMessagesPreservesToolCallProviderMetadata(t *testing.T) {
	meta := ProviderMetadata{"google": {"thoughtSignature": "sig-1"}}
	msgs := BuildStepMessages("", nil, nil, []ToolCall{{
		ToolCallID:       "call-1",
		ToolName:         "lookup",
		Input:            ParseToolArguments(`{"q":"memoh"}`),
		ProviderMetadata: meta,
	}}, nil, nil)

	if len(msgs) != 1 || len(msgs[0].Content) != 1 {
		t.Fatalf("unexpected messages: %#v", msgs)
	}
	part, ok := msgs[0].Content[0].(ToolCallPart)
	if !ok {
		t.Fatalf("content part = %T, want ToolCallPart", msgs[0].Content[0])
	}
	if got := part.ProviderMetadata.Get("google", "thoughtSignature"); got != "sig-1" {
		t.Fatalf("thoughtSignature = %q, want sig-1", got)
	}
}
