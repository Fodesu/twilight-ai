package generativeai

import (
	"testing"

	sdk "github.com/memohai/twilight-ai/sdk"
)

// Gemini binds a thought signature to the exact part that carries it — not
// only to thought parts. A response without a function call signs an ordinary
// text part (usually the last one), and Gemini 3 rejects a later request that
// fails to return it. See ai.google.dev/gemini-api/docs/generate-content/thought-signatures.

func TestParseResponseKeepsSignatureOnPlainTextPart(t *testing.T) {
	thought := true
	resp := &generateResponse{
		Candidates: []candidate{{
			Content: &content{Parts: []contentPart{
				{Text: "thinking...", Thought: &thought, ThoughtSignature: "SIG_THOUGHT"},
				{Text: "the answer", ThoughtSignature: "SIG_TEXT"},
			}},
			FinishReason: "STOP",
		}},
	}

	result, err := (&Provider{}).parseResponse(resp)
	if err != nil {
		t.Fatalf("parseResponse: %v", err)
	}

	if got := extractGoogleThoughtSignature(result.TextProviderMetadata); got != "SIG_TEXT" {
		t.Errorf("text signature: got %q, want SIG_TEXT — a signature on a plain part was dropped", got)
	}
	if len(result.ReasoningParts) != 1 {
		t.Fatalf("reasoning parts: got %d, want 1", len(result.ReasoningParts))
	}
	if got := extractGoogleThoughtSignature(result.ReasoningParts[0].ProviderMetadata); got != "SIG_THOUGHT" {
		t.Errorf("thought signature: got %q, want SIG_THOUGHT", got)
	}
}

// The signed text part must come back out on the wire with its signature in
// place. TextPart already carries ProviderMetadata on replay; this closes the
// loop from parse to request.
func TestAssistantTextSignatureRoundTrips(t *testing.T) {
	msg := sdk.Message{
		Role: sdk.MessageRoleAssistant,
		Content: []sdk.MessagePart{
			sdk.TextPart{
				Text:             "the answer",
				ProviderMetadata: googleThoughtSignatureMetadata("SIG_TEXT"),
			},
		},
	}

	converted := convertAssistantMessage(msg)
	if len(converted.Parts) != 1 {
		t.Fatalf("parts: got %d, want 1", len(converted.Parts))
	}
	if converted.Parts[0].ThoughtSignature != "SIG_TEXT" {
		t.Errorf("replayed signature: got %q, want SIG_TEXT", converted.Parts[0].ThoughtSignature)
	}
	if converted.Parts[0].Thought != nil && *converted.Parts[0].Thought {
		t.Error("a signed text part must stay an ordinary part, not become a thought")
	}
}
