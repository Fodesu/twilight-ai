package sdk

import (
	"context"
	"testing"
)

// The provider wire protocols treat each reasoning block as an opaque,
// indivisible round-trip unit: Anthropic rejects a modified sequence of
// thinking blocks with a 400, and OpenAI rejects reordered reasoning items.
// The stream carries per-block identity (ReasoningStartPart.ID), so the
// accumulator must preserve blocks rather than flattening them into one
// string with one metadata bag.

func reasoningMeta(sig string) ProviderMetadata {
	return ProviderMetadata{"anthropic": {"signature": sig}}
}

func anthropicSignature(t *testing.T, meta ProviderMetadata) string {
	t.Helper()
	return meta.Get("anthropic", "signature")
}

// Two thinking blocks, each with its own signature, as interleaved thinking
// produces. Both signatures must survive; pairing each with its own text is
// what makes the replay verifiable.
func TestToResultPreservesEveryReasoningBlockSignature(t *testing.T) {
	ch := make(chan StreamPart, 16)
	for _, p := range []StreamPart{
		&ReasoningStartPart{ID: "b1", Format: ReasoningFormatAnthropic},
		&ReasoningDeltaPart{ID: "b1", Text: "AAA"},
		&ReasoningEndPart{ID: "b1", ProviderMetadata: reasoningMeta("SIG_A")},
		&ReasoningStartPart{ID: "b2", Format: ReasoningFormatAnthropic},
		&ReasoningDeltaPart{ID: "b2", Text: "BBB"},
		&ReasoningEndPart{ID: "b2", ProviderMetadata: reasoningMeta("SIG_B")},
		&FinishPart{FinishReason: FinishReasonStop},
	} {
		ch <- p
	}
	close(ch)

	result, err := CollectStream(context.Background(), ch)
	if err != nil {
		t.Fatalf("CollectStream: %v", err)
	}

	if len(result.ReasoningParts) != 2 {
		t.Fatalf("ReasoningParts: got %d, want 2 (%+v)", len(result.ReasoningParts), result.ReasoningParts)
	}
	for i, want := range []struct{ text, sig string }{{"AAA", "SIG_A"}, {"BBB", "SIG_B"}} {
		block := result.ReasoningParts[i]
		if block.Text != want.text {
			t.Errorf("block %d text: got %q, want %q", i, block.Text, want.text)
		}
		if got := anthropicSignature(t, block.ProviderMetadata); got != want.sig {
			t.Errorf("block %d signature: got %q, want %q", i, got, want.sig)
		}
	}
	// The flat view stays available for display, derived from the blocks.
	if result.Reasoning != "AAABBB" {
		t.Errorf("Reasoning flat view: got %q, want %q", result.Reasoning, "AAABBB")
	}
}

// A redacted thinking block carries no readable text, only an encrypted blob.
// Anthropic requires it replayed verbatim, so an empty-text block is
// meaningful and must not be treated as absent. rig puts it well: emptiness
// is a property of the list, not of its members.
func TestToResultKeepsEmptyTextReasoningBlock(t *testing.T) {
	ch := make(chan StreamPart, 8)
	for _, p := range []StreamPart{
		&ReasoningStartPart{ID: "r1", Format: ReasoningFormatAnthropic},
		&ReasoningEndPart{ID: "r1", ProviderMetadata: ProviderMetadata{"anthropic": {"redactedData": "ENCRYPTED_BLOB"}}},
		&FinishPart{FinishReason: FinishReasonStop},
	} {
		ch <- p
	}
	close(ch)

	result, err := CollectStream(context.Background(), ch)
	if err != nil {
		t.Fatalf("CollectStream: %v", err)
	}

	if len(result.ReasoningParts) != 1 {
		t.Fatalf("ReasoningParts: got %d, want 1 — empty-text block was dropped", len(result.ReasoningParts))
	}
	if data := result.ReasoningParts[0].ProviderMetadata.Get("anthropic", "redactedData"); data != "ENCRYPTED_BLOB" {
		t.Errorf("redactedData: got %q, want %q", data, "ENCRYPTED_BLOB")
	}
}

// Signatures arrive as one atomic blob at block end, never as increments.
// Text deltas concatenate; metadata replaces.
func TestToResultReplacesRatherThanConcatenatesBlockMetadata(t *testing.T) {
	ch := make(chan StreamPart, 8)
	for _, p := range []StreamPart{
		&ReasoningStartPart{ID: "b1", Format: ReasoningFormatAnthropic, ProviderMetadata: reasoningMeta("PARTIAL")},
		&ReasoningDeltaPart{ID: "b1", Text: "AA"},
		&ReasoningDeltaPart{ID: "b1", Text: "BB"},
		&ReasoningEndPart{ID: "b1", ProviderMetadata: reasoningMeta("FINAL")},
		&FinishPart{FinishReason: FinishReasonStop},
	} {
		ch <- p
	}
	close(ch)

	result, err := CollectStream(context.Background(), ch)
	if err != nil {
		t.Fatalf("CollectStream: %v", err)
	}

	if len(result.ReasoningParts) != 1 {
		t.Fatalf("ReasoningParts: got %d, want 1", len(result.ReasoningParts))
	}
	block := result.ReasoningParts[0]
	if block.Text != "AABB" {
		t.Errorf("text: got %q, want %q (deltas concatenate)", block.Text, "AABB")
	}
	if got := anthropicSignature(t, block.ProviderMetadata); got != "FINAL" {
		t.Errorf("signature: got %q, want %q (metadata replaces)", got, "FINAL")
	}
}
