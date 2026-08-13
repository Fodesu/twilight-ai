package sdk

import "testing"

// The provider wire protocols treat each reasoning block as an opaque,
// indivisible round-trip unit: Anthropic rejects a modified sequence of
// thinking blocks with a 400, and OpenAI rejects reordered reasoning items.
// The stream carries per-block identity (ReasoningStartPart.ID), so the
// accumulator must preserve blocks rather than flattening them into one
// string with one metadata bag.

func reasoningMeta(sig string) map[string]any {
	return map[string]any{"anthropic": map[string]any{"signature": sig}}
}

func anthropicSignature(t *testing.T, meta map[string]any) string {
	t.Helper()
	am, ok := meta["anthropic"].(map[string]any)
	if !ok {
		return ""
	}
	sig, _ := am["signature"].(string)
	return sig
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

	result, err := (&StreamResult{Stream: ch}).ToResult()
	if err != nil {
		t.Fatalf("ToResult: %v", err)
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
		&ReasoningEndPart{ID: "r1", ProviderMetadata: map[string]any{
			"anthropic": map[string]any{"redactedData": "ENCRYPTED_BLOB"},
		}},
		&FinishPart{FinishReason: FinishReasonStop},
	} {
		ch <- p
	}
	close(ch)

	result, err := (&StreamResult{Stream: ch}).ToResult()
	if err != nil {
		t.Fatalf("ToResult: %v", err)
	}

	if len(result.ReasoningParts) != 1 {
		t.Fatalf("ReasoningParts: got %d, want 1 — empty-text block was dropped", len(result.ReasoningParts))
	}
	am, _ := result.ReasoningParts[0].ProviderMetadata["anthropic"].(map[string]any)
	if data, _ := am["redactedData"].(string); data != "ENCRYPTED_BLOB" {
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

	result, err := (&StreamResult{Stream: ch}).ToResult()
	if err != nil {
		t.Fatalf("ToResult: %v", err)
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

// Every reasoning block must reach the assistant message that gets replayed,
// each keeping its own opaque token.
func TestBuildStepMessagesEmitsOnePartPerReasoningBlock(t *testing.T) {
	blocks := []ReasoningPart{
		{Text: "AAA", ProviderMetadata: reasoningMeta("SIG_A")},
		{Text: "", ProviderMetadata: map[string]any{
			"anthropic": map[string]any{"redactedData": "BLOB"},
		}},
		{Text: "BBB", ProviderMetadata: reasoningMeta("SIG_B")},
	}

	msgs := buildStepMessages("answer", blocks, nil, nil, nil)
	if len(msgs) == 0 {
		t.Fatal("no messages produced")
	}

	var got []ReasoningPart
	for _, part := range msgs[0].Content {
		if rp, ok := part.(ReasoningPart); ok {
			got = append(got, rp)
		}
	}
	if len(got) != 3 {
		t.Fatalf("reasoning parts: got %d, want 3 (empty-text block must survive)", len(got))
	}
	if s := anthropicSignature(t, got[0].ProviderMetadata); s != "SIG_A" {
		t.Errorf("part 0 signature: got %q, want SIG_A", s)
	}
	if s := anthropicSignature(t, got[2].ProviderMetadata); s != "SIG_B" {
		t.Errorf("part 2 signature: got %q, want SIG_B", s)
	}
	// Reasoning must precede the answer text: Anthropic enforces thinking-first,
	// and OpenAI 400s on an orphaned trailing reasoning item.
	if _, ok := msgs[0].Content[0].(ReasoningPart); !ok {
		t.Errorf("content[0] = %T, want ReasoningPart to lead the message", msgs[0].Content[0])
	}
}
