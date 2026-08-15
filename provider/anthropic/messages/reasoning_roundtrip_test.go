package messages

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	sdk "github.com/memohai/twilight-ai/sdk"
)

// Anthropic rejects a modified sequence of thinking blocks in the latest
// assistant message with a 400: the blocks cannot be rearranged, edited, or
// partially dropped, and that includes redacted_thinking. Each block must
// therefore survive parsing with its own signature attached.

func anthropicMeta(t *testing.T, meta map[string]any, key string) string {
	t.Helper()
	am, ok := meta["anthropic"].(map[string]any)
	if !ok {
		return ""
	}
	value, _ := am[key].(string)
	return value
}

func TestParseResponseKeepsEveryThinkingBlockSignature(t *testing.T) {
	resp := &messagesResponse{
		ID: "msg_1", Model: "claude-opus-5",
		Content: []responseBlock{
			{Type: "thinking", Thinking: "AAA", Signature: "SIG_A"},
			{Type: "thinking", Thinking: "BBB", Signature: "SIG_B"},
			{Type: "text", Text: "answer"},
		},
	}

	result, err := (&Provider{}).parseResponse(resp)
	if err != nil {
		t.Fatalf("parseResponse: %v", err)
	}
	if len(result.ReasoningParts) != 2 {
		t.Fatalf("ReasoningParts: got %d, want 2", len(result.ReasoningParts))
	}
	for i, want := range []struct{ text, sig string }{{"AAA", "SIG_A"}, {"BBB", "SIG_B"}} {
		block := result.ReasoningParts[i]
		if block.Format != sdk.ReasoningFormatAnthropic {
			t.Errorf("block %d format: got %q, want %q", i, block.Format, sdk.ReasoningFormatAnthropic)
		}
		if block.Text != want.text {
			t.Errorf("block %d text: got %q, want %q", i, block.Text, want.text)
		}
		if got := anthropicMeta(t, block.ProviderMetadata, "signature"); got != want.sig {
			t.Errorf("block %d signature: got %q, want %q", i, got, want.sig)
		}
	}
}

// A redacted_thinking block has no readable text — its whole payload is the
// encrypted data field. Dropping it is the documented top cause of the 400.
func TestParseResponseKeepsRedactedThinkingData(t *testing.T) {
	resp := &messagesResponse{
		ID: "msg_1", Model: "claude-opus-5",
		Content: []responseBlock{
			{Type: "thinking", Thinking: "AAA", Signature: "SIG_A"},
			{Type: "redacted_thinking", Data: "ENCRYPTED_BLOB"},
			{Type: "text", Text: "answer"},
		},
	}

	result, err := (&Provider{}).parseResponse(resp)
	if err != nil {
		t.Fatalf("parseResponse: %v", err)
	}
	if len(result.ReasoningParts) != 2 {
		t.Fatalf("ReasoningParts: got %d, want 2 — redacted block was dropped", len(result.ReasoningParts))
	}
	redacted := result.ReasoningParts[1]
	if redacted.Text != "" {
		t.Errorf("redacted block text: got %q, want empty", redacted.Text)
	}
	if got := anthropicMeta(t, redacted.ProviderMetadata, "redactedData"); got != "ENCRYPTED_BLOB" {
		t.Errorf("redactedData: got %q, want ENCRYPTED_BLOB", got)
	}
}

// Replaying the assistant turn must reproduce both block types on the wire,
// in their original order, each with its own opaque token.
func TestConvertAssistantMessageReplaysThinkingAndRedactedBlocks(t *testing.T) {
	msg := sdk.Message{
		Role: sdk.MessageRoleAssistant,
		Content: []sdk.MessagePart{
			sdk.ReasoningPart{
				Text:             "AAA",
				Format:           sdk.ReasoningFormatAnthropic,
				ProviderMetadata: map[string]any{"anthropic": map[string]any{"signature": "SIG_A"}},
			},
			sdk.ReasoningPart{
				Format:           sdk.ReasoningFormatAnthropic,
				ProviderMetadata: map[string]any{"anthropic": map[string]any{"redactedData": "ENCRYPTED_BLOB"}},
			},
			sdk.TextPart{Text: "answer"},
		},
	}

	converted := convertAssistantMessage(msg, "claude-opus-5")
	if len(converted.Content) != 3 {
		t.Fatalf("blocks: got %d, want 3: %+v", len(converted.Content), converted.Content)
	}

	if converted.Content[0].Type != blockTypeThinking {
		t.Errorf("block 0 type: got %q, want %q", converted.Content[0].Type, blockTypeThinking)
	}
	if converted.Content[0].Signature != "SIG_A" {
		t.Errorf("block 0 signature: got %q, want SIG_A", converted.Content[0].Signature)
	}
	if converted.Content[1].Type != blockTypeRedactedThinking {
		t.Errorf("block 1 type: got %q, want %q", converted.Content[1].Type, blockTypeRedactedThinking)
	}
	if converted.Content[1].Data != "ENCRYPTED_BLOB" {
		t.Errorf("block 1 data: got %q, want ENCRYPTED_BLOB", converted.Content[1].Data)
	}
	if converted.Content[2].Type != blockTypeText {
		t.Errorf("block 2 type: got %q, want %q", converted.Content[2].Type, blockTypeText)
	}

	// A redacted block must not leak an empty "thinking" key onto the wire.
	raw, err := json.Marshal(converted.Content[1])
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var wire map[string]any
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, present := wire["thinking"]; present {
		t.Errorf("redacted block carries a thinking field: %s", raw)
	}
	if _, present := wire["signature"]; present {
		t.Errorf("redacted block carries a signature field: %s", raw)
	}
}

// Reasoning that carries no Anthropic opaque token cannot be replayed as a
// thinking block. Re-rendering it as ordinary text teaches the model to
// imitate its own inner monologue in user-visible output, so the part is
// dropped instead.
func TestConvertAssistantMessageDropsForeignReasoning(t *testing.T) {
	msg := sdk.Message{
		Role: sdk.MessageRoleAssistant,
		Content: []sdk.MessagePart{
			sdk.ReasoningPart{
				Text:             "reasoning from another provider",
				Format:           sdk.ReasoningFormatOpenAIResponses,
				ProviderMetadata: map[string]any{"openai": map[string]any{"itemId": "rs_1"}},
			},
			sdk.TextPart{Text: "answer"},
		},
	}

	converted := convertAssistantMessage(msg, "claude-opus-5")
	if len(converted.Content) != 1 {
		t.Fatalf("blocks: got %d, want 1: %+v", len(converted.Content), converted.Content)
	}
	if converted.Content[0].Type != blockTypeText || converted.Content[0].Text != "answer" {
		t.Errorf("surviving block = %+v, want the answer text", converted.Content[0])
	}
}

// Streaming delivers the signature at block end, and the block boundaries are
// what pair each signature with its own text.
func TestStreamHandlerKeepsPerBlockSignatures(t *testing.T) {
	ch := make(chan sdk.StreamPart, 32)
	h := &streamHandler{
		ch:           ch,
		ctx:          context.Background(),
		activeBlocks: make(map[int]*streamingBlock),
	}

	idx0, idx1 := 0, 1
	h.onBlockStart(&streamEvent{Index: &idx0, ContentBlock: &responseBlock{Type: "thinking"}})
	h.onBlockDelta(&streamEvent{Index: &idx0, Delta: &streamDelta{Type: "thinking_delta", Thinking: "AAA"}})
	h.onBlockDelta(&streamEvent{Index: &idx0, Delta: &streamDelta{Type: "signature_delta", Signature: "SIG_A"}})
	h.onBlockStop(&streamEvent{Index: &idx0})
	h.onBlockStart(&streamEvent{Index: &idx1, ContentBlock: &responseBlock{Type: "thinking"}})
	h.onBlockDelta(&streamEvent{Index: &idx1, Delta: &streamDelta{Type: "thinking_delta", Thinking: "BBB"}})
	h.onBlockDelta(&streamEvent{Index: &idx1, Delta: &streamDelta{Type: "signature_delta", Signature: "SIG_B"}})
	h.onBlockStop(&streamEvent{Index: &idx1})

	close(ch)
	var ends []*sdk.ReasoningEndPart
	for p := range ch {
		if end, ok := p.(*sdk.ReasoningEndPart); ok {
			ends = append(ends, end)
		}
	}
	if len(ends) != 2 {
		t.Fatalf("ReasoningEndPart count: got %d, want 2", len(ends))
	}
	for i, want := range []string{"SIG_A", "SIG_B"} {
		if got := anthropicMeta(t, ends[i].ProviderMetadata, "signature"); got != want {
			t.Errorf("end %d signature: got %q, want %q", i, got, want)
		}
	}
	if ends[0].ID == ends[1].ID {
		t.Errorf("both blocks share stream ID %q; signatures cannot be paired with their text", ends[0].ID)
	}
}

// A redacted_thinking block arrives whole, with no deltas.
func TestStreamHandlerEmitsRedactedThinkingBlock(t *testing.T) {
	ch := make(chan sdk.StreamPart, 32)
	h := &streamHandler{
		ch:           ch,
		ctx:          context.Background(),
		activeBlocks: make(map[int]*streamingBlock),
	}

	idx := 0
	h.onBlockStart(&streamEvent{Index: &idx, ContentBlock: &responseBlock{
		Type: "redacted_thinking", Data: "ENCRYPTED_BLOB",
	}})
	h.onBlockStop(&streamEvent{Index: &idx})

	close(ch)
	for p := range ch {
		if end, ok := p.(*sdk.ReasoningEndPart); ok {
			if got := anthropicMeta(t, end.ProviderMetadata, "redactedData"); got != "ENCRYPTED_BLOB" {
				t.Fatalf("redactedData: got %q, want ENCRYPTED_BLOB", got)
			}
			return
		}
	}
	t.Fatal("no ReasoningEndPart emitted for redacted_thinking block")
}

// Reasoning persisted before dialects were recorded carries no Format. Its
// block structure is unknown — the text may be several blocks concatenated
// under one signature — so it cannot be replayed as a verifiable thinking
// block.
func TestConvertAssistantMessageDropsUnmarkedReasoning(t *testing.T) {
	msg := sdk.Message{
		Role: sdk.MessageRoleAssistant,
		Content: []sdk.MessagePart{
			sdk.ReasoningPart{
				Text:             "legacy reasoning",
				ProviderMetadata: map[string]any{"anthropic": map[string]any{"signature": "SIG_OLD"}},
			},
			sdk.TextPart{Text: "answer"},
		},
	}

	converted := convertAssistantMessage(msg, "claude-opus-5")
	if len(converted.Content) != 1 || converted.Content[0].Type != blockTypeText {
		t.Fatalf("blocks: got %+v, want only the text block", converted.Content)
	}
}

// The whole point of the design: a turn containing several thinking blocks and
// a redacted one must come back on the wire exactly as it arrived. Anthropic
// rejects a modified sequence with a 400.
func TestInterleavedThinkingSurvivesReplay(t *testing.T) {
	var replayed []map[string]any
	turn := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		turn++
		if turn == 2 {
			var body struct {
				Messages []struct {
					Role    string           `json:"role"`
					Content []map[string]any `json:"content"`
				} `json:"messages"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode request: %v", err)
			}
			for _, m := range body.Messages {
				if m.Role == "assistant" {
					replayed = m.Content
				}
			}
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{
			"id": "msg_1", "type": "message", "model": "claude-opus-5", "role": "assistant",
			"content": []map[string]any{
				{"type": "thinking", "thinking": "first", "signature": "SIG_1"},
				{"type": "redacted_thinking", "data": "BLOB"},
				{"type": "thinking", "thinking": "second", "signature": "SIG_2"},
				{"type": "text", "text": "done"},
			},
			"stop_reason": "end_turn",
			"usage":       map[string]any{"input_tokens": 5, "output_tokens": 5},
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer srv.Close()

	p := New(WithAPIKey("k"), WithBaseURL(srv.URL))
	model := &sdk.Model{ID: "claude-opus-5"}

	first, err := p.DoGenerate(context.Background(), sdk.GenerateParams{
		Model:    model,
		Messages: []sdk.Message{sdk.UserMessage("hi")},
	})
	if err != nil {
		t.Fatalf("first turn: %v", err)
	}
	if len(first.ReasoningParts) != 3 {
		t.Fatalf("ReasoningParts: got %d, want 3", len(first.ReasoningParts))
	}

	content := make([]sdk.MessagePart, 0, len(first.ReasoningParts)+1)
	for _, part := range first.ReasoningParts {
		content = append(content, part)
	}
	content = append(content, sdk.TextPart{Text: first.Text})

	if _, err := p.DoGenerate(context.Background(), sdk.GenerateParams{
		Model: model,
		Messages: []sdk.Message{
			sdk.UserMessage("hi"),
			{Role: sdk.MessageRoleAssistant, Content: content},
			sdk.UserMessage("more"),
		},
	}); err != nil {
		t.Fatalf("second turn: %v", err)
	}

	want := []struct{ typ, key, value string }{
		{"thinking", "signature", "SIG_1"},
		{"redacted_thinking", "data", "BLOB"},
		{"thinking", "signature", "SIG_2"},
		{"text", "text", "done"},
	}
	if len(replayed) != len(want) {
		t.Fatalf("replayed %d blocks, want %d: %+v", len(replayed), len(want), replayed)
	}
	for i, w := range want {
		if replayed[i]["type"] != w.typ {
			t.Errorf("block %d type: got %v, want %s", i, replayed[i]["type"], w.typ)
		}
		if replayed[i][w.key] != w.value {
			t.Errorf("block %d %s: got %v, want %s", i, w.key, replayed[i][w.key], w.value)
		}
	}
}

// The live failure this guards: a conversation starts on one Claude and
// switches to another. The old model's thinking blocks cannot pass the new
// model's signature check — Anthropic rejects them with
// "messages.N.content: Invalid input" — so they are dropped on replay.
func TestConvertAssistantMessageDropsBlocksFromAnotherModel(t *testing.T) {
	msg := sdk.Message{
		Role: sdk.MessageRoleAssistant,
		Content: []sdk.MessagePart{
			sdk.ReasoningPart{
				Text:             "signed by sonnet 5",
				Format:           sdk.ReasoningFormatAnthropic,
				Model:            "anthropic/claude-sonnet-5",
				ProviderMetadata: map[string]any{"anthropic": map[string]any{"signature": "SIG_S5"}},
			},
			sdk.TextPart{Text: "answer"},
		},
	}

	converted := convertAssistantMessage(msg, "anthropic/claude-sonnet-4.6")
	if len(converted.Content) != 1 || converted.Content[0].Type != blockTypeText {
		t.Fatalf("blocks: got %+v, want only the text block", converted.Content)
	}
}

// The same model under different spellings is still the same model: its
// signatures are portable across the direct API, Bedrock and gateways, and
// dropping them on a spelling change would lose valid reasoning.
func TestConvertAssistantMessageKeepsBlocksAcrossSpellings(t *testing.T) {
	msg := sdk.Message{
		Role: sdk.MessageRoleAssistant,
		Content: []sdk.MessagePart{
			sdk.ReasoningPart{
				Text:             "signed by sonnet 5",
				Format:           sdk.ReasoningFormatAnthropic,
				Model:            "anthropic/claude-sonnet-5",
				ProviderMetadata: map[string]any{"anthropic": map[string]any{"signature": "SIG_S5"}},
			},
			sdk.TextPart{Text: "answer"},
		},
	}

	converted := convertAssistantMessage(msg, "us.anthropic.claude-sonnet-5-v1:0")
	if len(converted.Content) != 2 || converted.Content[0].Type != blockTypeThinking {
		t.Fatalf("blocks: got %+v, want thinking then text", converted.Content)
	}
}

// Blocks persisted before Model existed carry no producer. They cannot
// establish a mismatch, so they replay as before — the field tightens the
// guard going forward without invalidating existing history.
func TestConvertAssistantMessageKeepsLegacyBlocksWithoutModel(t *testing.T) {
	msg := sdk.Message{
		Role: sdk.MessageRoleAssistant,
		Content: []sdk.MessagePart{
			sdk.ReasoningPart{
				Text:             "pre-field block",
				Format:           sdk.ReasoningFormatAnthropic,
				ProviderMetadata: map[string]any{"anthropic": map[string]any{"signature": "SIG"}},
			},
			sdk.TextPart{Text: "answer"},
		},
	}

	converted := convertAssistantMessage(msg, "anthropic/claude-sonnet-5")
	if len(converted.Content) != 2 || converted.Content[0].Type != blockTypeThinking {
		t.Fatalf("blocks: got %+v, want thinking then text", converted.Content)
	}
}

// A summarized thinking block signs empty text, and Anthropic rejects a
// thinking block whose thinking key is missing outright — "messages.N.content:
// Invalid input", reproduced live against Sonnet 5. The empty string has to
// reach the wire, which omitempty on a plain string silently prevents.
func TestConvertAssistantMessageKeepsEmptyThinkingKeyOnWire(t *testing.T) {
	msg := sdk.Message{
		Role: sdk.MessageRoleAssistant,
		Content: []sdk.MessagePart{
			sdk.ReasoningPart{
				Text:             "",
				Format:           sdk.ReasoningFormatAnthropic,
				Model:            "claude-sonnet-5",
				ProviderMetadata: map[string]any{"anthropic": map[string]any{"signature": "SIG"}},
			},
			sdk.TextPart{Text: "answer"},
		},
	}

	converted := convertAssistantMessage(msg, "claude-sonnet-5")
	raw, err := json.Marshal(converted.Content[0])
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var wire map[string]any
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	thinking, present := wire["thinking"]
	if !present {
		t.Fatalf("thinking key missing from the wire: %s", raw)
	}
	if thinking != "" {
		t.Errorf("thinking: got %v, want the empty string", thinking)
	}
	if wire["signature"] != "SIG" {
		t.Errorf("signature: got %v, want SIG", wire["signature"])
	}
}
