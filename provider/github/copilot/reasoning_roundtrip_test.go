package copilot

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	sdk "github.com/memohai/twilight-ai/sdk"
)

// Copilot carries the upstream model's signature in reasoning_opaque and
// accepts reasoning_text only alongside it. Replaying text without the token
// would feed the model its own inner monologue as ordinary content, which
// teaches it to imitate the form in user-visible answers.

func TestParseResponseCapturesReasoningOpaque(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id": "c1", "model": "claude-sonnet-4.5", "created": 1,
			"choices": []map[string]any{{
				"index": 0,
				"message": map[string]any{
					"role":             "assistant",
					"content":          "answer",
					"reasoning_text":   "thought",
					"reasoning_opaque": "OPAQUE_TOKEN",
				},
				"finish_reason": "stop",
			}},
		})
	}))
	defer srv.Close()

	p := New(WithAPIKey("k"), WithBaseURL(srv.URL))
	result, err := p.DoGenerate(context.Background(), sdk.GenerateParams{
		Model:    &sdk.Model{ID: "claude-sonnet-4.5"},
		Messages: []sdk.Message{sdk.UserMessage("hi")},
	})
	if err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}

	if len(result.ReasoningParts) != 1 {
		t.Fatalf("ReasoningParts: got %d, want 1", len(result.ReasoningParts))
	}
	part := result.ReasoningParts[0]
	if part.Format != sdk.ReasoningFormatCopilot {
		t.Errorf("format: got %q, want %q", part.Format, sdk.ReasoningFormatCopilot)
	}
	if part.Text != "thought" {
		t.Errorf("text: got %q, want %q", part.Text, "thought")
	}
	if got := reasoningOpaqueOf(part.ProviderMetadata); got != "OPAQUE_TOKEN" {
		t.Errorf("opaque: got %q, want OPAQUE_TOKEN", got)
	}
}

func TestConvertAssistantMessageReplaysReasoningOpaque(t *testing.T) {
	msg := sdk.Message{
		Role: sdk.MessageRoleAssistant,
		Content: []sdk.MessagePart{
			sdk.ReasoningPart{
				Text:             "thought",
				Format:           sdk.ReasoningFormatCopilot,
				ProviderMetadata: reasoningOpaqueMetadata("OPAQUE_TOKEN"),
			},
			sdk.TextPart{Text: "answer"},
		},
	}

	cm := convertAssistantMessage(msg)
	if cm.ReasoningOpaque != "OPAQUE_TOKEN" {
		t.Errorf("reasoning_opaque: got %q, want OPAQUE_TOKEN", cm.ReasoningOpaque)
	}
	if cm.ReasoningText != "thought" {
		t.Errorf("reasoning_text: got %q, want %q", cm.ReasoningText, "thought")
	}

	raw, err := json.Marshal(cm)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var wire map[string]any
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// reasoning_content belongs to other vendors; sending it here is a no-op at
	// best and misleading at worst.
	if _, present := wire["reasoning_content"]; present {
		t.Errorf("wire carries reasoning_content: %s", raw)
	}
}

// Without the token the text cannot be verified, so neither field is sent.
func TestConvertAssistantMessageOmitsReasoningTextWithoutOpaque(t *testing.T) {
	msg := sdk.Message{
		Role: sdk.MessageRoleAssistant,
		Content: []sdk.MessagePart{
			sdk.ReasoningPart{Text: "thought", Format: sdk.ReasoningFormatCopilot},
			sdk.TextPart{Text: "answer"},
		},
	}

	cm := convertAssistantMessage(msg)
	if cm.ReasoningText != "" || cm.ReasoningOpaque != "" {
		t.Errorf("reasoning replayed without a token: text=%q opaque=%q", cm.ReasoningText, cm.ReasoningOpaque)
	}
}

func TestConvertAssistantMessageDropsForeignReasoning(t *testing.T) {
	msg := sdk.Message{
		Role: sdk.MessageRoleAssistant,
		Content: []sdk.MessagePart{
			sdk.ReasoningPart{
				Text:             "anthropic thinking",
				Format:           sdk.ReasoningFormatAnthropic,
				ProviderMetadata: map[string]any{"anthropic": map[string]any{"signature": "SIG"}},
			},
			sdk.TextPart{Text: "answer"},
		},
	}

	cm := convertAssistantMessage(msg)
	if cm.ReasoningText != "" || cm.ReasoningOpaque != "" {
		t.Errorf("foreign reasoning replayed: text=%q opaque=%q", cm.ReasoningText, cm.ReasoningOpaque)
	}
}
