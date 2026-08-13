package responses_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/memohai/twilight-ai/provider/openai/responses"
	sdk "github.com/memohai/twilight-ai/sdk"
)

// A reasoning item may carry several summary entries. Each becomes its own
// part so nothing is flattened, but all of them share the item's id — so
// replaying must regroup them into one item, not several.

func captureInput(t *testing.T, messages []sdk.Message) []map[string]any {
	t.Helper()
	var captured struct {
		Input []json.RawMessage `json:"input"`
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"id":"resp_1","status":"completed","output":[],"usage":{"input_tokens":1,"output_tokens":1}}`)
	}))
	defer srv.Close()

	p := responses.New(responses.WithAPIKey("k"), responses.WithBaseURL(srv.URL))
	if _, err := p.DoGenerate(context.Background(), sdk.GenerateParams{
		Model:    p.ChatModel("gpt-5.6"),
		Messages: messages,
	}); err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}

	items := make([]map[string]any, 0, len(captured.Input))
	for _, raw := range captured.Input {
		var item map[string]any
		if err := json.Unmarshal(raw, &item); err != nil {
			t.Fatalf("unmarshal item: %v", err)
		}
		items = append(items, item)
	}
	return items
}

func TestReplayRegroupsPartsSharingAnItemID(t *testing.T) {
	meta := map[string]any{"openai": map[string]any{
		"itemId":                    "rs_1",
		"reasoningEncryptedContent": "EC",
	}}
	items := captureInput(t, []sdk.Message{{
		Role: sdk.MessageRoleAssistant,
		Content: []sdk.MessagePart{
			sdk.ReasoningPart{ID: "rs_1", Text: "first", Format: sdk.ReasoningFormatOpenAIResponses, ProviderMetadata: meta},
			sdk.ReasoningPart{ID: "rs_1", Text: "second", Format: sdk.ReasoningFormatOpenAIResponses, ProviderMetadata: meta},
			sdk.TextPart{Text: "answer"},
		},
	}})

	var reasoning []map[string]any
	for _, item := range items {
		if item["type"] == "reasoning" {
			reasoning = append(reasoning, item)
		}
	}
	if len(reasoning) != 1 {
		t.Fatalf("reasoning items: got %d, want 1 — parts sharing an id must merge: %+v", len(reasoning), items)
	}
	if reasoning[0]["id"] != "rs_1" {
		t.Errorf("item id: got %v, want rs_1 (the API requires it)", reasoning[0]["id"])
	}
	summary, _ := reasoning[0]["summary"].([]any)
	if len(summary) != 2 {
		t.Fatalf("summary entries: got %d, want 2: %+v", len(summary), reasoning[0])
	}
	if reasoning[0]["encrypted_content"] != "EC" {
		t.Errorf("encrypted_content: got %v, want EC", reasoning[0]["encrypted_content"])
	}
}

// Distinct items stay distinct, in their original order.
func TestReplayKeepsDistinctItemsSeparate(t *testing.T) {
	items := captureInput(t, []sdk.Message{{
		Role: sdk.MessageRoleAssistant,
		Content: []sdk.MessagePart{
			sdk.ReasoningPart{Text: "a", Format: sdk.ReasoningFormatOpenAIResponses,
				ProviderMetadata: map[string]any{"openai": map[string]any{"itemId": "rs_1"}}},
			sdk.ReasoningPart{Text: "b", Format: sdk.ReasoningFormatOpenAIResponses,
				ProviderMetadata: map[string]any{"openai": map[string]any{"itemId": "rs_2"}}},
			sdk.TextPart{Text: "answer"},
		},
	}})

	var ids []any
	for _, item := range items {
		if item["type"] == "reasoning" {
			ids = append(ids, item["id"])
		}
	}
	if len(ids) != 2 || ids[0] != "rs_1" || ids[1] != "rs_2" {
		t.Fatalf("reasoning item ids: got %v, want [rs_1 rs_2]", ids)
	}
}

// An item whose payload is only encrypted content still has to be replayed:
// dropping it loses the reasoning chain. summary is schema-required, so the key
// must be present even when empty.
func TestReplayKeepsEncryptedOnlyItem(t *testing.T) {
	items := captureInput(t, []sdk.Message{{
		Role: sdk.MessageRoleAssistant,
		Content: []sdk.MessagePart{
			sdk.ReasoningPart{Format: sdk.ReasoningFormatOpenAIResponses,
				ProviderMetadata: map[string]any{"openai": map[string]any{
					"itemId":                    "rs_opaque",
					"reasoningEncryptedContent": "ENCRYPTED_ONLY",
				}}},
		},
	}})

	if len(items) != 1 || items[0]["type"] != "reasoning" {
		t.Fatalf("items: got %+v, want a single reasoning item", items)
	}
	if items[0]["encrypted_content"] != "ENCRYPTED_ONLY" {
		t.Errorf("encrypted_content: got %v", items[0]["encrypted_content"])
	}
	if _, present := items[0]["summary"]; !present {
		t.Errorf("summary key absent, but the schema requires it: %+v", items[0])
	}
}

func TestReplayDropsForeignReasoning(t *testing.T) {
	items := captureInput(t, []sdk.Message{{
		Role: sdk.MessageRoleAssistant,
		Content: []sdk.MessagePart{
			sdk.ReasoningPart{Text: "anthropic thinking", Format: sdk.ReasoningFormatAnthropic,
				ProviderMetadata: map[string]any{"anthropic": map[string]any{"signature": "SIG"}}},
			sdk.TextPart{Text: "answer"},
		},
	}})

	for _, item := range items {
		if item["type"] == "reasoning" {
			t.Fatalf("foreign reasoning replayed: %+v", item)
		}
	}
}

// Encrypted reasoning is returned only when asked for, and only a stateless
// request keeps the SDK's own message list authoritative.
func TestRequestAsksForEncryptedReasoning(t *testing.T) {
	var captured map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"id":"resp_1","status":"completed","output":[],"usage":{"input_tokens":1,"output_tokens":1}}`)
	}))
	defer srv.Close()

	p := responses.New(responses.WithAPIKey("k"), responses.WithBaseURL(srv.URL))
	if _, err := p.DoGenerate(context.Background(), sdk.GenerateParams{
		Model:    p.ChatModel("gpt-5.6"),
		Messages: []sdk.Message{sdk.UserMessage("hi")},
	}); err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}

	include, _ := captured["include"].([]any)
	if len(include) != 1 || include[0] != "reasoning.encrypted_content" {
		t.Errorf("include: got %v, want [reasoning.encrypted_content]", captured["include"])
	}
	if store, ok := captured["store"].(bool); !ok || store {
		t.Errorf("store: got %v, want false", captured["store"])
	}
}
