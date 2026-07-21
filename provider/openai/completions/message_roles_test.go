package completions_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/memohai/twilight-ai/provider/openai/completions"
	"github.com/memohai/twilight-ai/sdk"
)

type completionsWireMessage struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
}

func TestMessageRolesUseOpenAINativeRolesByDefault(t *testing.T) {
	srv := completionsMessageServer(t, func(messages []completionsWireMessage) {
		wantRoles := []string{"system", "user", "system", "developer"}
		assertCompletionsRoles(t, messages, wantRoles)
		if text := rawString(t, messages[2].Content); text != "runtime changed" {
			t.Fatalf("mid-system content: got %q", text)
		}
	})
	defer srv.Close()

	p := completions.New(completions.WithAPIKey("k"), completions.WithBaseURL(srv.URL))
	_, err := p.DoGenerate(context.Background(), sdk.GenerateParams{
		Model:  p.ChatModel("gpt-5"),
		System: "root policy",
		Messages: []sdk.Message{
			sdk.UserMessage("question"),
			sdk.SystemMessage("runtime changed"),
			sdk.DeveloperMessage("developer policy"),
		},
	})
	if err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}
}

func TestMessageRolesCanFallbackForCompatibleEndpoints(t *testing.T) {
	srv := completionsMessageServer(t, func(messages []completionsWireMessage) {
		wantRoles := []string{"system", "user", "user", "user"}
		assertCompletionsRoles(t, messages, wantRoles)
		wantSystem := "<system>\nruntime &lt;changed&gt; &amp; verified\n</system>"
		if text := rawString(t, messages[2].Content); text != wantSystem {
			t.Fatalf("mid-system fallback:\n got: %q\nwant: %q", text, wantSystem)
		}
		if text := rawString(t, messages[3].Content); text != "developer policy" {
			t.Fatalf("developer fallback content: got %q", text)
		}
	})
	defer srv.Close()

	p := completions.New(
		completions.WithAPIKey("k"),
		completions.WithBaseURL(srv.URL),
		completions.WithMessageRoleCapabilities(sdk.MessageRoleCapabilities{}),
	)
	_, err := p.DoGenerate(context.Background(), sdk.GenerateParams{
		Model:  p.ChatModel("compatible-model"),
		System: "root policy",
		Messages: []sdk.Message{
			sdk.UserMessage("question"),
			sdk.SystemMessage("runtime <changed> & verified"),
			sdk.DeveloperMessage("developer policy"),
		},
	})
	if err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}
}

func completionsMessageServer(t *testing.T, assert func([]completionsWireMessage)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Messages []completionsWireMessage `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		assert(body.Messages)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": "chatcmpl-roles", "created": 1700000000, "model": "test",
			"choices": []map[string]any{{
				"index": 0, "finish_reason": "stop",
				"message": map[string]any{"role": "assistant", "content": "ok"},
			}},
			"usage": map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
		})
	}))
}

func assertCompletionsRoles(t *testing.T, messages []completionsWireMessage, want []string) {
	t.Helper()
	if len(messages) != len(want) {
		t.Fatalf("message count: got %d, want %d", len(messages), len(want))
	}
	for i, role := range want {
		if messages[i].Role != role {
			t.Fatalf("message %d role: got %q, want %q", i, messages[i].Role, role)
		}
	}
}

func rawString(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatalf("decode string content %s: %v", raw, err)
	}
	return value
}
