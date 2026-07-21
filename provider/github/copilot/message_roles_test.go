package copilot_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/memohai/twilight-ai/provider/github/copilot"
	"github.com/memohai/twilight-ai/sdk"
)

type copilotWireMessage struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
}

func TestInstructionRolesFallbackConservatively(t *testing.T) {
	srv := copilotMessageServer(t, func(messages []copilotWireMessage) {
		wantRoles := []string{"system", "system", "user", "user", "user"}
		assertCopilotRoles(t, messages, wantRoles)
		wantSystem := "<system>\nruntime &lt;changed&gt; &amp; verified\n</system>"
		if text := copilotRawString(t, messages[3].Content); text != wantSystem {
			t.Fatalf("mid-system fallback:\n got: %q\nwant: %q", text, wantSystem)
		}
		if text := copilotRawString(t, messages[4].Content); text != "developer policy" {
			t.Fatalf("developer fallback content: got %q", text)
		}
	})
	defer srv.Close()

	p := copilot.New(copilot.WithGitHubToken("token"), copilot.WithBaseURL(srv.URL))
	_, err := p.DoGenerate(context.Background(), sdk.GenerateParams{
		Model:  p.ChatModel("copilot-model"),
		System: "root policy",
		Messages: []sdk.Message{
			sdk.SystemMessage("leading system"),
			sdk.UserMessage("question"),
			sdk.SystemMessage("runtime <changed> & verified"),
			sdk.DeveloperMessage("developer policy"),
		},
	})
	if err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}
}

func TestInstructionRolesCanUseConfiguredNativeSupport(t *testing.T) {
	srv := copilotMessageServer(t, func(messages []copilotWireMessage) {
		assertCopilotRoles(t, messages, []string{"user", "system", "developer"})
	})
	defer srv.Close()

	p := copilot.New(
		copilot.WithGitHubToken("token"),
		copilot.WithBaseURL(srv.URL),
		copilot.WithMessageRoleCapabilities(sdk.MessageRoleCapabilities{
			Developer:             true,
			MidConversationSystem: true,
		}),
	)
	_, err := p.DoGenerate(context.Background(), sdk.GenerateParams{
		Model: p.ChatModel("copilot-model"),
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

func copilotMessageServer(t *testing.T, assert func([]copilotWireMessage)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Messages []copilotWireMessage `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		assert(body.Messages)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": "copilot-roles", "created": 1700000000, "model": "copilot-model",
			"choices": []map[string]any{{
				"index": 0, "finish_reason": "stop",
				"message": map[string]any{"role": "assistant", "content": "ok"},
			}},
			"usage": map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
		})
	}))
}

func assertCopilotRoles(t *testing.T, messages []copilotWireMessage, want []string) {
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

func copilotRawString(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatalf("decode string content %s: %v", raw, err)
	}
	return value
}
