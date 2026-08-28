package codex_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/memohai/twilight/provider/openai/codex"
	"github.com/memohai/twilight/sdk"
)

func TestUnsupportedInstructionRolesFallbackWithoutHoisting(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Instructions string            `json:"instructions"`
			Input        []json.RawMessage `json:"input"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if body.Instructions != "root policy\n\nleading system" {
			t.Fatalf("instructions: got %q", body.Instructions)
		}
		wantRoles := []string{"user", "user", "assistant", "user"}
		if len(body.Input) != len(wantRoles) {
			t.Fatalf("input count: got %d, want %d", len(body.Input), len(wantRoles))
		}
		for i, want := range wantRoles {
			var item struct {
				Role    string `json:"role"`
				Content []struct {
					Text string `json:"text"`
				} `json:"content"`
			}
			if err := json.Unmarshal(body.Input[i], &item); err != nil {
				t.Fatalf("decode input %d: %v", i, err)
			}
			if item.Role != want {
				t.Fatalf("input %d role: got %q, want %q", i, item.Role, want)
			}
			if i == 1 {
				wantSystem := "<system>\nruntime &lt;changed&gt; &amp; verified\n</system>"
				if item.Content[0].Text != wantSystem {
					t.Fatalf("mid-system fallback:\n got: %q\nwant: %q", item.Content[0].Text, wantSystem)
				}
			}
			if i == 3 && item.Content[0].Text != "developer policy" {
				t.Fatalf("developer fallback content: got %q", item.Content[0].Text)
			}
		}

		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: response.created\n"))
		_, _ = w.Write([]byte("data: {\"response\":{\"id\":\"resp-roles\",\"created_at\":1700000000,\"model\":\"gpt-5\"}}\n\n"))
		_, _ = w.Write([]byte("event: response.output_item.added\n"))
		_, _ = w.Write([]byte("data: {\"output_index\":0,\"item\":{\"type\":\"message\",\"id\":\"msg-roles\"}}\n\n"))
		_, _ = w.Write([]byte("event: response.output_text.delta\n"))
		_, _ = w.Write([]byte("data: {\"item_id\":\"msg-roles\",\"delta\":\"ok\"}\n\n"))
		_, _ = w.Write([]byte("event: response.output_item.done\n"))
		_, _ = w.Write([]byte("data: {\"output_index\":0,\"item\":{\"type\":\"message\",\"id\":\"msg-roles\"}}\n\n"))
		_, _ = w.Write([]byte("event: response.completed\n"))
		_, _ = w.Write([]byte("data: {\"response\":{\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n"))
	}))
	defer srv.Close()

	p := codex.New(codex.WithAccessToken("token"), codex.WithBaseURL(srv.URL))
	_, err := p.DoGenerate(context.Background(), sdk.GenerateParams{
		Model:  p.ChatModel("gpt-5"),
		System: "root policy",
		Messages: []sdk.Message{
			sdk.SystemMessage("leading system"),
			sdk.UserMessage("question"),
			sdk.SystemMessage("runtime <changed> & verified"),
			sdk.AssistantMessage("answer"),
			sdk.DeveloperMessage("developer policy"),
		},
	})
	if err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}
}
