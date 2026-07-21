package messages_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/memohai/twilight-ai/provider/anthropic/messages"
	"github.com/memohai/twilight-ai/sdk"
)

type anthropicRoleBody struct {
	System []struct {
		Text string `json:"text"`
	} `json:"system"`
	Messages []struct {
		Role    string `json:"role"`
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
	} `json:"messages"`
}

func TestUnsupportedInstructionRolesFallbackToUser(t *testing.T) {
	srv := anthropicRoleServer(t, func(body anthropicRoleBody) {
		if len(body.System) != 2 || body.System[0].Text != "root policy" || body.System[1].Text != "leading system" {
			t.Fatalf("system blocks: %+v", body.System)
		}
		wantRoles := []string{"user", "assistant", "user"}
		if len(body.Messages) != len(wantRoles) {
			t.Fatalf("message count: got %d, want %d", len(body.Messages), len(wantRoles))
		}
		for i, want := range wantRoles {
			if body.Messages[i].Role != want {
				t.Fatalf("message %d role: got %q, want %q", i, body.Messages[i].Role, want)
			}
		}
		if len(body.Messages[0].Content) != 2 {
			t.Fatalf("first user content count: got %d", len(body.Messages[0].Content))
		}
		wantSystem := "<system>\nruntime &lt;changed&gt; &amp; verified\n</system>"
		if body.Messages[0].Content[1].Text != wantSystem {
			t.Fatalf("mid-system fallback:\n got: %q\nwant: %q", body.Messages[0].Content[1].Text, wantSystem)
		}
		if body.Messages[2].Content[0].Text != "developer policy" {
			t.Fatalf("developer fallback content: got %q", body.Messages[2].Content[0].Text)
		}
	})
	defer srv.Close()

	p := messages.New(messages.WithAPIKey("k"), messages.WithBaseURL(srv.URL))
	_, err := p.DoGenerate(context.Background(), sdk.GenerateParams{
		Model:  p.ChatModel("claude-test"),
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

func TestMidConversationSystemCanBeEnabledForSupportedModels(t *testing.T) {
	srv := anthropicRoleServer(t, func(body anthropicRoleBody) {
		wantRoles := []string{"user", "system", "assistant"}
		if len(body.Messages) != len(wantRoles) {
			t.Fatalf("message count: got %d, want %d", len(body.Messages), len(wantRoles))
		}
		for i, want := range wantRoles {
			if body.Messages[i].Role != want {
				t.Fatalf("message %d role: got %q, want %q", i, body.Messages[i].Role, want)
			}
		}
		if body.Messages[1].Content[0].Text != "runtime changed" {
			t.Fatalf("native system content: got %q", body.Messages[1].Content[0].Text)
		}
	})
	defer srv.Close()

	p := messages.New(
		messages.WithAPIKey("k"),
		messages.WithBaseURL(srv.URL),
		messages.WithMidConversationSystemMessages(true),
	)
	_, err := p.DoGenerate(context.Background(), sdk.GenerateParams{
		Model: p.ChatModel("claude-supported"),
		Messages: []sdk.Message{
			sdk.UserMessage("question"),
			sdk.SystemMessage("runtime changed"),
			sdk.AssistantMessage("answer"),
		},
	})
	if err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}
}

func anthropicRoleServer(t *testing.T, assert func(anthropicRoleBody)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body anthropicRoleBody
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		assert(body)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": "msg-roles", "type": "message", "model": "claude-test", "role": "assistant",
			"content":     []map[string]any{{"type": "text", "text": "ok"}},
			"stop_reason": "end_turn",
			"usage":       map[string]any{"input_tokens": 1, "output_tokens": 1},
		})
	}))
}
