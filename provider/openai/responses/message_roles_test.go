package responses_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/memohai/twilight/provider/openai/responses"
	"github.com/memohai/twilight/sdk"
)

func TestMessageRolesUseNativeResponsesInstructions(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Instructions string            `json:"instructions"`
			Input        []json.RawMessage `json:"input"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if body.Instructions != "root policy" {
			t.Fatalf("instructions: got %q", body.Instructions)
		}
		if len(body.Input) != 3 {
			t.Fatalf("input count: got %d, want 3", len(body.Input))
		}
		for i, want := range []string{"user", "system", "developer"} {
			var item struct {
				Role string `json:"role"`
			}
			if err := json.Unmarshal(body.Input[i], &item); err != nil {
				t.Fatalf("decode input %d: %v", i, err)
			}
			if item.Role != want {
				t.Fatalf("input %d role: got %q, want %q", i, item.Role, want)
			}
		}

		var developer struct {
			Content string `json:"content"`
		}
		if err := json.Unmarshal(body.Input[2], &developer); err != nil {
			t.Fatalf("decode developer message: %v", err)
		}
		if developer.Content != "developer policy" {
			t.Fatalf("developer content: got %q", developer.Content)
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": "resp-roles", "created_at": 1700000000, "model": "gpt-5",
			"output": []map[string]any{{
				"type": "message", "id": "msg-roles", "role": "assistant",
				"content": []map[string]any{{"type": "output_text", "text": "ok"}},
			}},
			"usage": map[string]any{"input_tokens": 1, "output_tokens": 1},
		})
	}))
	defer srv.Close()

	p := responses.New(responses.WithAPIKey("k"), responses.WithBaseURL(srv.URL))
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

func TestInstructionCannotInterruptToolExchange(t *testing.T) {
	p := responses.New(responses.WithAPIKey("k"))
	params := sdk.GenerateParams{
		Model: p.ChatModel("gpt-5"),
		Messages: []sdk.Message{
			{
				Role: sdk.MessageRoleAssistant,
				Content: []sdk.MessagePart{sdk.ToolCallPart{
					ToolCallID: "call-1",
					ToolName:   "lookup",
					Input:      map[string]any{"q": "value"},
				}},
			},
			sdk.DeveloperMessage("new policy"),
			sdk.ToolMessage(sdk.ToolResultPart{ToolCallID: "call-1", ToolName: "lookup", Result: "ok"}),
		},
	}

	_, err := p.DoGenerate(context.Background(), params)
	if !errors.Is(err, sdk.ErrInstructionInsideToolExchange) {
		t.Fatalf("DoGenerate error: got %v", err)
	}
	_, err = p.DoStream(context.Background(), params)
	if !errors.Is(err, sdk.ErrInstructionInsideToolExchange) {
		t.Fatalf("DoStream error: got %v", err)
	}
}
