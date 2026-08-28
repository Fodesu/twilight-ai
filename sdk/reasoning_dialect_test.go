package sdk_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	anthropicmessages "github.com/memohai/twilight/provider/anthropic/messages"
	"github.com/memohai/twilight/provider/github/copilot"
	googlegenerative "github.com/memohai/twilight/provider/google/generativeai"
	"github.com/memohai/twilight/provider/openai/completions"
	"github.com/memohai/twilight/provider/openai/responses"
	sdk "github.com/memohai/twilight/sdk"
)

// A reasoning part with no dialect is never replayed, so a provider that
// forgets to stamp Format silently stops sending reasoning back — it compiles,
// and every other test still passes. This walks each provider's parse path with
// a canned response and asserts the dialect actually arrives.
func TestEveryProviderStampsItsReasoningDialect(t *testing.T) {
	cases := []struct {
		name     string
		response string
		want     sdk.ReasoningFormat
		generate func(t *testing.T, baseURL string) (*sdk.GenerateResult, error)
	}{
		{
			name: "anthropic",
			response: `{"id":"msg_1","type":"message","model":"claude-opus-5","role":"assistant",
				"content":[{"type":"thinking","thinking":"t","signature":"SIG"},{"type":"text","text":"a"}],
				"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}`,
			want: sdk.ReasoningFormatAnthropic,
			generate: func(t *testing.T, baseURL string) (*sdk.GenerateResult, error) {
				p := anthropicmessages.New(anthropicmessages.WithAPIKey("k"), anthropicmessages.WithBaseURL(baseURL))
				return p.DoGenerate(context.Background(), sdk.GenerateParams{
					Model:    &sdk.Model{ID: "claude-opus-5"},
					Messages: []sdk.Message{sdk.UserMessage("hi")},
				})
			},
		},
		{
			name: "openai-responses",
			response: `{"id":"resp_1","status":"completed","model":"gpt-5.6","output":[
				{"type":"reasoning","id":"rs_1","summary":[{"type":"summary_text","text":"t"}],"encrypted_content":"EC"}],
				"usage":{"input_tokens":1,"output_tokens":1}}`,
			want: sdk.ReasoningFormatOpenAIResponses,
			generate: func(t *testing.T, baseURL string) (*sdk.GenerateResult, error) {
				p := responses.New(responses.WithAPIKey("k"), responses.WithBaseURL(baseURL))
				return p.DoGenerate(context.Background(), sdk.GenerateParams{
					Model:    p.ChatModel("gpt-5.6"),
					Messages: []sdk.Message{sdk.UserMessage("hi")},
				})
			},
		},
		{
			name: "google",
			response: `{"candidates":[{"content":{"parts":[
				{"text":"t","thought":true,"thoughtSignature":"SIG"},{"text":"a"}]},"finishReason":"STOP"}],
				"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1},"modelVersion":"gemini-3-pro"}`,
			want: sdk.ReasoningFormatGoogle,
			generate: func(t *testing.T, baseURL string) (*sdk.GenerateResult, error) {
				p := googlegenerative.New(googlegenerative.WithAPIKey("k"), googlegenerative.WithBaseURL(baseURL))
				return p.DoGenerate(context.Background(), sdk.GenerateParams{
					Model:    &sdk.Model{ID: "gemini-3-pro"},
					Messages: []sdk.Message{sdk.UserMessage("hi")},
				})
			},
		},
		{
			name: "copilot",
			response: `{"id":"c1","model":"m","created":1,"choices":[{"index":0,
				"message":{"role":"assistant","content":"a","reasoning_text":"t","reasoning_opaque":"OP"},
				"finish_reason":"stop"}]}`,
			want: sdk.ReasoningFormatCopilot,
			generate: func(t *testing.T, baseURL string) (*sdk.GenerateResult, error) {
				p := copilot.New(copilot.WithAPIKey("k"), copilot.WithBaseURL(baseURL))
				return p.DoGenerate(context.Background(), sdk.GenerateParams{
					Model:    &sdk.Model{ID: "m"},
					Messages: []sdk.Message{sdk.UserMessage("hi")},
				})
			},
		},
		{
			name: "openai-chat",
			response: `{"id":"c1","model":"deepseek-reasoner","created":1,"choices":[{"index":0,
				"message":{"role":"assistant","content":"a","reasoning_content":"t"},
				"finish_reason":"stop"}]}`,
			want: sdk.ReasoningFormatOpenAIChat,
			generate: func(t *testing.T, baseURL string) (*sdk.GenerateResult, error) {
				p := completions.New(completions.WithAPIKey("k"), completions.WithBaseURL(baseURL))
				return p.DoGenerate(context.Background(), sdk.GenerateParams{
					Model:    &sdk.Model{ID: "deepseek-reasoner"},
					Messages: []sdk.Message{sdk.UserMessage("hi")},
				})
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, tc.response)
			}))
			defer srv.Close()

			result, err := tc.generate(t, srv.URL)
			if err != nil {
				t.Fatalf("DoGenerate: %v", err)
			}
			if len(result.ReasoningParts) == 0 {
				t.Fatalf("no reasoning parts produced")
			}
			for i, part := range result.ReasoningParts {
				if part.Format != tc.want {
					t.Errorf("part %d format: got %q, want %q", i, part.Format, tc.want)
				}
				// The producing model must be stamped too: replay guards
				// compare it against the target model, and a missing stamp
				// silently disables that check.
				if part.Model == "" {
					t.Errorf("part %d has no producing model", i)
				}
			}
		})
	}
}

// Every dialect constant must be distinct, or a replay check would accept a
// foreign block.
func TestReasoningFormatsAreDistinct(t *testing.T) {
	formats := []sdk.ReasoningFormat{
		sdk.ReasoningFormatAnthropic,
		sdk.ReasoningFormatOpenAIResponses,
		sdk.ReasoningFormatGoogle,
		sdk.ReasoningFormatCopilot,
		sdk.ReasoningFormatOpenAIChat,
	}
	seen := make(map[sdk.ReasoningFormat]bool, len(formats))
	for _, f := range formats {
		if f == sdk.ReasoningFormatUnknown {
			t.Errorf("a named dialect must not equal the unknown dialect")
		}
		if seen[f] {
			t.Errorf("duplicate dialect value %q", f)
		}
		seen[f] = true
	}
}

// The dialect survives persistence: a part stored as JSON and read back keeps
// its Format, or every replayed conversation would look unmarked.
func TestReasoningFormatSurvivesJSONRoundTrip(t *testing.T) {
	original := sdk.Message{
		Role: sdk.MessageRoleAssistant,
		Content: []sdk.MessagePart{
			sdk.ReasoningPart{
				ID:               "rs_1",
				Text:             "t",
				Format:           sdk.ReasoningFormatOpenAIResponses,
				ProviderMetadata: map[string]any{"openai": map[string]any{"itemId": "rs_1"}},
			},
		},
	}

	raw, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var restored sdk.Message
	if err := json.Unmarshal(raw, &restored); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	part, ok := restored.Content[0].(sdk.ReasoningPart)
	if !ok {
		t.Fatalf("content[0] = %T, want ReasoningPart", restored.Content[0])
	}
	if part.Format != sdk.ReasoningFormatOpenAIResponses {
		t.Errorf("format: got %q, want %q", part.Format, sdk.ReasoningFormatOpenAIResponses)
	}
	if part.ID != "rs_1" {
		t.Errorf("id: got %q, want rs_1", part.ID)
	}
}
