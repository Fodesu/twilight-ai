package completions_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/felinics/twilight/provider/openai/completions"
	"github.com/felinics/twilight/sdk"
)

func chatResponse(finish string, message map[string]any) map[string]any {
	return map[string]any{
		"id": "chatcmpl", "model": "deepseek-v4-flash",
		"choices": []map[string]any{{"index": 0, "finish_reason": finish, "message": message}},
		"usage":   map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
	}
}

func streamEventTypes(t *testing.T, chunks []string) []sdk.StreamPartType {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		for _, c := range chunks {
			fmt.Fprintf(w, "data: %s\n\n", c)
			flusher.Flush()
		}
		fmt.Fprintf(w, "data: [DONE]\n\n")
		flusher.Flush()
	}))
	t.Cleanup(srv.Close)

	p := completions.New(completions.WithAPIKey("k"), completions.WithBaseURL(srv.URL))
	sr, err := p.DoStream(context.Background(), sdk.GenerateParams{
		Model:    &sdk.Model{ID: "deepseek-v4-flash"},
		Messages: []sdk.Message{sdk.UserMessage("weather")},
	})
	if err != nil {
		t.Fatalf("DoStream: %v", err)
	}
	var events []sdk.StreamPartType
	for part := range sr.Stream {
		events = append(events, part.Type())
	}
	return events
}

func countEvents(events []sdk.StreamPartType, want sdk.StreamPartType) int {
	n := 0
	for _, ev := range events {
		if ev == want {
			n++
		}
	}
	return n
}

// The first delta of a DeepSeek thinking-mode stream carries
// reasoning_content as "" (null afterwards). That first "" opens a reasoning
// block; a later "" never reopens a closed one; null alone opens nothing.
func TestDoStream_ReasoningContentMarkers(t *testing.T) {
	cases := []struct {
		name       string
		chunks     []string
		wantStarts int
		wantDeltas int
	}{
		{
			name: "empty first delta opens one block",
			chunks: []string{
				`{"id":"c1","choices":[{"index":0,"delta":{"role":"assistant","content":null,"reasoning_content":""},"finish_reason":null}]}`,
				`{"id":"c1","choices":[{"index":0,"delta":{"content":"Now Tokyo.","reasoning_content":null},"finish_reason":null}]}`,
				`{"id":"c1","choices":[{"index":0,"delta":{"content":null,"reasoning_content":null},"finish_reason":"stop"}],"usage":{"prompt_tokens":5,"completion_tokens":10,"total_tokens":15}}`,
			},
			wantStarts: 1,
		},
		{
			name: "null key opens nothing",
			chunks: []string{
				`{"id":"c1","choices":[{"index":0,"delta":{"role":"assistant","content":null,"reasoning_content":null},"finish_reason":null}]}`,
				`{"id":"c1","choices":[{"index":0,"delta":{"content":"Now Tokyo."},"finish_reason":null}]}`,
				`{"id":"c1","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":5,"completion_tokens":10,"total_tokens":15}}`,
			},
		},
		{
			name: "empty marker after text does not reopen a closed block",
			chunks: []string{
				`{"id":"c1","choices":[{"index":0,"delta":{"role":"assistant","reasoning_content":"Let me think."},"finish_reason":null}]}`,
				`{"id":"c1","choices":[{"index":0,"delta":{"content":"Answer","reasoning_content":null},"finish_reason":null}]}`,
				`{"id":"c1","choices":[{"index":0,"delta":{"content":" here.","reasoning_content":""},"finish_reason":null}]}`,
				`{"id":"c1","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":5,"completion_tokens":10,"total_tokens":15}}`,
			},
			wantStarts: 1,
			wantDeltas: 1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			events := streamEventTypes(t, tc.chunks)
			if n := countEvents(events, sdk.StreamPartTypeReasoningStart); n != tc.wantStarts {
				t.Fatalf("reasoning-start: got %d, want %d; events=%v", n, tc.wantStarts, events)
			}
			if n := countEvents(events, sdk.StreamPartTypeReasoningEnd); n != tc.wantStarts {
				t.Fatalf("reasoning-end: got %d, want %d; events=%v", n, tc.wantStarts, events)
			}
			if n := countEvents(events, sdk.StreamPartTypeReasoningDelta); n != tc.wantDeltas {
				t.Fatalf("reasoning-delta: got %d, want %d; events=%v", n, tc.wantDeltas, events)
			}
		})
	}
}

// A two-step run with no compat option. Step 1 answers with a tool call and
// "reasoning_content": "" (thinking mode, no reasoning produced); step 2
// answers with text and no key at all. The empty key must be recorded as a
// ReasoningPart and replayed on the second request, the absent key must not.
func TestGenerateTextResult_EmptyReasoningStepReplaysKey(t *testing.T) {
	var call int
	var secondRequest []map[string]json.RawMessage
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call++
		w.Header().Set("Content-Type", "application/json")
		switch call {
		case 1:
			json.NewEncoder(w).Encode(chatResponse("tool_calls", map[string]any{
				"role": "assistant", "content": "", "reasoning_content": "",
				"tool_calls": []map[string]any{{
					"id": "call_1", "type": "function",
					"function": map[string]any{"name": "get_weather", "arguments": `{"city":"Paris"}`},
				}},
			}))
		default:
			var body struct {
				Messages []map[string]json.RawMessage `json:"messages"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode request body: %v", err)
			}
			secondRequest = body.Messages
			json.NewEncoder(w).Encode(chatResponse("stop", map[string]any{
				"role": "assistant", "content": "Paris is 18C.",
			}))
		}
	}))
	defer srv.Close()

	tool := weatherTool()
	tool.Execute = func(ctx *sdk.ToolExecContext, input any) (any, error) { return "18C", nil }

	p := completions.New(completions.WithAPIKey("k"), completions.WithBaseURL(srv.URL))
	result, err := sdk.GenerateTextResult(
		context.Background(),
		sdk.WithModel(p.ChatModel("deepseek-v4-flash")),
		sdk.WithMessages([]sdk.Message{sdk.UserMessage("Weather in Paris?")}),
		sdk.WithTools([]sdk.Tool{tool}),
		sdk.WithMaxSteps(3),
	)
	if err != nil {
		t.Fatalf("GenerateTextResult: %v", err)
	}
	if result.Text != "Paris is 18C." || call != 2 || len(result.Steps) != 2 {
		t.Fatalf("text %q after %d calls, %d steps", result.Text, call, len(result.Steps))
	}

	if parts := result.Steps[0].ReasoningParts; len(parts) != 1 || parts[0].Text != "" || parts[0].Format != sdk.ReasoningFormatOpenAIChat {
		t.Fatalf("step 1 reasoning parts: got %#v, want one empty openai-chat-v1 part", parts)
	}
	if parts := result.Steps[1].ReasoningParts; len(parts) != 0 {
		t.Fatalf("step 2 reasoning parts: got %#v, want none when the key is absent", parts)
	}

	if len(secondRequest) != 3 {
		t.Fatalf("second request: expected user, assistant, tool messages, got %d", len(secondRequest))
	}
	assertReasoningContent(t, secondRequest, 0, nil)
	assertReasoningContent(t, secondRequest, 1, str(""))
	assertReasoningContent(t, secondRequest, 2, nil)
}
