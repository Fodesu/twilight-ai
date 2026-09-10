package completions_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/felinics/twilight/provider/openai/completions"
	"github.com/felinics/twilight/provider/providertest"
	"github.com/felinics/twilight/sdk"
)

// This file runs the seam conformance suite (provider/providertest) against the
// Chat Completions wire format. The suite reaches the provider only through
// sdk.Generate and sdk.Stream, so these fixtures stay valid across a change to
// the provider interface itself.

func conformanceProvider(baseURL string) sdk.Provider {
	return completions.New(completions.WithAPIKey("test-key"), completions.WithBaseURL(baseURL))
}

func sse(w http.ResponseWriter, chunks ...string) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	for _, chunk := range chunks {
		_, _ = w.Write([]byte("data: " + chunk + "\n\n"))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	}
	_, _ = w.Write([]byte("data: [DONE]\n\n"))
}

const (
	conformanceUsage = `"usage":{"prompt_tokens":5,"completion_tokens":2,"total_tokens":7}`
)

// textFixture answers with plain text on both paths.
func textFixture(t *testing.T) providertest.Fixture {
	return providertest.Fixture{
		NewProvider: conformanceProvider,
		ModelID:     "gpt-4o-mini",
		Options:     json.RawMessage(`{"temperature":0.42}`),
		Reply: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"chatcmpl-1","object":"chat.completion","created":1700000000,"model":"gpt-4o-mini",
				"choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"conformance text"}}],
				` + conformanceUsage + `}`))
		},
		ReplyStream: func(w http.ResponseWriter, r *http.Request) {
			sse(w,
				`{"id":"chatcmpl-1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"role":"assistant","content":"conformance"}}]}`,
				`{"id":"chatcmpl-1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":" text"}}]}`,
				`{"id":"chatcmpl-1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
				`{"id":"chatcmpl-1","object":"chat.completion.chunk","choices":[],`+conformanceUsage+`}`,
			)
		},
		ReplyError: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":{"message":"invalid api key","type":"invalid_request_error"}}`))
		},
		Want: providertest.Want{
			Text:         "conformance text",
			FinishReason: sdk.FinishReasonStop,
			TotalTokens:  7,
		},
	}
}

// toolCallFixture answers with one tool call on both paths. The streamed
// arguments arrive split across chunks, so this also covers accumulation.
func toolCallFixture(t *testing.T) providertest.Fixture {
	return providertest.Fixture{
		NewProvider: conformanceProvider,
		ModelID:     "gpt-4o-mini",
		Reply: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"chatcmpl-2","object":"chat.completion","created":1700000000,"model":"gpt-4o-mini",
				"choices":[{"index":0,"finish_reason":"tool_calls","message":{"role":"assistant","content":"",
				"tool_calls":[{"id":"call_1","type":"function","function":{"name":"providertest_tool_marker_2d91","arguments":"{\"city\":\"Paris\"}"}}]}}],
				` + conformanceUsage + `}`))
		},
		ReplyStream: func(w http.ResponseWriter, r *http.Request) {
			sse(w,
				`{"id":"chatcmpl-2","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"providertest_tool_marker_2d91","arguments":""}}]}}]}`,
				`{"id":"chatcmpl-2","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"city\":"}}]}}]}`,
				`{"id":"chatcmpl-2","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"Paris\"}"}}]}}]}`,
				`{"id":"chatcmpl-2","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`,
				`{"id":"chatcmpl-2","object":"chat.completion.chunk","choices":[],`+conformanceUsage+`}`,
			)
		},
		Want: providertest.Want{
			FinishReason: sdk.FinishReasonToolCalls,
			TotalTokens:  7,
			ToolCalls: []sdk.ToolCall{{
				ToolCallID: "call_1",
				ToolName:   "providertest_tool_marker_2d91",
				Input:      map[string]any{"city": "Paris"},
			}},
		},
	}
}

func TestSeamConformance(t *testing.T) {
	t.Run("text", func(t *testing.T) { providertest.Run(t, textFixture) })
	t.Run("tool-call", func(t *testing.T) { providertest.Run(t, toolCallFixture) })
}
