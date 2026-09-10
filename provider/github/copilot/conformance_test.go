package copilot_test

import (
	"net/http"
	"testing"

	"github.com/felinics/twilight/provider/github/copilot"
	"github.com/felinics/twilight/provider/providertest"
	"github.com/felinics/twilight/sdk"
)

// This file runs the seam conformance suite (provider/providertest) against the
// GitHub Copilot chat provider. The suite reaches the provider only through
// sdk.Generate and sdk.Stream, so these fixtures stay valid across a change to
// the provider interface itself.
//
// Reachability: the provider is drivable against a plain httptest server. It
// does not perform a Copilot token exchange and adds no Copilot-specific
// headers -- authHeaders() sends only "Authorization: Bearer <token>"
// (copilot.go:567-571) straight to <baseURL>/chat/completions (copilot.go:141-147),
// and WithBaseURL (copilot.go:40-44) redirects the whole call. That is exactly
// how the existing unit tests drive it (copilot_test.go:32-36, :68-71). The wire
// format is OpenAI Chat Completions (types.go:72-175), so the fixtures below use
// chat.completion / chat.completion.chunk shapes. No live Copilot token is
// needed and nothing is skipped.

const conformanceToken = "ghu_conformance_token"

func conformanceProvider(baseURL string) sdk.Provider {
	return copilot.New(
		copilot.WithGitHubToken(conformanceToken),
		copilot.WithBaseURL(baseURL),
	)
}

// assertCopilotRequest pins the transport the provider must use: the chat
// completions path with the configured token as a bearer credential.
func assertCopilotRequest(t *testing.T, r *http.Request) {
	t.Helper()
	if r.URL.Path != "/chat/completions" {
		t.Errorf("path = %q, want /chat/completions", r.URL.Path)
	}
	if got, want := r.Header.Get("Authorization"), "Bearer "+conformanceToken; got != want {
		t.Errorf("Authorization = %q, want %q", got, want)
	}
}

func copilotSSE(w http.ResponseWriter, chunks ...string) {
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

// conformanceUsage is the OpenAI-shaped usage object both paths must map to the
// same sdk.Usage.
const conformanceUsage = `"usage":{"prompt_tokens":5,"completion_tokens":2,"total_tokens":7}`

// conformanceTextFixture answers with plain text on both paths.
func conformanceTextFixture(t *testing.T) providertest.Fixture {
	return providertest.Fixture{
		NewProvider: conformanceProvider,
		ModelID:     "gpt-4.1",
		Reply: func(w http.ResponseWriter, r *http.Request) {
			assertCopilotRequest(t, r)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"copilotcmpl-1","object":"chat.completion","created":1700000000,"model":"gpt-4.1",
				"choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"conformance text"}}],
				` + conformanceUsage + `}`))
		},
		ReplyStream: func(w http.ResponseWriter, r *http.Request) {
			assertCopilotRequest(t, r)
			copilotSSE(w,
				`{"id":"copilotcmpl-1","object":"chat.completion.chunk","created":1700000000,"model":"gpt-4.1","choices":[{"index":0,"delta":{"role":"assistant","content":"conformance"},"finish_reason":null}]}`,
				`{"id":"copilotcmpl-1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":" text"},"finish_reason":null}]}`,
				`{"id":"copilotcmpl-1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
				`{"id":"copilotcmpl-1","object":"chat.completion.chunk","choices":[],`+conformanceUsage+`}`,
			)
		},
		ReplyError: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":{"message":"Bad credentials","type":"invalid_request_error"}}`))
		},
		Want: providertest.Want{
			Text:         "conformance text",
			FinishReason: sdk.FinishReasonStop,
			TotalTokens:  7,
		},
	}
}

// conformanceToolCallFixture answers with one tool call on both paths. The
// streamed arguments arrive split across chunks, so this also covers
// accumulation in the provider's streamingToolCall buffer (stream.go:151-185).
func conformanceToolCallFixture(t *testing.T) providertest.Fixture {
	return providertest.Fixture{
		NewProvider: conformanceProvider,
		ModelID:     "gpt-4.1",
		Reply: func(w http.ResponseWriter, r *http.Request) {
			assertCopilotRequest(t, r)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"copilotcmpl-2","object":"chat.completion","created":1700000000,"model":"gpt-4.1",
				"choices":[{"index":0,"finish_reason":"tool_calls","message":{"role":"assistant","content":"",
				"tool_calls":[{"id":"call_1","type":"function","function":{"name":"providertest_tool_marker_2d91","arguments":"{\"city\":\"Paris\"}"}}]}}],
				` + conformanceUsage + `}`))
		},
		ReplyStream: func(w http.ResponseWriter, r *http.Request) {
			assertCopilotRequest(t, r)
			copilotSSE(w,
				`{"id":"copilotcmpl-2","object":"chat.completion.chunk","created":1700000000,"model":"gpt-4.1","choices":[{"index":0,"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"providertest_tool_marker_2d91","arguments":""}}]},"finish_reason":null}]}`,
				`{"id":"copilotcmpl-2","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"city\":"}}]},"finish_reason":null}]}`,
				`{"id":"copilotcmpl-2","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"Paris\"}"}}]},"finish_reason":null}]}`,
				`{"id":"copilotcmpl-2","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`,
				`{"id":"copilotcmpl-2","object":"chat.completion.chunk","choices":[],`+conformanceUsage+`}`,
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

// Caps is the zero value for both fixtures: the Copilot wire format carries
// finish reasons and token usage, and the streamed reply is not asked to repeat
// the non-streamed response metadata, so no capability is waived.
func TestSeamConformance(t *testing.T) {
	t.Run("text", func(t *testing.T) { providertest.Run(t, conformanceTextFixture) })
	t.Run("tool-call", func(t *testing.T) { providertest.Run(t, conformanceToolCallFixture) })
}
