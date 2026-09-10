package messages_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/felinics/twilight/provider/anthropic/messages"
	"github.com/felinics/twilight/provider/providertest"
	"github.com/felinics/twilight/sdk"
)

// This file runs the seam conformance suite (provider/providertest) against the
// Anthropic Messages wire format. The suite reaches the provider only through
// sdk.Generate and sdk.Stream, so these fixtures stay valid across a change to
// the provider interface itself.
//
// The canned wire data below mirrors the shapes already proven by
// messages_test.go: TestDoGenerate / TestDoStream for the text reply and
// TestDoGenerate_ToolCall / TestDoStream_ToolCall for the tool_use reply, plus
// TestDoGenerate_ErrorResponse for the error body.

const conformanceModel = "claude-sonnet-4-20250514"

func conformanceProvider(baseURL string) sdk.Provider {
	return messages.New(messages.WithAPIKey("test-key"), messages.WithBaseURL(baseURL))
}

// sseEvents writes an Anthropic event stream. Anthropic names every event
// ("event: <type>") in addition to carrying the same type in the data payload,
// which is how the provider's existing stream tests emit them.
func sseEvents(w http.ResponseWriter, events ...[2]string) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	for _, e := range events {
		_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", e[0], e[1])
		if flusher != nil {
			flusher.Flush()
		}
	}
}

// textFixture answers with plain text on both paths.
func textFixture(t *testing.T) providertest.Fixture {
	return providertest.Fixture{
		NewProvider: conformanceProvider,
		ModelID:     conformanceModel,
		Options:     json.RawMessage(`{"temperature":0.42}`),
		Reply: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"msg_conf_text","type":"message","model":"` + conformanceModel + `","role":"assistant",
				"content":[{"type":"text","text":"conformance text"}],
				"stop_reason":"end_turn",
				"usage":{"input_tokens":5,"output_tokens":2}}`))
		},
		ReplyStream: func(w http.ResponseWriter, r *http.Request) {
			sseEvents(w,
				[2]string{"message_start", `{"type":"message_start","message":{"id":"msg_conf_text","type":"message","model":"` + conformanceModel + `","role":"assistant","content":[],"usage":{"input_tokens":5,"output_tokens":0}}}`},
				[2]string{"content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`},
				[2]string{"content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"conformance"}}`},
				[2]string{"content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":" text"}}`},
				[2]string{"content_block_stop", `{"type":"content_block_stop","index":0}`},
				[2]string{"message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}}`},
				[2]string{"message_stop", `{"type":"message_stop"}`},
			)
		},
		ReplyError: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"type":"error","error":{"type":"authentication_error","message":"invalid x-api-key"}}`))
		},
		Want: providertest.Want{
			Text:         "conformance text",
			FinishReason: sdk.FinishReasonStop,
			TotalTokens:  7,
		},
	}
}

// toolCallFixture answers with one tool_use content block on both paths. The
// streamed arguments arrive split across partial_json deltas, so this also
// covers accumulation.
func toolCallFixture(t *testing.T) providertest.Fixture {
	return providertest.Fixture{
		NewProvider: conformanceProvider,
		ModelID:     conformanceModel,
		Reply: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"msg_conf_tool","type":"message","model":"` + conformanceModel + `","role":"assistant",
				"content":[{"type":"tool_use","id":"toolu_conf_1","name":"providertest_tool_marker_2d91","input":{"city":"Paris"}}],
				"stop_reason":"tool_use",
				"usage":{"input_tokens":20,"output_tokens":10}}`))
		},
		ReplyStream: func(w http.ResponseWriter, r *http.Request) {
			sseEvents(w,
				[2]string{"message_start", `{"type":"message_start","message":{"id":"msg_conf_tool","type":"message","model":"` + conformanceModel + `","role":"assistant","content":[],"usage":{"input_tokens":20,"output_tokens":0}}}`},
				[2]string{"content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_conf_1","name":"providertest_tool_marker_2d91"}}`},
				[2]string{"content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"city\""}}`},
				[2]string{"content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":":\"Paris\"}"}}`},
				[2]string{"content_block_stop", `{"type":"content_block_stop","index":0}`},
				[2]string{"message_delta", `{"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":10}}`},
				[2]string{"message_stop", `{"type":"message_stop"}`},
			)
		},
		Want: providertest.Want{
			FinishReason: sdk.FinishReasonToolCalls,
			TotalTokens:  30,
			ToolCalls: []sdk.ToolCall{{
				ToolCallID: "toolu_conf_1",
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
