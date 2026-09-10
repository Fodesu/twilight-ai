package responses_test

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/felinics/twilight/provider/openai/responses"
	"github.com/felinics/twilight/provider/providertest"
	"github.com/felinics/twilight/sdk"
)

// This file runs the seam conformance suite (provider/providertest) against the
// OpenAI Responses wire format. The suite reaches the provider only through
// sdk.Generate and sdk.Stream, so these fixtures stay valid across a change to
// the provider interface itself.
//
// The canned wire data below mirrors the shapes already proven by
// responses_test.go: TestResponsesDoGenerate / TestResponsesDoStream for the
// text reply, TestResponsesDoGenerate_ToolCall / TestResponsesDoStream_ToolCall
// for the function_call reply, and TestProviderTest_Unhealthy for the error
// body.

const (
	conformanceModel     = "gpt-4o-mini"
	conformanceCreatedAt = 1700000000
)

func conformanceProvider(baseURL string) sdk.Provider {
	return responses.New(responses.WithAPIKey("test-key"), responses.WithBaseURL(baseURL))
}

// conformanceResponse is the response metadata both canned replies carry; the
// streamed response.created event repeats it, so generate and stream must agree
// on it.
func conformanceResponse(id string) *sdk.ResponseMetadata {
	return &sdk.ResponseMetadata{
		ID:        id,
		ModelID:   conformanceModel,
		Timestamp: time.Unix(conformanceCreatedAt, 0).UTC(),
	}
}

// sseEvents writes a Responses stream. The Responses API names every event
// ("event: <type>") and repeats the type inside the data payload, which is how
// the provider's existing stream tests emit them.
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
		Reply: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"resp_conf_text","created_at":1700000000,"model":"` + conformanceModel + `",
				"output":[{"type":"message","id":"msg_conf_text","role":"assistant",
				"content":[{"type":"output_text","text":"conformance text","annotations":[]}]}],
				"usage":{"input_tokens":5,"output_tokens":2}}`))
		},
		ReplyStream: func(w http.ResponseWriter, r *http.Request) {
			sseEvents(w,
				[2]string{"response.created", `{"type":"response.created","response":{"id":"resp_conf_text","created_at":1700000000,"model":"` + conformanceModel + `"}}`},
				[2]string{"response.output_item.added", `{"type":"response.output_item.added","output_index":0,"item":{"type":"message","id":"msg_conf_text"}}`},
				[2]string{"response.output_text.delta", `{"type":"response.output_text.delta","item_id":"msg_conf_text","delta":"conformance"}`},
				[2]string{"response.output_text.delta", `{"type":"response.output_text.delta","item_id":"msg_conf_text","delta":" text"}`},
				[2]string{"response.output_item.done", `{"type":"response.output_item.done","output_index":0,"item":{"type":"message","id":"msg_conf_text"}}`},
				[2]string{"response.completed", `{"type":"response.completed","response":{"usage":{"input_tokens":5,"output_tokens":2}}}`},
			)
		},
		ReplyError: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":{"message":"invalid api key"}}`))
		},
		Want: providertest.Want{
			Text:         "conformance text",
			FinishReason: sdk.FinishReasonStop,
			TotalTokens:  7,
			Response:     conformanceResponse("resp_conf_text"),
		},
	}
}

// toolCallFixture answers with one function_call on both paths. The streamed
// arguments arrive split across deltas, so this also covers accumulation.
func toolCallFixture(t *testing.T) providertest.Fixture {
	return providertest.Fixture{
		NewProvider: conformanceProvider,
		ModelID:     conformanceModel,
		Reply: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"resp_conf_tool","created_at":1700000000,"model":"` + conformanceModel + `",
				"output":[{"type":"function_call","id":"fc_conf_1","call_id":"call_conf_1",
				"name":"providertest_tool_marker_2d91","arguments":"{\"city\":\"Paris\"}"}],
				"usage":{"input_tokens":20,"output_tokens":10}}`))
		},
		ReplyStream: func(w http.ResponseWriter, r *http.Request) {
			sseEvents(w,
				[2]string{"response.created", `{"type":"response.created","response":{"id":"resp_conf_tool","created_at":1700000000,"model":"` + conformanceModel + `"}}`},
				[2]string{"response.output_item.added", `{"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","id":"fc_conf_1","call_id":"call_conf_1","name":"providertest_tool_marker_2d91"}}`},
				[2]string{"response.function_call_arguments.delta", `{"type":"response.function_call_arguments.delta","item_id":"fc_conf_1","output_index":0,"delta":"{\"city\""}`},
				[2]string{"response.function_call_arguments.delta", `{"type":"response.function_call_arguments.delta","item_id":"fc_conf_1","output_index":0,"delta":":\"Paris\"}"}`},
				[2]string{"response.output_item.done", `{"type":"response.output_item.done","output_index":0,"item":{"type":"function_call","id":"fc_conf_1","call_id":"call_conf_1","name":"providertest_tool_marker_2d91","arguments":"{\"city\":\"Paris\"}"}}`},
				[2]string{"response.completed", `{"type":"response.completed","response":{"usage":{"input_tokens":20,"output_tokens":10}}}`},
			)
		},
		Want: providertest.Want{
			FinishReason: sdk.FinishReasonToolCalls,
			TotalTokens:  30,
			Response:     conformanceResponse("resp_conf_tool"),
			ToolCalls: []sdk.ToolCall{{
				ToolCallID: "call_conf_1",
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
