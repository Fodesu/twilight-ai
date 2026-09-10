package codex_test

import (
	"encoding/json"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/felinics/twilight/provider/openai/codex"
	"github.com/felinics/twilight/provider/providertest"
	"github.com/felinics/twilight/sdk"
)

// This file runs the seam conformance suite (provider/providertest) against the
// Codex Responses wire format. The suite reaches the provider only through
// sdk.Generate and sdk.Stream, so these fixtures stay valid across a change to
// the provider interface itself.
//
// Two facts about this provider shape the fixtures below.
//
//  1. Codex has no separate non-streaming transport. DoGenerate (codex.go:127)
//     delegates to DoStream and buffers the SSE channel through
//     StreamResult.ToResult, so an sdk.Generate call issues the same
//     stream:true POST /codex/responses request that sdk.Stream does. Reply and
//     ReplyStream therefore both answer an SSE body; Reply is not a JSON
//     response, and asserting otherwise would contradict codex.go:148.
//
//  2. Because of (1) the suite's generate-vs-stream agreement case compares the
//     provider against itself rather than two independent code paths. It still
//     checks that the SSE-to-ModelResult mapping is stable, but a divergence
//     between a buffered and an incremental transport is not expressible here,
//     because the provider does not have two transports.
//
// The endpoint is reachable: the suite's httptest base URL reaches the provider
// through codex.WithBaseURL, and utils.FetchSSE posts to BaseURL+"/codex/responses"
// (codex.go:204, sse.go:46, fetch.go:53). No token exchange or websocket is
// involved — the bearer token and account id are read straight from the options
// (codex.go:589).

const (
	conformanceModelID   = "gpt-5.2"
	conformanceCreatedAt = 1700000000
)

func conformanceProvider(baseURL string) sdk.Provider {
	return codex.New(
		codex.WithAPIKey("test-key"),
		codex.WithAccountID("acct_conformance"),
		codex.WithBaseURL(baseURL),
	)
}

// openCodexStream starts an SSE response in the shape utils.FetchSSE expects.
func openCodexStream(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
}

// codexSSE writes one named Codex Responses SSE event and flushes it.
func codexSSE(w http.ResponseWriter, event, data string) {
	_, _ = w.Write([]byte("event: " + event + "\n"))
	_, _ = w.Write([]byte("data: " + data + "\n\n"))
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
}

// codexCreated is the response.created payload the provider reads for the
// response id, model, and creation time (types.go:87, codex.go:212).
func codexCreated(id string) string {
	return `{"response":{"id":"` + id + `","created_at":` + strconv.Itoa(conformanceCreatedAt) +
		`,"model":"` + conformanceModelID + `"}}`
}

// assertCodexRequest pins the two transport facts these fixtures rely on: the
// path the provider actually posts to, and the fact that even sdk.Generate
// travels the streaming wire path.
func assertCodexRequest(t *testing.T, r *http.Request) {
	t.Helper()
	if r.URL.Path != "/codex/responses" {
		t.Errorf("codex request path = %q, want /codex/responses (codex.go:207)", r.URL.Path)
	}
	var body struct {
		Stream bool `json:"stream"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		t.Errorf("decode codex request body: %v", err)
		return
	}
	if !body.Stream {
		t.Errorf("codex request did not set stream:true; sdk.Generate must route through DoStream (codex.go:148)")
	}
}

// conformanceError is a provider-shaped failure. The Codex endpoint reports an
// expired or invalid access token as a non-2xx JSON body, which FetchSSE turns
// into *utils.APIError (sse.go:57, fetch.go:212); the provider must surface
// that as an error rather than an empty success (codex.go:378).
func conformanceError(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnauthorized)
	_, _ = w.Write([]byte(`{"error":{"message":"invalid access token","type":"invalid_request_error","code":"invalid_api_key"}}`))
}

// textFixture answers with plain text. The answer arrives split across two
// output_text deltas so the fixture also covers delta accumulation.
func textFixture(t *testing.T) providertest.Fixture {
	reply := func(w http.ResponseWriter, r *http.Request) {
		assertCodexRequest(t, r)
		openCodexStream(w)
		codexSSE(w, "response.created", codexCreated("resp_conf_text"))
		codexSSE(w, "response.output_item.added", `{"output_index":0,"item":{"type":"message","id":"msg_conf_1"}}`)
		codexSSE(w, "response.output_text.delta", `{"item_id":"msg_conf_1","delta":"conformance"}`)
		codexSSE(w, "response.output_text.delta", `{"item_id":"msg_conf_1","delta":" text"}`)
		codexSSE(w, "response.output_item.done", `{"output_index":0,"item":{"type":"message","id":"msg_conf_1"}}`)
		codexSSE(w, "response.completed", `{"response":{"usage":{"input_tokens":5,"output_tokens":2}}}`)
	}
	return providertest.Fixture{
		NewProvider: conformanceProvider,
		ModelID:     conformanceModelID,
		Options:     json.RawMessage(`{"store":true}`),
		Reply:       reply,
		ReplyStream: reply,
		ReplyError:  conformanceError,
		Want: providertest.Want{
			Text:         "conformance text",
			FinishReason: sdk.FinishReasonStop,
			TotalTokens:  7,
			Response: &sdk.ResponseMetadata{
				ID:        "resp_conf_text",
				ModelID:   conformanceModelID,
				Timestamp: time.Unix(conformanceCreatedAt, 0).UTC(),
			},
		},
		// The whole response — including id, model, and created_at — is carried
		// by the same FinishStepPart on both paths, so the streamed metadata is
		// the generated metadata.
	}
}

// toolCallFixture answers with one function call on both paths. The arguments
// arrive split across two delta events, so this also covers accumulation and
// the output_item.done hand-off (codex.go:276, codex.go:317).
func toolCallFixture(t *testing.T) providertest.Fixture {
	reply := func(w http.ResponseWriter, r *http.Request) {
		assertCodexRequest(t, r)
		openCodexStream(w)
		codexSSE(w, "response.created", codexCreated("resp_conf_tool"))
		codexSSE(w, "response.output_item.added",
			`{"output_index":0,"item":{"type":"function_call","id":"fc_conf_1","call_id":"call_conf_1","name":"providertest_tool_marker_2d91"}}`)
		codexSSE(w, "response.function_call_arguments.delta", `{"output_index":0,"delta":"{\"city\":"}`)
		codexSSE(w, "response.function_call_arguments.delta", `{"output_index":0,"delta":"\"Paris\"}"}`)
		codexSSE(w, "response.output_item.done",
			`{"output_index":0,"item":{"type":"function_call","id":"fc_conf_1","call_id":"call_conf_1","name":"providertest_tool_marker_2d91","arguments":"{\"city\":\"Paris\"}"}}`)
		codexSSE(w, "response.completed", `{"response":{"usage":{"input_tokens":5,"output_tokens":2}}}`)
	}
	return providertest.Fixture{
		NewProvider: conformanceProvider,
		ModelID:     conformanceModelID,
		Reply:       reply,
		ReplyStream: reply,
		ReplyError:  conformanceError,
		Want: providertest.Want{
			FinishReason: sdk.FinishReasonToolCalls,
			TotalTokens:  7,
			ToolCalls: []sdk.ToolCall{{
				ToolCallID: "call_conf_1",
				ToolName:   "providertest_tool_marker_2d91",
				Input:      map[string]any{"city": "Paris"},
			}},
			Response: &sdk.ResponseMetadata{
				ID:        "resp_conf_tool",
				ModelID:   conformanceModelID,
				Timestamp: time.Unix(conformanceCreatedAt, 0).UTC(),
			},
		},
	}
}

func TestSeamConformance(t *testing.T) {
	t.Run("text", func(t *testing.T) { providertest.Run(t, textFixture) })
	t.Run("tool-call", func(t *testing.T) { providertest.Run(t, toolCallFixture) })
}
