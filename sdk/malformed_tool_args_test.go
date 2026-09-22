package sdk_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	anthropicmessages "github.com/felinics/twilight/provider/anthropic/messages"
	sdk "github.com/felinics/twilight/sdk"
	"github.com/google/jsonschema-go/jsonschema"
)

// A tool call whose streamed arguments are not a JSON document is still
// reported -- the model has to hear that it got the call wrong -- but it
// carries the text in ToolArguments.Text and no document, so whoever runs
// the calls can refuse it (Valid is false) instead of guessing arguments.
func TestMalformedToolArgsAreReportedInvalid(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		for _, chunk := range []string{
			`event: message_start
data: {"type":"message_start","message":{"id":"msg_1","model":"claude-opus-5","usage":{"input_tokens":5,"output_tokens":0}}}`,
			`event: content_block_start
data: {"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_1","name":"delete_file"}}`,
			`event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"path\": \"/etc"}}`,
			`event: content_block_stop
data: {"type":"content_block_stop","index":0}`,
			`event: message_delta
data: {"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":10}}`,
			`event: message_stop
data: {"type":"message_stop"}`,
		} {
			fmt.Fprintf(w, "%s\n\n", chunk)
		}
		w.(http.Flusher).Flush()
	}))
	defer srv.Close()

	provider := anthropicmessages.New(
		anthropicmessages.WithAPIKey("k"),
		anthropicmessages.WithBaseURL(srv.URL),
	)
	stream, err := provider.ChatModel("claude-opus-5").Stream(context.Background(), sdk.Request{
		Messages: []sdk.Message{sdk.UserMessage("delete something")},
		Tools:    []sdk.ToolDefinition{{Name: "delete_file", Description: "deletes a file", Parameters: &jsonschema.Schema{Type: "object"}}},
	})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	var calls []sdk.ToolCall
	for part := range stream.Parts {
		switch p := part.(type) {
		case *sdk.ErrorPart:
			t.Fatalf("malformed arguments surfaced as a stream error: %v", p.Error)
		case *sdk.StreamToolCallPart:
			calls = append(calls, sdk.ToolCall{ToolCallID: p.ToolCallID, ToolName: p.ToolName, Input: p.Input})
		}
	}
	result, err := stream.Result()
	if err != nil || result == nil {
		t.Fatalf("Result: %+v %v", result, err)
	}
	if len(calls) != 1 || len(result.ToolCalls) != 1 {
		t.Fatalf("tool calls: streamed %d, assembled %d, want 1 and 1", len(calls), len(result.ToolCalls))
	}
	in := result.ToolCalls[0].Input
	if in.Valid() || in.Text != `{"path": "/etc` {
		t.Fatalf("arguments = %+v, want the invalid text kept verbatim and no document", in)
	}
	if err := in.Unmarshal(&struct{}{}); !errors.Is(err, sdk.ErrInvalidToolArguments) {
		t.Fatalf("Unmarshal of invalid arguments = %v, want ErrInvalidToolArguments", err)
	}
}
