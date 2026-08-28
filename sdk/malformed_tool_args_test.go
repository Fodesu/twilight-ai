package sdk_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	anthropicmessages "github.com/memohai/twilight/provider/anthropic/messages"
	sdk "github.com/memohai/twilight/sdk"
)

// A tool call whose streamed arguments fail to parse must not run. Emitting it
// with nil input hands the tool empty arguments and executes it anyway — with
// whatever side effects that implies — and then commits the step as if nothing
// were wrong, leaving an error event and a completed call for the same block.
func TestMalformedToolArgsDoNotExecute(t *testing.T) {
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

	executed := false
	committed := 0
	provider := anthropicmessages.New(
		anthropicmessages.WithAPIKey("k"),
		anthropicmessages.WithBaseURL(srv.URL),
	)
	client := sdk.NewClient()

	result, err := client.StreamText(context.Background(),
		sdk.WithModel(provider.ChatModel("claude-opus-5")),
		sdk.WithMessages([]sdk.Message{sdk.UserMessage("delete something")}),
		sdk.WithTools([]sdk.Tool{{
			Name:        "delete_file",
			Description: "deletes a file",
			Parameters:  map[string]any{"type": "object"},
			Execute: func(ctx *sdk.ToolExecContext, input any) (any, error) {
				executed = true
				return "deleted", nil
			},
		}}),
		sdk.WithOnStepCommitted(func(ctx context.Context, stepIndex int, step *sdk.StepResult) error {
			committed++
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("StreamText: %v", err)
	}

	var sawError bool
	var calls int
	for part := range result.Stream {
		switch part.(type) {
		case *sdk.ErrorPart:
			sawError = true
		case *sdk.StreamToolCallPart:
			calls++
		}
	}

	if !sawError {
		t.Error("no ErrorPart for malformed tool arguments")
	}
	if calls != 0 {
		t.Errorf("StreamToolCallPart emitted %d time(s) for a call that could not be parsed", calls)
	}
	if executed {
		t.Error("the tool ran on nil input")
	}
	if committed != 0 {
		t.Errorf("OnStepCommitted fired %d time(s) for a failed step", committed)
	}
}
