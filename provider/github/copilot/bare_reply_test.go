package copilot_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/felinics/twilight/provider/github/copilot"
	"github.com/felinics/twilight/sdk"
)

// The Copilot endpoint may answer without id, model or created. Neither
// path may turn the missing created into 1970-01-01, and both must agree.
func TestBareReplyCarriesNoResponseMetadata(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["stream"] == true {
			w.Header().Set("Content-Type", "text/event-stream")
			for _, c := range []string{
				`{"object":"chat.completion.chunk","choices":[{"index":0,"delta":{"role":"assistant","content":"hi"}}]}`,
				`{"object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
			} {
				fmt.Fprintf(w, "data: %s\n\n", c)
			}
			fmt.Fprintf(w, "data: [DONE]\n\n")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}]}`))
	}))
	defer srv.Close()
	model := &sdk.Model{ID: "gpt-4o", Provider: copilot.New(copilot.WithAPIKey("k"), copilot.WithBaseURL(srv.URL))}
	req := sdk.Request{Model: "gpt-4o", Messages: []sdk.Message{sdk.UserMessage("hi")}}

	generated, err := model.Generate(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := model.Stream(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	for range stream.Parts {
	}
	streamed, err := stream.Result()
	if err != nil {
		t.Fatal(err)
	}
	for name, r := range map[string]sdk.ModelResult{"generate": generated, "stream": *streamed} {
		if !r.Response.IsZero() {
			t.Errorf("%s: response metadata = %+v, want none", name, r.Response)
		}
	}
}
