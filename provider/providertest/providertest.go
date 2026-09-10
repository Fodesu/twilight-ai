// Package providertest is the seam conformance suite for chat providers.
//
// It reaches a provider only through sdk.Generate and sdk.Stream, so the same
// fixtures keep working when the provider interface underneath changes: the
// suite asserts behavior, not method signatures. A provider package supplies a
// Fixture -- how to construct the provider against a test server, plus replies
// in its own wire format -- and every provider runs the same cases.
//
// The cases exist because a provider boundary has two failure modes that no
// compile error catches. A request field can be silently dropped on the way to
// the wire, and the streaming and non-streaming paths can disagree about the
// same response. Both are invisible until production, and both are checked here
// without the suite knowing any provider's wire format.
package providertest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/felinics/twilight/sdk"
)

// Fixture is one chat provider under test.
type Fixture struct {
	// NewProvider binds the provider to baseURL, the suite's test server.
	NewProvider func(baseURL string) sdk.Provider
	// ModelID is the model every case requests.
	ModelID string
	// Reply answers a non-streaming request in this provider's wire format. It
	// may assert on the request it received: t.Errorf is safe from the server
	// goroutine.
	Reply http.HandlerFunc
	// ReplyStream answers a streaming request. Nil skips the stream case.
	ReplyStream http.HandlerFunc
	// ReplyError answers a request with a provider-shaped error. Nil skips the
	// error case.
	ReplyError http.HandlerFunc
	// Options, when set, is sent as this provider's own entry in
	// Request.ProviderOptions (keyed by Provider.Name()) and must reach the
	// request body: an option the provider silently drops is indistinguishable
	// from one the caller never set.
	Options json.RawMessage
	// Want is what Reply must map to.
	Want Want
	// Caps records what this provider does not support.
	Caps Caps
}

// Want is the provider-neutral meaning of the Fixture's success reply.
type Want struct {
	Text         string
	Reasoning    string
	ToolCalls    []sdk.ToolCall
	FinishReason sdk.FinishReason
	// TotalTokens is the reply's total token count; 0 skips the usage check.
	TotalTokens int
	// Response is the reply's response metadata. When it is set, both paths
	// must produce it: a stream that silently drops what the generated call
	// carries is a divergence, not a detail. Leave it nil when the wire format
	// does not repeat the metadata on the stream.
	Response *sdk.ResponseMetadata
}

// Caps records behavior a provider legitimately does not have, so that a case
// is skipped rather than silently passing.
type Caps struct {
	// NoUsage: this provider's wire format carries no token counts.
	NoUsage bool
	// NoFinishReason: this provider's wire format carries no finish reason.
	NoFinishReason bool
}

// Factory builds a fresh Fixture for one subtest.
type Factory func(t *testing.T) Fixture

// Run executes the suite.
func Run(t *testing.T, factory Factory) {
	t.Helper()
	t.Run("request", func(t *testing.T) { testRequest(t, factory(t)) })
	t.Run("generate", func(t *testing.T) { testGenerate(t, factory(t)) })
	t.Run("stream", func(t *testing.T) { testStream(t, factory(t)) })
	t.Run("error", func(t *testing.T) { testError(t, factory(t)) })
}

// The markers travel through the Request untouched and must come out of the
// provider's wire encoding, whatever it is. They are alphanumeric with dashes
// and underscores so that no JSON encoder escapes them, which is what makes a
// substring search a valid check.
const (
	systemMarker   = "providertest-system-marker-2d91"
	userMarker     = "providertest-user-marker-2d91"
	toolMarker     = "providertest_tool_marker_2d91"
	toolDescMarker = "providertest-tool-description-marker-2d91"
)

func (f Fixture) model(p sdk.Provider) *sdk.Model {
	return &sdk.Model{ID: f.ModelID, Provider: p, Type: sdk.ModelTypeChat}
}

func request() sdk.Request {
	return sdk.Request{
		System:   systemMarker,
		Messages: []sdk.Message{sdk.UserMessage(userMarker)},
		Tools: []sdk.ToolDefinition{{
			Name:        toolMarker,
			Description: toolDescMarker,
			Parameters:  json.RawMessage(`{"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}`),
		}},
		ToolChoice: sdk.ToolChoice{Mode: sdk.ToolChoiceAuto},
	}
}

// recorder wraps a fixture handler to capture what the provider actually sent,
// so the suite can look for the markers and for the model.
type recorder struct {
	next http.HandlerFunc
	mu   sync.Mutex
	hits int
	seen []string
}

func (r *recorder) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	body, _ := io.ReadAll(req.Body)
	req.Body = io.NopCloser(bytes.NewReader(body))
	// A provider may carry the model in the path rather than the body (Google
	// puts it in the URL), so both count as reaching the wire.
	r.mu.Lock()
	r.hits++
	r.seen = append(r.seen, req.URL.String()+"\n"+string(body))
	r.mu.Unlock()
	r.next(w, req)
}

// wire reports what reached the server. The handler runs on the server
// goroutine, so its writes are guarded.
// withOptions attaches the fixture's provider options under the provider's own
// namespace.
func (f Fixture) withOptions(p sdk.Provider, req sdk.Request) sdk.Request {
	if len(f.Options) == 0 {
		return req
	}
	req.ProviderOptions = map[string]json.RawMessage{p.Name(): f.Options}
	return req
}

// wantOptionsOnWire requires the fixture's provider options to reach the request
// body, in the provider's own namespace.
func wantOptionsOnWire(t *testing.T, rec *recorder, f Fixture, op string) {
	t.Helper()
	if len(f.Options) == 0 {
		return
	}
	var want map[string]json.RawMessage
	if err := json.Unmarshal(f.Options, &want); err != nil {
		t.Fatalf("fixture options are not a JSON object: %v", err)
	}
	_, seen := rec.wire()
	flat := strings.Join(strings.Fields(seen), "")
	for key, value := range want {
		var compact bytes.Buffer
		if err := json.Compact(&compact, value); err != nil {
			t.Fatalf("fixture option %q is not JSON: %v", key, err)
		}
		pair := `"` + key + `":` + compact.String()
		if !strings.Contains(flat, pair) {
			t.Errorf("%s: provider option %s never reached the request body", op, pair)
		}
	}
}

func (r *recorder) wire() (int, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	seen := ""
	for _, s := range r.seen {
		seen += s
	}
	return r.hits, seen
}

// serve starts a server for one case and returns a provider bound to it.
func serve(t *testing.T, f Fixture, handler http.HandlerFunc) (sdk.Provider, *recorder) {
	t.Helper()
	rec := &recorder{next: handler}
	srv := httptest.NewServer(rec)
	t.Cleanup(srv.Close)
	if f.NewProvider == nil {
		t.Fatal("fixture has no NewProvider")
	}
	p := f.NewProvider(srv.URL)
	if p == nil {
		t.Fatal("fixture returned a nil provider")
	}
	return p, rec
}

// wantSentOnWire asserts that the Request the suite built survived the
// provider's encoding: every marker and the model ID must appear in either the
// URL or the body, and the handler must have been reached at all.
func wantSentOnWire(t *testing.T, rec *recorder, modelID string, op string) {
	t.Helper()
	hits, seen := rec.wire()
	if hits == 0 {
		t.Fatalf("%s: the provider sent no request", op)
	}
	for _, want := range []struct{ what, marker string }{
		{"model", modelID},
		{"system", systemMarker},
		{"user message", userMarker},
		{"tool name", toolMarker},
		{"tool description", toolDescMarker},
	} {
		if want.marker == "" {
			continue
		}
		if !contains(seen, want.marker) {
			t.Errorf("%s: the %s never reached the wire (%q absent from URL and body)", op, want.what, want.marker)
		}
	}
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

// testRequest covers the drop-a-field failure: sdk.Request must be what the
// provider encodes, not a subset of it.
func testRequest(t *testing.T, f Fixture) {
	ctx := context.Background()
	p, rec := serve(t, f, f.Reply)
	req := f.withOptions(p, request())
	req.Model = f.ModelID
	if _, err := sdk.Generate(ctx, f.model(p), req); err != nil {
		t.Fatalf("generate: %v", err)
	}
	wantSentOnWire(t, rec, f.ModelID, "request")
	wantOptionsOnWire(t, rec, f, "request")
}

// testGenerate covers the map-the-response failure: the wire reply must arrive
// as a ModelResult whose fields mean what the reply said.
func testGenerate(t *testing.T, f Fixture) {
	ctx := context.Background()
	p, rec := serve(t, f, f.Reply)
	req := f.withOptions(p, request())
	req.Model = f.ModelID
	got, err := sdk.Generate(ctx, f.model(p), req)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	wantSentOnWire(t, rec, f.ModelID, "generate")
	wantOptionsOnWire(t, rec, f, "generate")
	wantResult(t, "generate", f, got)
}

// testStream covers the paths-disagree failure: the streamed reply carries the
// same logical response as the non-streamed one, so the assembled ModelResult
// must say the same thing.
func testStream(t *testing.T, f Fixture) {
	ctx := context.Background()
	if f.ReplyStream == nil {
		t.Skip("provider does not stream")
	}
	p, rec := serve(t, f, f.ReplyStream)
	req := f.withOptions(p, request())
	req.Model = f.ModelID
	stream, err := sdk.Stream(ctx, f.model(p), req)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	var parts int
	for range stream.Parts {
		parts++
	}
	got, err := stream.Result()
	if err != nil {
		t.Fatalf("stream result: %v", err)
	}
	if parts == 0 {
		t.Fatal("stream produced no parts")
	}
	if got == nil {
		t.Fatal("stream produced no result")
	}
	// Most providers encode the streaming request on a different path than the
	// non-streaming one (stream=true, different instruction and tool framing),
	// so the markers are checked again here rather than inferred from generate.
	// This must happen after the parts are drained: a provider sends the request
	// from the goroutine that produces them.
	wantSentOnWire(t, rec, f.ModelID, "stream")
	wantOptionsOnWire(t, rec, f, "stream")
	wantResult(t, "stream", f, *got)
}

// testError covers the swallow-the-error failure: a provider-shaped error reply
// must become an error rather than an empty success.
func testError(t *testing.T, f Fixture) {
	ctx := context.Background()
	if f.ReplyError == nil {
		t.Skip("provider has no error fixture")
	}
	p, _ := serve(t, f, f.ReplyError)
	req := f.withOptions(p, request())
	req.Model = f.ModelID
	if result, err := sdk.Generate(ctx, f.model(p), req); err == nil {
		t.Fatalf("an error reply mapped to a success: %+v", result)
	}
}

// wantResult asserts the provider-neutral meaning of a result, on the same
// substantive fields for both paths.
func wantResult(t *testing.T, op string, f Fixture, got sdk.ModelResult) {
	t.Helper()
	if want := f.Want.Text; got.Text != want {
		t.Errorf("%s: text = %q, want %q", op, got.Text, want)
	}
	if want := f.Want.Reasoning; got.Reasoning != want {
		t.Errorf("%s: reasoning = %q, want %q", op, got.Reasoning, want)
	}
	if !f.Caps.NoFinishReason {
		if want := f.Want.FinishReason; got.FinishReason != want {
			t.Errorf("%s: finish reason = %q, want %q", op, got.FinishReason, want)
		}
	}
	if !f.Caps.NoUsage && f.Want.TotalTokens != 0 && got.Usage.TotalTokens != f.Want.TotalTokens {
		t.Errorf("%s: total tokens = %d, want %d", op, got.Usage.TotalTokens, f.Want.TotalTokens)
	}
	if len(got.ToolCalls) != len(f.Want.ToolCalls) {
		t.Fatalf("%s: %d tool calls, want %d (%+v)", op, len(got.ToolCalls), len(f.Want.ToolCalls), got.ToolCalls)
	}
	for i, want := range f.Want.ToolCalls {
		got := got.ToolCalls[i]
		if got.ToolName != want.ToolName {
			t.Errorf("%s: tool call %d name = %s, want %s", op, i, got.ToolName, want.ToolName)
		}
		// An empty ToolCallID means the wire format carries no id and the
		// provider must mint one, so only non-emptiness is required.
		if want.ToolCallID == "" {
			if got.ToolCallID == "" {
				t.Errorf("%s: tool call %d has no id, want a generated one", op, i)
			}
		} else if got.ToolCallID != want.ToolCallID {
			t.Errorf("%s: tool call %d id = %s, want %s", op, i, got.ToolCallID, want.ToolCallID)
		}
		if a, b := jsonOf(got.Input), jsonOf(want.Input); a != b {
			t.Errorf("%s: tool call %d input = %s, want %s", op, i, a, b)
		}
	}
	if f.Want.Response != nil {
		if jsonOf(got.Response) != jsonOf(f.Want.Response) {
			t.Errorf("%s: response metadata = %s, want %s", op, jsonOf(got.Response), jsonOf(f.Want.Response))
		}
	}
}

func jsonOf(v any) string {
	encoded, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("<unencodable: %v>", err)
	}
	return string(encoded)
}
