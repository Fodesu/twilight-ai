package messages_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/memohai/twilight-ai/provider/anthropic/messages"
	"github.com/memohai/twilight-ai/sdk"
)

// capturedThinking mirrors the wire shape of the request fields this file cares
// about, so assertions run against the JSON that actually leaves the process
// rather than against internal state.
type capturedThinking struct {
	MaxTokens int `json:"max_tokens"`
	Thinking  *struct {
		Type         string `json:"type"`
		BudgetTokens *int   `json:"budget_tokens"`
	} `json:"thinking"`
	OutputConfig *struct {
		Effort string `json:"effort"`
	} `json:"output_config"`
}

// captureRequest runs one DoGenerate against a stub server and returns the
// decoded request body.
func captureRequest(t *testing.T, opts []messages.Option, params sdk.GenerateParams) capturedThinking {
	t.Helper()

	var captured capturedThinking
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": "msg_capture", "type": "message", "model": "claude-test", "role": "assistant",
			"content":     []map[string]any{{"type": "text", "text": "OK"}},
			"stop_reason": "end_turn",
			"usage":       map[string]any{"input_tokens": 1, "output_tokens": 1},
		})
	}))
	defer srv.Close()

	all := append([]messages.Option{
		messages.WithAPIKey("test-key"),
		messages.WithBaseURL(srv.URL),
	}, opts...)

	params.Model = &sdk.Model{ID: "claude-test"}
	if params.Messages == nil {
		params.Messages = []sdk.Message{sdk.UserMessage("Hi")}
	}

	if _, err := messages.New(all...).DoGenerate(context.Background(), params); err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}
	return captured
}

func effortPtr(s string) *string { return &s }

func intPtr(i int) *int { return &i }

// TestResolveMaxTokens_Matrix pins the max_tokens arithmetic across every
// combination of thinking mode and reasoning effort.
//
// This is the regression surface flagged in the design doc: max_tokens is
// derived from whether thinking is active and whether it carries an explicit
// budget. Getting it wrong silently truncates thinking output on legacy Claude
// models, which no other assertion in this package would catch. The expected
// values here were captured from the pre-union implementation and must not
// change.
func TestResolveMaxTokens_Matrix(t *testing.T) {
	const (
		defaultMax   = 4096  // no reasoning at all
		reasoningMax = 32000 // reasoning active without an explicit budget
	)

	tests := []struct {
		name          string
		opts          []messages.Option
		effort        *string
		explicitMax   *int
		wantMaxTokens int
	}{
		// --- enabled: budget is added on top of the answer budget ---
		{
			name:          "enabled/budget 1024",
			opts:          []messages.Option{messages.WithThinkingEnabled(1024)},
			wantMaxTokens: defaultMax + 1024,
		},
		{
			name:          "enabled/budget 5000",
			opts:          []messages.Option{messages.WithThinkingEnabled(5000)},
			wantMaxTokens: defaultMax + 5000,
		},
		{
			name:          "enabled/budget 50000",
			opts:          []messages.Option{messages.WithThinkingEnabled(50000)},
			wantMaxTokens: defaultMax + 50000,
		},
		{
			// A zero budget is not an explicit budget, so this falls through to
			// the reasoning-aware default rather than staying at 4096.
			name:          "enabled/budget 0",
			opts:          []messages.Option{messages.WithThinkingEnabled(0)},
			wantMaxTokens: reasoningMax,
		},
		{
			name:          "enabled/budget 8000 + effort",
			opts:          []messages.Option{messages.WithThinkingEnabled(8000)},
			effort:        effortPtr("high"),
			wantMaxTokens: defaultMax + 8000,
		},

		// --- adaptive: never carries a budget ---
		{
			name:          "adaptive",
			opts:          []messages.Option{messages.WithThinkingAdaptive()},
			wantMaxTokens: reasoningMax,
		},
		{
			name:          "adaptive + effort",
			opts:          []messages.Option{messages.WithThinkingAdaptive()},
			effort:        effortPtr("xhigh"),
			wantMaxTokens: reasoningMax,
		},

		// --- disabled: reasoning is off, so the plain default applies ---
		{
			name:          "disabled",
			opts:          []messages.Option{messages.WithThinkingDisabled()},
			wantMaxTokens: defaultMax,
		},
		{
			// output_config.effort still counts as reasoning even when the
			// thinking block is explicitly disabled.
			name:          "disabled + effort",
			opts:          []messages.Option{messages.WithThinkingDisabled()},
			effort:        effortPtr("medium"),
			wantMaxTokens: reasoningMax,
		},

		// --- effort only, no thinking option ---
		{
			name:          "effort only",
			effort:        effortPtr("high"),
			wantMaxTokens: reasoningMax,
		},
		{
			name:          "effort blank is not reasoning",
			effort:        effortPtr("   "),
			wantMaxTokens: defaultMax,
		},

		// --- neither ---
		{
			name:          "no thinking, no effort",
			wantMaxTokens: defaultMax,
		},

		// --- explicit MaxTokens always wins ---
		{
			name:          "explicit max_tokens beats enabled budget",
			opts:          []messages.Option{messages.WithThinkingEnabled(5000)},
			explicitMax:   intPtr(777),
			wantMaxTokens: 777,
		},
		{
			name:          "explicit max_tokens beats adaptive",
			opts:          []messages.Option{messages.WithThinkingAdaptive()},
			explicitMax:   intPtr(888),
			wantMaxTokens: 888,
		},
		{
			name:          "explicit max_tokens beats effort",
			effort:        effortPtr("high"),
			explicitMax:   intPtr(999),
			wantMaxTokens: 999,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := captureRequest(t, tc.opts, sdk.GenerateParams{
				ReasoningEffort: tc.effort,
				MaxTokens:       tc.explicitMax,
			})
			if got.MaxTokens != tc.wantMaxTokens {
				t.Errorf("max_tokens: got %d, want %d", got.MaxTokens, tc.wantMaxTokens)
			}
		})
	}
}

// TestThinkingWireShape checks what each option actually serializes to,
// independent of the max_tokens arithmetic.
func TestThinkingWireShape(t *testing.T) {
	tests := []struct {
		name           string
		opts           []messages.Option
		wantAbsent     bool
		wantType       string
		wantBudget     *int
		wantBudgetSent bool
	}{
		{
			name:           "enabled sends type and budget",
			opts:           []messages.Option{messages.WithThinkingEnabled(5000)},
			wantType:       "enabled",
			wantBudget:     intPtr(5000),
			wantBudgetSent: true,
		},
		{
			name:     "adaptive sends type only",
			opts:     []messages.Option{messages.WithThinkingAdaptive()},
			wantType: "adaptive",
		},
		{
			// Disabled thinking is expressed by omitting the block entirely,
			// matching the pre-union behaviour.
			name:       "disabled omits the thinking block",
			opts:       []messages.Option{messages.WithThinkingDisabled()},
			wantAbsent: true,
		},
		{
			name:       "no option omits the thinking block",
			wantAbsent: true,
		},
		{
			name:     "last thinking option wins",
			opts:     []messages.Option{messages.WithThinkingEnabled(5000), messages.WithThinkingAdaptive()},
			wantType: "adaptive",
		},
		{
			name:       "adaptive then disabled omits the block",
			opts:       []messages.Option{messages.WithThinkingAdaptive(), messages.WithThinkingDisabled()},
			wantAbsent: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := captureRequest(t, tc.opts, sdk.GenerateParams{})

			if tc.wantAbsent {
				if got.Thinking != nil {
					t.Fatalf("thinking should be absent, got %+v", *got.Thinking)
				}
				return
			}

			if got.Thinking == nil {
				t.Fatalf("thinking should be present, got nil")
			}
			if got.Thinking.Type != tc.wantType {
				t.Errorf("thinking.type: got %q, want %q", got.Thinking.Type, tc.wantType)
			}

			switch {
			case tc.wantBudgetSent:
				if got.Thinking.BudgetTokens == nil {
					t.Errorf("budget_tokens: got absent, want %d", *tc.wantBudget)
				} else if *got.Thinking.BudgetTokens != *tc.wantBudget {
					t.Errorf("budget_tokens: got %d, want %d", *got.Thinking.BudgetTokens, *tc.wantBudget)
				}
			default:
				if got.Thinking.BudgetTokens != nil {
					t.Errorf("budget_tokens must be omitted for %s, got %d", tc.wantType, *got.Thinking.BudgetTokens)
				}
			}
		})
	}
}

// TestWithThinking_BackwardCompatibility proves the legacy flat-struct API
// still produces byte-identical wire output after the union refactor.
//
// ThinkingConfig and WithThinking are exported and in use downstream (Memoh
// constructs them by generation), so the union is additive: the old path must
// keep working, not merely compile.
func TestWithThinking_BackwardCompatibility(t *testing.T) {
	tests := []struct {
		name   string
		legacy messages.ThinkingConfig
		modern []messages.Option
	}{
		{
			name:   "enabled with budget",
			legacy: messages.ThinkingConfig{Type: "enabled", BudgetTokens: 5000},
			modern: []messages.Option{messages.WithThinkingEnabled(5000)},
		},
		{
			name:   "adaptive",
			legacy: messages.ThinkingConfig{Type: "adaptive"},
			modern: []messages.Option{messages.WithThinkingAdaptive()},
		},
		{
			name:   "disabled",
			legacy: messages.ThinkingConfig{Type: "disabled"},
			modern: []messages.Option{messages.WithThinkingDisabled()},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			legacy := captureRequest(t, []messages.Option{messages.WithThinking(tc.legacy)}, sdk.GenerateParams{})
			modern := captureRequest(t, tc.modern, sdk.GenerateParams{})

			legacyJSON, err := json.Marshal(legacy)
			if err != nil {
				t.Fatalf("marshal legacy: %v", err)
			}
			modernJSON, err := json.Marshal(modern)
			if err != nil {
				t.Fatalf("marshal modern: %v", err)
			}
			if string(legacyJSON) != string(modernJSON) {
				t.Errorf("wire output diverged:\n legacy = %s\n modern = %s", legacyJSON, modernJSON)
			}
		})
	}
}

// TestWithThinking_LegacyExactWire pins the exact bytes the legacy call path
// produced before the union existed, so a refactor cannot quietly change what
// downstream callers send.
func TestWithThinking_LegacyExactWire(t *testing.T) {
	got := captureRequest(t,
		[]messages.Option{messages.WithThinking(messages.ThinkingConfig{Type: "enabled", BudgetTokens: 5000})},
		sdk.GenerateParams{},
	)

	if got.Thinking == nil {
		t.Fatal("thinking block missing")
	}
	if got.Thinking.Type != "enabled" {
		t.Errorf("thinking.type: got %q, want %q", got.Thinking.Type, "enabled")
	}
	if got.Thinking.BudgetTokens == nil || *got.Thinking.BudgetTokens != 5000 {
		t.Errorf("budget_tokens: got %v, want 5000", got.Thinking.BudgetTokens)
	}
	if got.MaxTokens != 4096+5000 {
		t.Errorf("max_tokens: got %d, want %d", got.MaxTokens, 4096+5000)
	}
	if got.OutputConfig != nil {
		t.Errorf("output_config should be absent, got %+v", *got.OutputConfig)
	}
}

// TestWithThinking_LegacyUnknownType keeps the pre-union passthrough behaviour
// for type strings the SDK does not recognise: they are forwarded verbatim and
// the server decides. The SDK does not validate what a caller declared.
func TestWithThinking_LegacyUnknownType(t *testing.T) {
	got := captureRequest(t,
		[]messages.Option{messages.WithThinking(messages.ThinkingConfig{Type: "future_mode", BudgetTokens: 1234})},
		sdk.GenerateParams{},
	)

	if got.Thinking == nil {
		t.Fatal("thinking block missing")
	}
	if got.Thinking.Type != "future_mode" {
		t.Errorf("thinking.type: got %q, want %q", got.Thinking.Type, "future_mode")
	}
	if got.Thinking.BudgetTokens == nil || *got.Thinking.BudgetTokens != 1234 {
		t.Errorf("budget_tokens: got %v, want 1234", got.Thinking.BudgetTokens)
	}
	if got.MaxTokens != 4096+1234 {
		t.Errorf("max_tokens: got %d, want %d", got.MaxTokens, 4096+1234)
	}
}

// TestWithThinking_LegacyEmptyType covers the zero-value ThinkingConfig, which
// the pre-union code treated as "no thinking configured".
func TestWithThinking_LegacyEmptyType(t *testing.T) {
	got := captureRequest(t,
		[]messages.Option{messages.WithThinking(messages.ThinkingConfig{})},
		sdk.GenerateParams{},
	)

	if got.Thinking != nil {
		t.Errorf("thinking should be absent for a zero-value config, got %+v", *got.Thinking)
	}
	if got.MaxTokens != 4096 {
		t.Errorf("max_tokens: got %d, want 4096", got.MaxTokens)
	}
}
