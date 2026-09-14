package decision_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/felinics/twilight/agent/decision"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/loop"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/chatlog"
	"github.com/felinics/twilight/agent/session/extension"
	"github.com/felinics/twilight/agent/turn"
)

// fixedSource serves one Context state at one head, the way an authority or
// an observer would for the same stream position.
type fixedSource struct {
	state chatlog.Context
	head  session.Head
}

func (s fixedSource) Load(_ context.Context, _ session.SessionID, id extension.ProjectionID, _ extension.ProjectionVersion) (any, session.Head, error) {
	if id != chatlog.ContextProjectionID {
		return nil, session.Head{}, errors.New("unexpected projection")
	}
	return s.state, s.head, nil
}

func preset() turn.AgentPreset {
	return turn.AgentPreset{SchemaVersion: 1, Model: "m-1", Prompt: decision.PromptContextV1, SystemPrompt: "be brief"}
}

func entries() chatlog.Context {
	in := chatlog.Input{ID: "in-1", TurnID: "t1", Content: decision.InputContent("hello")}
	as := chatlog.Assistant{ID: "a-1", TurnID: "t1", Parts: chatlog.Parts{chatlog.TextPart{Text: "hi"}}}
	return chatlog.Context{Entries: []chatlog.Entry{
		{Kind: chatlog.EntryInput, ID: "in-1", Seq: 1, Input: &in},
		{Kind: chatlog.EntryAssistant, ID: "a-1", Seq: 2, Assistant: &as},
	}}
}

// DEC-CAT-2 / DEC-PMT-1: two authorities resolving the same AgentPreset
// against the same projection state build the same prompt; the registry
// refuses refs it does not hold.
func TestPromptBuildersResolveDeterministically(t *testing.T) {
	src := fixedSource{state: entries(), head: session.Head{Next: 3, Digest: "d3"}}
	input := run.PromptInput{Session: "s", Inputs: []run.AgentInput{{ID: "in-1", Payload: decision.InputContent("hello")}}}
	var prompts []loop.Prompt
	for i := 0; i < 2; i++ {
		builders := decision.DefaultPromptBuilders() // a fresh process builds its own registry
		builder, err := builders.Resolve(preset(), src)
		if err != nil {
			t.Fatal(err)
		}
		prompt, err := builder.Build(context.Background(), input)
		if err != nil {
			t.Fatal(err)
		}
		prompts = append(prompts, prompt)
	}
	if !reflect.DeepEqual(prompts[0], prompts[1]) {
		t.Fatalf("prompts differ across processes:\n%+v\n%+v", prompts[0], prompts[1])
	}
	if prompts[0].Token != "3:d3" || len(prompts[0].Request.Messages) != 3 || prompts[0].Model != "m-1" {
		t.Fatalf("prompt = %+v", prompts[0])
	}

	p := preset()
	p.Prompt = "x/builder"
	if _, err := decision.DefaultPromptBuilders().Resolve(p, src); !errors.Is(err, decision.ErrUnknownPromptBuilder) {
		t.Fatalf("unknown builder: err = %v, want %v", err, decision.ErrUnknownPromptBuilder)
	}
	var none *decision.PromptBuilders
	if _, err := none.Resolve(preset(), src); err == nil {
		t.Fatal("nil registry resolved")
	}
}

// DEC-CAT-1: registration rejects empty refs, nil factories and duplicates.
func TestPromptBuilderRegistration(t *testing.T) {
	builders, err := decision.NewPromptBuilders(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := builders.Register("", decision.NewContextPromptBuilder); err == nil {
		t.Fatal("empty builder ref accepted")
	}
	if err := builders.Register("p", nil); err == nil {
		t.Fatal("nil factory accepted")
	}
	if err := builders.Register("p", decision.NewContextPromptBuilder); err != nil {
		t.Fatal(err)
	}
	if err := builders.Register("p", decision.NewContextPromptBuilder); err == nil {
		t.Fatal("duplicate builder accepted")
	}
}

// DEC-INP-1: the v1 input shape round-trips.
func TestInputContentRoundTrip(t *testing.T) {
	for _, text := range []string{"hello", "", `quote " and \ slash`, "多字节"} {
		got, err := decision.InputText(decision.InputContent(text))
		if err != nil || got != text {
			t.Fatalf("round trip %q: got %q err %v", text, got, err)
		}
	}
}

func TestPromptRejectsUnpairedToolHistory(t *testing.T) {
	call := chatlog.Entry{Kind: chatlog.EntryAssistant, Assistant: &chatlog.Assistant{Parts: chatlog.Parts{
		chatlog.ToolCallPart{CallID: "call", ProviderCallID: "provider-call", Name: "tool", Input: run.MustParseCanonicalJSON(`{}`)},
	}}}
	result := chatlog.Entry{Kind: chatlog.EntryToolResult, ToolResult: &chatlog.ToolResult{CallID: "call", Status: chatlog.ToolError}}
	input := chatlog.Entry{Kind: chatlog.EntryInput, Input: &chatlog.Input{Content: decision.InputContent("next")}}
	for _, tc := range []struct {
		name    string
		entries []chatlog.Entry
	}{
		{"unfinished call", []chatlog.Entry{call, input}},
		{"orphan result", []chatlog.Entry{result}},
		{"duplicate result", []chatlog.Entry{call, result, result}},
		{"interleaved assistant", []chatlog.Entry{call, {Kind: chatlog.EntryAssistant}, result}},
		{"interleaved summary", []chatlog.Entry{call, {Kind: chatlog.EntrySummary}, result}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			builder := decision.NewContextPromptBuilder(preset(), fixedSource{state: chatlog.Context{Entries: tc.entries}})
			if _, err := builder.Build(context.Background(), run.PromptInput{Session: "s"}); err == nil {
				t.Fatal("unpaired history produced a provider request")
			}
		})
	}
	builder := decision.NewContextPromptBuilder(preset(), fixedSource{state: chatlog.Context{Entries: []chatlog.Entry{call, input, result}}})
	prompt, err := builder.Build(context.Background(), run.PromptInput{Session: "s"})
	if err != nil {
		t.Fatal(err)
	}
	msgs := prompt.Request.Messages
	if len(msgs) != 4 || msgs[2].Role != "tool" || msgs[3].Role != "user" {
		t.Fatalf("paired context = %+v", msgs)
	}
}
