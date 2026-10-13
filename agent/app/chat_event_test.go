package app

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/observe"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/agentcore/run/sessionstore"
)

func TestChatFactNormalization(t *testing.T) {
	cases := []struct {
		fact    run.Fact
		started string
		ended   string
		runEnd  bool
	}{
		{run.ModelStepStarted{Effect: "e"}, "model", "", false},
		{run.ToolCallStarted{Effect: "e", CallID: "call"}, "tool", "", false},
		{run.ModelStepCompleted{Effect: "e"}, "", "e", false},
		{run.ModelStepFailed{Effect: "e"}, "", "e", false},
		{run.ModelStepRecovered{Effect: "e"}, "", "e", false},
		{run.ModelStepRejected{Effect: "e"}, "", "e", false},
		{run.ToolCallCompleted{Effect: "e"}, "", "e", false},
		{run.ToolCallFailed{Effect: "e"}, "", "e", false},
		{run.ToolCallFailed{}, "", "", false},
		{run.RunEnded{End: run.RunCompletedEnd{}}, "", "", true},
		{run.InputAccepted{}, "", "", false},
	}
	for _, tc := range cases {
		event := Event{Session: "s", Position: ledger.Position{Commit: 9, Index: 3}, Row: ledger.Event{Type: "durable"},
			Value: sessionstore.Event{RunID: "r", Fact: tc.fact}}
		normalized, ok, err := NormalizeChatEvent(event)
		if err != nil || !ok {
			t.Fatalf("%T: %v %v", tc.fact, ok, err)
		}
		assertChatFact(t, normalized.(ChatCommitted), tc.started, tc.ended, tc.runEnd)
	}
}

func assertChatFact(t *testing.T, event ChatCommitted, started, ended string, runEnd bool) {
	t.Helper()
	if event.Kind != "committed" || event.Session != "s" || event.Cursor != (ledger.Position{Commit: 9, Index: 3}) {
		t.Fatalf("identity: %+v", event)
	}
	wantStarted := []ChatActiveEffect{}
	if started != "" {
		label := "model"
		if started == "tool" {
			label = "call"
		}
		wantStarted = append(wantStarted, chatEffect("r", "e", started, label))
	}
	wantEnded := []ChatEffectKey{}
	if ended != "" {
		wantEnded = append(wantEnded, ChatEffectKey{RunID: "r", EffectID: ended})
	}
	wantRuns := []string{}
	if runEnd {
		wantRuns = append(wantRuns, "r")
	}
	if !reflect.DeepEqual(event.EffectsStarted, wantStarted) || !reflect.DeepEqual(event.EffectsEnded, wantEnded) || !reflect.DeepEqual(event.EndedRuns, wantRuns) {
		t.Fatalf("settlement: %+v", event)
	}
}

func TestChatUnknownAndErrors(t *testing.T) {
	event := Event{Session: "s", Row: ledger.Event{Type: "future"}, Unknown: true}
	value, ok, err := NormalizeChatEvent(event)
	if err != nil || !ok {
		t.Fatalf("unknown event: %v %v", ok, err)
	}
	wire, err := json.Marshal(value)
	if err != nil || string(wire) != `{"kind":"committed","session":"s","cursor":{"commit":0,"index":0},"effectsStarted":[],"effectsEnded":[],"endedRuns":[]}` {
		t.Fatalf("wire: %s %v", wire, err)
	}
	event.Err = errors.New("decode failed")
	value, ok, err = NormalizeChatEvent(event)
	if !errors.Is(err, event.Err) || ok || value != nil {
		t.Fatalf("error became durable: %v %v %v", value, ok, err)
	}
}

func TestChatProgressNormalization(t *testing.T) {
	cases := []struct {
		kind    loop.EventKind
		payload string
		update  string
		text    string
	}{
		{loop.EventModelTextDelta, `"hello\nworld"`, "text_delta", "hello\nworld"},
		{loop.EventModelReasoningDelta, `"thinking"`, "reasoning_delta", "thinking"},
		{loop.EventToolProgress, ` {"done":2,"total":8} `, "tool_progress", ` {"done":2,"total":8} `},
		{loop.EventToolProgress, `null`, "tool_progress", `null`},
		{loop.EventToolProgress, `42`, "tool_progress", `42`},
		{loop.EventModelTextDelta, ` "quoted" `, "text_delta", "quoted"},
		{loop.EventProgressReset, "", "reset", ""},
		{loop.EventToolStarted, `"ignored"`, "", ""},
		{loop.EventRunFinished, `"ignored"`, "", ""},
		{loop.EventKind("future"), `"ignored"`, "", ""},
	}
	for _, tc := range cases {
		p := observe.Progress{RunID: "r", Effect: "e", Generation: 4, Sequence: 7, Kind: string(tc.kind), Payload: json.RawMessage(tc.payload)}
		value, ok, err := NormalizeChatEvent(Event{Session: "s", Progress: &p})
		if err != nil || ok != (tc.update != "") {
			t.Fatalf("%s: %v %v", tc.kind, ok, err)
		}
		if ok {
			assertChatProgress(t, value.(ChatProgress), tc.update, tc.text)
		}
	}
}

func assertChatProgress(t *testing.T, got ChatProgress, update, text string) {
	t.Helper()
	want := ChatProgress{Kind: "progress", Session: "s", ChatEffectKey: ChatEffectKey{RunID: "r", EffectID: "e"},
		Generation: 4, Sequence: 7, Update: update, Text: text}
	if got != want {
		t.Fatalf("progress: %+v, want %+v", got, want)
	}
}

func TestChatSnapshotRetryBoundAndContext(t *testing.T) {
	calls := 0
	read := func() (ChatSnapshot, bool, error) {
		calls++
		return ChatSnapshot{}, false, nil
	}
	_, err := retryChatSnapshot(context.Background(), read)
	if !errors.Is(err, ErrChatSnapshotBusy) || calls != 8 {
		t.Fatalf("busy: %v calls=%d", err, calls)
	}
	ctx, cancel := context.WithCancel(context.Background())
	calls = 0
	_, err = retryChatSnapshot(ctx, func() (ChatSnapshot, bool, error) {
		calls++
		cancel()
		return ChatSnapshot{}, false, nil
	})
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("cancel: %v calls=%d", err, calls)
	}
	calls = 0
	value, err := retryChatSnapshot(context.Background(), func() (ChatSnapshot, bool, error) {
		calls++
		return ChatSnapshot{Version: 2}, calls == 3, nil
	})
	if err != nil || calls != 3 || value.Version != 2 {
		t.Fatalf("retry success: %+v %v calls=%d", value, err, calls)
	}
}

func TestChatMachineEffects(t *testing.T) {
	state := run.MachineState{Current: run.ToolStep{Calls: []run.ToolCallState{
		{ToolRef: "lookup", Effect: "e", Status: run.ToolExecuting},
		{ToolRef: "pending", Status: run.ToolPending},
		{ToolRef: "waiting", Status: run.ToolWaiting},
	}}}
	got := chatMachineEffects("r", &state)
	want := []ChatActiveEffect{chatEffect("r", "e", "tool", "lookup")}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("tool effects: %+v", got)
	}
	state.Current = run.ModelStep{Status: run.ModelPrepared, Effect: "old"}
	if got := chatMachineEffects("r", &state); len(got) != 0 {
		t.Fatalf("prepared model is not active: %+v", got)
	}
}
