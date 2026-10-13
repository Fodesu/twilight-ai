package http_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	stdhttp "net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/felinics/twilight/agent/app"
	ownerhttp "github.com/felinics/twilight/agent/app/http"
	"github.com/felinics/twilight/agentcore/chatlog"
	"github.com/felinics/twilight/agentcore/inbox"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/observe"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/agentcore/run/sessionstore"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/sessionkernel"
	"github.com/felinics/twilight/agentcore/turn"
)

func TestChatCapabilitiesHTTP(t *testing.T) {
	_, client := newOwner(t)
	var capabilities app.ChatCapabilities
	chatGetJSON(t, context.Background(), client.BaseURL+"/chat/capabilities", &capabilities)
	want := app.ChatCapabilities{Version: 2, Submit: true, Stop: true, Resume: true}
	if capabilities != want {
		t.Fatalf("capabilities: %+v", capabilities)
	}
	server := httptest.NewServer((&ownerhttp.Server{App: &app.Application{}}).Handler())
	defer server.Close()
	chatGetJSON(t, context.Background(), server.URL+"/chat/capabilities", &capabilities)
	if capabilities != (app.ChatCapabilities{Version: 2}) {
		t.Fatalf("no inbox capabilities: %+v", capabilities)
	}
}

func TestChatSnapshotEmptyAndCompletedHTTP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	a, client := newOwner(t)
	const sid session.SessionID = "chat-history"
	if err := client.Ensure(ctx, sid); err != nil {
		t.Fatal(err)
	}
	var empty map[string]json.RawMessage
	chatGetJSON(t, ctx, client.BaseURL+"/sessions/chat-history/chat/snapshot", &empty)
	for _, field := range []string{"transcript", "turns", "activeEffects", "approvals"} {
		if string(empty[field]) != "[]" {
			t.Fatalf("empty %s: %s", field, empty[field])
		}
	}
	if _, err := client.Open(ctx, sid, ownerhttp.OpenRequest{Preset: "p1"}); err != nil {
		t.Fatal(err)
	}
	chatSubmit(t, ctx, a, client, sid, "hello")
	var snapshot app.ChatSnapshot
	chatGetJSON(t, ctx, client.BaseURL+"/sessions/chat-history/chat/snapshot", &snapshot)
	history, err := client.History(ctx, sid)
	if err != nil || snapshot.NextCommit != history.Head.Next || snapshot.Version != 2 || snapshot.Session != sid {
		t.Fatalf("watermark: %+v %v", snapshot, err)
	}
	if len(snapshot.Transcript) != 2 || len(snapshot.Turns) != 1 || len(snapshot.ActiveEffects) != 0 || len(snapshot.Approvals) != 0 {
		t.Fatalf("completed snapshot: %+v", snapshot)
	}
	if snapshot.Transcript[0].Text != "hello" || snapshot.Transcript[1].Text != "echo: hello" || snapshot.Turns[0].Status != "completed" {
		t.Fatalf("completed transcript/turn: %+v", snapshot)
	}
	if len(snapshot.Turns[0].InputIDs) != 1 || snapshot.Transcript[1].RunID != snapshot.Turns[0].RunID {
		t.Fatalf("turn identity: %+v", snapshot)
	}
	for i, message := range snapshot.Transcript {
		if message.Position != history.Messages[i].Position || message.Position.Commit >= snapshot.NextCommit {
			t.Fatalf("message watermark: %+v", message)
		}
	}
}

func TestChatEventsInclusiveReplayHTTP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	a, client := newOwner(t)
	const sid session.SessionID = "chat-replay"
	if err := client.Ensure(ctx, sid); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Open(ctx, sid, ownerhttp.OpenRequest{Preset: "p1"}); err != nil {
		t.Fatal(err)
	}
	chatSubmit(t, ctx, a, client, sid, "replay")
	history, err := a.History(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	from := history.Head.Next - 1
	res := chatOpenEvents(t, ctx, client.BaseURL+"/sessions/chat-replay/chat/events?from="+strconv.FormatUint(uint64(from), 10))
	defer res.Body.Close()
	scanner := bufio.NewScanner(res.Body)
	first := chatReadCommitted(t, scanner)
	second := chatReadCommitted(t, scanner)
	if first.Cursor.Commit != from || second.Cursor.Commit != from || second.Cursor.Index != first.Cursor.Index+1 {
		t.Fatalf("inclusive same-commit indexes: %+v %+v", first, second)
	}
	if len(first.EffectsEnded) != 1 || len(second.EndedRuns) != 1 || first.EffectsEnded[0].RunID != second.EndedRuns[0] {
		t.Fatalf("effect/run settlement: %+v %+v", first, second)
	}
}

func TestChatTransientResetAndFailureHTTP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	a, client := newOwner(t)
	const sid session.SessionID = "chat-progress"
	if err := client.Ensure(ctx, sid); err != nil {
		t.Fatal(err)
	}
	res := chatOpenEvents(t, ctx, client.BaseURL+"/sessions/chat-progress/chat/events?from=0")
	defer res.Body.Close()
	scanner := bufio.NewScanner(res.Body)
	// Flushed SSE headers establish the subscription before publishing.
	for _, kind := range []loop.EventKind{loop.EventToolStarted, loop.EventModelTextDelta, loop.EventModelReasoningDelta, loop.EventToolProgress, loop.EventProgressReset} {
		a.Execution.Progress.Publish(sid, observe.Progress{RunID: "r", Effect: "e", Generation: 2, Sequence: 3,
			Kind: string(kind), Payload: json.RawMessage(`"hello"`)})
	}
	for _, update := range []string{"text_delta", "reasoning_delta", "tool_progress", "reset"} {
		var progress app.ChatProgress
		chatReadEvent(t, scanner, &progress)
		if progress.Kind != "progress" || progress.Update != update || progress.Text != "hello" || progress.RunID != "r" || progress.EffectID != "e" || progress.Generation != 2 || progress.Sequence != 3 {
			t.Fatalf("typed progress: %+v", progress)
		}
	}
	a.Execution.Progress.Failed(sid, errors.New("observation failed"))
	if scanner.Scan() || scanner.Err() != nil {
		t.Fatalf("failure must close without Core/error payload: %q %v", scanner.Text(), scanner.Err())
	}
}

func TestChatInvalidCursorHTTP(t *testing.T) {
	_, client := newOwner(t)
	res := chatResponse(t, client.BaseURL+"/sessions/s/chat/events?from=invalid")
	defer res.Body.Close()
	if res.StatusCode != stdhttp.StatusBadRequest {
		t.Fatalf("invalid from: %s", res.Status)
	}
}

// skewChatProjections simulates a commit advancing exactly one read model.
// This exercises the actual three-reader consistency check through HTTP.
type skewChatProjections struct {
	skew  module.ProjectionID
	reads int
}

func (p *skewChatProjections) Load(_ context.Context, _ session.SessionID, id module.ProjectionID, _ module.ProjectionVersion) (any, ledger.Head, error) {
	p.reads++
	var state any
	var err error
	switch id {
	case chatlog.SurfaceProjectionID:
		state, err = chatlog.SurfaceProjection.Initial()
	case turn.SurfaceProjectionID:
		state, err = turn.SurfaceProjection.Initial()
	case sessionstore.MachineProjectionID:
		state, err = sessionstore.MachineProjection.Initial()
	}
	head := ledger.Head{}
	if id == p.skew {
		head.Next = 1
	}
	return state, head, err
}

func TestChatSnapshotBusyHTTP(t *testing.T) {
	for _, projection := range []module.ProjectionID{chatlog.SurfaceProjectionID, turn.SurfaceProjectionID, sessionstore.MachineProjectionID} {
		assertChatSnapshotBusyHTTP(t, projection)
	}
}

func assertChatSnapshotBusyHTTP(t *testing.T, projection module.ProjectionID) {
	t.Helper()
	reader := &skewChatProjections{skew: projection}
	a := &app.Application{Kernel: &sessionkernel.Kernel{Projections: reader}}
	server := httptest.NewServer((&ownerhttp.Server{App: a}).Handler())
	defer server.Close()
	res := chatResponse(t, server.URL+"/sessions/s/chat/snapshot")
	defer res.Body.Close()
	var body ownerhttp.Error
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != stdhttp.StatusServiceUnavailable || body.Code != ownerhttp.CodeUnavailable || reader.reads != 24 {
		t.Fatalf("skew %s: %s %+v reads=%d", projection, res.Status, body, reader.reads)
	}
}

func chatSubmit(t *testing.T, ctx context.Context, a *app.Application, client *ownerhttp.Client, sid session.SessionID, text string) {
	t.Helper()
	command, err := app.NewCommand(inbox.CommandID(text), app.CommandSubmit, app.SubmitCommand{InputID: "input", Text: text})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Enqueue(ctx, sid, command); err != nil {
		t.Fatal(err)
	}
	if result, err := client.Await(ctx, sid, command.ID); err != nil || result.Status != inbox.StatusApplied {
		t.Fatalf("submit: %+v %v", result, err)
	}
	opened, _ := a.Opened(sid)
	if err := opened.Wait(ctx); err != nil {
		t.Fatal(err)
	}
}

func chatResponse(t *testing.T, url string) *stdhttp.Response {
	t.Helper()
	req, err := stdhttp.NewRequestWithContext(t.Context(), stdhttp.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := stdhttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func chatGetJSON(t *testing.T, ctx context.Context, url string, out any) {
	t.Helper()
	req, err := stdhttp.NewRequestWithContext(ctx, stdhttp.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := stdhttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != stdhttp.StatusOK {
		t.Fatalf("GET %s: %s", url, res.Status)
	}
	if err := json.NewDecoder(res.Body).Decode(out); err != nil {
		t.Fatal(err)
	}
}

func chatOpenEvents(t *testing.T, ctx context.Context, url string) *stdhttp.Response {
	t.Helper()
	req, err := stdhttp.NewRequestWithContext(ctx, stdhttp.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := stdhttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != stdhttp.StatusOK || res.Header.Get("Content-Type") != "text/event-stream" {
		res.Body.Close()
		t.Fatalf("events response: %s", res.Status)
	}
	return res
}

func chatReadCommitted(t *testing.T, scanner *bufio.Scanner) app.ChatCommitted {
	t.Helper()
	var value map[string]json.RawMessage
	chatReadEvent(t, scanner, &value)
	for _, field := range []string{"type", "module", "version", "payload", "value", "unknown", "error"} {
		if _, exists := value[field]; exists {
			t.Fatalf("Core field leaked: %s", field)
		}
	}
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var committed app.ChatCommitted
	if err := json.Unmarshal(raw, &committed); err != nil || committed.Kind != "committed" {
		t.Fatalf("committed event: %s %v", raw, err)
	}
	return committed
}

func chatReadEvent(t *testing.T, scanner *bufio.Scanner, out any) {
	t.Helper()
	if !scanner.Scan() || !strings.HasPrefix(scanner.Text(), "data: ") {
		t.Fatalf("SSE data: %q %v", scanner.Text(), scanner.Err())
	}
	if err := json.Unmarshal([]byte(strings.TrimPrefix(scanner.Text(), "data: ")), out); err != nil {
		t.Fatal(err)
	}
	if !scanner.Scan() || scanner.Text() != "" {
		t.Fatalf("SSE separator: %q %v", scanner.Text(), scanner.Err())
	}
}
