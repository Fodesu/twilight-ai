package ref_test

import (
	"context"
	"testing"

	"github.com/felinics/twilight/agent/ref"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/filestore"
	"github.com/felinics/twilight/sdk"
)

// gateModel blocks its first Generate until release, capturing the request:
// the process "crashes" while the ModelStep is Executing.
type gateModel struct {
	started chan sdk.Request
	release chan struct{}
}

func (m *gateModel) Generate(_ context.Context, req sdk.Request) (sdk.ModelResult, error) {
	m.started <- req
	<-m.release
	return sdk.ModelResult{Text: "late", FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}}, nil
}

// The file-backed FrozenValueStore closes the model-interruption recovery
// path: process 1 dies while a ModelStep is Executing, and process 2 — whose
// FrozenValues instance is new, so the body can only come from disk — takes
// over (RecoverModelExecution returns the step to Prepared) and Resume
// replays the same frozen request to its model (RUN-WIR-4, RUN-CMT-7).
func TestFrozenRequestReplayedAcrossRestart(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	const sid session.SessionID = "s-frozen"

	// ---- process 1: the model call hangs; the process dies -------------------
	store1, err := filestore.New(root)
	if err != nil {
		t.Fatal(err)
	}
	frozen1, err := filestore.NewFrozenValues(root)
	if err != nil {
		t.Fatal(err)
	}
	p1, err := ref.New(ref.Options{Store: store1, Frozen: frozen1})
	if err != nil {
		t.Fatal(err)
	}
	gate := &gateModel{started: make(chan sdk.Request, 1), release: make(chan struct{})}
	agent1, err := ref.NewAgent("m-1", gate, ref.WithSystemPrompt("be brief"))
	if err != nil {
		t.Fatal(err)
	}
	profile, err := p1.Agents.Register("a1", agent1)
	if err != nil {
		t.Fatal(err)
	}
	s1, err := p1.OpenSession(ctx, sid, ref.SessionOptions{Profile: profile})
	if err != nil {
		t.Fatal(err)
	}
	sendErr := make(chan error, 1)
	go func() {
		_, err := s1.Send(ctx, "what is the weather?")
		sendErr <- err
	}()
	var sent sdk.Request
	select {
	case sent = <-gate.started: // the ModelStep is Executing; its body is on disk
	case err := <-sendErr:
		t.Fatalf("send returned before the model executed: %v", err)
	}

	// ---- process 2: fresh instances over the same root ------------------------
	store2, err := filestore.New(root)
	if err != nil {
		t.Fatal(err)
	}
	frozen2, err := filestore.NewFrozenValues(root)
	if err != nil {
		t.Fatal(err)
	}
	p2, err := ref.New(ref.Options{Store: store2, Frozen: frozen2, Ownership: session.OpenOptions{Takeover: true}})
	if err != nil {
		t.Fatal(err)
	}
	replay := &scriptedRequests{}
	agent2, err := ref.NewAgent("m-1", replay, ref.WithSystemPrompt("be brief"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p2.Agents.Register("a1", agent2); err != nil {
		t.Fatal(err)
	}
	s2, err := p2.OpenSession(ctx, sid, ref.SessionOptions{Profile: profile})
	if err != nil {
		t.Fatal(err)
	}
	if s2.Recovered != 1 {
		t.Fatalf("recovered = %d, want 1 (the executing ModelStep)", s2.Recovered)
	}
	results, resumed, err := s2.Resume(ctx)
	if err != nil || !resumed || len(results) == 0 {
		t.Fatalf("resume = %+v %v %v", results, resumed, err)
	}
	if results[0].Status != "completed" {
		t.Fatalf("turn = %s, want completed (a missing frozen body would leave it waiting)", results[0].Status)
	}

	// The replayed request is the frozen one: same conversation, read from disk
	// by a store instance that never saw the Put.
	if len(replay.seen) != 1 {
		t.Fatalf("replay model saw %d requests, want 1", len(replay.seen))
	}
	if got, want := len(replay.seen[0].Messages), len(sent.Messages); got != want {
		t.Fatalf("replayed request has %d messages, frozen one had %d", got, want)
	}

	// The dead process's worker returns and is fenced.
	close(gate.release)
	if err := <-sendErr; err == nil {
		t.Fatal("the superseded process's Send settled without an ownership error")
	}
}
