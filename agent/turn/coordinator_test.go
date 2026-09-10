package turn

import (
	"context"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/chatlog"
	"github.com/felinics/twilight/agent/session/extension"
	runmod "github.com/felinics/twilight/agent/session/run"
	"github.com/felinics/twilight/agent/session/writer"
	"testing"
)

// The Coordinator is pure protocol: Start, Deliver and Status commit and read
// without any driver, registry or Loop in the assembly. The Run stays Open
// until a host drives it (REF-DRV-1).
func TestCoordinatorCommitsWithoutDriver(t *testing.T) {
	ctx := context.Background()
	const sid session.SessionID = "s-protocol"
	registry, err := extension.BuildRegistry(session.ProtocolVersion1, chatlog.Module, runmod.Module, Module)
	if err != nil {
		t.Fatal(err)
	}
	store := session.NewMemoryStore()
	if _, err := store.Create(ctx, session.CreateRequest{ProtocolVersion: session.ProtocolVersion1, SessionID: sid, CreatedAtUnixMilli: 1}); err != nil {
		t.Fatal(err)
	}
	writers := writer.NewWriters(store, registry, writer.Admission{}, session.OpenOptions{}, writer.WritersConfig{})
	runtime, err := runmod.NewRuntime(runmod.Config{Writers: writers, Registry: registry, Store: store,
		Frozen: run.NewMemoryFrozenValues(), Companion: CompanionV1{}})
	if err != nil {
		t.Fatal(err)
	}
	c := &Coordinator{Writers: writers, Runtime: runtime}

	submit := func(id chatlog.InputID) run.AgentInput {
		t.Helper()
		content := run.MustParseCanonicalJSON(`{"text":"hi"}`)
		w, err := writers.Writer(ctx, sid)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Commit(ctx, func(writer.View) (*writer.SemanticGroup, error) {
			return &writer.SemanticGroup{CommitID: session.CommitID("submit/" + string(id)), Events: []writer.TypedEvent{{
				Type: chatlog.TypeInputSubmitted, RecordedAtUnixMilli: 1,
				Value: chatlog.InputSubmittedPayload{InputID: id, Content: content, SubmittedAtUnixMilli: 1},
			}}}, nil
		}); err != nil {
			t.Fatal(err)
		}
		return run.AgentInput{ID: run.InputID(id), Payload: content}
	}

	ref := TurnRef{SessionID: sid, TurnID: "t1"}
	profile := ProfileRef{ID: "p1", Digest: "sha256:p1"}
	start := StartRequest{Ref: ref, Inputs: []run.AgentInput{submit("in-1")}, Profile: profile, Companion: CompanionV1Version}
	resp, err := c.Start(ctx, start)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if resp.Status != TurnActive || resp.Attempt != 1 || resp.RunID == "" || resp.Disposition != "" {
		t.Fatalf("start response = %+v, want active attempt 1 with no disposition", resp)
	}
	if again, err := c.Start(ctx, start); err != nil || again.RunID != resp.RunID {
		t.Fatalf("start replay = %+v %v", again, err)
	}
	if _, err := c.Deliver(ctx, DeliverRequest{Ref: ref, Inputs: []run.AgentInput{submit("in-2")}}); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	status, err := c.Status(ctx, ref)
	if err != nil || status.Status != TurnActive || status.RunID != resp.RunID {
		t.Fatalf("status = %+v %v", status, err)
	}
	snap, err := runtime.Load(ctx, sid, resp.RunID)
	if err != nil || snap.State.Status.Terminal() {
		t.Fatalf("run advanced without a driver: %+v %v", snap.State.Status, err)
	}
}
