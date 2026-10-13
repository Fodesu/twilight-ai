package app_test

import (
	"context"
	"encoding/json"
	stdhttp "net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/felinics/twilight/agent/app"
	ownerhttp "github.com/felinics/twilight/agent/app/http"
	"github.com/felinics/twilight/agent/executor/local"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/sdk"
)

type chatBlockingModel struct {
	started chan struct{}
	release chan struct{}
}

func (m *chatBlockingModel) Generate(ctx context.Context, _ sdk.Request) (sdk.ModelResult, error) {
	close(m.started)
	select {
	case <-ctx.Done():
		return sdk.ModelResult{}, ctx.Err()
	case <-m.release:
		return sdk.ModelResult{Text: "answer", FinishReason: sdk.FinishReasonStop}, nil
	}
}

func TestChatActiveEffectSnapshotHTTP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	model := &chatBlockingModel{started: make(chan struct{}), release: make(chan struct{})}
	h := newHost(t, app.Config{}, map[run.ModelRef]local.ModelInvoker{"m-1": model})
	t.Cleanup(func() { _ = h.Close(context.Background()) })
	preset, err := h.RegisterPreset("p", mustPreset("m-1", nil))
	if err != nil {
		t.Fatal(err)
	}
	s, err := h.OpenSession(ctx, "active-chat", app.SessionOptions{Preset: preset})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer((&ownerhttp.Server{App: h.Application}).Handler())
	defer server.Close()
	defer close(model.release)
	if _, err := s.Submit(ctx, "hello"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-model.started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := s.QueueInput(ctx, "queued-id", "queued"); err != nil {
		t.Fatal(err)
	}
	snapshot := readActiveChatSnapshot(t, ctx, server.URL)
	if len(snapshot.ActiveEffects) != 1 || len(snapshot.Turns) != 1 || len(snapshot.Transcript) != 2 {
		t.Fatalf("active snapshot: %+v", snapshot)
	}
	effect := snapshot.ActiveEffects[0]
	if effect.Kind != "model" || effect.Label != "m-1" || effect.EffectID == "" || effect.RunID != snapshot.Turns[0].RunID {
		t.Fatalf("effect identity: %+v", effect)
	}
	if snapshot.Turns[0].Status != "active" || snapshot.Transcript[0].Text != "hello" {
		t.Fatalf("active turn/history: %+v", snapshot)
	}
	if len(snapshot.Turns[0].Items) != 1 || snapshot.Turns[0].Items[0].Text != "hello" {
		t.Fatalf("turn presentation: %+v", snapshot.Turns[0].Items)
	}
	if len(snapshot.Queued) != 1 || snapshot.Queued[0].Text != "queued" {
		t.Fatalf("queued presentation: %+v", snapshot.Queued)
	}
	if snapshot.Transcript[0].Position.Commit >= snapshot.NextCommit || len(snapshot.Approvals) != 0 {
		t.Fatalf("watermark/approvals: %+v", snapshot)
	}
}

func readActiveChatSnapshot(t *testing.T, ctx context.Context, url string) app.ChatSnapshot {
	t.Helper()
	req, err := stdhttp.NewRequestWithContext(ctx, stdhttp.MethodGet, url+"/sessions/active-chat/chat/snapshot", nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := stdhttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var snapshot app.ChatSnapshot
	if res.StatusCode != stdhttp.StatusOK || json.NewDecoder(res.Body).Decode(&snapshot) != nil {
		t.Fatalf("snapshot response: %s", res.Status)
	}
	return snapshot
}
