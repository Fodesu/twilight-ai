package cloudtest_test

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/felinics/twilight/agent/app"
	ownerhttp "github.com/felinics/twilight/agent/app/http"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/sdk"
)

// Opt-in local validation only: the Rust client against the real four-role
// Cloud fixture, not a fake HTTP server. This does not deploy to Kubernetes.
func TestRustChatClientCloud(t *testing.T) {
	binary := os.Getenv("TWILIGHT_CHAT_TEST_BIN")
	if binary == "" {
		t.Skip("set TWILIGHT_CHAT_TEST_BIN to the built Rust client for local validation")
	}
	model := &scripted{answers: []sdk.ModelResult{shellCall("printf 'chat-cloud-tool-ok\\n'"), {Text: "tool completed", FinishReason: sdk.FinishReasonStop}}}
	cloud := newCluster(t, model)
	owner, client := cloud.startOwner("chat-owner-a", false)
	const sid session.SessionID = "rust-cloud-chat"
	if err := client.Ensure(cloud.ctx, sid); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Open(cloud.ctx, sid, ownerhttp.OpenRequest{Preset: "ws"}); err != nil {
		t.Fatal(err)
	}
	workspace, err := client.AllocateWorkspace(cloud.ctx, ownerhttp.AllocateWorkspaceRequest{Project: "rust-chat-validation"})
	if err != nil {
		t.Fatal(err)
	}
	cloud.enqueue(client, sid, "bind", app.CommandBindWorkspace, app.BindWorkspaceCommand{WorkspaceID: workspace.ID})
	history := runRustCloudChat(t, binary, client.BaseURL, string(sid), "--send", "first", "--send", "second")
	if len(history.Transcript) != 6 {
		t.Fatalf("Cloud history: %+v", history)
	}
	assertCloudToolHistory(t, history)
	if err := owner.Close(cloud.ctx); err != nil {
		t.Fatal(err)
	}
	_, replacement := cloud.startOwner("chat-owner-b", false)
	restored := runRustCloudChat(t, binary, replacement.BaseURL, string(sid))
	if len(restored.Transcript) != len(history.Transcript) {
		t.Fatalf("Owner replacement lost history: %+v", restored)
	}
	if _, err := replacement.Open(cloud.ctx, sid, ownerhttp.OpenRequest{Preset: "ws"}); err != nil {
		t.Fatal(err)
	}
	resumed := runRustCloudChat(t, binary, replacement.BaseURL, string(sid), "--resume")
	if len(resumed.Transcript) != len(history.Transcript) {
		t.Fatal("Idle resume fabricated history")
	}
	t.Log("Rust Cloud client: two turns, remote Tool Call, history after Owner replacement, idle Resume passed")
}

func runRustCloudChat(t *testing.T, binary, endpoint, sid string, extra ...string) app.ChatSnapshot {
	t.Helper()
	arguments := []string{"--url", endpoint, "--session", sid, "--headless", "--wait-seconds", "20"}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary, append(arguments, extra...)...)
	command.WaitDelay = time.Second
	output, err := command.Output()
	if err != nil {
		t.Fatalf("Rust Cloud chat: %v", err)
	}
	var history app.ChatSnapshot
	if err := json.Unmarshal(output, &history); err != nil {
		t.Fatalf("Rust history: %v", err)
	}
	return history
}

func assertCloudToolHistory(t *testing.T, history app.ChatSnapshot) {
	t.Helper()
	calls, results := 0, 0
	for _, message := range history.Transcript {
		calls += len(message.Calls)
		if message.Kind == "tool_result" && message.Status == "success" {
			results++
		}
	}
	if calls != 1 || results != 1 {
		t.Fatalf("Cloud tool history: calls=%d results=%d", calls, results)
	}
}
