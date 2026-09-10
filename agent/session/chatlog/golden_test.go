package chatlog_test

import (
	"testing"

	"github.com/felinics/twilight/agent/jsonstable"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/chatlog"
	"github.com/felinics/twilight/agent/session/extension"
)

func freezeChatlog(t *testing.T, name, got, want string) {
	t.Helper()
	if got == want {
		return
	}
	if want == "" {
		t.Errorf("UNSET %s = %s", name, got)
		return
	}
	t.Errorf("golden %s drifted — an intentional wire change must update this fixture and agent-session-chatlog.md:\n got: %s\nwant: %s", name, got, want)
}

// TestChatlogWireGolden freezes the checkpoint digest preimages and one
// registry-encoded payload of payload version 1.
func TestChatlogWireGolden(t *testing.T) {
	pairs := []chatlog.EntryDigestPair{
		{Kind: chatlog.EntryInput, ID: "in-1", Digest: "sha256:aa"},
		{Kind: chatlog.EntryAssistant, ID: "as-1", Digest: "sha256:bb"},
	}
	base, err := chatlog.DigestBaseContext(pairs)
	if err != nil {
		t.Fatal(err)
	}
	freezeChatlog(t, "base context digest", string(base), "sha256:7f40cdee9d90c376de1c8767e44ee9f44d1e1f1615874f0aed859729d48091fc")

	emptyA, err := chatlog.DigestBaseContext(nil)
	if err != nil {
		t.Fatal(err)
	}
	emptyB, err := chatlog.DigestBaseContext([]chatlog.EntryDigestPair{})
	if err != nil {
		t.Fatal(err)
	}
	if emptyA != emptyB {
		t.Fatal("nil and empty base must digest identically")
	}
	freezeChatlog(t, "empty base context digest", string(emptyA), "sha256:8e33dda0292040be127597da4f3cba570557cf1fd28cb893472a45654da9ed67")

	cp := chatlog.CheckpointCreatedPayload{
		CheckpointID:      "ckpt-1",
		CoveredThrough:    7,
		BaseContextDigest: base,
		SummaryID:         "sum-1",
		SummaryDigest:     "sha256:cc",
		Retained:          pairs[1:],
	}
	cpd, err := chatlog.DigestCheckpoint(&cp)
	if err != nil {
		t.Fatal(err)
	}
	freezeChatlog(t, "checkpoint digest", string(cpd), "sha256:739d88db6e3a394d8443528c0099935d7cd46f3fe293db027b1f95d63e644bbb")

	reg, err := extension.BuildRegistry(session.ProtocolVersion1, chatlog.Module)
	if err != nil {
		t.Fatal(err)
	}
	wire, _, err := reg.Encode(chatlog.TypeInputSubmitted, chatlog.InputSubmittedPayload{
		InputID: "in-1", Content: jsonstable.MustParse(`{"text":"hi"}`), SubmittedAtUnixMilli: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	freezeChatlog(t, "input_submitted wire", wire.String(), `{"content":{"text":"hi"},"inputId":"in-1","submittedAtUnixMilli":1,"v":1}`)
}
