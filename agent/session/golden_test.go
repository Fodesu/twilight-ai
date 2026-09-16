package session_test

import (
	"encoding/json"
	"testing"

	"github.com/felinics/twilight/agent/es"
	"github.com/felinics/twilight/agent/jsonstable"
	"github.com/felinics/twilight/agent/session"
)

// freezeKernel guards the kernel wire of ProtocolVersion 2. A failure with a
// non-empty want is wire drift: an intentional protocol change must update the
// fixture (and agent-session.md); anything else is accidental.
func freezeKernel(t *testing.T, name, got, want string) {
	t.Helper()
	if got == want {
		return
	}
	if want == "" {
		t.Errorf("UNSET %s = %s", name, got)
		return
	}
	t.Errorf("golden %s drifted — an intentional wire change must update this fixture and agent-session.md:\n got: %s\nwant: %s", name, got, want)
}

// TestKernelWireGolden freezes the header digest, the chained batch and
// commit digests and the canonical JSON commit shape (the exact log.jsonl
// line bytes a durable store writes).
func TestKernelWireGolden(t *testing.T) {
	profile := session.ProfileV2()
	header := session.SessionHeader{
		ProtocolVersion:    session.ProtocolVersion2,
		SessionID:          "golden",
		CreatedAtUnixMilli: 1,
		CausationID:        es.CausationID("cause-1"),
		Metadata:           jsonstable.MustParse(`{"k":"v"}`),
	}
	hd, err := profile.HeaderDigest(header)
	if err != nil {
		t.Fatal(err)
	}
	freezeKernel(t, "header digest", string(hd), "sha256:0615836b75bd9d3fac1862c7cff54b3f713b367b8efcae1c0aa1315a4430ab3e")

	sess := session.StreamRef{Kind: session.StreamKindSession}
	run := session.StreamRef{Kind: session.StreamKindRun, ID: "r7"}
	c0 := session.Commit{Seq: 0, CommitID: "c1", Epoch: 1, Batches: []session.StreamBatch{
		{Stream: sess, Events: []session.Event{
			{Type: "twilight/x/a", RecordedAtUnixMilli: 1, Payload: jsonstable.MustParse(`{"a":1}`)},
			{Type: "twilight/x/b", RecordedAtUnixMilli: 2, Payload: jsonstable.MustParse(`{}`)},
		}},
	}}
	if err := session.SealCommit(profile, hd, header.SessionID, &c0); err != nil {
		t.Fatal(err)
	}
	c1 := session.Commit{Seq: 1, CommitID: "c2", Epoch: 1, Batches: []session.StreamBatch{
		{Stream: sess, Events: []session.Event{
			{Type: "twilight/x/c", RecordedAtUnixMilli: 3, Payload: jsonstable.MustParse(`{"c":3}`)},
		}},
		{Stream: run, Events: []session.Event{
			{Type: "twilight/run/created", RecordedAtUnixMilli: 3, Payload: jsonstable.MustParse(`{"runId":"r7"}`)},
		}},
	}}
	if err := session.SealCommit(profile, c0.Digest, header.SessionID, &c1); err != nil {
		t.Fatal(err)
	}
	freezeKernel(t, "commit 0 digest", string(c0.Digest), "sha256:1ca9b8d816e064256a065f6dc3e2cf2a8abcb857a7d10250977f6b0f69d4d482")
	freezeKernel(t, "commit 1 digest", string(c1.Digest), "sha256:b8c5d952d330f10f3bf4d4a820295fa68470e2e1a0bb320d3c3b3c6de2e3197b")

	line0, err := json.Marshal(c0)
	if err != nil {
		t.Fatal(err)
	}
	freezeKernel(t, "commit 0 json", string(line0), `{"seq":0,"commitId":"c1","epoch":1,"batches":[{"stream":{"kind":"session"},"events":[{"type":"twilight/x/a","recordedAtUnixMilli":1,"payload":{"a":1}},{"type":"twilight/x/b","recordedAtUnixMilli":2,"payload":{}}]}],"prevDigest":"sha256:0615836b75bd9d3fac1862c7cff54b3f713b367b8efcae1c0aa1315a4430ab3e","digest":"sha256:1ca9b8d816e064256a065f6dc3e2cf2a8abcb857a7d10250977f6b0f69d4d482"}`)
	line1, err := json.Marshal(c1)
	if err != nil {
		t.Fatal(err)
	}
	freezeKernel(t, "commit 1 json", string(line1), `{"seq":1,"commitId":"c2","epoch":1,"batches":[{"stream":{"kind":"session"},"events":[{"type":"twilight/x/c","recordedAtUnixMilli":3,"payload":{"c":3}}]},{"stream":{"kind":"run","id":"r7"},"events":[{"type":"twilight/run/created","recordedAtUnixMilli":3,"payload":{"runId":"r7"}}]}],"prevDigest":"sha256:1ca9b8d816e064256a065f6dc3e2cf2a8abcb857a7d10250977f6b0f69d4d482","digest":"sha256:b8c5d952d330f10f3bf4d4a820295fa68470e2e1a0bb320d3c3b3c6de2e3197b"}`)

	header.HeaderDigest = hd
	if err := session.ValidateLedger(profile, header, []session.Commit{c0, c1}); err != nil {
		t.Fatalf("golden commits do not validate: %v", err)
	}
}
