package session_test

import (
	"encoding/json"
	"testing"

	"github.com/felinics/twilight/agent/es"
	"github.com/felinics/twilight/agent/jsonstable"
	"github.com/felinics/twilight/agent/session"
)

// freezeKernel guards the kernel wire of ProtocolVersion 1. A failure with a
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
	profile := session.ProfileV1()
	header := session.SegmentHeader{
		ProtocolVersion: session.ProtocolVersion1,
		Nonce:           "golden",
		CausationID:     es.CausationID("cause-1"),
		Metadata:        jsonstable.MustParse(`{"k":"v"}`),
	}
	hd, err := profile.HeaderDigest(header)
	if err != nil {
		t.Fatal(err)
	}
	freezeKernel(t, "header digest", string(hd), "sha256:1eb09b1cf1e2d020e3892b04e9dad1f4b2db377f7b3bb7731a27a081b2dab258")
	segment := session.SegmentID(hd)

	sess := session.StreamRef{Domain: "chat"}
	run := session.StreamRef{Domain: "run", ID: "r7"}
	c0 := session.Commit{Seq: 0, CommitID: "c1", Epoch: 1, Batches: []session.StreamBatch{
		{Stream: sess, Events: []session.Event{
			{Type: "twilight/x/a", RecordedAtUnixMilli: 1, Payload: jsonstable.MustParse(`{"a":1}`)},
			{Type: "twilight/x/b", RecordedAtUnixMilli: 2, Payload: jsonstable.MustParse(`{}`)},
		}},
	}}
	if err := session.SealCommit(profile, hd, segment, &c0); err != nil {
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
	if err := session.SealCommit(profile, c0.Digest, segment, &c1); err != nil {
		t.Fatal(err)
	}
	freezeKernel(t, "commit 0 digest", string(c0.Digest), "sha256:cfd211456c9b6f329e500394c2780a237ceffc575b6e5e4bf31e6f0aa6c0938a")
	freezeKernel(t, "commit 1 digest", string(c1.Digest), "sha256:7cf8a013be115e2901f4028daf95bbacdfcb54296631bd339d6806969e4f766f")

	line0, err := json.Marshal(c0)
	if err != nil {
		t.Fatal(err)
	}
	freezeKernel(t, "commit 0 json", string(line0), `{"seq":0,"commitId":"c1","epoch":1,"batches":[{"stream":{"domain":"chat"},"events":[{"type":"twilight/x/a","recordedAtUnixMilli":1,"payload":{"a":1}},{"type":"twilight/x/b","recordedAtUnixMilli":2,"payload":{}}]}],"prevDigest":"sha256:1eb09b1cf1e2d020e3892b04e9dad1f4b2db377f7b3bb7731a27a081b2dab258","digest":"sha256:cfd211456c9b6f329e500394c2780a237ceffc575b6e5e4bf31e6f0aa6c0938a"}`)
	line1, err := json.Marshal(c1)
	if err != nil {
		t.Fatal(err)
	}
	freezeKernel(t, "commit 1 json", string(line1), `{"seq":1,"commitId":"c2","epoch":1,"batches":[{"stream":{"domain":"chat"},"events":[{"type":"twilight/x/c","recordedAtUnixMilli":3,"payload":{"c":3}}]},{"stream":{"domain":"run","id":"r7"},"events":[{"type":"twilight/run/created","recordedAtUnixMilli":3,"payload":{"runId":"r7"}}]}],"prevDigest":"sha256:cfd211456c9b6f329e500394c2780a237ceffc575b6e5e4bf31e6f0aa6c0938a","digest":"sha256:7cf8a013be115e2901f4028daf95bbacdfcb54296631bd339d6806969e4f766f"}`)

	header.HeaderDigest = hd
	if err := session.ValidateLedger(profile, header, []session.Commit{c0, c1}); err != nil {
		t.Fatalf("golden commits do not validate: %v", err)
	}
}
