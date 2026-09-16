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
	header := session.SessionHeader{
		ProtocolVersion:    session.ProtocolVersion1,
		SessionID:          "golden",
		CreatedAtUnixMilli: 1,
		CausationID:        es.CausationID("cause-1"),
		Metadata:           jsonstable.MustParse(`{"k":"v"}`),
	}
	hd, err := profile.HeaderDigest(header)
	if err != nil {
		t.Fatal(err)
	}
	freezeKernel(t, "header digest", string(hd), "sha256:29333838670a343459d7326bce6a93031f4ac41e2a002f8c4b5baf6b5feafe47")

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
	freezeKernel(t, "commit 0 digest", string(c0.Digest), "sha256:19f3eb6148c45c936680d53f1249bda560cebabb648fc512b02958e4eca3d9e8")
	freezeKernel(t, "commit 1 digest", string(c1.Digest), "sha256:57aecb4ba38897412c473a93a838ff7ddf89d5b0cc906171f9c1efa038b3d5f6")

	line0, err := json.Marshal(c0)
	if err != nil {
		t.Fatal(err)
	}
	freezeKernel(t, "commit 0 json", string(line0), `{"seq":0,"commitId":"c1","epoch":1,"batches":[{"stream":{"kind":"session"},"events":[{"type":"twilight/x/a","recordedAtUnixMilli":1,"payload":{"a":1}},{"type":"twilight/x/b","recordedAtUnixMilli":2,"payload":{}}]}],"prevDigest":"sha256:29333838670a343459d7326bce6a93031f4ac41e2a002f8c4b5baf6b5feafe47","digest":"sha256:19f3eb6148c45c936680d53f1249bda560cebabb648fc512b02958e4eca3d9e8"}`)
	line1, err := json.Marshal(c1)
	if err != nil {
		t.Fatal(err)
	}
	freezeKernel(t, "commit 1 json", string(line1), `{"seq":1,"commitId":"c2","epoch":1,"batches":[{"stream":{"kind":"session"},"events":[{"type":"twilight/x/c","recordedAtUnixMilli":3,"payload":{"c":3}}]},{"stream":{"kind":"run","id":"r7"},"events":[{"type":"twilight/run/created","recordedAtUnixMilli":3,"payload":{"runId":"r7"}}]}],"prevDigest":"sha256:19f3eb6148c45c936680d53f1249bda560cebabb648fc512b02958e4eca3d9e8","digest":"sha256:57aecb4ba38897412c473a93a838ff7ddf89d5b0cc906171f9c1efa038b3d5f6"}`)

	header.HeaderDigest = hd
	if err := session.ValidateLedger(profile, header, []session.Commit{c0, c1}); err != nil {
		t.Fatalf("golden commits do not validate: %v", err)
	}
}
