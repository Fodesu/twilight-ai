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

// TestKernelWireGolden freezes the header digest, the per-row chained digests
// and the canonical JSON row shape (the exact log.jsonl line bytes).
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
	freezeKernel(t, "header digest", string(hd), "sha256:413913826bb3e8068c6aa1b6c963c15a4a2686bf955b8d3aa4eab42830465b96")

	rows := []session.SessionEvent{
		{Seq: 0, CommitID: "c1", Index: 0, Last: false, Type: "twilight/x/a", RecordedAtUnixMilli: 1, Payload: jsonstable.MustParse(`{"a":1}`)},
		{Seq: 1, CommitID: "c1", Index: 1, Last: true, Type: "twilight/x/b", RecordedAtUnixMilli: 2, SourceSeqs: []session.Seq{0}, Ignorable: true, Payload: jsonstable.MustParse(`{}`)},
	}
	prev := hd
	for i := range rows {
		d, err := profile.EventDigest(prev, header.SessionID, rows[i])
		if err != nil {
			t.Fatal(err)
		}
		rows[i].Digest = d
		prev = d
	}
	freezeKernel(t, "row 0 digest", string(rows[0].Digest), "sha256:caa903dd54bf264f8c2c13b26256d90d5c2e317c8938a0085f03120640bcbfe8")
	freezeKernel(t, "row 1 digest", string(rows[1].Digest), "sha256:93d7eb549fb525a0a49edd147dbb1a8859542fe0fbde439ea92ed37297b5dc7b")

	line0, err := json.Marshal(rows[0])
	if err != nil {
		t.Fatal(err)
	}
	freezeKernel(t, "row 0 json", string(line0), `{"seq":0,"commitId":"c1","index":0,"last":false,"type":"twilight/x/a","recordedAtUnixMilli":1,"payload":{"a":1},"digest":"sha256:caa903dd54bf264f8c2c13b26256d90d5c2e317c8938a0085f03120640bcbfe8"}`)
	line1, err := json.Marshal(rows[1])
	if err != nil {
		t.Fatal(err)
	}
	freezeKernel(t, "row 1 json", string(line1), `{"seq":1,"commitId":"c1","index":1,"last":true,"type":"twilight/x/b","recordedAtUnixMilli":2,"sourceSeqs":[0],"ignorable":true,"payload":{},"digest":"sha256:93d7eb549fb525a0a49edd147dbb1a8859542fe0fbde439ea92ed37297b5dc7b"}`)

	header.HeaderDigest = hd
	if err := session.ValidateChain(profile, header, rows); err != nil {
		t.Fatalf("golden rows do not validate: %v", err)
	}
}
