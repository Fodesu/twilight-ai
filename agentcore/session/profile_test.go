package session

import (
	"testing"

	"github.com/felinics/twilight/agentcore/es"
	"github.com/felinics/twilight/agentcore/jsonstable"
)

// TestProfileVersionSeparatesDigests pins SES-VER-2 at the one place it can
// silently break. A batch digest preimage carries SegmentID, Stream and the
// events; a commit digest preimage carries prev, SegmentID, Seq, CommitID,
// Epoch and the batch digests — but neither carries the ProtocolVersion as a
// field. The version therefore reaches a digest only through the digest
// domain separator, so a profile whose separator ignored the version would
// let a later ProtocolVersion reproduce v2 digests byte for byte.
//
// version 3 is not a registered protocol version; it stands in for any
// future one, which is exactly the case this pins.
func TestProfileVersionSeparatesDigests(t *testing.T) {
	v2 := profileV1{version: ProtocolVersion1}
	v3 := profileV1{version: 3}

	batch := StreamBatch{Stream: StreamRef{Domain: "chat"}, Events: []Event{
		{Type: "twilight/x/a", RecordedAtUnixMilli: 1, Payload: jsonstable.MustParse(`{"a":1}`)},
	}}
	prev := es.Digest("sha256:0000000000000000000000000000000000000000000000000000000000000000")

	got2, err := v2.BatchDigest("s", batch)
	if err != nil {
		t.Fatal(err)
	}
	again, err := v2.BatchDigest("s", batch)
	if err != nil {
		t.Fatal(err)
	}
	if got2 != again {
		t.Fatal("BatchDigest is not deterministic")
	}
	got3, err := v3.BatchDigest("s", batch)
	if err != nil {
		t.Fatal(err)
	}
	if got2 == got3 {
		t.Fatal("batch digests ignore the ProtocolVersion: a new version would reuse v2 digests")
	}

	c2, err := v2.CommitDigest(prev, "s", 0, "c1", 1, "", []es.Digest{got2}, jsonstable.Value{})
	if err != nil {
		t.Fatal(err)
	}
	c3, err := v3.CommitDigest(prev, "s", 0, "c1", 1, "", []es.Digest{got3}, jsonstable.Value{})
	if err != nil {
		t.Fatal(err)
	}
	if c2 == c3 {
		t.Fatal("commit digests ignore the ProtocolVersion")
	}

	// The header carries the version as a field too, so it separates for two
	// independent reasons; both must hold.
	h2 := SegmentHeader{ProtocolVersion: ProtocolVersion1, Nonce: "s"}
	h3 := h2
	h3.ProtocolVersion = 3
	hd2, err := v2.HeaderDigest(h2)
	if err != nil {
		t.Fatal(err)
	}
	hd3, err := v3.HeaderDigest(h3)
	if err != nil {
		t.Fatal(err)
	}
	if hd2 == hd3 {
		t.Fatal("header digests must separate across protocol versions")
	}

	// LedgerProfileFor must hand back the version it was asked for; the
	// registered profile and the bare constructor must agree.
	p, err := LedgerProfileFor(ProtocolVersion1)
	if err != nil {
		t.Fatal(err)
	}
	if p.Version() != ProfileV1().Version() {
		t.Fatalf("LedgerProfileFor(2).Version() = %d, ProfileV1().Version() = %d", p.Version(), ProfileV1().Version())
	}
	if _, err := LedgerProfileFor(3); err == nil {
		t.Fatal("an unregistered protocol version must be rejected")
	}
}
