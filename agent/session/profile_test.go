package session

import (
	"testing"

	"github.com/felinics/twilight/agent/es"
	"github.com/felinics/twilight/agent/jsonstable"
)

// TestProfileVersionSeparatesDigests pins SES-VER-2 at the one place it can
// silently break. A row digest preimage carries prev, SessionID, Seq, CommitID,
// Index, Last, Type, time, SourceSeqs, Ignorable and Payload — but not the
// ProtocolVersion. The version therefore reaches a row digest only through the
// digest domain separator, so a profile whose separator ignored the version
// would let a later ProtocolVersion reproduce v1 row digests byte for byte.
//
// version 2 is not a registered protocol version; it stands in for any future
// one, which is exactly the case this pins.
func TestProfileVersionSeparatesDigests(t *testing.T) {
	v1 := profileV1{version: ProtocolVersion1}
	v2 := profileV1{version: 2}

	row := SessionEvent{
		Seq: 0, CommitID: "c1", Index: 0, Last: true, Type: "twilight/x/a",
		RecordedAtUnixMilli: 1, Payload: jsonstable.MustParse(`{"a":1}`),
	}
	prev := es.Digest("sha256:0000000000000000000000000000000000000000000000000000000000000000")

	got1, err := v1.EventDigest(prev, "s", row)
	if err != nil {
		t.Fatal(err)
	}
	again, err := v1.EventDigest(prev, "s", row)
	if err != nil {
		t.Fatal(err)
	}
	if got1 != again {
		t.Fatal("EventDigest is not deterministic")
	}
	got2, err := v2.EventDigest(prev, "s", row)
	if err != nil {
		t.Fatal(err)
	}
	if got1 == got2 {
		t.Fatal("row digests ignore the ProtocolVersion: a new version would reuse v1 digests")
	}

	// The header carries the version as a field too, so it separates for two
	// independent reasons; both must hold.
	h1 := SessionHeader{ProtocolVersion: ProtocolVersion1, SessionID: "s", CreatedAtUnixMilli: 1}
	h2 := h1
	h2.ProtocolVersion = 2
	hd1, err := v1.HeaderDigest(h1)
	if err != nil {
		t.Fatal(err)
	}
	hd2, err := v2.HeaderDigest(h2)
	if err != nil {
		t.Fatal(err)
	}
	if hd1 == hd2 {
		t.Fatal("header digests must separate across protocol versions")
	}

	// ProfileFor must hand back the version it was asked for; the registered
	// profile and the bare constructor must agree.
	p, err := ProfileFor(ProtocolVersion1)
	if err != nil {
		t.Fatal(err)
	}
	if p.Version() != ProfileV1().Version() {
		t.Fatalf("ProfileFor(1).Version() = %d, ProfileV1().Version() = %d", p.Version(), ProfileV1().Version())
	}
	if _, err := ProfileFor(2); err == nil {
		t.Fatal("an unregistered protocol version must be rejected")
	}
}
