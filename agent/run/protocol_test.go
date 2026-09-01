package run

import "testing"

func TestProtocolForSelectsV1(t *testing.T) {
	p, err := ProtocolFor(SchemaVersion1)
	if err != nil {
		t.Fatal(err)
	}
	if p.Version() != SchemaVersion1 {
		t.Fatalf("version = %d", p.Version())
	}
	if p != ProtocolV1 {
		t.Fatal("ProtocolFor(1) did not return ProtocolV1")
	}
	if _, err := ProtocolFor(0); err == nil {
		t.Fatal("schema 0 accepted")
	}
	if _, err := ProtocolFor(2); err == nil {
		t.Fatal("schema 2 accepted")
	}
}

func TestRuntimeSnapshotProtocol(t *testing.T) {
	snap := RuntimeSnapshot{SchemaVersion: SchemaVersion1}
	p, err := snap.Protocol()
	if err != nil {
		t.Fatal(err)
	}
	if p.Version() != SchemaVersion1 {
		t.Fatalf("version = %d", p.Version())
	}
	if _, err := (RuntimeSnapshot{}).Protocol(); err == nil {
		t.Fatal("zero snapshot protocol accepted")
	}
}
