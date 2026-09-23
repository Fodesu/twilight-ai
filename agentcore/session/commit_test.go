package session

import (
	"strings"
	"testing"

	"github.com/felinics/twilight/agentcore/es"
	"github.com/felinics/twilight/agentcore/jsonstable"
)

// v2Header builds a sealed root segment header whose nonce is nonce.
func v2Header(t *testing.T, nonce string) SegmentHeader {
	t.Helper()
	h := SegmentHeader{ProtocolVersion: ProtocolVersion1, Nonce: nonce}
	d, err := ProfileV1().HeaderDigest(h)
	if err != nil {
		t.Fatal(err)
	}
	h.HeaderDigest = d
	return h
}

func oneEventBatch(stream StreamRef, typ, payload string) StreamBatch {
	return StreamBatch{Stream: stream, Events: []Event{
		{Type: EventType(typ), RecordedAtUnixMilli: 1, Payload: jsonstable.MustParse(payload)},
	}}
}

func TestValidateStreamRef(t *testing.T) {
	cases := []struct {
		name string
		ref  StreamRef
		want string // error substring; "" means valid
	}{
		{"singleton stream", StreamRef{Domain: "chat"}, ""},
		{"keyed stream", StreamRef{Domain: "run", ID: "r7"}, ""},
		{"empty domain", StreamRef{ID: "r7"}, "stream domain is empty"},
		{"domain with separator", StreamRef{Domain: "run/r7"}, `contains "/"`},
		{"invalid UTF-8 domain", StreamRef{Domain: string([]byte{0xff})}, "not valid UTF-8"},
		{"invalid UTF-8 ID", StreamRef{Domain: "run", ID: string([]byte{0xff})}, "not valid UTF-8"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateStreamRef(tc.ref)
			if tc.want == "" {
				if err != nil {
					t.Fatalf("ValidateStreamRef(%v) = %v", tc.ref, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ValidateStreamRef(%v) = %v, want substring %q", tc.ref, err, tc.want)
			}
		})
	}
}

func TestValidateBatches(t *testing.T) {
	chat := StreamRef{Domain: "chat"}
	run := StreamRef{Domain: "run", ID: "r7"}
	cases := []struct {
		name    string
		batches []StreamBatch
		want    string
	}{
		{"one batch", []StreamBatch{oneEventBatch(chat, "twilight/x/a", `{"a":1}`)}, ""},
		{"two streams in one commit", []StreamBatch{
			oneEventBatch(chat, "twilight/x/a", `{"a":1}`),
			oneEventBatch(run, "twilight/run/created", `{"runId":"r7"}`),
		}, ""},
		{"no batches", nil, "without batches"},
		{"batch without events", []StreamBatch{{Stream: chat}}, "no events"},
		{"same stream twice in one commit", []StreamBatch{
			oneEventBatch(chat, "twilight/x/a", `{"a":1}`),
			oneEventBatch(chat, "twilight/x/b", `{"b":2}`),
		}, "appears twice"},
		{"stream domain with separator", []StreamBatch{
			oneEventBatch(StreamRef{Domain: "run/r7"}, "twilight/run/created", `{"runId":"r7"}`),
		}, `contains "/"`},
		{"empty event type", []StreamBatch{{Stream: chat, Events: []Event{
			{RecordedAtUnixMilli: 1, Payload: jsonstable.MustParse(`{}`)},
		}}}, "EventType"},
		{"array payload", []StreamBatch{oneEventBatch(chat, "twilight/x/a", `[1]`)}, "object"},
		{"zero payload", []StreamBatch{{Stream: chat, Events: []Event{
			{Type: "twilight/x/a", RecordedAtUnixMilli: 1},
		}}}, "empty payload"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateBatches(tc.batches)
			if tc.want == "" {
				if err != nil {
					t.Fatalf("ValidateBatches = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ValidateBatches = %v, want substring %q", err, tc.want)
			}
		})
	}
}

func TestSealCommit(t *testing.T) {
	p := ProfileV1()
	h := v2Header(t, "s")
	c := Commit{Seq: 0, CommitID: "c1", Batches: []StreamBatch{
		oneEventBatch(StreamRef{Domain: "chat"}, "twilight/x/a", `{"a":1}`),
		oneEventBatch(StreamRef{Domain: "run", ID: "r7"}, "twilight/run/created", `{"runId":"r7"}`),
	}}
	if err := SealCommit(p, h.HeaderDigest, SegmentIDOf(h), &c); err != nil {
		t.Fatal(err)
	}
	if c.PrevDigest != h.HeaderDigest {
		t.Fatal("PrevDigest not stamped")
	}
	if c.Digest == "" {
		t.Fatal("Digest not stamped")
	}
	resealed := c
	resealed.PrevDigest, resealed.Digest = "", ""
	if err := SealCommit(p, h.HeaderDigest, SegmentIDOf(h), &resealed); err != nil {
		t.Fatal(err)
	}
	if resealed.Digest != c.Digest {
		t.Fatal("SealCommit is not deterministic")
	}

	// Event order inside a batch and batch order inside a commit are both
	// canonical: swapping either changes the digest.
	swappedEvents := Commit{Seq: 0, CommitID: "c1", Batches: []StreamBatch{
		{Stream: StreamRef{Domain: "chat"}, Events: []Event{
			{Type: "twilight/x/b", RecordedAtUnixMilli: 1, Payload: jsonstable.MustParse(`{"b":2}`)},
			{Type: "twilight/x/a", RecordedAtUnixMilli: 1, Payload: jsonstable.MustParse(`{"a":1}`)},
		}},
	}}
	if err := SealCommit(p, h.HeaderDigest, SegmentIDOf(h), &swappedEvents); err != nil {
		t.Fatal(err)
	}
	twoEvents := Commit{Seq: 0, CommitID: "c1", Batches: []StreamBatch{
		{Stream: StreamRef{Domain: "chat"}, Events: []Event{
			{Type: "twilight/x/a", RecordedAtUnixMilli: 1, Payload: jsonstable.MustParse(`{"a":1}`)},
			{Type: "twilight/x/b", RecordedAtUnixMilli: 1, Payload: jsonstable.MustParse(`{"b":2}`)},
		}},
	}}
	if err := SealCommit(p, h.HeaderDigest, SegmentIDOf(h), &twoEvents); err != nil {
		t.Fatal(err)
	}
	if swappedEvents.Digest == twoEvents.Digest {
		t.Fatal("event order inside a batch does not reach the digest")
	}
	swappedBatches := Commit{Seq: 0, CommitID: "c1", Batches: []StreamBatch{
		oneEventBatch(StreamRef{Domain: "run", ID: "r7"}, "twilight/run/created", `{"runId":"r7"}`),
		oneEventBatch(StreamRef{Domain: "chat"}, "twilight/x/a", `{"a":1}`),
	}}
	if err := SealCommit(p, h.HeaderDigest, SegmentIDOf(h), &swappedBatches); err != nil {
		t.Fatal(err)
	}
	if swappedBatches.Digest == c.Digest {
		t.Fatal("batch order inside a commit does not reach the digest")
	}

	if err := SealCommit(p, h.HeaderDigest, SegmentIDOf(h), &Commit{Seq: 0, Batches: c.Batches}); err == nil {
		t.Fatal("empty CommitID accepted")
	}
	if err := SealCommit(p, h.HeaderDigest, SegmentIDOf(h), &Commit{Seq: 0, CommitID: "c2"}); err == nil {
		t.Fatal("commit without batches accepted")
	}
}

// sealedPair builds a two-commit sealed ledger: one chat batch, then a
// commit spanning the chat stream and run stream r7.
func sealedPair(t *testing.T) (LedgerProfile, SegmentHeader, []Commit) {
	t.Helper()
	p := ProfileV1()
	h := v2Header(t, "s")
	c0 := Commit{Seq: 0, CommitID: "c1", Batches: []StreamBatch{
		oneEventBatch(StreamRef{Domain: "chat"}, "twilight/x/a", `{"a":1}`),
	}}
	if err := SealCommit(p, h.HeaderDigest, SegmentIDOf(h), &c0); err != nil {
		t.Fatal(err)
	}
	c1 := Commit{Seq: 1, CommitID: "c2", Batches: []StreamBatch{
		oneEventBatch(StreamRef{Domain: "chat"}, "twilight/x/b", `{"b":2}`),
		oneEventBatch(StreamRef{Domain: "run", ID: "r7"}, "twilight/run/created", `{"runId":"r7"}`),
	}}
	if err := SealCommit(p, c0.Digest, SegmentIDOf(h), &c1); err != nil {
		t.Fatal(err)
	}
	return p, h, []Commit{c0, c1}
}

func TestValidateLedgerDetects(t *testing.T) {
	zero := es.Digest("sha256:0000000000000000000000000000000000000000000000000000000000000000")
	cases := []struct {
		name   string
		mutate func(*[]Commit)
	}{
		{"valid chain", nil},
		{"empty ledger", func(cs *[]Commit) { *cs = nil }},
		{"seq gap", func(cs *[]Commit) { (*cs)[1].Seq = 9 }},
		{"payload tamper", func(cs *[]Commit) {
			(*cs)[0].Batches[0].Events[0].Payload = jsonstable.MustParse(`{"a":2}`)
		}},
		{"commit id tamper", func(cs *[]Commit) { (*cs)[0].CommitID = "other" }},
		{"batch stream tamper", func(cs *[]Commit) { (*cs)[1].Batches[1].Stream.ID = "r8" }},
		{"stored digest tamper", func(cs *[]Commit) { (*cs)[1].Digest = zero }},
		{"stored prev tamper", func(cs *[]Commit) { (*cs)[1].PrevDigest = zero }},
		// Commits the profile refuses to reseal are corrupt too, not raw seal errors.
		{"empty commit id", func(cs *[]Commit) { (*cs)[1].CommitID = "" }},
		{"batch without events", func(cs *[]Commit) { (*cs)[1].Batches[0].Events = nil }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, h, commits := sealedPair(t)
			if tc.mutate != nil {
				tc.mutate(&commits)
			}
			err := ValidateLedger(p, h, commits)
			if tc.mutate == nil || tc.name == "empty ledger" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if !IsCode(err, ErrCorrupt) {
				t.Fatalf("ValidateLedger = %v, want code corrupt", err)
			}
		})
	}

	// A ledger validated under another segment's header must fail at commit 0:
	// the seed digest and the segment the commits are bound to both differ.
	p, _, commits := sealedPair(t)
	if err := ValidateLedger(p, v2Header(t, "other"), commits); !IsCode(err, ErrCorrupt) {
		t.Fatalf("ValidateLedger under foreign header = %v, want code corrupt", err)
	}
}
