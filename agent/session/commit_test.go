package session

import (
	"strings"
	"testing"

	"github.com/felinics/twilight/agent/es"
	"github.com/felinics/twilight/agent/jsonstable"
)

func v2Header(t *testing.T, sid SessionID) SessionHeader {
	t.Helper()
	h := SessionHeader{ProtocolVersion: ProtocolVersion1, SessionID: sid, CreatedAtUnixMilli: 1}
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
		{"session stream", StreamRef{Kind: StreamKindSession}, ""},
		{"session stream must not carry an ID", StreamRef{Kind: StreamKindSession, ID: "r7"}, "must not carry"},
		{"run stream", StreamRef{Kind: StreamKindRun, ID: "r7"}, ""},
		{"run stream needs an ID", StreamRef{Kind: StreamKindRun}, "RunID"},
		{"unknown kind", StreamRef{Kind: StreamKind("lane")}, "unknown stream kind"},
		{"invalid UTF-8 ID", StreamRef{Kind: StreamKindRun, ID: string([]byte{0xff})}, "not valid UTF-8"},
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
	session := StreamRef{Kind: StreamKindSession}
	run := StreamRef{Kind: StreamKindRun, ID: "r7"}
	cases := []struct {
		name    string
		batches []StreamBatch
		want    string
	}{
		{"one batch", []StreamBatch{oneEventBatch(session, "twilight/x/a", `{"a":1}`)}, ""},
		{"two streams in one commit", []StreamBatch{
			oneEventBatch(session, "twilight/x/a", `{"a":1}`),
			oneEventBatch(run, "twilight/run/created", `{"runId":"r7"}`),
		}, ""},
		{"no batches", nil, "without batches"},
		{"batch without events", []StreamBatch{{Stream: session}}, "no events"},
		{"same stream twice in one commit", []StreamBatch{
			oneEventBatch(session, "twilight/x/a", `{"a":1}`),
			oneEventBatch(session, "twilight/x/b", `{"b":2}`),
		}, "appears twice"},
		{"run stream without ID", []StreamBatch{
			oneEventBatch(StreamRef{Kind: StreamKindRun}, "twilight/run/created", `{"runId":"r7"}`),
		}, "RunID"},
		{"empty event type", []StreamBatch{{Stream: session, Events: []Event{
			{RecordedAtUnixMilli: 1, Payload: jsonstable.MustParse(`{}`)},
		}}}, "EventType"},
		{"array payload", []StreamBatch{oneEventBatch(session, "twilight/x/a", `[1]`)}, "object"},
		{"zero payload", []StreamBatch{{Stream: session, Events: []Event{
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
	c := Commit{Seq: 0, CommitID: "c1", Epoch: 3, Batches: []StreamBatch{
		oneEventBatch(StreamRef{Kind: StreamKindSession}, "twilight/x/a", `{"a":1}`),
		oneEventBatch(StreamRef{Kind: StreamKindRun, ID: "r7"}, "twilight/run/created", `{"runId":"r7"}`),
	}}
	if err := SealCommit(p, h.HeaderDigest, "s", &c); err != nil {
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
	if err := SealCommit(p, h.HeaderDigest, "s", &resealed); err != nil {
		t.Fatal(err)
	}
	if resealed.Digest != c.Digest {
		t.Fatal("SealCommit is not deterministic")
	}

	// The epoch reaches the commit preimage.
	otherEpoch := c
	otherEpoch.Epoch = 4
	otherEpoch.PrevDigest, otherEpoch.Digest = "", ""
	if err := SealCommit(p, h.HeaderDigest, "s", &otherEpoch); err != nil {
		t.Fatal(err)
	}
	if otherEpoch.Digest == c.Digest {
		t.Fatal("epoch does not reach the commit digest")
	}

	// Event order inside a batch and batch order inside a commit are both
	// canonical: swapping either changes the digest.
	swappedEvents := Commit{Seq: 0, CommitID: "c1", Epoch: 3, Batches: []StreamBatch{
		{Stream: StreamRef{Kind: StreamKindSession}, Events: []Event{
			{Type: "twilight/x/b", RecordedAtUnixMilli: 1, Payload: jsonstable.MustParse(`{"b":2}`)},
			{Type: "twilight/x/a", RecordedAtUnixMilli: 1, Payload: jsonstable.MustParse(`{"a":1}`)},
		}},
	}}
	if err := SealCommit(p, h.HeaderDigest, "s", &swappedEvents); err != nil {
		t.Fatal(err)
	}
	twoEvents := Commit{Seq: 0, CommitID: "c1", Epoch: 3, Batches: []StreamBatch{
		{Stream: StreamRef{Kind: StreamKindSession}, Events: []Event{
			{Type: "twilight/x/a", RecordedAtUnixMilli: 1, Payload: jsonstable.MustParse(`{"a":1}`)},
			{Type: "twilight/x/b", RecordedAtUnixMilli: 1, Payload: jsonstable.MustParse(`{"b":2}`)},
		}},
	}}
	if err := SealCommit(p, h.HeaderDigest, "s", &twoEvents); err != nil {
		t.Fatal(err)
	}
	if swappedEvents.Digest == twoEvents.Digest {
		t.Fatal("event order inside a batch does not reach the digest")
	}
	swappedBatches := Commit{Seq: 0, CommitID: "c1", Epoch: 3, Batches: []StreamBatch{
		oneEventBatch(StreamRef{Kind: StreamKindRun, ID: "r7"}, "twilight/run/created", `{"runId":"r7"}`),
		oneEventBatch(StreamRef{Kind: StreamKindSession}, "twilight/x/a", `{"a":1}`),
	}}
	if err := SealCommit(p, h.HeaderDigest, "s", &swappedBatches); err != nil {
		t.Fatal(err)
	}
	if swappedBatches.Digest == c.Digest {
		t.Fatal("batch order inside a commit does not reach the digest")
	}

	if err := SealCommit(p, h.HeaderDigest, "s", &Commit{Seq: 0, Batches: c.Batches}); err == nil {
		t.Fatal("empty CommitID accepted")
	}
	if err := SealCommit(p, h.HeaderDigest, "s", &Commit{Seq: 0, CommitID: "c2"}); err == nil {
		t.Fatal("commit without batches accepted")
	}
}

// sealedPair builds a two-commit sealed ledger: one session batch, then a
// commit spanning the session stream and run stream r7.
func sealedPair(t *testing.T) (LedgerProfile, SessionHeader, []Commit) {
	t.Helper()
	p := ProfileV1()
	h := v2Header(t, "s")
	c0 := Commit{Seq: 0, CommitID: "c1", Epoch: 1, Batches: []StreamBatch{
		oneEventBatch(StreamRef{Kind: StreamKindSession}, "twilight/x/a", `{"a":1}`),
	}}
	if err := SealCommit(p, h.HeaderDigest, "s", &c0); err != nil {
		t.Fatal(err)
	}
	c1 := Commit{Seq: 1, CommitID: "c2", Epoch: 1, Batches: []StreamBatch{
		oneEventBatch(StreamRef{Kind: StreamKindSession}, "twilight/x/b", `{"b":2}`),
		oneEventBatch(StreamRef{Kind: StreamKindRun, ID: "r7"}, "twilight/run/created", `{"runId":"r7"}`),
	}}
	if err := SealCommit(p, c0.Digest, "s", &c1); err != nil {
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

	// A ledger validated under another session's header must fail at commit 0.
	p, h, commits := sealedPair(t)
	h.SessionID = "other"
	if err := ValidateLedger(p, h, commits); !IsCode(err, ErrCorrupt) {
		t.Fatalf("ValidateLedger under foreign header = %v, want code corrupt", err)
	}
}
