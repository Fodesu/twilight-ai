package session_test

import (
	"context"
	"testing"

	"github.com/felinics/twilight/agent/jsonstable"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/filestore/filestoretest"
)

// TestKernelExtSlots covers SES-WIR-5: the kernel extension object on a
// header and on a commit is absent or a canonical JSON object, it enters the
// digest byte for byte, and an absent slot seals exactly as if the slot did
// not exist.
func TestKernelExtSlots(t *testing.T) {
	profile := session.ProfileV1()
	base := session.SegmentHeader{ProtocolVersion: session.ProtocolVersion1, Nonce: "n"}
	baseDigest, err := profile.HeaderDigest(base)
	if err != nil {
		t.Fatal(err)
	}
	batch := []session.StreamBatch{{Stream: session.StreamRef{Domain: "chat"}, Events: []session.Event{
		{Type: "twilight/x/a", RecordedAtUnixMilli: 1, Payload: jsonstable.MustParse(`{}`)}}}}
	sealed := func(ext jsonstable.Value) (session.Commit, error) {
		c := session.Commit{Seq: 0, CommitID: "c1", Epoch: 1, Batches: batch, Ext: ext}
		err := session.SealCommit(profile, baseDigest, session.SegmentID(baseDigest), &c)
		return c, err
	}
	plain, err := sealed(jsonstable.Value{})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name    string
		ext     jsonstable.Value
		invalid bool
		same    bool // digest equals the slot-less digest
	}{
		{name: "absent", same: true},
		{name: "object", ext: jsonstable.MustParse(`{"k":1}`)},
		{name: "other object", ext: jsonstable.MustParse(`{"k":2}`)},
		{name: "array", ext: jsonstable.MustParse(`[1]`), invalid: true},
		{name: "scalar", ext: jsonstable.MustParse(`1`), invalid: true},
	}
	seen := map[string]string{}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := base
			h.Ext = tc.ext
			hd, herr := profile.HeaderDigest(h)
			if herr != nil {
				t.Fatal(herr)
			}
			h.HeaderDigest = hd
			verr := profile.ValidateHeader(h)
			c, cerr := sealed(tc.ext)
			if tc.invalid {
				if !session.IsCode(verr, session.ErrInvalid) || cerr == nil {
					t.Fatalf("invalid ext accepted: header=%v commit=%v", verr, cerr)
				}
				return
			}
			if verr != nil || cerr != nil {
				t.Fatalf("valid ext rejected: header=%v commit=%v", verr, cerr)
			}
			if (hd == baseDigest) != tc.same || (c.Digest == plain.Digest) != tc.same {
				t.Fatalf("digest equality with slot-less = header %v commit %v, want %v", hd == baseDigest, c.Digest == plain.Digest, tc.same)
			}
			if prev, dup := seen[string(hd)]; dup {
				t.Fatalf("header digest collides with case %q", prev)
			}
			seen[string(hd)] = tc.name
		})
	}
}

// TestKernelExtRoundTrip appends a commit carrying Ext through a Store, reads
// it back byte for byte and reopens the Session so Open's chain validation
// sees the slot.
func TestKernelExtRoundTrip(t *testing.T) {
	ctx := context.Background()
	store := filestoretest.Store(t)
	ext := jsonstable.MustParse(`{"kernel":"later"}`)
	if _, err := store.Create(ctx, session.CreateRequest{ProtocolVersion: session.ProtocolVersion1, SessionID: "s", Ext: ext}); err != nil {
		t.Fatal(err)
	}
	h, err := store.Open(ctx, "s", session.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	c, err := h.Append(ctx, session.Proposal{CommitID: "c1", Ext: ext, Batches: []session.StreamBatch{{Stream: session.StreamRef{Domain: "chat"},
		Events: []session.Event{{Type: "twilight/x/a", RecordedAtUnixMilli: 1, Payload: jsonstable.MustParse(`{}`)}}}}})
	if err != nil || c.Ext.String() != ext.String() {
		t.Fatalf("append = %+v, %v", c, err)
	}
	if err := h.Close(ctx); err != nil {
		t.Fatal(err)
	}
	header, err := store.Header(ctx, "s")
	if err != nil || header.Ext.String() != ext.String() {
		t.Fatalf("header = %+v, %v", header, err)
	}
	page, err := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "s"})
	if err != nil || len(page.Commits) != 1 || page.Commits[0].Ext.String() != ext.String() {
		t.Fatalf("read = %+v, %v", page, err)
	}
	if h, err = store.Open(ctx, "s", session.OpenOptions{}); err != nil {
		t.Fatalf("reopen with ext: %v", err)
	}
	_ = h.Close(ctx)
}
