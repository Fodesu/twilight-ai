package session_test

import (
	"context"
	"testing"

	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/filestore/filestoretest"
)

// TestKernelExtSlots covers SES-WIR-5: extension slots are keyed by module
// and hold JSON the kernel does not interpret; a malformed key or value is
// ErrInvalid.
func TestKernelExtSlots(t *testing.T) {
	batch := []session.StreamBatch{{Stream: session.StreamRef{Domain: "chat"}, Events: []session.Event{
		{Type: "twilight/x/a", RecordedAtUnixMilli: 1, Payload: jsonstable.MustParse(`{}`)}}}}
	run := session.ModuleKey{Source: "twilight", ID: "run"}
	cases := []struct {
		name    string
		ext     session.Extensions
		invalid bool
	}{
		{name: "absent"},
		{name: "object", ext: session.Extensions{run: session.RawValue(`{"k":1}`)}},
		{name: "scalar", ext: session.Extensions{run: session.RawValue(`1`)}},
		{name: "two modules", ext: session.Extensions{run: session.RawValue(`{}`), {Source: "acme", ID: "audit"}: session.RawValue(`[1]`)}},
		{name: "empty value", ext: session.Extensions{run: nil}, invalid: true},
		{name: "not JSON", ext: session.Extensions{run: session.RawValue(`{`)}, invalid: true},
		{name: "source with separator", ext: session.Extensions{{Source: "a/b", ID: "x"}: session.RawValue(`1`)}, invalid: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			herr := session.ValidateHeader(session.SegmentHeader{ID: "seg", ProtocolVersion: session.ProtocolVersion1, Ext: tc.ext})
			cerr := session.ValidateCommit(&session.Commit{CommitID: "c1", Batches: batch, Ext: tc.ext})
			if tc.invalid {
				if !session.IsCode(herr, session.ErrInvalid) || cerr == nil {
					t.Fatalf("invalid ext accepted: header=%v commit=%v", herr, cerr)
				}
				return
			}
			if herr != nil || cerr != nil {
				t.Fatalf("valid ext rejected: header=%v commit=%v", herr, cerr)
			}
		})
	}
}

// TestKernelExtRoundTrip appends a commit carrying Ext through a Store, reads
// it back byte for byte under its module key and reopens the Session.
func TestKernelExtRoundTrip(t *testing.T) {
	ctx := context.Background()
	store := filestoretest.Store(t)
	key := session.ModuleKey{Source: "acme", ID: "audit"}
	ext := session.Extensions{key: session.RawValue(`{"by":"later"}`)}
	same := func(got session.Extensions) bool { return len(got) == 1 && string(got[key]) == string(ext[key]) }
	if _, err := store.Create(ctx, session.CreateRequest{ProtocolVersion: session.ProtocolVersion1, SessionID: "s", Ext: ext}); err != nil {
		t.Fatal(err)
	}
	h, err := store.Open(ctx, "s", session.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	c, err := h.Append(ctx, session.Proposal{CommitID: "c1", Ext: ext, Batches: []session.StreamBatch{{Stream: session.StreamRef{Domain: "chat"},
		Events: []session.Event{{Type: "twilight/x/a", RecordedAtUnixMilli: 1, Payload: jsonstable.MustParse(`{}`)}}}}})
	if err != nil || !same(c.Ext) {
		t.Fatalf("append = %+v, %v", c, err)
	}
	if err := h.Close(ctx); err != nil {
		t.Fatal(err)
	}
	header, err := store.Header(ctx, "s")
	if err != nil || !same(header.Ext) {
		t.Fatalf("header = %+v, %v", header, err)
	}
	page, err := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "s"})
	if err != nil || len(page.Commits) != 1 || !same(page.Commits[0].Ext) {
		t.Fatalf("read = %+v, %v", page, err)
	}
	if h, err = store.Open(ctx, "s", session.OpenOptions{}); err != nil {
		t.Fatalf("reopen with ext: %v", err)
	}
	_ = h.Close(ctx)
}
