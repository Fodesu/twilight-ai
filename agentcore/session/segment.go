package session

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/felinics/twilight/agentcore/es"
)

// The Session lineage is a tree (agent-session.md section 8, SES-LIN-1):
// immutable commit segments are its nodes, a segment's single parent anchor
// is an edge, and a Session is a root that names the segment it appends to.
// A segment has at most one parent, so the segments under one root segment
// form a tree and all of them a forest; no operation gives an existing
// segment a second parent. These are the domain types; the Ledger implements
// every operation over them and the adapters store them.

// SegmentID identifies one commit segment independently of any Session: 128
// random bits the kernel draws when the segment is created (SES-WIR-4).
type SegmentID string

// SegmentHeader is the immutable creation record of a commit segment
// (agent-session.md section 8): a node of the lineage tree. It names no
// Session: which roots append to or include the segment is the roots'
// business (SessionRecord). The segment remains while a live path span or a
// child edge still names it. ID is the segment's identity,
// drawn at random, so two otherwise identical records are two segments.
// Readers of a Session see the header of the segment its root names as its
// tip.
type SegmentHeader struct {
	// ID is the segment's identity: 128 random bits the kernel draws at
	// Create, hex encoded. Nothing derives it, so two segments
	// with otherwise equal records are two nodes (SES-WIR-4).
	ID          SegmentID      `json:"id"`
	Parent      *CommitRef     `json:"parent,omitempty"` // nil for a root segment; the edge to the parent otherwise
	CausationID es.CausationID `json:"causationId,omitempty"`
	// Ext holds the module extension slots of the creation record, one raw
	// value per module (SES-WIR-5); a reader that knows none of the modules
	// keeps them.
	Ext Extensions `json:"ext,omitempty"`
}

// Segment is one node of the lineage forest. It stores a creation record
// and the append-only log of commits written to this node, numbered from
// Header.Seed(). The log may be empty. A session path names the segment in
// one span and includes only that span's range, so commits past a closed
// bound are not part of that session. Header.Parent is the edge to the
// parent segment; a root segment has none. The segment's identity is Header.ID.
type Segment struct {
	Header SegmentHeader
}

// ID is the segment's identity.
func (s Segment) ID() SegmentID { return s.Header.ID }

// NewSegmentID returns a fresh segment identity: 128 random bits, hex encoded.
func NewSegmentID() (SegmentID, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("session: segment id: %w", err)
	}
	return SegmentID(hex.EncodeToString(b[:])), nil
}

// Parent returns the edge to the parent segment, or nil for a root.
func (s Segment) Parent() *CommitRef {
	if s.Header.Parent == nil {
		return nil
	}
	edge := *s.Header.Parent
	return &edge
}

// Seed is the head of the segment while it holds no commits of its own.
func (s Segment) Seed() Head { return s.Header.Seed() }

// Seed is the head of a segment that holds no commits of its own: a root
// segment starts at 0, a child continues its parent's numbering at
// Parent.Seq+1 (SES-FRK-2).
func (h SegmentHeader) Seed() Head {
	if h.Parent != nil {
		return Head{Next: h.Parent.Seq + 1}
	}
	return Head{}
}

// Validate checks the shape of a segment's creation record: a non-empty ID,
// a well-formed edge and a well-formed Ext.
func (h SegmentHeader) Validate() error {
	if err := validIdentity("segment ID", string(h.ID)); err != nil {
		return newError(ErrInvalid, "header", "", err.Error())
	}
	if err := h.Parent.Validate(); err != nil {
		return newError(ErrInvalid, "header", "", err.Error())
	}
	if err := ValidateExtensions(h.Ext); err != nil {
		return newError(ErrInvalid, "header", "", err.Error())
	}
	return nil
}
