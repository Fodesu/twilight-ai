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
// Session: which roots append to or inherit from the segment is the roots'
// business (SessionRecord), and a segment outlives every Session that named
// it for as long as some root reaches it. ID is the segment's identity,
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

// Segment is a node: an immutable creation record whose own commits start
// at SegmentSeed(Header). Header.Parent is the edge to the parent segment; a
// root segment has none. ID equals Header.ID.
type Segment struct {
	ID     SegmentID
	Header SegmentHeader
}

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
func (s Segment) Seed() Head { return SegmentSeed(s.Header) }

// SegmentSeed is the head of a segment that holds no commits of its own: a
// root segment starts at 0, a child continues its parent's numbering at
// Parent.Seq+1 (SES-FRK-2).
func SegmentSeed(h SegmentHeader) Head {
	if h.Parent != nil {
		return Head{Next: h.Parent.Seq + 1}
	}
	return Head{}
}

// ValidateHeader checks the shape of a segment's creation record: a
// non-empty ID, a well-formed edge and a well-formed Ext. Whether the
// version is one the Ledger serves is the Ledger's check.
func ValidateHeader(h SegmentHeader) error {
	if err := validIdentity("segment ID", string(h.ID)); err != nil {
		return newError(ErrInvalid, "header", "", err.Error())
	}
	if err := ValidateEdge(h.Parent); err != nil {
		return newError(ErrInvalid, "header", "", err.Error())
	}
	if err := ValidateExtensions(h.Ext); err != nil {
		return newError(ErrInvalid, "header", "", err.Error())
	}
	return nil
}
