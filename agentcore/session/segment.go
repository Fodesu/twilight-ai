package session

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/felinics/twilight/agentcore/ledger"
)

// SegmentID identifies one commit segment independently of any Session
// (SES-WIR-4).
type SegmentID string

// SegmentHeader is the immutable creation record of a commit segment. It
// names no Session: which roots append to or include the segment is the
// roots' business (SessionRecord).
type SegmentHeader struct {
	// ID is the segment's identity, drawn at random: two segments with
	// equal records are still two nodes (SES-WIR-4).
	ID          SegmentID          `json:"id"`
	Parent      *CommitRef         `json:"parent,omitempty"` // nil for a root segment; the edge to the parent otherwise
	CausationID ledger.CausationID `json:"causationId,omitempty"`
	// Ext are the module extension slots of the creation record, opaque to
	// readers that know no module (SES-WIR-5).
	Ext Extensions `json:"ext,omitempty"`
}

// Segment is one node of the lineage forest: a creation record. Its
// commits live in the store, numbered from Header.Seed().
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
