package session

import (
	"errors"
	"fmt"

	"github.com/felinics/twilight/agent/es"
	"github.com/felinics/twilight/agent/jsonstable"
)

// CommitSeq is the position of one Commit in the ledger and the canonical
// total order of the authority. Per-stream local positions are read
// optimizations derived from the ledger, never a second ordering.
type CommitSeq uint64

// StreamKind partitions the ledger into the long-lived session semantic
// stream and per-run execution streams.
type StreamKind string

const (
	StreamKindSession StreamKind = "session"
	StreamKindRun     StreamKind = "run"
)

// StreamRef names the logical stream one batch belongs to. The session
// stream has an empty ID; a run stream's ID is the RunID, so a run's facts
// always name the stream they belong to.
type StreamRef struct {
	Kind StreamKind `json:"kind"`
	ID   string     `json:"id,omitempty"`
}

// String renders the stream for diagnostics and indexes.
func (r StreamRef) String() string {
	if r.ID == "" {
		return string(r.Kind)
	}
	return fmt.Sprintf("%s/%s", r.Kind, r.ID)
}

// Event is one committed payload. Unlike the v1 row it carries no transaction
// metadata: canonical order comes from CommitSeq plus the event's position
// inside its batch.
type Event struct {
	Type                EventType        `json:"type"`
	RecordedAtUnixMilli int64            `json:"recordedAtUnixMilli"`
	Payload             jsonstable.Value `json:"payload"`
}

// Position is the ledger position of one event: the commit it landed in and
// its index among that commit's events in batch order. Positions order every
// event of a Session totally, so a projection that needs to order what it
// derives records the position of the event that produced it instead of
// keeping a counter of its own.
type Position struct {
	Commit CommitSeq `json:"commit"`
	Index  uint32    `json:"index"`
}

// Less reports whether p precedes q in the ledger.
func (p Position) Less(q Position) bool {
	if p.Commit != q.Commit {
		return p.Commit < q.Commit
	}
	return p.Index < q.Index
}

// StreamBatch is the ordered slice of one commit that belongs to one stream.
// Batches are the chain leaves: a commit digest binds every batch, and a
// batch digest binds every event, in order.
type StreamBatch struct {
	Stream StreamRef `json:"stream"`
	Events []Event   `json:"events"`
}

// Commit is one atomic unit of the ledger. One Append persists exactly one
// Commit; the Commit may span several streams, and the store either lands
// every batch or none (SES-APP-1). Digest chains Commit to Commit; the empty
// ledger head is {Next: 0, Digest: HeaderDigest}.
type Commit struct {
	Seq        CommitSeq     `json:"seq"`
	CommitID   CommitID      `json:"commitId"`
	Epoch      Epoch         `json:"epoch"`
	Batches    []StreamBatch `json:"batches"`
	PrevDigest es.Digest     `json:"prevDigest"`
	Digest     es.Digest     `json:"digest"`
}

// ValidateStreamRef checks a batch's stream attribution.
func ValidateStreamRef(r StreamRef) error {
	switch r.Kind {
	case StreamKindSession:
		if r.ID != "" {
			return errors.New("session stream must not carry an ID")
		}
		return nil
	case StreamKindRun:
		return validIdentity("RunID", r.ID)
	default:
		return fmt.Errorf("unknown stream kind %q", r.Kind)
	}
}

// ValidateEvent checks one event before it is sealed.
func ValidateEvent(e *Event) error {
	return validateEventShape(e.Type, e.Payload)
}

// ValidateBatches checks the shape of one proposed commit before sealing:
// non-empty batches, unique streams within the commit, valid attribution and
// canonical payloads. Duplicate CommitIDs and epoch fencing are store duties.
func ValidateBatches(batches []StreamBatch) error {
	if len(batches) == 0 {
		return errors.New("commit without batches")
	}
	seen := make(map[StreamRef]struct{}, len(batches))
	for i := range batches {
		b := &batches[i]
		if err := ValidateStreamRef(b.Stream); err != nil {
			return fmt.Errorf("batch %d: %w", i, err)
		}
		if _, dup := seen[b.Stream]; dup {
			return fmt.Errorf("batch %d: stream %s appears twice in one commit", i, b.Stream)
		}
		seen[b.Stream] = struct{}{}
		if len(b.Events) == 0 {
			return fmt.Errorf("batch %d: no events", i)
		}
		for j := range b.Events {
			if err := ValidateEvent(&b.Events[j]); err != nil {
				return fmt.Errorf("batch %d event %d: %w", i, j, err)
			}
		}
	}
	return nil
}

// LedgerProfile freezes the commit-ledger wire for one ProtocolVersion: the
// header digest plus the batch and commit digest preimages (SES-WIR-2).
// Every preimage binds to the segment, never to a Session: the segment is
// the canonical object, roots only name it.
type LedgerProfile interface {
	Version() uint16
	HeaderDigest(SegmentHeader) (es.Digest, error)
	BatchDigest(segment SegmentID, batch StreamBatch) (es.Digest, error)
	CommitDigest(prev es.Digest, segment SegmentID, seq CommitSeq, commitID CommitID, epoch Epoch, batches []es.Digest) (es.Digest, error)
	ValidateHeader(SegmentHeader) error
}

// ProfileV1 returns the ProtocolVersion1 commit-ledger profile.
func ProfileV1() LedgerProfile { return profileV1{version: ProtocolVersion1} }

// LedgerProfileFor returns the commit-ledger profile bound to version.
func LedgerProfileFor(version uint16) (LedgerProfile, error) {
	if version == ProtocolVersion1 {
		return profileV1{version: version}, nil
	}
	return nil, &Error{Code: ErrUnsupportedProfile, Operation: "profile", Detail: fmt.Sprintf("protocol version %d", version)}
}

// profileV1 freezes the commit wire of one ProtocolVersion. The version is a
// field so it reaches every digest domain separator (SES-VER-2).
type profileV1 struct{ version uint16 }

func (p profileV1) Version() uint16 { return p.version }

type batchEventBody struct {
	Type                EventType
	RecordedAtUnixMilli int64
	Payload             jsonstable.Value
}

type batchDigestBody struct {
	Segment SegmentID
	Stream  StreamRef
	Events  []batchEventBody
}

type commitDigestBody struct {
	Prev     es.Digest
	Segment  SegmentID
	Seq      CommitSeq
	CommitID CommitID
	Epoch    Epoch
	Batches  []es.Digest
}

func (p profileV1) HeaderDigest(h SegmentHeader) (es.Digest, error) {
	return digestDomain(p.version, "twilight/session/header", headerDigestBody{h.ProtocolVersion, h.Parent, h.Nonce, h.CausationID, h.Metadata})
}

func (p profileV1) BatchDigest(segment SegmentID, batch StreamBatch) (es.Digest, error) {
	body := batchDigestBody{Segment: segment, Stream: batch.Stream, Events: make([]batchEventBody, len(batch.Events))}
	for i := range batch.Events {
		e := &batch.Events[i]
		body.Events[i] = batchEventBody{e.Type, e.RecordedAtUnixMilli, e.Payload}
	}
	return digestDomain(p.version, "twilight/session/batch", body)
}

func (p profileV1) CommitDigest(prev es.Digest, segment SegmentID, seq CommitSeq, commitID CommitID, epoch Epoch, batches []es.Digest) (es.Digest, error) {
	return digestDomain(p.version, "twilight/session/commit", commitDigestBody{prev, segment, seq, commitID, epoch, batches})
}

func (p profileV1) ValidateHeader(h SegmentHeader) error {
	if h.ProtocolVersion != p.version {
		return &Error{Code: ErrUnsupportedProfile, Operation: "header"}
	}
	if err := validIdentity("Nonce", h.Nonce); err != nil {
		return newError(ErrInvalid, "header", "", err.Error())
	}
	if err := ValidateEdge(h.Parent); err != nil {
		return newError(ErrInvalid, "header", "", err.Error())
	}
	want, err := p.HeaderDigest(h)
	if err != nil {
		return err
	}
	if h.HeaderDigest != want {
		return newError(ErrCorrupt, "header", "", "header digest mismatch")
	}
	return nil
}

// SealCommit stamps PrevDigest and Digest onto c, chaining from prev and
// binding the commit to the segment it belongs to. It validates the batches
// first, so a sealed Commit is always well-formed. The kernel seals with its
// current Epoch and the head digest; validation reseals and compares.
func SealCommit(p LedgerProfile, prev es.Digest, segment SegmentID, c *Commit) error {
	if err := validIdentity("CommitID", string(c.CommitID)); err != nil {
		return err
	}
	if err := ValidateBatches(c.Batches); err != nil {
		return err
	}
	batchDigests := make([]es.Digest, len(c.Batches))
	for i := range c.Batches {
		d, err := p.BatchDigest(segment, c.Batches[i])
		if err != nil {
			return err
		}
		batchDigests[i] = d
	}
	d, err := p.CommitDigest(prev, segment, c.Seq, c.CommitID, c.Epoch, batchDigests)
	if err != nil {
		return err
	}
	c.PrevDigest = prev
	c.Digest = d
	return nil
}

// ValidateEdge checks the shape of a parent edge: nil is a root segment;
// otherwise it names a parent segment and a commit digest. Whether the
// parent holds that commit is the Ledger's check at Create (SES-FRK-1).
func ValidateEdge(edge *LedgerRef) error {
	if edge == nil {
		return nil
	}
	if err := validIdentity("Parent.Segment", string(edge.Segment)); err != nil {
		return err
	}
	if edge.Digest == "" {
		return fmt.Errorf("parent edge has no digest")
	}
	return nil
}

// LedgerSeed is the head of a segment that holds no commits of its own: the
// chain start every own commit is sealed from (SES-FRK-2). A root segment
// seeds at {0, HeaderDigest}; a child continues the parent's chain at
// {Parent.Seq+1, Parent.Digest}, so its own commits are verifiable from the
// edge alone while their digests bind them to this segment.
func LedgerSeed(h SegmentHeader) Head {
	if h.Parent != nil {
		return Head{Next: h.Parent.Seq + 1, Digest: h.Parent.Digest}
	}
	return Head{Next: 0, Digest: h.HeaderDigest}
}

// ValidateLedger recomputes every commit digest from the segment's seed and
// reports the first corrupt commit (SES-REP-1). commits are the segment's own
// commits, contiguous from LedgerSeed(header).Next; an inherited prefix is
// validated under its own segment's header.
func ValidateLedger(p LedgerProfile, header SegmentHeader, commits []Commit) error {
	seed := LedgerSeed(header)
	segment := SegmentIDOf(header)
	prev := seed.Digest
	for i := range commits {
		c := &commits[i]
		if c.Seq != seed.Next+CommitSeq(i) {
			return &Error{Code: ErrCorrupt, Operation: "read", Detail: fmt.Sprintf("segment %s: seq gap at %d", segment, i)}
		}
		resealed := *c
		resealed.PrevDigest = ""
		resealed.Digest = ""
		if err := SealCommit(p, prev, segment, &resealed); err != nil {
			// A stored commit the profile cannot reseal is corrupt to a reader, whatever the seal rejected.
			return &Error{Code: ErrCorrupt, Operation: "read", CommitID: c.CommitID, Detail: fmt.Sprintf("segment %s: commit %d cannot be resealed: %v", segment, i, err)}
		}
		if c.PrevDigest != prev || c.Digest != resealed.Digest {
			return &Error{Code: ErrCorrupt, Operation: "read", CommitID: c.CommitID, Detail: fmt.Sprintf("segment %s: digest mismatch at commit %d", segment, i)}
		}
		prev = c.Digest
	}
	return nil
}
