// Package migrate moves a logical Session from one Schema to another
// (SES-MIG). A Session's Schema belongs to its tip segment and a segment
// never changes Schema (EXT-SCH-1); a migration publishes a new segment
// under the target Schema at the tip's head, and the Session continues
// there. It is an explicit management operation of the Session authority --
// a Run, a Turn or a projection never triggers one -- and it happens only
// at a quiescent point: no active Turn, no active Run, nothing in flight
// whose outcome the Session does not yet hold. The transition itself is the
// Writer's Advance (EXT-WRT-10): one fenced, atomic root move (SES-ADV-2), so
// a failure before the publication point leaves the Session on the source
// Schema entirely and one after it leaves the Session on the target
// entirely.
//
// The bootstrap of the new segment is derived from the source's
// authoritative state, never by feeding source events to target projectors:
// the Migrator folds the source segment under the source Schema (through the
// View's projections) and constructs the target's bootstrap facts
// deterministically, so the same source head under the same Profile yields
// the same target segment.
package migrate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/felinics/twilight/agent/es"
	"github.com/felinics/twilight/agent/jsonstable"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/extension"
	"github.com/felinics/twilight/agent/session/writer"
)

// Profile names one deterministic migration procedure, source and target
// included, such as "twilight/session-migration/v1-to-v2@1". Two
// migrations of the same source head under the same Profile produce the
// same bootstrap; a change to the procedure is a new Profile.
type Profile string

// Migrator is one migration procedure between two Schemas. Bootstrap reads
// the source segment's authoritative state through the View -- folded under
// the source Schema -- and returns the groups that seed the target segment,
// encoded under Target by the Writer. It may return no groups when the
// target needs no bootstrap. A CommitID it leaves empty is filled from the
// MigrationID, so a deterministic Bootstrap is idempotent by construction.
type Migrator interface {
	Profile() Profile
	Source() extension.SchemaVersion
	Target() extension.SchemaVersion
	Bootstrap(ctx context.Context, view writer.View) ([]writer.SemanticGroup, error)
}

// Guard is one quiescence precondition, evaluated inside the Writer's
// critical section against the tip: a non-nil error refuses the migration
// as ErrNotQuiescent. The turn and run modules provide theirs
// (turn.RequireNoActiveTurn, runmod.RequireNoActiveRun); a deployment adds
// its own for effects it tracks outside the Session.
type Guard func(writer.View) error

// ErrNotQuiescent reports a migration attempted while a Guard sees the
// Session busy: an active Turn, an active Run, or an effect whose outcome
// is not settled. The refusal wraps the Guard's own error.
var ErrNotQuiescent = errors.New("migrate: session is not at a quiescent point")

// Outcome is the semantic answer of a Migrate.
type Outcome string

const (
	// Applied: this call published the target segment.
	Applied Outcome = "applied"
	// AlreadyApplied: the Session already sits on the target Schema, by
	// this migration (ID set) or by whatever created it there (ID empty).
	AlreadyApplied Outcome = "already_applied"
)

// Result is what a Migrate returns. Header and Commits are set for Applied.
type Result struct {
	Outcome Outcome
	ID      es.Digest
	Header  session.SegmentHeader
	Commits []session.Commit
}

// ProvenanceKey is the first-level key of the target segment's metadata
// object holding its Provenance.
const ProvenanceKey = "twilight/migration"

// Provenance records on the target segment where it came from
// (SES-MIG-3): the source segment and head, both Schemas, the Profile and
// the MigrationID. Head is spelled out field by field so the record has a
// wire form of its own.
type Provenance struct {
	ID              es.Digest               `json:"id"`
	PreviousSegment session.SegmentID       `json:"previous_segment"`
	PreviousSeq     session.CommitSeq       `json:"previous_seq"`
	PreviousDigest  es.Digest               `json:"previous_digest"`
	Source          extension.SchemaVersion `json:"source"`
	Target          extension.SchemaVersion `json:"target"`
	Profile         Profile                 `json:"profile"`
}

// identity is the preimage of a MigrationID.
type identity struct {
	SessionID session.SessionID       `json:"session_id"`
	Segment   session.SegmentID       `json:"segment"`
	Seq       session.CommitSeq       `json:"seq"`
	Digest    es.Digest               `json:"digest"`
	Source    extension.SchemaVersion `json:"source"`
	Target    extension.SchemaVersion `json:"target"`
}

const (
	identityType = "twilight/session/migration"
	opMigrate    = "migrate"
)

// IDOf is the MigrationID (SES-MIG-2): the digest of the Session, the
// source segment at its head, and the two Schemas. It names the slot one
// migration of this Session fills, whatever Profile fills it: the same slot
// under another Profile is a conflict, not a second migration.
func IDOf(sid session.SessionID, segment session.SegmentID, head session.Head, source, target extension.SchemaVersion) es.Digest {
	raw, err := es.EncodeTypedPayload(1, identityType, identity{SessionID: sid, Segment: segment, Seq: head.Next - 1, Digest: head.Digest, Source: source, Target: target})
	if err != nil {
		panic(err) // fixed shape of plain fields; cannot fail
	}
	return es.DigestBytes(raw)
}

// BootstrapCommitID is the CommitID of the i-th bootstrap group of a
// migration that names none itself.
func BootstrapCommitID(id es.Digest, i int) session.CommitID {
	return session.CommitID(fmt.Sprintf("migration:%s:%d", id, i))
}

// Declare returns meta with the Provenance recorded. A meta already holding
// a record is refused; the Schema declaration stays the Writer's.
func Declare(meta jsonstable.Value, prov Provenance) (jsonstable.Value, error) {
	m := map[string]json.RawMessage{}
	if !meta.IsZero() && string(meta.Bytes()) != "null" {
		if err := json.Unmarshal(meta.Bytes(), &m); err != nil {
			return jsonstable.Value{}, errors.New("migrate: segment metadata is not an object")
		}
	}
	if _, ok := m[ProvenanceKey]; ok {
		return jsonstable.Value{}, errors.New("migrate: segment metadata already records a migration")
	}
	raw, err := json.Marshal(prov)
	if err != nil {
		return jsonstable.Value{}, err
	}
	m[ProvenanceKey] = raw
	return jsonstable.FromValue(m)
}

// Record reads the Provenance a segment header carries; ok is false for a
// segment no migration created. A record whose ID does not recompute from
// its fields, or whose source head is not the header's parent edge, is
// corrupt: the header and the record disagree about where the segment
// came from.
func Record(h session.SegmentHeader) (Provenance, bool, error) {
	if h.Metadata.IsZero() {
		return Provenance{}, false, nil
	}
	var m map[string]jsonstable.Value
	if err := json.Unmarshal(h.Metadata.Bytes(), &m); err != nil {
		return Provenance{}, false, nil // not an object: no record
	}
	raw, ok := m[ProvenanceKey]
	if !ok {
		return Provenance{}, false, nil
	}
	var prov Provenance
	if err := extension.StrictDecode(raw, &prov); err != nil {
		return Provenance{}, false, fmt.Errorf("migrate: %s: %w", ProvenanceKey, err)
	}
	if h.Parent == nil || h.Parent.Segment != prov.PreviousSegment || h.Parent.Seq != prov.PreviousSeq || h.Parent.Digest != prov.PreviousDigest {
		return Provenance{}, false, errors.New("migrate: migration record names a source head other than the segment's parent")
	}
	return prov, true, nil
}

// Migrate runs m against the Session w owns (SES-MIG-1). Inside the Writer's
// critical section it reads the tip: on the target Schema already, the tip's
// record decides between AlreadyApplied (same Profile) and ErrConflict
// (another Profile filled the slot); on the source Schema, every guard must
// pass, then m's bootstrap is published as the new tip under the target
// Schema with the Provenance in its metadata; on any other Schema the
// migration does not apply (ErrInvalid). The Writer's own refusals of the
// request surface as ErrInvalid too. w is closed by the caller; the
// authority holds it exclusively for the duration (AUTH-MIG-1).
func Migrate(ctx context.Context, w writer.Writer, m Migrator, guards ...Guard) (Result, error) {
	if w == nil || m == nil {
		return Result{}, errors.New("migrate: writer and migrator are required")
	}
	if m.Source() == 0 || m.Target() == 0 || m.Source() == m.Target() || m.Profile() == "" {
		return Result{}, fmt.Errorf("migrate: profile %q must name two distinct schemas", m.Profile())
	}
	sid := w.SessionID()
	var result Result
	res, err := w.Advance(ctx, func(view writer.View) (*writer.AdvanceRequest, error) {
		header := view.Header()
		switch schema := view.Schema(); {
		case schema == m.Target():
			prov, ok, err := Record(header)
			if err != nil {
				return nil, &session.Error{Code: session.ErrCorrupt, Operation: opMigrate, SessionID: sid, Detail: err.Error()}
			}
			if !ok {
				result = Result{Outcome: AlreadyApplied}
				return nil, nil
			}
			if prov.Source != m.Source() || prov.Profile != m.Profile() {
				return nil, &session.Error{Code: session.ErrConflict, Operation: opMigrate, SessionID: sid,
					Detail: fmt.Sprintf("tip was migrated from schema %d under %s, not from %d under %s", prov.Source, prov.Profile, m.Source(), m.Profile())}
			}
			result = Result{Outcome: AlreadyApplied, ID: prov.ID}
			return nil, nil
		case schema != m.Source():
			return nil, &session.Error{Code: session.ErrInvalid, Operation: opMigrate, SessionID: sid,
				Detail: fmt.Sprintf("tip is on schema %d; %s migrates %d to %d", schema, m.Profile(), m.Source(), m.Target())}
		}
		for _, guard := range guards {
			if err := guard(view); err != nil {
				return nil, fmt.Errorf("%w: %w", ErrNotQuiescent, err)
			}
		}
		head := view.Head()
		id := IDOf(sid, session.SegmentIDOf(header), head, m.Source(), m.Target())
		groups, err := m.Bootstrap(ctx, view)
		if err != nil {
			return nil, err
		}
		for i := range groups {
			if groups[i].CommitID == "" {
				groups[i].CommitID = BootstrapCommitID(id, i)
			}
		}
		meta, err := Declare(jsonstable.Value{}, Provenance{ID: id, PreviousSegment: session.SegmentIDOf(header), PreviousSeq: head.Next - 1, PreviousDigest: head.Digest,
			Source: m.Source(), Target: m.Target(), Profile: m.Profile()})
		if err != nil {
			return nil, err
		}
		result = Result{Outcome: Applied, ID: id}
		return &writer.AdvanceRequest{Target: m.Target(), CausationID: es.CausationID(id), Metadata: meta, Bootstrap: groups}, nil
	})
	if err != nil {
		return Result{}, err
	}
	switch res.Outcome {
	case writer.AdvanceApplied:
		result.Header, result.Commits = res.Header, res.Commits
		return result, nil
	case writer.AdvanceNoop:
		return result, nil
	default:
		return Result{}, &session.Error{Code: session.ErrInvalid, Operation: opMigrate, SessionID: sid, Detail: fmt.Sprintf("%s: %s", res.Outcome, res.Detail)}
	}
}
