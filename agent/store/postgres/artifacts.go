package postgres

import (
	"context"
	"encoding/json"

	"github.com/felinics/twilight/agent/store/postgres/internal/db"
	"github.com/felinics/twilight/agentcore/artifact"
)

// BindingStore is artifact.BindingStore over the bindings table (ART-BND-1).
type BindingStore struct{ d *DB }

var _ artifact.BindingStore = (*BindingStore)(nil)

// Bindings is the artifact BindingStore over this database.
func (d *DB) Bindings() *BindingStore { return &BindingStore{d: d} }

func lookupBinding(ctx context.Context, q *db.Queries, id artifact.BindingID) (artifact.Binding, bool, error) {
	raw, err := q.Binding(ctx, string(id))
	if noRows(err) {
		return artifact.Binding{}, false, nil
	}
	if err != nil {
		return artifact.Binding{}, false, err
	}
	var b artifact.Binding
	if err := json.Unmarshal([]byte(raw), &b); err != nil {
		return artifact.Binding{}, false, &artifact.Error{Code: artifact.ErrCorrupt, Operation: "lookup_binding", Identity: string(id), Detail: err.Error()}
	}
	return b, true, nil
}

func (s *BindingStore) CreateBinding(ctx context.Context, b artifact.Binding) (artifact.Binding, error) { //nolint:gocritic // hugeParam: BindingStore contract takes the Binding by value
	want, err := artifact.DigestBinding(b.ID, b.Ref)
	if err != nil {
		return artifact.Binding{}, err
	}
	if b.Digest != want {
		return artifact.Binding{}, &artifact.Error{Code: artifact.ErrInvalid, Operation: "create_binding", Identity: string(b.ID), Detail: "binding digest mismatch"}
	}
	raw, err := json.Marshal(b)
	if err != nil {
		return artifact.Binding{}, err
	}
	var out artifact.Binding
	err = s.d.tx(ctx, "binding:"+string(b.ID), func(q *db.Queries) error {
		existing, ok, err := lookupBinding(ctx, q, b.ID)
		if err != nil {
			return err
		}
		if ok {
			if existing.Digest != b.Digest {
				return &artifact.Error{Code: artifact.ErrConflict, Operation: "create_binding", Identity: string(b.ID)}
			}
			out = existing
			return nil
		}
		out = b
		return q.InsertBinding(ctx, db.InsertBindingParams{ID: string(b.ID), Digest: string(b.Digest), Binding: string(raw)})
	})
	if err != nil {
		return artifact.Binding{}, err
	}
	return out, nil
}

func (s *BindingStore) LookupBinding(ctx context.Context, id artifact.BindingID) (artifact.Binding, bool, error) {
	return lookupBinding(ctx, s.d.q, id)
}

func (s *BindingStore) ResolveBinding(ctx context.Context, id artifact.BindingID) (artifact.Binding, error) {
	b, ok, err := s.LookupBinding(ctx, id)
	if err != nil {
		return artifact.Binding{}, err
	}
	if !ok {
		return artifact.Binding{}, &artifact.Error{Code: artifact.ErrNotFound, Operation: "resolve_binding", Identity: string(id)}
	}
	return b, nil
}

// RetentionLedger is artifact.RetentionLedger over the claims table
// (ART-RET-2, ART-RET-3).
type RetentionLedger struct {
	d       *DB
	builder artifact.BindingSetBuilder
}

var _ artifact.RetentionLedger = (*RetentionLedger)(nil)

// Ledger is the artifact RetentionLedger over this database; builder, when
// set, rebuilds and verifies every incoming BindingSet (ART-RET-1).
func (d *DB) Ledger(builder artifact.BindingSetBuilder) *RetentionLedger {
	return &RetentionLedger{d: d, builder: builder}
}

const opActivate = "activate"

func lookupClaim(ctx context.Context, q *db.Queries, id artifact.ClaimID) (artifact.RetentionClaim, bool, error) {
	raw, err := q.Claim(ctx, string(id))
	if noRows(err) {
		return artifact.RetentionClaim{}, false, nil
	}
	if err != nil {
		return artifact.RetentionClaim{}, false, err
	}
	var c artifact.RetentionClaim
	if err := json.Unmarshal([]byte(raw), &c); err != nil {
		return artifact.RetentionClaim{}, false, &artifact.Error{Code: artifact.ErrCorrupt, Operation: "lookup_claim", Identity: string(id), Detail: err.Error()}
	}
	return c, true, nil
}

func (l *RetentionLedger) Activate(ctx context.Context, id artifact.ClaimID, owner artifact.ClaimOwner, set artifact.BindingSet) (artifact.RetentionClaim, error) {
	if id == "" || owner.Kind == "" || owner.Identity == "" {
		return artifact.RetentionClaim{}, &artifact.Error{Code: artifact.ErrInvalid, Operation: opActivate, Identity: string(id), Detail: "empty claim identity or owner"}
	}
	if len(set.BindingIDs) == 0 || set.RefSetDigest == "" {
		return artifact.RetentionClaim{}, &artifact.Error{Code: artifact.ErrInvalid, Operation: opActivate, Identity: string(id), Detail: "empty binding set"}
	}
	if l.builder != nil {
		rebuilt, err := l.builder.Build(ctx, set.BindingIDs)
		if err != nil {
			return artifact.RetentionClaim{}, err
		}
		if !sameSet(rebuilt, set) {
			return artifact.RetentionClaim{}, &artifact.Error{Code: artifact.ErrInvalid, Operation: opActivate, Identity: string(id), Detail: "binding set does not verify"}
		}
	}
	claim := artifact.RetentionClaim{ID: id, Owner: owner, BindingSet: set, State: artifact.ClaimActive}
	raw, err := json.Marshal(claim)
	if err != nil {
		return artifact.RetentionClaim{}, err
	}
	var out artifact.RetentionClaim
	err = l.d.tx(ctx, "claim:"+string(id), func(q *db.Queries) error {
		existing, ok, err := lookupClaim(ctx, q, id)
		if err != nil {
			return err
		}
		if ok {
			if existing.State != artifact.ClaimActive || existing.Owner != owner || !sameSet(existing.BindingSet, set) {
				return &artifact.Error{Code: artifact.ErrConflict, Operation: opActivate, Identity: string(id)}
			}
			out = existing
			return nil
		}
		out = claim
		return q.InsertClaim(ctx, db.InsertClaimParams{ID: string(id), OwnerKind: owner.Kind, OwnerAuthority: owner.Authority, OwnerIdentity: owner.Identity, State: string(artifact.ClaimActive), Claim: string(raw)})
	})
	if err != nil {
		return artifact.RetentionClaim{}, err
	}
	return out, nil
}

func (l *RetentionLedger) LookupClaim(ctx context.Context, id artifact.ClaimID) (artifact.RetentionClaim, bool, error) {
	return lookupClaim(ctx, l.d.q, id)
}

func (l *RetentionLedger) ReleaseActive(ctx context.Context, id artifact.ClaimID) error {
	return l.d.tx(ctx, "claim:"+string(id), func(q *db.Queries) error {
		c, ok, err := lookupClaim(ctx, q, id)
		if err != nil {
			return err
		}
		if !ok {
			return &artifact.Error{Code: artifact.ErrNotFound, Operation: "release", Identity: string(id)}
		}
		c.State = artifact.ClaimReleased
		raw, err := json.Marshal(c)
		if err != nil {
			return err
		}
		return q.UpdateClaim(ctx, db.UpdateClaimParams{State: string(artifact.ClaimReleased), Claim: string(raw), ID: string(id)})
	})
}

// ClaimsByOwner pages the owner's claims in ClaimID order under a watermark
// cursor (ART-RET-3); the identity rule is applied while scanning.
func (l *RetentionLedger) ClaimsByOwner(ctx context.Context, query artifact.ClaimOwnerQuery, cursor artifact.ClaimCursor) (artifact.ClaimPage, error) {
	if query.Kind == "" || query.Authority == "" {
		return artifact.ClaimPage{}, &artifact.Error{Code: artifact.ErrInvalid, Operation: "claims_by_owner", Detail: "empty owner kind or authority"}
	}
	limit := query.Limit
	if limit <= 0 {
		limit = artifact.DefaultClaimPageSize
	}
	rows, err := l.d.q.ClaimsByOwnerAfter(ctx, db.ClaimsByOwnerAfterParams{OwnerKind: query.Kind, OwnerAuthority: query.Authority, ID: string(cursor.After)})
	if err != nil {
		return artifact.ClaimPage{}, err
	}
	watermark := cursor.Watermark
	var items []artifact.RetentionClaim
	for _, r := range rows {
		if watermark != "" && artifact.ClaimID(r.ID) > watermark {
			break
		}
		if !matchesIdentity(query, r.OwnerIdentity) {
			continue
		}
		if len(items) == limit {
			if watermark == "" {
				watermark, err = l.lastMatching(ctx, query)
				if err != nil {
					return artifact.ClaimPage{}, err
				}
			}
			return artifact.ClaimPage{Items: items, Next: &artifact.ClaimCursor{Watermark: watermark, After: items[len(items)-1].ID}}, nil
		}
		var c artifact.RetentionClaim
		if err := json.Unmarshal([]byte(r.Claim), &c); err != nil {
			return artifact.ClaimPage{}, &artifact.Error{Code: artifact.ErrCorrupt, Operation: "claims_by_owner", Identity: r.ID, Detail: err.Error()}
		}
		items = append(items, c)
	}
	return artifact.ClaimPage{Items: items}, nil
}

func (l *RetentionLedger) lastMatching(ctx context.Context, query artifact.ClaimOwnerQuery) (artifact.ClaimID, error) {
	rows, err := l.d.q.ClaimIdentitiesByOwnerDesc(ctx, db.ClaimIdentitiesByOwnerDescParams{OwnerKind: query.Kind, OwnerAuthority: query.Authority})
	if err != nil {
		return "", err
	}
	for _, r := range rows {
		if matchesIdentity(query, r.OwnerIdentity) {
			return artifact.ClaimID(r.ID), nil
		}
	}
	return "", nil
}

func matchesIdentity(q artifact.ClaimOwnerQuery, identity string) bool {
	if identity == "" {
		return false
	}
	if q.Identities == nil {
		return true
	}
	for _, want := range q.Identities {
		if want != "" && want == identity {
			return true
		}
	}
	return false
}

func sameSet(a, b artifact.BindingSet) bool {
	if a.RefSetDigest != b.RefSetDigest || len(a.BindingIDs) != len(b.BindingIDs) {
		return false
	}
	for i := range a.BindingIDs {
		if a.BindingIDs[i] != b.BindingIDs[i] {
			return false
		}
	}
	return true
}
