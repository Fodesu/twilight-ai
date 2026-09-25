package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/felinics/twilight/agentcore/artifact"
)

// RetentionLedger is artifact.RetentionLedger over the claims table
// (ART-RET-2, ART-RET-3). Activate returns once the claim's row is
// committed, which is what makes the claim durable.
type RetentionLedger struct {
	db      *sql.DB
	builder artifact.BindingSetBuilder
}

var _ artifact.RetentionLedger = (*RetentionLedger)(nil)

const opActivate = "activate"

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
	err = tx(ctx, l.db, func(t *sql.Tx) error {
		existing, ok, err := lookupClaim(ctx, t, id)
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
		_, err = t.ExecContext(ctx, `INSERT INTO claims (id, owner_kind, owner_authority, owner_identity, state, claim) VALUES (?, ?, ?, ?, ?, ?)`,
			string(id), owner.Kind, owner.Authority, owner.Identity, string(artifact.ClaimActive), string(raw))
		return err
	})
	if err != nil {
		return artifact.RetentionClaim{}, err
	}
	return out, nil
}

func lookupClaim(ctx context.Context, q querier, id artifact.ClaimID) (artifact.RetentionClaim, bool, error) {
	var raw string
	err := q.QueryRowContext(ctx, `SELECT claim FROM claims WHERE id = ?`, string(id)).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
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

func (l *RetentionLedger) LookupClaim(ctx context.Context, id artifact.ClaimID) (artifact.RetentionClaim, bool, error) {
	return lookupClaim(ctx, l.db, id)
}

func (l *RetentionLedger) ReleaseActive(ctx context.Context, id artifact.ClaimID) error {
	return tx(ctx, l.db, func(t *sql.Tx) error {
		c, ok, err := lookupClaim(ctx, t, id)
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
		_, err = t.ExecContext(ctx, `UPDATE claims SET state = ?, claim = ? WHERE id = ?`, string(artifact.ClaimReleased), string(raw), string(id))
		return err
	})
}

// ClaimsByOwner pages the owner's claims in ClaimID order under a watermark
// cursor (ART-RET-3). The rows are selected by Kind and Authority in SQL;
// the identity rule (nil selects every identity, a list selects its
// non-empty members) is applied while scanning, so no query is built from
// caller strings.
func (l *RetentionLedger) ClaimsByOwner(ctx context.Context, q artifact.ClaimOwnerQuery, cursor artifact.ClaimCursor) (artifact.ClaimPage, error) {
	if q.Kind == "" || q.Authority == "" {
		return artifact.ClaimPage{}, &artifact.Error{Code: artifact.ErrInvalid, Operation: "claims_by_owner", Detail: "empty owner kind or authority"}
	}
	limit := q.Limit
	if limit <= 0 {
		limit = artifact.DefaultClaimPageSize
	}
	rows, err := l.db.QueryContext(ctx, `SELECT id, owner_identity, claim FROM claims WHERE owner_kind = ? AND owner_authority = ? AND id > ? ORDER BY id`,
		q.Kind, q.Authority, string(cursor.After))
	if err != nil {
		return artifact.ClaimPage{}, err
	}
	defer rows.Close()
	watermark := cursor.Watermark
	var items []artifact.RetentionClaim
	for rows.Next() {
		var id, identity, raw string
		if err := rows.Scan(&id, &identity, &raw); err != nil {
			return artifact.ClaimPage{}, err
		}
		if watermark != "" && artifact.ClaimID(id) > watermark {
			break
		}
		if !matchesIdentity(q, identity) {
			continue
		}
		if len(items) == limit {
			// A first page fixes its watermark at the last matching id, so
			// claims activated while paging stay out of the enumeration.
			if watermark == "" {
				watermark, err = l.lastMatching(ctx, q)
				if err != nil {
					return artifact.ClaimPage{}, err
				}
			}
			return artifact.ClaimPage{Items: items, Next: &artifact.ClaimCursor{Watermark: watermark, After: items[len(items)-1].ID}}, nil
		}
		var c artifact.RetentionClaim
		if err := json.Unmarshal([]byte(raw), &c); err != nil {
			return artifact.ClaimPage{}, &artifact.Error{Code: artifact.ErrCorrupt, Operation: "claims_by_owner", Identity: id, Detail: err.Error()}
		}
		items = append(items, c)
	}
	if err := rows.Err(); err != nil {
		return artifact.ClaimPage{}, err
	}
	return artifact.ClaimPage{Items: items}, nil
}

// lastMatching is the highest ClaimID the query matches right now.
func (l *RetentionLedger) lastMatching(ctx context.Context, q artifact.ClaimOwnerQuery) (artifact.ClaimID, error) {
	rows, err := l.db.QueryContext(ctx, `SELECT id, owner_identity FROM claims WHERE owner_kind = ? AND owner_authority = ? ORDER BY id DESC`, q.Kind, q.Authority)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	for rows.Next() {
		var id, identity string
		if err := rows.Scan(&id, &identity); err != nil {
			return "", err
		}
		if matchesIdentity(q, identity) {
			return artifact.ClaimID(id), rows.Err()
		}
	}
	return "", rows.Err()
}

// matchesIdentity is ART-RET-3: nil selects every identity, an explicit
// list selects its members, and an empty identity never matches.
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
