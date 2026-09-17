package writer

import (
	"context"
	"errors"

	"github.com/felinics/twilight/agent/artifact"
	"github.com/felinics/twilight/agent/session"
)

// Delete drops a Session's root (SES-GC-1) and releases every retention
// claim the Session owns (EXT-WRT-9): its commit claims and, when it is a
// fork, its prefix claim. Content the Session's commits reference stays
// retained exactly as long as a live fork inherits those commits, through
// that fork's own prefix claim (EXT-WRT-8). The Session must not be open in
// this process; close its Writer first. Delete is idempotent across the two
// consistency domains: a root already deleted is not an error, so a call
// that failed after the root was dropped can be repeated to release the
// remaining claims.
func Delete(ctx context.Context, store session.Store, admission Admission, sid session.SessionID) error {
	if store == nil {
		return errors.New("writer: nil store")
	}
	if err := store.Delete(ctx, sid); err != nil && !session.IsCode(err, session.ErrNotFound) {
		return err
	}
	if admission.Ledger == nil {
		return nil
	}
	for _, kind := range []string{ClaimOwnerKind, ForkOwnerKind} {
		claims, err := artifact.ActiveClaims(ctx, admission.Ledger, artifact.ClaimOwnerScope{Kind: kind, Authority: string(sid)})
		if err != nil {
			return err
		}
		for _, c := range claims {
			if err := admission.Ledger.ReleaseActive(ctx, c.ID); err != nil {
				return err
			}
		}
	}
	return nil
}

// Collect reclaims the commit segments and suffixes no live Session reaches
// (SES-GC-2). Claims were released at Delete; Collect only frees storage.
func Collect(ctx context.Context, store session.Store) (session.CollectReport, error) {
	if store == nil {
		return session.CollectReport{}, errors.New("writer: nil store")
	}
	return store.Collect(ctx)
}
