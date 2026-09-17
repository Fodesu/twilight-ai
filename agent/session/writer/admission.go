package writer

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/felinics/twilight/agent/artifact"
	"github.com/felinics/twilight/agent/es"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/extension"
)

// Admission supplies Binding admission and the claim ledger. Both may be nil
// while no committed group actually references an artifact: an event type
// declaring Bindings only means its payloads *may* carry references, so a
// deployment that never does needs neither.
type Admission struct {
	Bindings artifact.BindingResolver
	Ledger   artifact.RetentionLedger
}

// ClaimOwnerKind is the ClaimOwner.Kind of Session commits (EXT-WRT-5).
const ClaimOwnerKind = "twilight/session/commit"

// DeriveClaimID is EXT-WRT-5.
func DeriveClaimID(protocolVersion uint16, sid session.SessionID, commitID session.CommitID, refSet artifact.RefSetDigest) artifact.ClaimID {
	raw, _ := es.EncodeTypedPayload(session.ProtocolVersion1, "twilight/session-extension/claim", []string{"1", fmt.Sprintf("%d", protocolVersion), string(sid), string(commitID), string(refSet)})
	return artifact.ClaimID(es.DigestBytes(raw))
}

func nextClaimID(released artifact.ClaimID) artifact.ClaimID {
	raw, _ := es.EncodeTypedPayload(session.ProtocolVersion1, "twilight/session-extension/claim-successor", []string{"1", string(released)})
	return artifact.ClaimID(es.DigestBytes(raw))
}

// CommitOwner is the ClaimOwner of a Session commit.
func CommitOwner(sid session.SessionID, id session.CommitID) artifact.ClaimOwner {
	return artifact.ClaimOwner{Kind: ClaimOwnerKind, Authority: string(sid), Identity: string(id)}
}

// admitter is the artifact stage of the commit pipeline: it admits the
// Bindings a group references against their declarations (EXT-REF-2),
// activates the commit's retention claim before Append (EXT-WRT-3) and
// reconciles this Session's claims when the Writer opens (ART-RET-3). It
// knows nothing of projections or ownership.
type admitter struct {
	Admission
	protocol uint16
	sid      session.SessionID
}

// reconcile settles this Session's claims against the log at open; w answers
// which owner commits exist.
func (a *admitter) reconcile(ctx context.Context, w artifact.OwnerVerifier) error {
	if a.Ledger == nil {
		return nil
	}
	_, err := artifact.Reconcile(ctx, a.Ledger, artifact.ClaimOwnerScope{Kind: ClaimOwnerKind, Authority: string(a.sid)}, w)
	return err
}

func (a *admitter) admit(ctx context.Context, id artifact.BindingID, decl *extension.BindingReferenceDefinition) (string, error) {
	if a.Bindings == nil {
		// A configuration error, not a verdict on the commit: returning it as an
		// error keeps it from reading like a data rejection.
		return "", errors.New("writer: the event references artifacts but no binding resolver is configured")
	}
	binding, err := a.Bindings.ResolveBinding(ctx, id)
	if err != nil {
		var aerr *artifact.Error
		if errors.As(err, &aerr) {
			return fmt.Sprintf("binding %s: %v", id, aerr), nil
		}
		return "", err
	}
	if len(decl.AllowedSchemes) > 0 {
		allowed := false
		for _, s := range decl.AllowedSchemes {
			if s == binding.Ref.Scheme {
				allowed = true
			}
		}
		if !allowed {
			return fmt.Sprintf("binding %s: scheme %s not allowed", id, binding.Ref.Scheme), nil
		}
	}
	if binding.Ref.Durability.Rank() < decl.RequiredDurability.Rank() {
		return fmt.Sprintf("binding %s: durability %s below required %s", id, binding.Ref.Durability, decl.RequiredDurability), nil
	}
	return "", nil
}

// claim activates the retention claim before Append (EXT-WRT-3); a commit
// without references claims nothing.
func (a *admitter) claim(ctx context.Context, commitID session.CommitID, refs []artifact.BindingID) (*artifact.RetentionClaim, string, error) {
	if len(refs) == 0 {
		return nil, "", nil
	}
	if a.Ledger == nil {
		// See admit: a missing ledger is a configuration error.
		return nil, "", errors.New("writer: the commit references artifacts but no retention ledger is configured")
	}
	set, err := artifact.SetBuilder{Resolver: a.Bindings}.Build(ctx, refs)
	if err != nil {
		var aerr *artifact.Error
		if errors.As(err, &aerr) {
			return nil, "binding set: " + aerr.Error(), nil
		}
		return nil, "", err
	}
	id := DeriveClaimID(a.protocol, a.sid, commitID, set.RefSetDigest)
	owner := CommitOwner(a.sid, commitID)
	for {
		existing, ok, err := a.Ledger.LookupClaim(ctx, id)
		if err != nil {
			return nil, "", err
		}
		if !ok {
			break
		}
		if existing.ID != id || existing.Owner != owner || existing.BindingSet.RefSetDigest != set.RefSetDigest || !slices.Equal(existing.BindingSet.BindingIDs, set.BindingIDs) {
			return nil, fmt.Sprintf("claim %s: owner or binding set conflicts", id), nil
		}
		if existing.State != artifact.ClaimReleased {
			break
		}
		// Released claims remain terminal; a replay acquires a new retention root.
		id = nextClaimID(id)
	}
	claim, err := a.Ledger.Activate(ctx, id, owner, set)
	if err != nil {
		var aerr *artifact.Error
		if errors.As(err, &aerr) {
			return nil, "claim: " + aerr.Error(), nil
		}
		return nil, "", err
	}
	return &claim, "", nil
}

// release gives a claim back when the Append it was activated for is known
// not to have written; best effort, reconcile covers the rest.
func (a *admitter) release(ctx context.Context, claim *artifact.RetentionClaim) {
	if a.Ledger != nil && claim != nil {
		_ = a.Ledger.ReleaseActive(ctx, claim.ID)
	}
}
