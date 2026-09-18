package writer

import (
	"context"
	"errors"
	"fmt"
	"github.com/felinics/twilight/agent/jsonstable"

	"github.com/felinics/twilight/agent/artifact"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/extension"
)

// ForkOwnerKind is the ClaimOwner.Kind of the retention claim a fork holds
// over the artifacts its inherited prefix references (EXT-WRT-8). It is not
// a commit owner: OpenWriter's reconciliation of commit claims never touches
// it, and it is released only when the fork itself is retired.
const ForkOwnerKind = "twilight/session/fork"

// ForkRequest creates a child Session from a parent's ledger prefix.
type ForkRequest struct {
	Parent session.SessionID
	// At is the last parent commit the child inherits.
	At                 session.CommitSeq
	Child              session.SessionID
	CreatedAtUnixMilli int64
	// Metadata is the child segment's creation metadata; it enters the
	// segment digest.
	Metadata jsonstable.Value
}

// ForkOwner is the ClaimOwner of a fork's prefix claim: the child Session
// and the edge it was created with.
func ForkOwner(child session.SessionID, edge session.LedgerRef) artifact.ClaimOwner {
	return artifact.ClaimOwner{Kind: ForkOwnerKind, Authority: string(child), Identity: fmt.Sprintf("%s@%d", edge.Segment, edge.Seq)}
}

// forkClaimCommitID names the fork claim in DeriveClaimID's CommitID slot.
func forkClaimCommitID(edge session.LedgerRef) session.CommitID {
	return session.CommitID(fmt.Sprintf("fork:%s@%d", edge.Segment, edge.Seq))
}

// Fork creates req.Child from req.Parent's history at commit req.At
// (SES-FRK-1) and, when a ledger is configured, activates one retention claim
// over every artifact the inherited prefix references (EXT-WRT-8): the
// parent's own commit claims keep that content today, and the fork claim
// keeps it should the parent be deleted first. Fork is idempotent: a repeat
// with the same arguments returns the same header and leaves the claim as it
// is.
func Fork(ctx context.Context, store session.Store, registry *extension.Registry, admission Admission, req ForkRequest) (session.SegmentHeader, error) {
	if store == nil || registry == nil {
		return session.SegmentHeader{}, errors.New("writer: nil store or registry")
	}
	if req.Parent == "" || req.Child == "" {
		return session.SegmentHeader{}, errors.New("writer: fork requires parent and child session ids")
	}
	create := session.CreateRequest{ProtocolVersion: registry.ProtocolVersion, SessionID: req.Child,
		CreatedAtUnixMilli: req.CreatedAtUnixMilli, Fork: &session.ForkOrigin{Session: req.Parent, Seq: req.At}, Metadata: req.Metadata}
	if admission.Ledger == nil {
		return store.Create(ctx, create)
	}
	// The Session store and the artifact ledger are two consistency domains,
	// so Fork is a saga ordered for safety (EXT-WRT-8): the retention claim
	// over the inherited prefix is activated first and the child root is
	// created second. A crash between the two leaves an unreferenced claim,
	// which retains content until reconciliation releases it; the reverse
	// order could leave a child whose inherited bodies are gone.
	parentHeader, err := store.Header(ctx, req.Parent)
	if err != nil {
		return session.SegmentHeader{}, err
	}
	fork := session.LedgerRef{Segment: session.SegmentIDOf(parentHeader), Seq: req.At}
	refs, err := prefixBindings(ctx, store, registry, req.Parent, fork)
	if err != nil {
		return session.SegmentHeader{}, err
	}
	var claim *artifact.ClaimID
	if len(refs) > 0 {
		if admission.Bindings == nil {
			return session.SegmentHeader{}, errors.New("writer: the inherited prefix references artifacts but no binding resolver is configured")
		}
		set, err := artifact.SetBuilder{Resolver: admission.Bindings}.Build(ctx, refs)
		if err != nil {
			return session.SegmentHeader{}, err
		}
		id := DeriveClaimID(registry.ProtocolVersion, req.Child, forkClaimCommitID(fork), set.RefSetDigest)
		owner := ForkOwner(req.Child, fork)
		existing, ok, err := admission.Ledger.LookupClaim(ctx, id)
		if err != nil {
			return session.SegmentHeader{}, err
		}
		if ok {
			if existing.Owner != owner || existing.BindingSet.RefSetDigest != set.RefSetDigest {
				return session.SegmentHeader{}, &session.Error{Code: session.ErrConflict, Operation: "fork", SessionID: req.Child, Detail: "fork claim owner or binding set conflicts"}
			}
		} else {
			if _, err := admission.Ledger.Activate(ctx, id, owner, set); err != nil {
				return session.SegmentHeader{}, err
			}
			claim = &id
		}
	}
	header, err := store.Create(ctx, create)
	if err != nil {
		if claim != nil {
			// This call activated the claim and could not create the child:
			// release it so a definite creation failure leaks nothing.
			_ = admission.Ledger.ReleaseActive(context.WithoutCancel(ctx), *claim)
		}
		return session.SegmentHeader{}, err
	}
	if header.Parent == nil || header.Parent.Segment != fork.Segment || header.Parent.Seq != fork.Seq {
		return session.SegmentHeader{}, &session.Error{Code: session.ErrConflict, Operation: "fork", SessionID: req.Child, Detail: "child edge does not match the claimed prefix"}
	}
	return header, nil
}

// prefixBindings collects, in commit order, every BindingID the events of
// the parent's prefix [0, fork.Seq] reference, through the extractors their
// event definitions declare. It fails closed: an event type this registry
// does not know, or a payload version it cannot decode of a type that
// declares bindings, may reference content the child must keep alive, and
// nothing here can prove it does not, so the fork is refused rather than
// risk a child whose history names collected artifacts (EXT-WRT-8).
func prefixBindings(ctx context.Context, store session.Store, registry *extension.Registry, parent session.SessionID, fork session.LedgerRef) ([]artifact.BindingID, error) {
	page, err := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: parent, Limit: uint32(fork.Seq) + 1})
	if err != nil {
		return nil, err
	}
	var refs []artifact.BindingID
	seen := map[artifact.BindingID]bool{}
	for _, c := range page.Commits {
		if c.Seq > fork.Seq {
			break
		}
		for _, b := range c.Batches {
			for _, e := range b.Events {
				_, def, ok := registry.LookupEvent(e.Type)
				if !ok {
					return nil, &session.Error{Code: session.ErrUnsupported, Operation: "fork", SessionID: parent,
						Detail: fmt.Sprintf("commit %s: event type %s is unknown to this registry; cannot prove the prefix references no artifact", c.CommitID, e.Type)}
				}
				if len(def.Bindings) == 0 {
					continue
				}
				decoded, err := registry.Decode(e)
				if err != nil {
					return nil, fmt.Errorf("writer: fork: commit %s: decode %s: %w", c.CommitID, e.Type, err)
				}
				if decoded.Unknown {
					return nil, &session.Error{Code: session.ErrUnsupported, Operation: "fork", SessionID: parent,
						Detail: fmt.Sprintf("commit %s: %s v%d is not decodable by this registry; cannot extract its artifact references", c.CommitID, e.Type, decoded.Version)}
				}
				for _, decl := range def.Bindings {
					ids, err := decl.Extractor.BindingIDs(decoded.Value)
					if err != nil {
						return nil, fmt.Errorf("writer: fork: commit %s: binding extraction: %w", c.CommitID, err)
					}
					for _, id := range ids {
						if !seen[id] {
							seen[id] = true
							refs = append(refs, id)
						}
					}
				}
			}
		}
	}
	return refs, nil
}
