package writer

import (
	"github.com/felinics/twilight/agent/jsonstable"
	"context"
	"errors"
	"fmt"

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
	header, err := store.Create(ctx, session.CreateRequest{ProtocolVersion: registry.ProtocolVersion, SessionID: req.Child,
		CreatedAtUnixMilli: req.CreatedAtUnixMilli, Fork: &session.ForkOrigin{Session: req.Parent, Seq: req.At}, Metadata: req.Metadata})
	if err != nil {
		return session.SegmentHeader{}, err
	}
	fork := *header.Parent
	if admission.Ledger == nil {
		return header, nil
	}
	refs, err := prefixBindings(ctx, store, registry, req.Child, fork)
	if err != nil {
		return session.SegmentHeader{}, err
	}
	if len(refs) == 0 {
		return header, nil
	}
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
		return header, nil
	}
	if _, err := admission.Ledger.Activate(ctx, id, owner, set); err != nil {
		return session.SegmentHeader{}, err
	}
	return header, nil
}

// prefixBindings collects, in commit order, every BindingID the events of a
// fork's inherited prefix reference, through the extractors their event
// definitions declare. Events the registry cannot decode carry no known
// references and are skipped.
func prefixBindings(ctx context.Context, store session.Store, registry *extension.Registry, child session.SessionID, fork session.LedgerRef) ([]artifact.BindingID, error) {
	page, err := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: child, Limit: uint32(fork.Seq) + 1})
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
				if !ok || len(def.Bindings) == 0 {
					continue
				}
				decoded, err := registry.Decode(e)
				if err != nil || decoded.Unknown {
					continue
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
