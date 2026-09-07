package extension

import (
	"context"
	"errors"
	"fmt"

	"github.com/memohai/twilight/agent/artifact"
	"github.com/memohai/twilight/agent/es"
	"github.com/memohai/twilight/agent/session"
)

// ClaimNamespace is the control-plane namespace artifact claims live in.
const ClaimNamespace session.ControlNamespace = "twilight/artifact/claim"

// TypedEvent is a module value plus event metadata; it carries no EventID
// (EXT-APP-5).
type TypedEvent struct {
	Type                session.EventType
	RecordedAtUnixMilli int64
	SourceEvents        []session.EventID
	Value               any
}

type SemanticGroup struct {
	CommitID      session.CommitID
	CausationID   es.CausationID
	CorrelationID string
	Events        []TypedEvent
}

type SemanticAppendRequest struct {
	SessionID    session.SessionID
	ExpectedHead session.Head
	Group        SemanticGroup
}

// SemanticTx is the kernel transaction plus typed decode.
type SemanticTx interface {
	session.SessionTx
	Decode(session.SessionEvent) (DecodedEvent, error)
	SessionID() session.SessionID
	Registry() *Registry
}

type SemanticCommitFn func(SemanticTx) (*SemanticGroup, error)

type SemanticAppendOutcome string

const (
	SemanticApplied        SemanticAppendOutcome = "applied"
	SemanticAlreadyApplied SemanticAppendOutcome = "already_applied"
	SemanticHeadConflict   SemanticAppendOutcome = "head_conflict"
	SemanticCommitConflict SemanticAppendOutcome = "commit_conflict"
	SemanticInvalid        SemanticAppendOutcome = "invalid"
	SemanticNoop           SemanticAppendOutcome = "noop"
)

type SemanticAppendResult struct {
	Outcome SemanticAppendOutcome
	Commit  *session.SessionCommit
	Claim   *artifact.RetentionClaim
	Detail  string
}

// SemanticAppender is the only write path (EXT-SCP-1).
type SemanticAppender interface {
	AppendSemantic(context.Context, SemanticAppendRequest) (SemanticAppendResult, error)
	AppendSemanticIn(context.Context, session.SessionID, SemanticCommitFn) (SemanticAppendResult, error)
}

// DeriveEventID is the Appender's EventID rule (EXT-APP-5).
func DeriveEventID(typ session.EventType, commitID session.CommitID, index int) session.EventID {
	raw, _ := es.EncodeTypedPayload(session.ProtocolVersion1, "twilight/session-extension/event-id", []string{string(typ), string(commitID), fmt.Sprintf("%d", index)})
	return session.EventID(es.DigestBytes(raw))
}

// DeriveClaimID is EXT-APP-2.
func DeriveClaimID(protocolVersion uint16, sid session.SessionID, commitID session.CommitID, refSet artifact.RefSetDigest) artifact.ClaimID {
	raw, _ := es.EncodeTypedPayload(session.ProtocolVersion1, "twilight/session-extension/claim", []string{"1", fmt.Sprintf("%d", protocolVersion), string(sid), string(commitID), string(refSet)})
	return artifact.ClaimID(es.DigestBytes(raw))
}

type appender struct {
	store    session.Store
	registry *Registry
	builder  artifact.BindingSetBuilder
	ledger   artifact.RetentionLedger
}

// NewSemanticAppender assembles the write path. builder and ledger may be nil
// only when no registered event declares Bindings.
func NewSemanticAppender(store session.Store, registry *Registry, builder artifact.BindingSetBuilder, ledger artifact.RetentionLedger) (SemanticAppender, error) {
	if store == nil || registry == nil {
		return nil, errors.New("extension: appender: nil store or registry")
	}
	return &appender{store: store, registry: registry, builder: builder, ledger: ledger}, nil
}

type semanticTx struct {
	session.SessionTx
	sid      session.SessionID
	registry *Registry
}

func (t *semanticTx) Decode(e session.SessionEvent) (DecodedEvent, error) {
	return t.registry.Decode(e)
}
func (t *semanticTx) SessionID() session.SessionID { return t.sid }
func (t *semanticTx) Registry() *Registry          { return t.registry }

type txClaimKV struct {
	tx session.SessionTx
}

func (k txClaimKV) Get(key string) ([]byte, bool, error) {
	e, ok, err := k.tx.ControlGet(ClaimNamespace, key)
	if err != nil || !ok {
		return nil, false, err
	}
	return e.Value, true, nil
}
func (k txClaimKV) Put(key string, value []byte) error {
	return k.tx.ControlPut(ClaimNamespace, key, value, 0)
}
func (k txClaimKV) Delete(key string) error { return k.tx.ControlDelete(ClaimNamespace, key) }

// StoreClaimKV is the outside-the-transaction ClaimKV over a Store, for the
// ledger's LookupClaim and ReleaseActive.
func StoreClaimKV(store session.Store, sid session.SessionID) func(context.Context) artifact.ClaimKV {
	return func(ctx context.Context) artifact.ClaimKV { return storeClaimKV{store, sid, ctx} }
}

type storeClaimKV struct {
	store session.Store
	sid   session.SessionID
	ctx   context.Context
}

func (k storeClaimKV) Get(key string) ([]byte, bool, error) {
	e, ok, err := k.store.ControlGet(k.ctx, k.sid, ClaimNamespace, key)
	if err != nil || !ok {
		return nil, false, err
	}
	return e.Value, true, nil
}
func (k storeClaimKV) Put(key string, value []byte) error {
	return k.store.ControlPut(k.ctx, k.sid, ClaimNamespace, key, value, 0)
}
func (k storeClaimKV) Delete(key string) error {
	return k.store.ControlDelete(k.ctx, k.sid, ClaimNamespace, key)
}

func (a *appender) AppendSemantic(ctx context.Context, req SemanticAppendRequest) (SemanticAppendResult, error) {
	return a.AppendSemanticIn(ctx, req.SessionID, func(tx SemanticTx) (*SemanticGroup, error) {
		group := req.Group
		// A retry of an already committed group is judged by fingerprint, not
		// by the head it was first attempted against (SES-APP-1).
		if _, found, err := tx.LookupCommit(group.CommitID); err != nil {
			return nil, err
		} else if found {
			return &group, nil
		}
		if tx.Head() != req.ExpectedHead {
			return nil, errHeadConflict
		}
		return &group, nil
	})
}

var errHeadConflict = errors.New("extension: head conflict")

// errDiscard aborts the kernel transaction so a rejected group leaves no
// snapshot or KV write behind; the semantic outcome is carried separately.
var errDiscard = errors.New("extension: discard transaction")

// AppendSemanticIn is EXT-APP-3: fn decides the group inside the kernel's
// critical section; codec, admission, claim and append happen in the same
// transaction.
func (a *appender) AppendSemanticIn(ctx context.Context, sid session.SessionID, fn SemanticCommitFn) (SemanticAppendResult, error) {
	if fn == nil {
		return SemanticAppendResult{}, errors.New("extension: appender: nil fn")
	}
	var result SemanticAppendResult
	header, err := a.store.Header(ctx, sid)
	if err != nil {
		return SemanticAppendResult{}, err
	}
	appendRes, err := a.store.CommitIn(ctx, sid, func(tx session.SessionTx) (*session.AppendRequest, error) {
		stx := &semanticTx{SessionTx: tx, sid: sid, registry: a.registry}
		group, err := fn(stx)
		if err != nil {
			if errors.Is(err, errHeadConflict) {
				result = SemanticAppendResult{Outcome: SemanticHeadConflict}
				return nil, errDiscard
			}
			return nil, err
		}
		if group == nil {
			result = SemanticAppendResult{Outcome: SemanticNoop}
			return nil, nil
		}
		if group.CommitID == "" || len(group.Events) == 0 {
			result = SemanticAppendResult{Outcome: SemanticInvalid, Detail: "empty CommitID or event group"}
			return nil, errDiscard
		}
		req, claim, invalid, err := a.prepare(ctx, tx, &header, sid, group)
		if err != nil {
			return nil, err
		}
		if invalid != "" {
			result = SemanticAppendResult{Outcome: SemanticInvalid, Detail: invalid}
			return nil, errDiscard
		}
		// Exact replay short-circuits before admission writes anything new;
		// the claim of a replayed commit is re-activated idempotently.
		if existing, ok, err := tx.LookupCommit(group.CommitID); err != nil {
			return nil, err
		} else if ok {
			have, err := a.registry.Profile.FingerprintAppend(*req)
			if err != nil {
				return nil, err
			}
			want, err := fingerprintOf(a.registry.Profile, &existing)
			if err != nil {
				return nil, err
			}
			if have != want {
				result = SemanticAppendResult{Outcome: SemanticCommitConflict}
				return nil, errDiscard
			}
			if claim != nil {
				if _, err := a.ledger.ActivateIn(txClaimKV{tx}, claim.ID, claim.Owner, claim.BindingSet); err != nil {
					return nil, err
				}
			}
			c := existing
			result = SemanticAppendResult{Outcome: SemanticAlreadyApplied, Commit: &c, Claim: claim}
			return nil, nil
		}
		if claim != nil {
			if _, err := a.ledger.ActivateIn(txClaimKV{tx}, claim.ID, claim.Owner, claim.BindingSet); err != nil {
				return nil, err
			}
		}
		result = SemanticAppendResult{Claim: claim}
		req.ExpectedHead = tx.Head()
		return req, nil
	})
	if err != nil {
		if errors.Is(err, errDiscard) {
			return result, nil
		}
		return SemanticAppendResult{}, err
	}
	if result.Outcome != "" {
		return result, nil
	}
	switch appendRes.Disposition {
	case session.AppendApplied:
		result.Outcome = SemanticApplied
		result.Commit = appendRes.Commit
	case session.AppendAlreadyApplied:
		result.Outcome = SemanticAlreadyApplied
		result.Commit = appendRes.Commit
	case session.AppendCommitConflict:
		result.Outcome = SemanticCommitConflict
	case session.AppendHeadConflict:
		result.Outcome = SemanticHeadConflict
	default:
		result.Outcome = SemanticInvalid
		result.Detail = appendRes.Detail
	}
	return result, nil
}

// prepare validates, encodes and extracts bindings for the group. It returns
// the kernel AppendRequest (ExpectedHead unset) and the claim to activate.
func (a *appender) prepare(ctx context.Context, tx session.SessionTx, header *session.SessionHeader, sid session.SessionID, group *SemanticGroup) (*session.AppendRequest, *artifact.RetentionClaim, string, error) {
	req := &session.AppendRequest{SessionID: sid, CommitID: group.CommitID, CausationID: group.CausationID, CorrelationID: group.CorrelationID,
		Events: make([]session.UncommittedEvent, len(group.Events))}
	var refs []artifact.BindingID
	for i, te := range group.Events {
		_, def, ok := a.registry.LookupEvent(te.Type)
		if !ok {
			return nil, nil, fmt.Sprintf("event %d: unknown type %s", i, te.Type), nil
		}
		payload, _, err := a.registry.Encode(te.Type, te.Value)
		if err != nil {
			return nil, nil, fmt.Sprintf("event %d: %v", i, err), nil
		}
		for _, decl := range def.Bindings {
			ids, err := decl.extract(te.Value, payload)
			if err != nil {
				return nil, nil, fmt.Sprintf("event %d: binding extraction: %v", i, err), nil
			}
			if uint32(len(ids)) < decl.Cardinality.Min || (decl.Cardinality.Max != nil && uint32(len(ids)) > *decl.Cardinality.Max) {
				return nil, nil, fmt.Sprintf("event %d: binding cardinality violated", i), nil
			}
			for _, id := range ids {
				if invalid, err := a.admit(ctx, id, &decl); err != nil {
					return nil, nil, "", err
				} else if invalid != "" {
					return nil, nil, fmt.Sprintf("event %d: %s", i, invalid), nil
				}
			}
			refs = append(refs, ids...)
		}
		req.Events[i] = session.UncommittedEvent{
			EventID:             DeriveEventID(te.Type, group.CommitID, i),
			Type:                te.Type,
			RecordedAtUnixMilli: te.RecordedAtUnixMilli,
			SourceEvents:        session.SortedUniqueEventIDs(te.SourceEvents),
			Payload:             payload,
		}
	}
	if len(refs) == 0 {
		return req, nil, "", nil
	}
	if a.builder == nil || a.ledger == nil {
		return nil, nil, "group references artifacts but no ledger is configured", nil
	}
	set, err := a.builder.Build(ctx, refs)
	if err != nil {
		var aerr *artifact.Error
		if errors.As(err, &aerr) {
			return nil, nil, "binding set: " + aerr.Error(), nil
		}
		return nil, nil, "", err
	}
	claim := &artifact.RetentionClaim{
		ID:         DeriveClaimID(header.ProtocolVersion, sid, group.CommitID, set.RefSetDigest),
		Owner:      artifact.ClaimOwner{Kind: "twilight/session/commit", Authority: string(sid), Identity: string(group.CommitID)},
		BindingSet: set,
		State:      artifact.ClaimActive,
	}
	return req, claim, "", nil
}

func (a *appender) admit(ctx context.Context, id artifact.BindingID, decl *BindingReferenceDefinition) (string, error) {
	resolver, ok := a.builder.(interface {
		ResolveBinding(context.Context, artifact.BindingID) (artifact.Binding, error)
	})
	if !ok {
		if sb, isSet := a.builder.(artifact.SetBuilder); isSet {
			resolver = sb.Resolver
		}
	}
	if resolver == nil {
		return "no binding resolver configured", nil
	}
	binding, err := resolver.ResolveBinding(ctx, id)
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

func fingerprintOf(p session.ProtocolProfile, c *session.SessionCommit) (es.Digest, error) {
	events := make([]session.UncommittedEvent, len(c.Events))
	for i, e := range c.Events {
		events[i] = session.UncommittedEvent{EventID: e.EventID, Type: e.Type, SourceEvents: e.SourceEvents, Payload: e.Payload}
	}
	return p.FingerprintAppend(session.AppendRequest{SessionID: c.SessionID, CommitID: c.CommitID, CausationID: c.CausationID, CorrelationID: c.CorrelationID, Events: events})
}
