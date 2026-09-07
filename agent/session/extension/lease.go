package extension

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/memohai/twilight/agent/es"
	"github.com/memohai/twilight/agent/jsonstable"
	"github.com/memohai/twilight/agent/session"
)

type LeaseToken string

// Lease is one occupancy record in the control-plane KV (EXT-LSE).
type Lease struct {
	Namespace         session.ControlNamespace
	Key               string
	Holder            string
	Token             LeaseToken
	DeadlineUnixMilli int64
	Attrs             jsonstable.Value
}

type AcquireLeaseRequest struct {
	Namespace session.ControlNamespace
	Key       string
	Holder    string
	TTL       time.Duration
	Attrs     jsonstable.Value
}

type leaseValue struct {
	Holder string           `json:"holder"`
	Token  LeaseToken       `json:"token"`
	Attrs  jsonstable.Value `json:"attrs,omitempty"`
}

// DeriveLeaseToken is the pure Token derivation of EXT-LSE-1.
func DeriveLeaseToken(sid session.SessionID, ns session.ControlNamespace, key, holder string, commitID session.CommitID) LeaseToken {
	raw, _ := es.EncodeTypedPayload(session.ProtocolVersion1, "twilight/session-extension/lease", []string{string(sid), string(ns), key, holder, string(commitID)})
	return LeaseToken(es.DigestBytes(raw))
}

// AcquireLease writes or idempotently confirms a lease inside the commit
// transaction (EXT-LSE-2). sid is the Session the tx belongs to.
func AcquireLease(tx session.SessionTx, sid session.SessionID, commitID session.CommitID, now int64, req AcquireLeaseRequest) (Lease, error) {
	if req.Namespace == "" || req.Key == "" || req.Holder == "" {
		return Lease{}, &Error{Code: ErrInvalid, Detail: "lease requires namespace, key and holder"}
	}
	existing, ok, err := LookupLease(tx, req.Namespace, req.Key)
	if err != nil {
		return Lease{}, err
	}
	if ok {
		if existing.Holder == req.Holder {
			return existing, nil
		}
		return Lease{}, &Error{Code: ErrConflict, Detail: "lease held by another holder"}
	}
	lease := Lease{Namespace: req.Namespace, Key: req.Key, Holder: req.Holder,
		Token: DeriveLeaseToken(sid, req.Namespace, req.Key, req.Holder, commitID), Attrs: req.Attrs}
	if req.TTL > 0 {
		lease.DeadlineUnixMilli = now + req.TTL.Milliseconds()
	}
	value, err := encodeLease(&lease)
	if err != nil {
		return Lease{}, err
	}
	if err := tx.ControlPut(req.Namespace, req.Key, value, lease.DeadlineUnixMilli); err != nil {
		return Lease{}, err
	}
	return lease, nil
}

// ReleaseLease deletes the lease when token matches; otherwise ErrStale.
func ReleaseLease(tx session.SessionTx, ns session.ControlNamespace, key string, token LeaseToken) error {
	existing, ok, err := LookupLease(tx, ns, key)
	if err != nil {
		return err
	}
	if !ok || existing.Token != token {
		return &Error{Code: ErrStale, Detail: "lease missing or token mismatch"}
	}
	return tx.ControlDelete(ns, key)
}

func LookupLease(tx session.SessionTx, ns session.ControlNamespace, key string) (Lease, bool, error) {
	entry, ok, err := tx.ControlGet(ns, key)
	if err != nil || !ok {
		return Lease{}, false, err
	}
	lease, err := decodeLease(entry)
	if err != nil {
		return Lease{}, false, err
	}
	return lease, true, nil
}

// Leases is the outside-the-critical-section facade (EXT-LSE-3).
type Leases struct{ Store session.Store }

func (l Leases) Lookup(ctx context.Context, sid session.SessionID, ns session.ControlNamespace, key string) (Lease, bool, error) {
	entry, ok, err := l.Store.ControlGet(ctx, sid, ns, key)
	if err != nil || !ok {
		return Lease{}, false, err
	}
	lease, err := decodeLease(entry)
	if err != nil {
		return Lease{}, false, err
	}
	return lease, true, nil
}

// Renew pushes the deadline by ttl with a conditional write; a missing entry,
// a token mismatch or a lost race with Release returns ErrStale. ttl zero only
// validates the token.
func (l Leases) Renew(ctx context.Context, sid session.SessionID, ns session.ControlNamespace, key string, token LeaseToken, ttl time.Duration, now int64) error {
	entry, ok, err := l.Store.ControlGet(ctx, sid, ns, key)
	if err != nil {
		return err
	}
	if !ok {
		return &Error{Code: ErrStale, Detail: "lease missing"}
	}
	lease, err := decodeLease(entry)
	if err != nil {
		return err
	}
	if lease.Token != token {
		return &Error{Code: ErrStale, Detail: "lease token mismatch"}
	}
	if ttl <= 0 {
		return nil
	}
	written, err := l.Store.ControlCompareAndPut(ctx, sid, ns, key, entry.Value, entry.Value, now+ttl.Milliseconds())
	if err != nil {
		return err
	}
	if !written {
		return &Error{Code: ErrStale, Detail: "lease released or rewritten during renew"}
	}
	return nil
}

// Expired enumerates leases whose deadline passed before now.
func (l Leases) Expired(ctx context.Context, ns session.ControlNamespace, now int64, fn func(session.SessionID, Lease) (bool, error)) error {
	return l.Store.ControlExpired(ctx, ns, now, func(entry session.ControlEntry) (bool, error) {
		lease, err := decodeLease(entry)
		if err != nil {
			return false, err
		}
		return fn(entry.SessionID, lease)
	})
}

func encodeLease(l *Lease) ([]byte, error) {
	return jsonstable.MarshalCanonical(leaseValue{Holder: l.Holder, Token: l.Token, Attrs: l.Attrs})
}

func decodeLease(entry session.ControlEntry) (Lease, error) {
	var v leaseValue
	if err := json.Unmarshal(entry.Value, &v); err != nil {
		return Lease{}, errors.New("extension: lease: corrupt value")
	}
	return Lease{Namespace: entry.Namespace, Key: entry.Key, Holder: v.Holder, Token: v.Token, DeadlineUnixMilli: entry.DeadlineUnixMilli, Attrs: v.Attrs}, nil
}
