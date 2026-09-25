package executor

import (
	"context"
	"sync"

	"github.com/felinics/twilight/agentcore/run/effect"
)

// DefaultSettlementWindow is the Settlements a SettlementHub keeps when
// built with a zero window: enough for a subscriber to reconnect after a
// brief drop without re-reading everything it waits on.
const DefaultSettlementWindow = 4096

// SettlementHub is the Worker's in-memory settlement log, the
// effect.SettlementPort it serves. Every settlement the Worker writes to
// the execution ledger is recorded here afterwards, stamped with the hub's
// Epoch (one per incarnation) and a running Sequence, and every subscriber
// is woken. The hub is not a fact store: the ledger is. It is the notice
// that a fact landed, kept in a bounded ring so a subscriber can catch up
// after a short gap and told (ErrSettlementsEvicted) when it cannot.
type SettlementHub struct {
	epoch  string
	window int

	mu      sync.Mutex
	next    uint64
	log     []effect.Settlement
	changed chan struct{} // closed and replaced on every record; waiters select on it
	closed  bool
}

// NewSettlementHub returns a hub for one incarnation named epoch, keeping
// window Settlements (zero takes DefaultSettlementWindow).
func NewSettlementHub(epoch string, window int) *SettlementHub {
	if window <= 0 {
		window = DefaultSettlementWindow
	}
	return &SettlementHub{epoch: epoch, window: window, changed: make(chan struct{})}
}

// Epoch names this incarnation of the hub.
func (h *SettlementHub) Epoch() string { return h.epoch }

// Record notes that key settled and wakes every subscriber.
func (h *SettlementHub) Record(key effect.AssignmentKey) {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	h.next++
	h.log = append(h.log, effect.Settlement{Key: key, Epoch: h.epoch, Sequence: h.next})
	if len(h.log) > h.window {
		h.log = h.log[len(h.log)-h.window:]
	}
	close(h.changed)
	h.changed = make(chan struct{})
}

// Close ends every subscription: the hub's incarnation is over, and a
// subscriber that returns from Settlements with nil re-reads and
// subscribes again against whatever serves the port next.
func (h *SettlementHub) Close() {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	h.closed = true
	close(h.changed)
	h.changed = make(chan struct{})
}

// Settlements is effect.SettlementPort. An epoch other than this hub's, or
// an empty one, subscribes from the head; a Sequence older than the ring
// holds is ErrSettlementsEvicted.
func (h *SettlementHub) Settlements(ctx context.Context, epoch string, after uint64, fn func(effect.Settlement) bool) error {
	h.mu.Lock()
	if epoch != h.epoch {
		after = h.next
	} else if len(h.log) > 0 && after < h.log[0].Sequence-1 {
		h.mu.Unlock()
		return effect.ErrSettlementsEvicted
	} else if len(h.log) == 0 && after < h.next {
		h.mu.Unlock()
		return effect.ErrSettlementsEvicted
	}
	h.mu.Unlock()
	for {
		h.mu.Lock()
		var pending []effect.Settlement
		for i := range h.log {
			if h.log[i].Sequence > after {
				pending = append(pending, h.log[i])
			}
		}
		wait, closed := h.changed, h.closed
		h.mu.Unlock()
		for _, s := range pending {
			after = s.Sequence
			if !fn(s) {
				return nil
			}
		}
		if closed {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wait:
		}
	}
}

var _ effect.SettlementPort = (*SettlementHub)(nil)
