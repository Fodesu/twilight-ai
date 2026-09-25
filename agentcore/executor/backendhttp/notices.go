package backendhttp

import (
	"context"
	"errors"
	"sync"
)

// defaultNoticeWindow is the notices a Server keeps for resubscription.
const defaultNoticeWindow = 4096

// errNoticesEvicted reports a subscription that asked for a sequence the
// ring no longer holds: the subscriber re-reads every ref it waits on.
var errNoticesEvicted = errors.New("backendhttp: notices before the requested sequence were evicted")

// noticeHub is the Server's in-memory notice log: one Notice per ref whose
// Outcome the backend returned, in a bounded ring, with every subscriber
// woken on each record. The same shape as executor.SettlementHub, keyed by
// ref because the wire names executions by ref.
type noticeHub struct {
	epoch  string
	window int

	mu      sync.Mutex
	next    uint64
	log     []Notice
	changed chan struct{}
}

func newNoticeHub(epoch string, window int) *noticeHub {
	if window <= 0 {
		window = defaultNoticeWindow
	}
	return &noticeHub{epoch: epoch, window: window, changed: make(chan struct{})}
}

func (h *noticeHub) record(ref string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.next++
	h.log = append(h.log, Notice{Ref: ref, Epoch: h.epoch, Sequence: h.next})
	if len(h.log) > h.window {
		h.log = h.log[len(h.log)-h.window:]
	}
	close(h.changed)
	h.changed = make(chan struct{})
}

// subscribe delivers every Notice after `after` in epoch, then new ones as
// they land, until fn returns false or ctx ends. Another epoch subscribes
// from the head; an evicted position is errNoticesEvicted.
func (h *noticeHub) subscribe(ctx context.Context, epoch string, after uint64, fn func(Notice) bool) error {
	h.mu.Lock()
	switch {
	case epoch != h.epoch:
		after = h.next
	case len(h.log) > 0 && after < h.log[0].Sequence-1, len(h.log) == 0 && after < h.next:
		h.mu.Unlock()
		return errNoticesEvicted
	}
	h.mu.Unlock()
	for {
		h.mu.Lock()
		var pending []Notice
		for i := range h.log {
			if h.log[i].Sequence > after {
				pending = append(pending, h.log[i])
			}
		}
		wait := h.changed
		h.mu.Unlock()
		for _, n := range pending {
			after = n.Sequence
			if !fn(n) {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wait:
		}
	}
}
