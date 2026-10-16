// Package ownership manages acquired Session capabilities, independently of
// recovery, execution and activation.
package ownership

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/writer"
)

// Manager acquires independent handles. It neither caches nor serializes
// acquisitions: concurrent calls follow Store.Open's CP/Takeover semantics.
// Closing an older handle cannot release a newer epoch. Configure before use.
type Manager struct {
	Writers *writer.Factory
	Options session.OpenOptions
}

func (m *Manager) Acquire(ctx context.Context, sid session.SessionID) (*Handle, error) {
	if m == nil || m.Writers == nil || m.Writers.Store == nil {
		return nil, errors.New("ownership: nil writer factory or store")
	}
	lease, err := m.Writers.Store.Open(ctx, sid, m.Options)
	if err != nil {
		return nil, err
	}
	h := &Handle{lease: lease, done: make(chan struct{})}
	// Renewal starts before Bind, which may perform slow reads/reconciliation.
	h.start(m.Options.LeaseDuration)
	w, err := m.Writers.Bind(ctx, lease)
	if err != nil {
		_ = h.Close(context.Background())
		return nil, err
	}
	h.writer = w
	h.mu.Lock()
	err = h.lost
	h.mu.Unlock()
	if err != nil {
		_ = h.Close(context.Background())
		return nil, err
	}
	return h, nil
}

// Handle owns one acquired epoch and its local Writer. Close waits for renewal
// to exit, closes the Writer, then explicitly releases that epoch, exactly once.
// Writer use must finish before Close; Renew is serialized with Close.
type Handle struct {
	lease    session.Handle
	writer   writer.Writer
	mu       sync.Mutex
	closed   bool
	lost     error
	cancel   context.CancelFunc
	done     chan struct{}
	once     sync.Once
	closeErr error
}

func (h *Handle) Writer() writer.Writer { return h.writer }
func (h *Handle) Lease() session.Lease  { return h.lease.Lease() }

func (h *Handle) Renew(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return errors.New("ownership: handle closed")
	}
	if h.lost != nil {
		return h.lost
	}
	err := h.lease.Renew(ctx)
	if session.IsCode(err, session.ErrOwnershipLost) {
		h.lost = err
	}
	return err
}

func (h *Handle) start(duration time.Duration) {
	ctx, cancel := context.WithCancel(context.Background()) //nolint:gosec // G118: stored on the handle; Close cancels and joins renewal.
	h.cancel = cancel
	if duration <= 0 {
		close(h.done)
		return
	}
	go func() {
		defer close(h.done)
		ticker := time.NewTicker(max(duration/3, time.Millisecond))
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := h.Renew(ctx); session.IsCode(err, session.ErrOwnershipLost) {
					return
				}
			}
		}
	}()
}

func (h *Handle) Close(ctx context.Context) error {
	h.once.Do(func() {
		// Cancel in-flight renewal before joining it; a fenced renewal never
		// cancels an external effect or impersonates the new owner.
		h.cancel()
		<-h.done
		h.mu.Lock()
		h.closed = true
		var writerErr error
		if h.writer != nil {
			writerErr = h.writer.Close(ctx)
		}
		h.closeErr = errors.Join(writerErr, h.lease.Close(ctx))
		h.mu.Unlock()
	})
	return h.closeErr
}
