package writer

import (
	"context"
	"errors"
	"sync"

	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/session"
)

// Test-only lifecycle composition. Production callers explicitly acquire and
// release their Session handles; Factory itself never owns a lease.
func acquireTestWriter(ctx context.Context, store session.Store, registry *module.Registry, admission Admission, sid session.SessionID, opts session.OpenOptions) (Writer, error) {
	return acquireTestWriterConfig(ctx, store, registry, admission, sid, opts, WritersConfig{})
}
func acquireTestWriterConfig(ctx context.Context, store session.Store, registry *module.Registry, admission Admission, sid session.SessionID, opts session.OpenOptions, cfg WritersConfig) (Writer, error) {
	h, err := store.Open(ctx, sid, opts)
	if err != nil {
		return nil, err
	}
	w, err := (&Factory{Store: store, Registry: registry, Admission: admission, Config: cfg}).Bind(ctx, h)
	if err != nil {
		_ = h.Close(context.Background())
		return nil, err
	}
	return &testOwnedWriter{Writer: w, handle: h}, nil
}

type testOwnedWriter struct {
	Writer
	handle session.Handle
	once   sync.Once
	err    error
}

func (w *testOwnedWriter) Close(ctx context.Context) error {
	w.once.Do(func() { w.err = errors.Join(w.Writer.Close(ctx), w.handle.Close(ctx)) })
	return w.err
}
