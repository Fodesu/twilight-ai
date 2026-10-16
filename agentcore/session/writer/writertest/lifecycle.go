// Package writertest composes explicit acquisition, binding and cleanup for
// tests. It is not a production ownership implementation and never renews.
package writertest

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/writer"
)

// Open explicitly acquires a handle and binds a Writer. The test's cleanup and
// the returned Writer's Close both close local state before releasing the
// handle, once. Failed Bind releases only the handle acquired here.
func Open(t testing.TB, ctx context.Context, f *writer.Factory, sid session.SessionID, opts session.OpenOptions) (writer.Writer, error) {
	t.Helper()
	h, err := f.Store.Open(ctx, sid, opts)
	if err != nil {
		return nil, err
	}
	w, err := f.Bind(ctx, h)
	if err != nil {
		_ = h.Close(context.Background())
		return nil, err
	}
	owned := &ownedWriter{Writer: w, handle: h}
	t.Cleanup(func() { _ = owned.Close(context.Background()) })
	return owned, nil
}

type ownedWriter struct {
	writer.Writer
	handle session.Handle
	once   sync.Once
	err    error
}

func (w *ownedWriter) Close(ctx context.Context) error {
	w.once.Do(func() { w.err = errors.Join(w.Writer.Close(ctx), w.handle.Close(ctx)) })
	return w.err
}
