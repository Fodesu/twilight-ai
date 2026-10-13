package http

import (
	"context"
	"errors"
	stdhttp "net/http"
	"strconv"

	"github.com/felinics/twilight/agent/app"
	"github.com/felinics/twilight/agent/observability"
	"github.com/felinics/twilight/agentcore/ledger"
)

func (s *Server) chatCapabilities(w stdhttp.ResponseWriter, _ *stdhttp.Request) {
	writeJSON(w, stdhttp.StatusOK, s.App.ChatCapabilities())
}

func (s *Server) chatSnapshot(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	snapshot, err := s.App.ChatSnapshot(r.Context(), sid(r))
	if errors.Is(err, app.ErrChatSnapshotBusy) {
		writeError(w, &Error{Status: stdhttp.StatusServiceUnavailable, Code: CodeUnavailable, Message: err.Error()})
		return
	}
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, snapshot)
}

func (s *Server) chatEvents(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	from := r.URL.Query().Get("from")
	if from == "" {
		from = "0"
	}
	n, err := strconv.ParseUint(from, 10, 64)
	if err != nil {
		writeError(w, &Error{Status: stdhttp.StatusBadRequest, Code: CodeInvalid, Message: "from must be a commit sequence"})
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	cursor := ledger.CommitSeq(n)
	events, err := s.App.EventsFrom(ctx, sid(r), cursor)
	if err != nil {
		fail(w, err)
		return
	}
	stream := observability.Default.OpenEvents(ctx, sid(r), &cursor)
	writeErr := sendChatEvents(ctx, sse(w), events, stream.Sent)
	stream.Close(writeErr)
}

func sendChatEvents(ctx context.Context, send func(any) bool, events <-chan app.Event, sent func(ledger.Position, bool)) error {
	for event := range events {
		normalized, supported, err := app.NormalizeChatEvent(event)
		if err != nil {
			return err
		}
		if !supported {
			continue
		}
		if !send(normalized) {
			return errors.New("chat event stream write failed")
		}
		sent(event.Position, event.Row.Type != "")
	}
	return ctx.Err()
}
