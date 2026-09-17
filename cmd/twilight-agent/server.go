package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/felinics/twilight/agent/app"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/turn"
)

func agentHandler(s *app.Session, store session.Store, sid session.SessionID) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	})
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		status, err := s.Status(r.Context())
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"sessionId": sid, "active": status.Active, "failed": status.Failed, "recovered": s.Recovered})
	})
	mux.HandleFunc("POST /messages", func(w http.ResponseWriter, r *http.Request) {
		text, ok := readMessage(w, r)
		if !ok {
			return
		}
		ref, err := s.Submit(r.Context(), text)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]any{"sessionId": sid, "turnId": ref.TurnID})
	})
	mux.HandleFunc("POST /send", func(w http.ResponseWriter, r *http.Request) {
		text, ok := readMessage(w, r)
		if !ok {
			return
		}
		results, err := s.Send(r.Context(), text)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"results": results})
	})
	mux.HandleFunc("POST /resume", func(w http.ResponseWriter, r *http.Request) {
		results, resumed, err := s.Resume(r.Context())
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"resumed": resumed, "results": results})
	})
	mux.HandleFunc("POST /retry", func(w http.ResponseWriter, r *http.Request) {
		results, retried, err := s.Retry(r.Context())
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"retried": retried, "results": results})
	})
	mux.HandleFunc("GET /events", func(w http.ResponseWriter, r *http.Request) {
		var from uint64
		if value := r.URL.Query().Get("from"); value != "" {
			var err error
			from, err = strconv.ParseUint(value, 10, 64)
			if err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "from must be a non-negative sequence number"})
				return
			}
		}
		page, err := store.ReadCommits(r.Context(), session.CommitReadRequest{SessionID: sid, From: session.CommitSeq(from), Limit: 200})
		if err != nil {
			writeError(w, err)
			return
		}
		var events []session.Event
		for _, c := range page.Commits {
			for _, b := range c.Batches {
				events = append(events, b.Events...)
			}
		}
		next := session.CommitSeq(from)
		if len(page.Commits) > 0 {
			next = page.Commits[len(page.Commits)-1].Seq + 1
		}
		writeJSON(w, http.StatusOK, map[string]any{"events": events, "next": next, "hasMore": page.HasMore, "head": page.Head})
	})
	return mux
}

func readMessage(w http.ResponseWriter, r *http.Request) (string, bool) {
	var body struct {
		Text string `json:"text"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	defer r.Body.Close()
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil || strings.TrimSpace(body.Text) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "expected a JSON object with non-empty text (max 64 KiB)"})
		return "", false
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "expected one JSON object"})
		return "", false
	}
	return body.Text, true
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

// writeError maps the error classes the handlers surface onto HTTP statuses:
// a turn conflict and every kernel conflict (duplicate CommitID, owned or
// superseded Session) are 409, an unknown Session is 404, a malformed request
// is 400, and anything else is 500.
func writeError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, turn.ErrConflict), session.IsCode(err, session.ErrConflict),
		session.IsCode(err, session.ErrOwned), session.IsCode(err, session.ErrOwnershipLost):
		status = http.StatusConflict
	case session.IsCode(err, session.ErrNotFound):
		status = http.StatusNotFound
	case session.IsCode(err, session.ErrInvalid):
		status = http.StatusBadRequest
	}
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func serveAgent(listener net.Listener, s *app.Session, store session.Store, sid session.SessionID) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	resumeDone := make(chan struct{})
	go func() {
		defer close(resumeDone)
		results, _, err := s.Resume(ctx)
		report(results, err)
	}()
	defer func() {
		stop()
		<-resumeDone
		_ = s.Close(context.Background())
	}()
	server := &http.Server{Handler: agentHandler(s, store, sid), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: time.Minute,
		BaseContext: func(net.Listener) context.Context { return ctx }}
	defer server.Close()
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	fmt.Printf("agent http://%s (pid %d, session %s)\n", listener.Addr(), os.Getpid(), sid)
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			_ = server.Close()
		}
		<-done
		return nil
	}
}
