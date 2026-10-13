package observability

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/felinics/twilight/agentcore/ledger"
)

func TestEventsRecordCursorWithoutIDLabels(t *testing.T) {
	var logs bytes.Buffer
	o := New(slog.New(slog.NewJSONHandler(&logs, nil)))
	from := ledger.CommitSeq(0) // zero is replay, not live-only
	s := o.OpenEvents(context.Background(), "unique-session", &from)
	from = 99 // stream keeps the original cursor
	s.Sent(ledger.Position{Commit: 7, Index: 2}, true)
	s.Sent(ledger.Position{}, false)
	s.Close(context.Canceled)
	metrics := scrape(t, o)
	for _, kind := range []string{"durable", "transient"} {
		if !strings.Contains(metrics, `twilight_event_frames_total{kind="`+kind+`",mode="from"} 1`) {
			t.Fatalf("missing %s count", kind)
		}
	}
	if strings.Contains(metrics, "unique-session") {
		t.Fatal("session label")
	}
	for _, attr := range []string{`"from_commit":0`, `"last_commit":7`, `"last_index":2`, `"frames_written":2`, `"mode":"from"`} {
		if !strings.Contains(logs.String(), attr) {
			t.Errorf("missing %s in %s", attr, logs.String())
		}
	}
}
