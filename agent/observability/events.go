package observability

import (
	"context"
	"time"

	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/session"
)

// EventStream records cursor metadata, never the event's contents. From-mode
// includes both history and subsequent live events: it is not a replay-only
// metric. Use the durable ledger to assert gaps/order/client de-duplication.
// Its methods are called serially by one HTTP handler.
type EventStream struct {
	o       *Observer
	ctx     context.Context
	sid     session.SessionID
	from    *ledger.CommitSeq
	mode    string
	start   time.Time
	frames  uint64
	last    ledger.Position
	hasLast bool
}

func (o *Observer) OpenEvents(ctx context.Context, sid session.SessionID, from *ledger.CommitSeq) *EventStream {
	s := &EventStream{o: o, ctx: ctx, sid: sid, start: time.Now(), mode: "live"}
	if from != nil {
		n := *from
		s.from = &n
		s.mode = "from"
	}
	o.log().InfoContext(ctx, "Event stream opened", s.attrs("event_stream_opened")...)
	return s
}

func (s *EventStream) Sent(position ledger.Position, durable bool) {
	kind := "transient"
	if durable {
		kind = "durable"
		s.last = position
		s.hasLast = true
	}
	s.frames++
	s.o.eventFrames.WithLabelValues(s.mode, kind).Inc()
}

func (s *EventStream) Close(err error) {
	attrs := append(s.attrs("event_stream_closed"), "result", Result(err), "frames_written", s.frames, "duration_seconds", time.Since(s.start).Seconds())
	if s.hasLast {
		attrs = append(attrs, "last_commit", s.last.Commit, "last_index", s.last.Index)
	}
	s.o.log().InfoContext(s.ctx, "Event stream closed", attrs...)
}

func (s *EventStream) attrs(event string) []any {
	attrs := append(Correlation(s.ctx), "event", event, "session_id", s.sid, "mode", s.mode)
	if s.from != nil {
		attrs = append(attrs, "from_commit", *s.from)
	}
	return attrs
}
