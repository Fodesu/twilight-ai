package loop

import (
	"context"
	"sync"

	run "github.com/felinics/twilight/agentcore/run"
)

type serializedEventSink struct {
	sink EventSink
	mu   *sync.Mutex
}

func (s *serializedEventSink) Emit(ctx context.Context, event Event) error { //nolint:gocritic // hugeParam: EventSink contract takes the Event by value
	if s == nil || s.sink == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sink.Emit(ctx, event)
}

func (l *Loop) emitCommitted(ctx context.Context, events EventSink, scope run.Scope, runID run.RunID, committed []run.Fact) {
	if events == nil || len(committed) == 0 {
		return
	}
	_ = events.Emit(ctx, Event{
		Session:    scope,
		RunID:      runID,
		Kind:       EventAgentCommitted,
		Durability: EventCommitted,
		Committed:  append([]run.Fact(nil), committed...),
	})
}
