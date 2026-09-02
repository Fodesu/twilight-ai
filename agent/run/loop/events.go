package loop

import (
	"context"
	"encoding/json"
	"sync"

	run "github.com/memohai/twilight/agent/run"
)

type serializedEventSink struct {
	sink EventSink
	mu   *sync.Mutex
}

func (s *serializedEventSink) Emit(ctx context.Context, event Event) error {
	if s == nil || s.sink == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sink.Emit(ctx, event)
}

func (l *Loop) emitCommitted(ctx context.Context, events EventSink, runID run.RunID, committed []run.AgentEvent) {
	if events == nil {
		return
	}
	for i := range committed {
		e := committed[i]
		_ = events.Emit(ctx, Event{
			RunID:      runID,
			Kind:       EventAgentCommitted,
			Durability: EventCommitted,
			Canonical:  &e,
		})
	}
}

type progressSink struct {
	events EventSink
	run    run.RunID
	step   run.StepID
	call   run.CallID
	seq    uint64
	mu     sync.Mutex
}

func (p *progressSink) Publish(ctx context.Context, progress ToolProgress) {
	if p.events == nil {
		return
	}
	p.mu.Lock()
	p.seq++
	seq := p.seq
	p.mu.Unlock()
	_ = p.events.Emit(ctx, Event{
		RunID: p.run, StepID: p.step, CallID: p.call,
		Sequence: seq, Kind: EventToolProgress, Durability: EventProvisional,
		Payload: progress.Payload,
	})
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return []byte("null")
	}
	return b
}
