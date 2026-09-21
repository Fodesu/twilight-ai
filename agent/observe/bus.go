// Package observe is the Session event stream (OBS-1): a
// writer.CommitObserver that decodes every applied commit through the
// Registry and fans it out, in commit order, to the subscribers of each
// Session. UI, SSE and CLI observation all derive from this one source;
// history is read from the Store or a projection, never from here.
package observe

import (
	"context"
	"sync"

	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/extension"
)

// Event is one item of a Session's event stream. Every observation of a
// Session derives from applied commits, so an Event is normally one
// committed event decoded through the Registry: Module, Version and Value
// are the decoded payload, Unknown reports a type or version this process
// has no codec for (the event is still delivered). An Event with Err set and
// a zero Row is a failure of background work (a drive that errored); it is
// reported here for the same audience but never enters the stream.
type Event struct {
	Session session.SessionID
	Row     session.Event
	Module  extension.ModuleKey
	Version extension.PayloadVersion
	Value   any
	Unknown bool
	Err     error
}

// Bus decodes applied commits and fans them out per Session.
type Bus struct {
	registry *extension.Registry
	mu       sync.Mutex
	subs     map[session.SessionID]map[*subscriber]struct{}
}

// NewBus returns a Bus decoding through registry.
func NewBus(registry *extension.Registry) *Bus {
	return &Bus{registry: registry, subs: make(map[session.SessionID]map[*subscriber]struct{})}
}

// Committed is writer.CommitObserver: one Event per committed event, in
// commit order.
func (b *Bus) Committed(_ context.Context, sid session.SessionID, commit session.Commit) {
	var events []Event
	for _, batch := range commit.Batches {
		for _, row := range batch.Events {
			e := Event{Session: sid, Row: row}
			decoded, err := b.registry.Decode(row)
			if err != nil {
				e.Unknown, e.Err = true, err
			} else {
				e.Module, e.Version, e.Value, e.Unknown = decoded.Module, decoded.Version, decoded.Value, decoded.Unknown
			}
			events = append(events, e)
		}
	}
	b.publish(sid, events...)
}

// Failed reports a background failure to the Session's subscribers.
func (b *Bus) Failed(sid session.SessionID, err error) {
	b.publish(sid, Event{Session: sid, Err: err})
}

func (b *Bus) publish(sid session.SessionID, events ...Event) {
	b.mu.Lock()
	subs := make([]*subscriber, 0, len(b.subs[sid]))
	for s := range b.subs[sid] {
		subs = append(subs, s)
	}
	b.mu.Unlock()
	for _, s := range subs {
		s.push(events)
	}
}

// Subscribe registers a subscriber to one Session's stream from this moment
// on; the channel closes when ctx is done.
func (b *Bus) Subscribe(ctx context.Context, sid session.SessionID) <-chan Event {
	s := &subscriber{out: make(chan Event, 64), wake: make(chan struct{}, 1)}
	b.mu.Lock()
	if b.subs[sid] == nil {
		b.subs[sid] = make(map[*subscriber]struct{})
	}
	b.subs[sid][s] = struct{}{}
	b.mu.Unlock()
	go s.drain(ctx, func() {
		b.mu.Lock()
		delete(b.subs[sid], s)
		if len(b.subs[sid]) == 0 {
			delete(b.subs, sid)
		}
		b.mu.Unlock()
	})
	return s.out
}

// subscriber decouples the committer from the consumer: push appends to an
// unbounded queue and never blocks, so a slow reader delays only its own
// delivery, never a Commit; drain moves the queue into the channel in order
// and closes it when the subscription ends.
type subscriber struct {
	mu    sync.Mutex
	queue []Event
	out   chan Event
	wake  chan struct{}
}

func (s *subscriber) push(events []Event) {
	s.mu.Lock()
	s.queue = append(s.queue, events...)
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *subscriber) drain(ctx context.Context, unsubscribe func()) {
	defer func() {
		unsubscribe()
		close(s.out)
	}()
	for {
		s.mu.Lock()
		batch := s.queue
		s.queue = nil
		s.mu.Unlock()
		for i := range batch {
			select {
			case s.out <- batch[i]:
			case <-ctx.Done():
				return
			}
		}
		select {
		case <-s.wake:
		case <-ctx.Done():
			return
		}
	}
}
