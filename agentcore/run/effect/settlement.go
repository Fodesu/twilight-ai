package effect

import (
	"context"
	"errors"
	"time"
)

// Settlement is the executor's notice that the execution of Key reached a
// terminal state and its Outcome is readable through GetOutcome. It carries
// no Outcome: the notice says when to read, the read says what. A caller
// that missed a notice (it was not subscribed, its stream dropped) loses
// nothing it cannot recover by reading, so a subscriber and a reader agree
// without any coordination between them.
//
// Sequence orders notices within one port incarnation, so a subscriber that
// reconnects can ask for the ones after the last it saw. It is not durable:
// a port that restarts starts a new incarnation (Epoch changes) and the
// subscriber treats the change as a gap, re-reading every key it waits on.
type Settlement struct {
	Key      AssignmentKey `json:"key"`
	Epoch    string        `json:"epoch"`
	Sequence uint64        `json:"sequence"`
}

// SettlementPort is the notification side of an ExecutionPort, an optional
// capability like ProgressPort. Settlements delivers to fn, in order, every
// Settlement the port records with Sequence greater than after in the
// incarnation named by epoch, then every new one as it happens, until fn
// returns false or ctx ends. An empty epoch, or one the port does not
// recognise, subscribes from the head of the current incarnation: the
// caller re-reads what it waits on (GetOutcome is a plain read) and relies
// on the stream only for what settles afterwards. The port keeps a bounded
// window of past Settlements; asking for a Sequence it has evicted is
// ErrSettlementsEvicted, and the caller re-reads as for an unknown epoch.
//
// One subscription per port covers every key that port settles, whoever
// dispatched them: the cost of waiting is per executor, not per effect.
type SettlementPort interface {
	Settlements(ctx context.Context, epoch string, after uint64, fn func(Settlement) bool) error
}

// ErrSettlementsEvicted reports a Settlements subscription that asked for a
// Sequence the port no longer holds: the subscriber must re-read the keys
// it waits on and subscribe again from the head.
var ErrSettlementsEvicted = errors.New("agent: effect: settlements before the requested sequence were evicted")

// AwaitOutcome reads the Outcome of key, waiting for its Settlement when the
// execution is unsettled: the synchronous form of the two primitives, for a
// caller that dispatched one effect and has nothing else to do until it
// answers. It subscribes before it reads, so a settlement between the read
// and the subscription is not missed; a port without SettlementPort is read
// at poll intervals instead. Any GetOutcome error other than
// ErrOutcomeNotReady ends the wait.
func AwaitOutcome(ctx context.Context, port ExecutionPort, key AssignmentKey, poll time.Duration) (Outcome, error) {
	if poll <= 0 {
		poll = time.Second
	}
	settlements, ok := port.(SettlementPort)
	if !ok {
		return pollOutcome(ctx, port, key, poll)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	settled := make(chan struct{}, 1)
	streamErr := make(chan error, 1)
	go func() {
		err := settlements.Settlements(ctx, "", 0, func(s Settlement) bool {
			if s.Key == key {
				select {
				case settled <- struct{}{}:
				default:
				}
			}
			return true
		})
		streamErr <- err
	}()
	for {
		out, err := port.GetOutcome(ctx, key)
		if !errors.Is(err, ErrOutcomeNotReady) {
			return out, err
		}
		// The stream may have ended (the port went away): fall back to
		// polling so the wait is bounded by ctx alone, not by the stream.
		timer := time.NewTimer(poll)
		select {
		case <-settled:
			timer.Stop()
		case err := <-streamErr:
			timer.Stop()
			if ctx.Err() != nil {
				return Outcome{}, ctx.Err()
			}
			_ = err
			return pollOutcome(ctx, port, key, poll)
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return Outcome{}, ctx.Err()
		}
	}
}

func pollOutcome(ctx context.Context, port ExecutionPort, key AssignmentKey, poll time.Duration) (Outcome, error) {
	for {
		out, err := port.GetOutcome(ctx, key)
		if !errors.Is(err, ErrOutcomeNotReady) {
			return out, err
		}
		timer := time.NewTimer(poll)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return Outcome{}, ctx.Err()
		}
	}
}
