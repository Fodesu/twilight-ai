package reconcile

import (
	"context"
	"errors"
	"testing"

	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/process"
	"github.com/felinics/twilight/agentcore/run/effect"
)

// attempts is an in-memory dispatch ledger.
type attempts struct {
	commits map[effect.AssignmentKey][]process.Commit
	epoch   map[effect.AssignmentKey]process.Epoch
}

func newAttempts() *attempts {
	return &attempts{commits: map[effect.AssignmentKey][]process.Commit{}, epoch: map[effect.AssignmentKey]process.Epoch{}}
}

func (a *attempts) Load(_ context.Context, k effect.AssignmentKey) (process.State, process.Head, bool, error) {
	cs := a.commits[k]
	state := process.State{Key: k}
	for i := range cs {
		var err error
		if state, err = process.Fold(state, &cs[i]); err != nil {
			return process.State{}, process.Head{}, false, err
		}
	}
	return state, process.Head{Next: process.CommitSeq(len(cs))}, len(cs) > 0, nil
}

func (a *attempts) Read(_ context.Context, k effect.AssignmentKey, _ process.CommitSeq) ([]process.Commit, process.Head, error) {
	return a.commits[k], process.Head{Next: process.CommitSeq(len(a.commits[k]))}, nil
}

func (a *attempts) Append(ctx context.Context, epoch process.Epoch, k effect.AssignmentKey, c process.Commit) error {
	for _, have := range a.commits[k] {
		if have.CommitID == c.CommitID {
			return ledger.ErrAlreadyApplied
		}
	}
	if epoch < a.epoch[k] {
		return ledger.ErrFenced
	}
	state, head, _, err := a.Load(ctx, k)
	if err != nil {
		return err
	}
	if c.Seq != head.Next {
		return ledger.ErrConflict
	}
	if _, err := process.Fold(state, &c); err != nil {
		return err
	}
	a.epoch[k] = epoch
	a.commits[k] = append(a.commits[k], c)
	return nil
}

// RUN-EXE-15: an effect the executor holds nothing for is handed over again
// within the redispatch budget, each attempt recorded before it is made;
// a refusal the executor may lift leaves the target Executing for the next
// reconciliation; the budget's end and a definite rejection dispose.
func TestPlanRedispatchesMissingWithinBudget(t *testing.T) {
	ctx := context.Background()
	key := effect.AssignmentKey{Session: "s", RunID: "r1", Effect: "c1"}
	cases := []struct {
		name       string
		redispatch error
		prior      int // attempts already recorded
		givenUp    bool
		want       Verdict
		attempts   int
		gaveUp     bool
		calls      int
	}{
		{"first redispatch", nil, 0, false, Redispatch, 1, false, 1},
		{"refusal within budget", effect.ErrDispatchRetryable, 1, false, Defer, 2, false, 1},
		{"lost response", effect.ErrDispatchUnknown, 0, false, Defer, 1, false, 1},
		{"budget exhausted", nil, 2, false, Dispose, 2, true, 0},
		{"already given up", nil, 0, true, Dispose, 0, true, 0},
		{"definite rejection", errors.New("run no longer executes the effect"), 0, false, Dispose, 1, true, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newAttempts()
			for i := 1; i <= tc.prior; i++ {
				if _, err := process.Attempt(ctx, store, 1, key, 1); err != nil {
					t.Fatal(err)
				}
			}
			if tc.givenUp {
				if err := process.GiveUp(ctx, store, 1, key, "before", 1); err != nil {
					t.Fatal(err)
				}
			}
			calls := 0
			r := &Reconciler{Executions: &fakePort{state: effect.AttachmentMissing}, Attempts: store, Epoch: 1, MaxRedispatches: 2,
				Redispatch: func(_ context.Context, k effect.AssignmentKey) error {
					calls++
					if k != key {
						t.Fatalf("redispatch of %+v", k)
					}
					return tc.redispatch
				}}
			decisions, err := r.Plan(ctx, "s", executingModel("c1"))
			if err != nil || len(decisions) != 1 {
				t.Fatalf("plan = %+v %v", decisions, err)
			}
			d := decisions[0]
			if d.Verdict != tc.want || (d.Recovery != nil) != (tc.want == Dispose) || calls != tc.calls {
				t.Fatalf("decision = %+v calls=%d, want %s calls=%d", d, calls, tc.want, tc.calls)
			}
			state, _, _, err := store.Load(ctx, key)
			if err != nil || state.Attempts != tc.attempts || state.GivenUp != tc.gaveUp {
				t.Fatalf("ledger = %+v %v, want attempts=%d givenUp=%v", state, err, tc.attempts, tc.gaveUp)
			}
		})
	}
	// Redispatch without a ledger is a configuration error, not a disposal.
	r := &Reconciler{Executions: &fakePort{state: effect.AttachmentMissing}, Redispatch: func(context.Context, effect.AssignmentKey) error { return nil }}
	if _, err := r.Plan(ctx, "s", executingModel("c1")); err == nil {
		t.Fatal("redispatch without a dispatch ledger accepted")
	}
}
