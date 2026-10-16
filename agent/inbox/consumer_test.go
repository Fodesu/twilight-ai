package inbox_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/felinics/twilight/agent/inbox"
	"github.com/felinics/twilight/agent/inbox/inboxtest"
	"github.com/felinics/twilight/agent/ownership"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/filestore"
	"github.com/felinics/twilight/agentcore/session/unit"
	"github.com/felinics/twilight/agentcore/session/writer"
)

const consumerFactType ledger.EventType = "inbox-test/domain/effect"

var consumerFactStream = module.StreamDefinition{Domain: "inbox-test-effects", Inheritance: module.Inherited}

type consumerFact struct {
	CommandID inbox.CommandID `json:"commandId"`
	Value     string          `json:"value"`
}

// The test host explicitly registers the agent Module. Core does not acquire
// an implicit dependency on either inbox receipts or this application domain.
var consumerDomain = module.ModuleDescriptor{
	Source: "inbox-test", ID: "domain",
	Streams: []module.StreamDefinition{consumerFactStream},
	Events: []module.EventDefinition{{Type: consumerFactType, Domain: consumerFactStream.Domain,
		Codecs: map[module.PayloadVersion]module.PayloadCodec{1: module.JSONCodec[consumerFact]{}}}},
}

type consumerFixture struct {
	root     string
	store    *filestore.Store
	registry *module.Registry
	inbox    *inboxtest.Map
	now      time.Time
}

func newConsumerFixture(t *testing.T) *consumerFixture {
	t.Helper()
	f := &consumerFixture{root: t.TempDir(), inbox: &inboxtest.Map{}, now: time.UnixMilli(1_700_000_000_000)}
	var err error
	f.registry, err = module.BuildRegistry(consumerDomain, inbox.Module)
	if err != nil {
		t.Fatal(err)
	}
	f.store = f.reopen(t)
	if _, err := f.store.Create(context.Background(), session.CreateRequest{SessionID: "s"}); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *consumerFixture) reopen(t *testing.T) *filestore.Store {
	t.Helper()
	s, err := filestore.New(f.root)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func (f *consumerFixture) acquire(t *testing.T, store session.Store, sid session.SessionID, takeover bool) *ownership.Handle {
	t.Helper()
	manager := &ownership.Manager{
		Writers: &writer.Factory{Store: store, Registry: f.registry},
		Options: session.OpenOptions{Owner: "consumer-test", Takeover: takeover},
	}
	h, err := manager.Acquire(context.Background(), sid)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := h.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return h
}

func consumerCommand(id string) inbox.Command {
	return inbox.Command{ID: inbox.CommandID(id), Kind: "apply", Payload: run.MustParseCanonicalJSON(`{"value":"original"}`)}
}

func factPart(c inbox.Command) unit.Part {
	return unit.PartFunc(func(_ context.Context, _ writer.View, at int64) ([]writer.TypedBatch, error) {
		return []writer.TypedBatch{{Domain: consumerFactStream.Ref(""), Events: []writer.TypedEvent{{
			Type: consumerFactType, RecordedAtUnixMilli: at,
			Value: consumerFact{CommandID: c.ID, Value: c.Payload.String()},
		}}}}, nil
	})
}

func (f *consumerFixture) consumer(handler inbox.Handler) *inbox.Consumer {
	return &inbox.Consumer{Store: f.inbox, Handler: handler, Now: func() time.Time { return f.now }}
}

func (f *consumerFixture) enqueue(t *testing.T, sid session.SessionID, c inbox.Command) inbox.Entry {
	t.Helper()
	e, err := f.inbox.Enqueue(context.Background(), sid, c)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func (f *consumerFixture) commits(t *testing.T, sid session.SessionID) []ledger.Commit {
	t.Helper()
	// Read through a fresh file-store handle, not the Writer's local state.
	page, err := f.reopen(t).ReadCommits(context.Background(), session.CommitReadRequest{SessionID: sid})
	if err != nil {
		t.Fatal(err)
	}
	return page.Commits
}

func (f *consumerFixture) events(t *testing.T, sid session.SessionID, domain ledger.Domain, inheritance module.Inheritance) []ledger.Event {
	t.Helper()
	page, err := f.reopen(t).ReadStream(context.Background(), session.StreamReadRequest{
		SessionID: sid, Domain: domain, Inheritance: inheritance,
	})
	if err != nil {
		t.Fatal(err)
	}
	return page.Events
}

func (f *consumerFixture) receipts(t *testing.T, sid session.SessionID) []inbox.Receipt {
	t.Helper()
	stream := inbox.Module.Streams[0]
	events := f.events(t, sid, stream.Ref(""), stream.Inheritance)
	var receipts []inbox.Receipt
	for _, event := range events {
		if event.Type != inbox.ReceiptType {
			t.Fatalf("unexpected receipt-stream event: %+v", event)
		}
		var receipt inbox.Receipt
		if err := json.Unmarshal(event.Payload.Bytes(), &receipt); err != nil {
			t.Fatal(err)
		}
		receipts = append(receipts, receipt)
	}
	return receipts
}

func (f *consumerFixture) pending(t *testing.T, sid session.SessionID, ids ...inbox.CommandID) {
	t.Helper()
	entries, err := f.inbox.Pending(context.Background(), sid)
	if err != nil {
		t.Fatal(err)
	}
	var got []inbox.CommandID
	for _, e := range entries {
		got = append(got, e.Command.ID)
	}
	if !reflect.DeepEqual(got, ids) {
		t.Fatalf("pending = %v; want %v", got, ids)
	}
}

func TestConsumerAppliedAndRejectedReceiptsAreImmutable(t *testing.T) {
	for _, status := range []inbox.Status{inbox.StatusApplied, inbox.StatusRejected} {
		t.Run(string(status), func(t *testing.T) {
			ctx := context.Background()
			f := newConsumerFixture(t)
			h := f.acquire(t, f.store, "s", false)
			command := consumerCommand("c1")
			f.enqueue(t, "s", command)
			calls := 0
			consumer := f.consumer(inbox.HandlerFunc(func(_ context.Context, v writer.View, c inbox.Command) (inbox.Decision, error) {
				calls++
				if v.Epoch() != h.Lease().Epoch || v.Head().Next != 0 {
					t.Fatalf("handler did not receive initial transactional View: epoch=%v head=%+v", v.Epoch(), v.Head())
				}
				d := inbox.Decision{Status: status, Reason: "original decision"}
				if status == inbox.StatusApplied {
					d.Parts = []unit.Part{factPart(c)}
				}
				return d, nil
			}))
			first, err := consumer.Apply(ctx, h.Writer(), command)
			if err != nil || first.CommandID != command.ID || first.Digest == "" || first.Result.Status != status || first.Result.Reason != "original decision" || first.Result.ResolvedAtUnixMilli != f.now.UnixMilli() {
				t.Fatalf("Apply = %+v, %v", first, err)
			}
			f.pending(t, "s", command.ID) // Apply commits proof, not acknowledgement.
			commits := f.commits(t, "s")
			wantEvents := 1
			if status == inbox.StatusApplied {
				wantEvents++
			}
			if len(commits) != 1 || len(unit.Events(commits[0])) != wantEvents {
				t.Fatalf("domain effect and receipt must share exactly one commit: %+v", commits)
			}
			if got := f.receipts(t, "s"); !reflect.DeepEqual(got, []inbox.Receipt{first}) {
				t.Fatalf("durable receipt = %+v; want %+v", got, first)
			}
			before := commits[0]
			f.now = f.now.Add(time.Hour)
			consumer.Handler = inbox.HandlerFunc(func(context.Context, writer.View, inbox.Command) (inbox.Decision, error) {
				calls++
				t.Fatal("receipt replay invoked Handler")
				return inbox.Decision{}, nil
			})
			again, err := consumer.Apply(ctx, h.Writer(), command)
			if err != nil || again != first {
				t.Fatalf("replayed receipt changed: got %+v, %v; want %+v", again, err, first)
			}
			if n, err := consumer.Consume(ctx, h.Writer(), 1); err != nil || n != 1 {
				t.Fatalf("Consume committed receipt = %d, %v", n, err)
			}
			f.pending(t, "s")
			stored, found, err := f.inbox.Lookup(ctx, "s", command.ID)
			// Resolve stamps its own delivery-acknowledgement time. The
			// immutable Session receipt retains the original commit time.
			if err != nil || !found || stored.Result == nil || stored.Result.Status != first.Result.Status || stored.Result.Reason != first.Result.Reason || stored.Result.ResolvedAtUnixMilli == 0 {
				t.Fatalf("acknowledgement = %+v, %v, %v; want receipt disposition", stored, found, err)
			}
			if calls != 1 || !reflect.DeepEqual(f.commits(t, "s"), []ledger.Commit{before}) {
				t.Fatalf("replay changed durable facts or reran handler: calls=%d", calls)
			}
		})
	}
}

func TestConsumerChangedPayloadConflictsWithoutCallingHandler(t *testing.T) {
	ctx := context.Background()
	f := newConsumerFixture(t)
	h := f.acquire(t, f.store, "s", false)
	calls := 0
	consumer := f.consumer(inbox.HandlerFunc(func(_ context.Context, _ writer.View, c inbox.Command) (inbox.Decision, error) {
		calls++
		return inbox.Decision{Status: inbox.StatusApplied, Parts: []unit.Part{factPart(c)}}, nil
	}))
	original := consumerCommand("c1")
	f.enqueue(t, "s", original)
	first, err := consumer.Apply(ctx, h.Writer(), original)
	if err != nil {
		t.Fatal(err)
	}
	changed := original
	changed.Payload = run.MustParseCanonicalJSON(`{"value":"changed"}`)
	for _, c := range []inbox.Command{changed, {ID: original.ID, Kind: "different-kind", Payload: original.Payload}} {
		if _, err := consumer.Apply(ctx, h.Writer(), c); !errors.Is(err, inbox.ErrCommandConflict) {
			t.Fatalf("Apply changed command = %v; want ErrCommandConflict", err)
		}
		if _, err := f.inbox.Enqueue(ctx, "s", c); !errors.Is(err, inbox.ErrCommandConflict) {
			t.Fatalf("Enqueue changed command = %v; want ErrCommandConflict", err)
		}
	}
	if calls != 1 || len(f.commits(t, "s")) != 1 || !reflect.DeepEqual(f.receipts(t, "s"), []inbox.Receipt{first}) {
		t.Fatalf("changed command mutated receipt/facts or called Handler: calls=%d", calls)
	}
	f.pending(t, "s", original.ID)
}

// Only delivery acknowledgement is fault-injected; commits and ownership use
// the actual file ledger and Writer throughout.
type consumerResolveStore struct {
	inbox.Store
	calls  int
	fail   error
	before func(session.SessionID, uint64, inbox.Result)
}

func (s *consumerResolveStore) Resolve(ctx context.Context, sid session.SessionID, seq uint64, result inbox.Result) error {
	s.calls++
	if s.before != nil {
		s.before(sid, seq, result)
	}
	if s.fail != nil {
		err := s.fail
		s.fail = nil
		return err
	}
	return s.Store.Resolve(ctx, sid, seq, result)
}

func TestConsumerResolveFailureReplaysReceiptInNewEpoch(t *testing.T) {
	ctx := context.Background()
	f := newConsumerFixture(t)
	old := f.acquire(t, f.store, "s", false)
	command := consumerCommand("c1")
	f.enqueue(t, "s", command)
	calls := 0
	consumer := f.consumer(inbox.HandlerFunc(func(_ context.Context, _ writer.View, c inbox.Command) (inbox.Decision, error) {
		calls++
		return inbox.Decision{Status: inbox.StatusApplied, Parts: []unit.Part{factPart(c)}}, nil
	}))
	resolveFailure := errors.New("resolve transport unavailable")
	store := &consumerResolveStore{Store: f.inbox, fail: resolveFailure}
	store.before = func(sid session.SessionID, _ uint64, result inbox.Result) {
		proof := f.receipts(t, sid)
		if len(proof) != 1 || proof[0].Result != result || len(f.commits(t, sid)) != 1 {
			t.Fatal("Resolve ran before the domain effect and durable receipt committed")
		}
	}
	consumer.Store = store
	if n, err := consumer.Consume(ctx, old.Writer(), 1); n != 0 || !errors.Is(err, resolveFailure) {
		t.Fatalf("failed Resolve = %d, %v", n, err)
	}
	f.pending(t, "s", command.ID)
	before := f.commits(t, "s")
	// Leave the old Writer alive: this is a real takeover/fence, not merely
	// reconstructing a local object after a successful release.
	current := f.acquire(t, f.reopen(t), "s", true)
	if current.Lease().Epoch <= old.Lease().Epoch {
		t.Fatal("takeover did not create a new epoch")
	}
	f.now = f.now.Add(time.Hour)
	consumer.Handler = inbox.HandlerFunc(func(context.Context, writer.View, inbox.Command) (inbox.Decision, error) {
		calls++
		t.Fatal("new epoch reran Handler instead of reading durable receipt")
		return inbox.Decision{}, nil
	})
	if n, err := consumer.Consume(ctx, current.Writer(), 1); n != 1 || err != nil {
		t.Fatalf("replay after lost Resolve = %d, %v", n, err)
	}
	f.pending(t, "s")
	if calls != 1 || store.calls != 2 || !reflect.DeepEqual(f.commits(t, "s"), before) {
		t.Fatalf("retry duplicated domain effect/receipt: handlers=%d resolves=%d", calls, store.calls)
	}
}

func TestConsumerDecisionFailuresLeaveNoReceiptAndDoNotResolve(t *testing.T) {
	for _, stage := range []string{"handler", "part"} {
		t.Run(stage, func(t *testing.T) {
			ctx := context.Background()
			f := newConsumerFixture(t)
			h := f.acquire(t, f.store, "s", false)
			command := consumerCommand("c1")
			f.enqueue(t, "s", command)
			failure := errors.New(stage + " failed")
			prepared := 0
			consumer := f.consumer(inbox.HandlerFunc(func(_ context.Context, _ writer.View, c inbox.Command) (inbox.Decision, error) {
				if stage == "handler" {
					return inbox.Decision{}, failure
				}
				return inbox.Decision{Status: inbox.StatusApplied, Parts: []unit.Part{
					factPart(c),
					unit.PartFunc(func(context.Context, writer.View, int64) ([]writer.TypedBatch, error) {
						prepared++
						return nil, failure
					}),
				}}, nil
			}))
			store := &consumerResolveStore{Store: f.inbox}
			consumer.Store = store
			if n, err := consumer.Consume(ctx, h.Writer(), 1); n != 0 || !errors.Is(err, failure) {
				t.Fatalf("decision failure = %d, %v; want original error", n, err)
			}
			if store.calls != 0 || len(f.commits(t, "s")) != 0 || len(f.receipts(t, "s")) != 0 {
				t.Fatal("failed decision wrote receipt/domain facts or acknowledged command")
			}
			if stage == "part" && prepared != 1 {
				t.Fatal("failing Part was not reached after preparing earlier domain Part")
			}
			f.pending(t, "s", command.ID)
			consumer.Handler = inbox.HandlerFunc(func(_ context.Context, _ writer.View, c inbox.Command) (inbox.Decision, error) {
				return inbox.Decision{Status: inbox.StatusApplied, Parts: []unit.Part{factPart(c)}}, nil
			})
			if n, err := consumer.Consume(ctx, h.Writer(), 1); n != 1 || err != nil {
				t.Fatalf("retry after failure = %d, %v", n, err)
			}
			if store.calls != 1 || len(f.commits(t, "s")) != 1 || len(f.receipts(t, "s")) != 1 {
				t.Fatal("failed attempt consumed identity or retry duplicated facts")
			}
		})
	}
}

func TestConsumerFencedWriterCannotCommitNewReceipt(t *testing.T) {
	ctx := context.Background()
	f := newConsumerFixture(t)
	old := f.acquire(t, f.store, "s", false)
	current := f.acquire(t, f.reopen(t), "s", true)
	command := consumerCommand("after-takeover")
	f.enqueue(t, "s", command)
	consumer := f.consumer(inbox.HandlerFunc(func(_ context.Context, _ writer.View, c inbox.Command) (inbox.Decision, error) {
		return inbox.Decision{Status: inbox.StatusApplied, Parts: []unit.Part{factPart(c)}}, nil
	}))
	store := &consumerResolveStore{Store: f.inbox}
	consumer.Store = store
	if n, err := consumer.Consume(ctx, old.Writer(), 1); n != 0 || !errors.Is(err, &ledger.Error{Code: ledger.CodeOwnershipLost}) {
		t.Fatalf("fenced consumer = %d, %v; want ownership-lost", n, err)
	}
	if store.calls != 0 || len(f.commits(t, "s")) != 0 || len(f.receipts(t, "s")) != 0 {
		t.Fatal("fenced Writer committed proof/effect or resolved the command")
	}
	f.pending(t, "s", command.ID)
	if n, err := consumer.Consume(ctx, current.Writer(), 1); n != 1 || err != nil {
		t.Fatalf("current Writer = %d, %v", n, err)
	}
	if got := f.commits(t, "s"); len(got) != 1 || len(unit.Events(got[0])) != 2 || store.calls != 1 {
		t.Fatalf("current epoch must commit effect and receipt once: %+v resolves=%d", got, store.calls)
	}
}

func TestConsumerForkDoesNotInheritParentReceipt(t *testing.T) {
	ctx := context.Background()
	f := newConsumerFixture(t)
	parent := f.acquire(t, f.store, "s", false)
	command := consumerCommand("same-command-id")
	var decidedIn []session.SegmentID
	consumer := f.consumer(inbox.HandlerFunc(func(_ context.Context, v writer.View, c inbox.Command) (inbox.Decision, error) {
		decidedIn = append(decidedIn, v.Header().ID)
		return inbox.Decision{Status: inbox.StatusApplied, Parts: []unit.Part{factPart(c)}}, nil
	}))
	parentReceipt, err := consumer.Apply(ctx, parent.Writer(), command)
	if err != nil {
		t.Fatal(err)
	}
	commits := f.commits(t, "s")
	if _, err := writer.Fork(ctx, f.store, f.registry, writer.ForkRequest{
		Parent: "s", At: commits[len(commits)-1].Seq, Child: "child",
	}); err != nil {
		t.Fatal(err)
	}
	child := f.acquire(t, f.store, "child", false)
	if receipts := f.receipts(t, "child"); len(receipts) != 0 {
		t.Fatalf("Own receipt stream inherited parent receipts: %+v", receipts)
	}
	if facts := f.events(t, "child", consumerFactStream.Ref(""), module.Inherited); len(facts) != 1 {
		t.Fatal("fixture fork did not inherit parent's domain history")
	}
	f.enqueue(t, "child", command)
	f.now = f.now.Add(time.Hour)
	if n, err := consumer.Consume(ctx, child.Writer(), 1); n != 1 || err != nil {
		t.Fatalf("child command = %d, %v", n, err)
	}
	if !reflect.DeepEqual(decidedIn, []session.SegmentID{parent.Writer().Header().ID, child.Writer().Header().ID}) {
		t.Errorf("child reused parent's command identity instead of deciding locally: decidedIn=%v", decidedIn)
	}
	if receipts := f.receipts(t, "child"); len(receipts) != 1 || receipts[0].Result.ResolvedAtUnixMilli != f.now.UnixMilli() {
		t.Errorf("child must commit its own receipt, not acknowledge parent proof: %+v", receipts)
	}
	if facts := f.events(t, "child", consumerFactStream.Ref(""), module.Own); len(facts) != 1 {
		t.Errorf("child command did not commit its own domain effect: %+v", facts)
	}
	if receipts := f.receipts(t, "s"); !reflect.DeepEqual(receipts, []inbox.Receipt{parentReceipt}) {
		t.Errorf("child changed parent receipt: %+v", receipts)
	}
	f.pending(t, "child")
}

func TestConsumerCommandBudgetLeavesRemainingEntriesPending(t *testing.T) {
	ctx := context.Background()
	f := newConsumerFixture(t)
	h := f.acquire(t, f.store, "s", false)
	for _, id := range []string{"c1", "c2", "c3"} {
		f.enqueue(t, "s", consumerCommand(id))
	}
	var calls []inbox.CommandID
	consumer := f.consumer(inbox.HandlerFunc(func(_ context.Context, _ writer.View, c inbox.Command) (inbox.Decision, error) {
		calls = append(calls, c.ID)
		return inbox.Decision{Status: inbox.StatusApplied, Parts: []unit.Part{factPart(c)}}, nil
	}))
	for _, budget := range []int{0, -1} {
		if n, err := consumer.Consume(ctx, h.Writer(), budget); n != 0 || err == nil {
			t.Fatalf("invalid budget %d = %d, %v", budget, n, err)
		}
	}
	f.pending(t, "s", "c1", "c2", "c3")
	if len(calls) != 0 || len(f.commits(t, "s")) != 0 {
		t.Fatal("invalid budget processed a command")
	}
	if n, err := consumer.Consume(ctx, h.Writer(), 1); n != 1 || err != nil {
		t.Fatalf("budget 1 = %d, %v", n, err)
	}
	f.pending(t, "s", "c2", "c3")
	if len(f.receipts(t, "s")) != 1 || !reflect.DeepEqual(calls, []inbox.CommandID{"c1"}) {
		t.Fatalf("budget exhaustion applied commands beyond the limit: %v", calls)
	}
	if n, err := consumer.Consume(ctx, h.Writer(), 1); n != 1 || err != nil {
		t.Fatalf("second budget 1 = %d, %v", n, err)
	}
	f.pending(t, "s", "c3")
	if n, err := consumer.Consume(ctx, h.Writer(), 10); n != 1 || err != nil {
		t.Fatalf("remaining command = %d, %v", n, err)
	}
	f.pending(t, "s")
	if !reflect.DeepEqual(calls, []inbox.CommandID{"c1", "c2", "c3"}) || len(f.receipts(t, "s")) != 3 {
		t.Fatalf("commands not consumed exactly once in Seq order: %v", calls)
	}
}
