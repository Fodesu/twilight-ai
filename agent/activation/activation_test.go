package activation_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/felinics/twilight/agent/activation"
	"github.com/felinics/twilight/agent/inbox"
	"github.com/felinics/twilight/agent/inbox/inboxtest"
	"github.com/felinics/twilight/agent/ownership"
	"github.com/felinics/twilight/agent/scheduling"
	"github.com/felinics/twilight/agent/scheduling/schedulingtest"
	"github.com/felinics/twilight/agentcore/artifact/artifacttest"
	"github.com/felinics/twilight/agentcore/chatlog"
	"github.com/felinics/twilight/agentcore/executor"
	"github.com/felinics/twilight/agentcore/executor/store/storetest"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/prompt"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/model"
	"github.com/felinics/twilight/agentcore/run/sessionstore"
	runtime "github.com/felinics/twilight/agentcore/runtime"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/filestore/filestoretest"
	"github.com/felinics/twilight/agentcore/session/unit"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/sessionkernel"
	"github.com/felinics/twilight/agentcore/testutil/local"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/sdk"
)

const sid session.SessionID = "dormant-session"
const modelID run.ModelRef = "test-model"

type invoker struct {
	calls   atomic.Int32
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (i *invoker) Generate(ctx context.Context, _ sdk.Request) (sdk.ModelResult, error) {
	if i.calls.Add(1) == 1 {
		i.once.Do(func() { close(i.started) })
		select {
		case <-i.release:
		case <-ctx.Done():
			return sdk.ModelResult{}, ctx.Err()
		}
	}
	return sdk.ModelResult{Text: "answer"}, nil
}

type builder struct{}

func (builder) Build(_ context.Context, in prompt.Input) (prompt.Prompt, error) {
	ids := make([]run.InputID, len(in.Inputs))
	for i := range in.Inputs {
		ids[i] = in.Inputs[i].ID
	}
	return prompt.Prompt{Model: modelID, Request: model.ModelRequest{Model: string(modelID)}, InputIDs: ids, Token: "context"}, nil
}

type trackedStore struct {
	session.Store
	mu     sync.Mutex
	epochs []ledger.Epoch
}

func (s *trackedStore) Open(ctx context.Context, id session.SessionID, opts session.OpenOptions) (session.Handle, error) {
	h, err := s.Store.Open(ctx, id, opts)
	if err == nil {
		s.mu.Lock()
		s.epochs = append(s.epochs, h.Epoch())
		s.mu.Unlock()
	}
	return h, err
}

type fixture struct {
	t         *testing.T
	ctx       context.Context
	clock     atomic.Int64
	k         *sessionkernel.Kernel
	queue     *schedulingtest.Map
	mail      *inboxtest.Map
	consumer  *inbox.Consumer
	inv       *invoker
	backend   *local.LocalExecutor
	records   *storetest.Map
	worker    *executor.Worker
	x         *runtime.Execution
	builders  *prompt.Catalog
	preset    preset.PresetRef
	owners    *ownership.Manager
	tracked   *trackedStore
	discovery *activation.Discovery
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	f := &fixture{t: t, ctx: ctx, inv: &invoker{started: make(chan struct{}), release: make(chan struct{})}}
	f.clock.Store(time.Now().UnixMilli())
	now := func() time.Time { return time.UnixMilli(f.clock.Load()) }
	bindings, retention := artifacttest.Stores(t)
	var err error
	f.k, err = sessionkernel.New(sessionkernel.Ports{Store: filestoretest.Store(t),
		Content: filestoretest.Content(t, sessionstore.FrozenAuthority), Clock: now,
		Artifacts: sessionkernel.Artifacts{Bindings: bindings, Ledger: retention}, Modules: []module.ModuleDescriptor{inbox.Module}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.k.Close(context.Background()) })
	if err := f.k.CreateSession(ctx, sid, nil); err != nil {
		t.Fatal(err)
	}
	f.queue = schedulingtest.NewMap(now)
	f.mail = &inboxtest.Map{Now: now}
	f.consumer = &inbox.Consumer{Store: f.mail, Now: now, Handler: inbox.HandlerFunc(func(_ context.Context, _ writer.View, c inbox.Command) (inbox.Decision, error) {
		return inbox.Decision{Status: inbox.StatusApplied, Parts: []unit.Part{chatlog.SubmitInput(run.InputID(c.ID), c.Payload)}}, nil
	})}
	catalog, err := local.NewCatalog(map[run.ModelRef]local.ModelInvoker{modelID: f.inv})
	if err != nil {
		t.Fatal(err)
	}
	f.backend, err = local.NewLocalExecutor(catalog, catalog, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	f.records = storetest.NewMap(now)
	f.builders, err = prompt.NewCatalog(map[prompt.BuilderRef]prompt.PromptBuilderFactory{"test": func(preset.AgentPreset, prompt.Sources) prompt.Builder { return builder{} }})
	if err != nil {
		t.Fatal(err)
	}
	f.newWorker("worker-A")
	f.tracked = &trackedStore{Store: f.k.Store}
	factory := *f.k.Writers
	factory.Store = f.tracked
	f.owners = &ownership.Manager{Writers: &factory, Options: session.OpenOptions{Owner: "activation", LeaseDuration: time.Second}}
	f.discovery = &activation.Discovery{Sessions: f.k.Store.(session.Directory), Projections: f.k.Projections,
		Executions: f.worker, Inbox: f.mail, Queue: f.queue, PageSize: 1, Now: now, ProbeInterval: time.Second}
	t.Cleanup(func() {
		select {
		case <-f.inv.release:
		default:
			close(f.inv.release)
		}
	})
	return f
}

func (f *fixture) newWorker(id string) {
	f.t.Helper()
	now := func() time.Time { return time.UnixMilli(f.clock.Load()) }
	var err error
	f.worker, err = executor.NewWorker(f.ctx, f.records, []executor.Route{local.Route(f.backend)}, executor.WorkerOptions{ID: id, Clock: now, LeaseDuration: 100 * time.Millisecond})
	if err != nil {
		f.t.Fatal(err)
	}
	f.t.Cleanup(f.worker.Close)
	f.x, err = runtime.NewExecution(runtime.ExecutionConfig{Executor: f.worker, PromptBuilders: f.builders},
		runtime.ExecutionSources{Runs: f.k.Runs, Projections: f.k.Projections, Content: f.k.Content, Frozen: f.k.Frozen})
	if err != nil {
		f.t.Fatal(err)
	}
	f.preset, err = f.x.Presets.Register("test", preset.AgentPreset{Model: modelID, PromptBuilder: "test"})
	if err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) runner(owner string) *activation.Runner {
	return &activation.Runner{Queue: f.queue, Ownership: f.owners, Kernel: f.k, Execution: f.x, Preset: f.preset,
		Inbox: f.consumer, Owner: owner, Now: func() time.Time { return time.UnixMilli(f.clock.Load()) }, ProbeInterval: time.Second}
}

func (f *fixture) input(id string) {
	f.t.Helper()
	if _, err := f.mail.Enqueue(f.ctx, sid, inbox.Command{ID: inbox.CommandID(id), Kind: "input", Payload: run.MustParseCanonicalJSON(`{"text":"hello"}`)}); err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) sweep() {
	f.t.Helper()
	if err := f.discovery.Sweep(f.ctx); err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) run(r *activation.Runner) {
	f.t.Helper()
	if worked, err := r.RunOne(f.ctx); err != nil || !worked {
		f.t.Fatalf("activation worked=%v error=%v", worked, err)
	}
}

func (f *fixture) noOwner() {
	f.t.Helper()
	if _, held, err := f.k.Store.LeaseOf(f.ctx, sid); err != nil || held {
		f.t.Fatalf("Session still owned=%v error=%v", held, err)
	}
}

func (f *fixture) terminalOutcome() {
	f.t.Helper()
	// No settlement subscription is ever established. This is observation in
	// the test, not an activation wakeup; the sweep must rediscover the result.
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		page, err := f.worker.ListOutstanding(f.ctx, effect.AssignmentKey{}, 10)
		if err != nil {
			f.t.Fatal(err)
		}
		for _, item := range page.Items {
			if item.Status.Terminal() {
				return
			}
		}
		select {
		case <-f.ctx.Done():
			f.t.Fatal(f.ctx.Err())
		case <-ticker.C:
		}
	}
}

func TestDormantSessionContinuesOnAnotherActivationWithoutNotices(t *testing.T) {
	f := newFixture(t)
	f.input("first") // crash gap: durable enqueue without a scheduling signal
	f.sweep()
	f.run(f.runner("owner-A"))
	select {
	case <-f.inv.started:
	case <-f.ctx.Done():
		t.Fatal(f.ctx.Err())
	}
	f.noOwner()
	work, err := runtime.Inspect(f.ctx, f.k.Projections, sid)
	if err != nil || !work.Pending || work.Ready {
		t.Fatalf("dormant work=%+v error=%v", work, err)
	}
	close(f.inv.release)
	f.terminalOutcome()
	f.sweep()
	f.run(f.runner("owner-B"))
	f.noOwner()
	surface, err := turn.ReadSurface(f.ctx, f.k.Projections, sid)
	if err != nil {
		t.Fatal(err)
	}
	if len(surface.Turns) != 1 {
		t.Fatalf("turns=%d", len(surface.Turns))
	}
	for _, view := range surface.Turns {
		if view.Status != turn.TurnCompleted {
			t.Fatalf("turn=%+v", view)
		}
	}
	if f.inv.calls.Load() != 1 {
		t.Fatalf("model dispatch repeated: %d", f.inv.calls.Load())
	}
	f.tracked.mu.Lock()
	defer f.tracked.mu.Unlock()
	if len(f.tracked.epochs) != 2 || f.tracked.epochs[1] <= f.tracked.epochs[0] {
		t.Fatalf("epochs=%v", f.tracked.epochs)
	}
}

func TestOwnedSessionReschedulesInsteadOfConsumingWakeup(t *testing.T) {
	f := newFixture(t)
	held, err := f.owners.Acquire(f.ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close(context.Background())
	f.input("blocked")
	f.sweep()
	if worked, err := f.runner("contender").RunOne(f.ctx); !worked || !session.IsCode(err, session.ErrOwned) {
		t.Fatalf("worked=%v err=%v", worked, err)
	}
	if _, ok, err := f.queue.Claim(f.ctx, "other", time.Second); err != nil || ok {
		t.Fatalf("backoff ok=%v err=%v", ok, err)
	}
	if err := held.Close(f.ctx); err != nil {
		t.Fatal(err)
	}
	f.clock.Add(1001)
	f.run(f.runner("successor"))
	entry, _, err := f.mail.Lookup(f.ctx, sid, "blocked")
	if err != nil || entry.Result == nil {
		t.Fatalf("lost command: %+v %v", entry, err)
	}
	f.noOwner()
}

func TestTimeSlicePersistsContinuationWithoutDispatchingYet(t *testing.T) {
	f := newFixture(t)
	f.input("time-slice")
	f.sweep()
	r := f.runner("short-task")
	r.StepBudget = 1
	f.run(r) // route the Turn, then yield before model preparation
	f.noOwner()
	if f.inv.calls.Load() != 0 {
		t.Fatal("one-step activation waited for or dispatched the model")
	}
	f.run(r) // the durable continuation, without any new notification
	select {
	case <-f.inv.started:
	case <-f.ctx.Done():
		t.Fatal(f.ctx.Err())
	}
	f.noOwner()
}

type slowOpen struct {
	session.Store
	delay time.Duration
}

func (s slowOpen) Open(ctx context.Context, id session.SessionID, options session.OpenOptions) (session.Handle, error) {
	timer := time.NewTimer(s.delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return s.Store.Open(ctx, id, options)
	}
}

type renewalCounter struct {
	scheduling.Store
	calls atomic.Int32
}

func (s *renewalCounter) Renew(ctx context.Context, claim scheduling.Claim, duration time.Duration) error {
	s.calls.Add(1)
	return s.Store.Renew(ctx, claim, duration)
}

func TestClaimRenewalCoversSlowOwnershipAcquisition(t *testing.T) {
	f := newFixture(t)
	f.queue.Now = time.Now // expiry must advance while Open is blocked
	factory := *f.owners.Writers
	factory.Store = slowOpen{Store: factory.Store, delay: 200 * time.Millisecond}
	f.owners.Writers = &factory
	f.input("slow-open")
	f.sweep()
	counter := &renewalCounter{Store: f.queue}
	r := f.runner("slow-activation")
	r.Queue = counter
	r.ClaimDuration = 40 * time.Millisecond
	r.MaxDuration = 2 * time.Second
	f.run(r)
	if counter.calls.Load() < 3 {
		t.Fatalf("claim was not renewed while binding: %d", counter.calls.Load())
	}
	select {
	case <-f.inv.started:
	case <-f.ctx.Done():
		t.Fatal(f.ctx.Err())
	}
	f.noOwner()
}

func TestExpiredJobClaimDoesNotRevokeSessionWriteAuthority(t *testing.T) {
	f := newFixture(t)
	f.queue.Now = time.Now
	factory := *f.owners.Writers
	factory.Store = slowOpen{Store: factory.Store, delay: 25 * time.Millisecond}
	f.owners.Writers = &factory
	f.input("tiny-visibility-window")
	f.sweep()
	r := f.runner("still-fenced-writer")
	r.ClaimDuration, r.StepBudget = time.Millisecond, 1
	worked, err := r.RunOne(f.ctx)
	if !worked || !errors.Is(err, scheduling.ErrClaimLost) {
		t.Fatalf("worked=%v error=%v", worked, err)
	}
	// Missing a scheduler visibility deadline must not repeatedly throw away
	// all computation before the first semantic commit. Session fencing,
	// not a job claim, authorizes this bounded progress.
	surface, err := turn.ReadSurface(f.ctx, f.k.Projections, sid)
	if err != nil || len(surface.Turns) != 1 {
		t.Fatalf("slow activation made no progress: %+v %v", surface, err)
	}
	f.noOwner()
}

func TestActivationDeadlineIncludesOwnershipAcquisition(t *testing.T) {
	f := newFixture(t)
	factory := *f.owners.Writers
	factory.Store = slowOpen{Store: factory.Store, delay: time.Second}
	f.owners.Writers = &factory
	f.input("deadline-open")
	f.sweep()
	r := f.runner("bounded-activation")
	r.MaxDuration = 10 * time.Millisecond
	worked, err := r.RunOne(f.ctx)
	if !worked || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("worked=%v error=%v", worked, err)
	}
	if f.inv.calls.Load() != 0 {
		t.Fatal("expired activation started an effect")
	}
	f.noOwner()
}

type finishHook struct {
	scheduling.Store
	once sync.Once
	hook func()
}

func (s *finishHook) Finish(ctx context.Context, c scheduling.Claim, next int64) error {
	s.once.Do(s.hook)
	return s.Store.Finish(ctx, c, next)
}

func TestInputBetweenReleaseAndFinishKeepsNewGeneration(t *testing.T) {
	f := newFixture(t)
	f.input("before-yield")
	f.sweep()
	r := f.runner("old-activation")
	r.Queue = &finishHook{Store: f.queue, hook: func() {
		f.noOwner()
		f.input("during-yield")
		f.sweep()
	}}
	f.run(r)
	// The old Finish requested a future probe, but must not postpone the
	// uncovered new input that arrived after Release.
	f.run(f.runner("new-activation"))
	entry, _, err := f.mail.Lookup(f.ctx, sid, "during-yield")
	if err != nil || entry.Result == nil {
		t.Fatalf("lost concurrent input: %+v %v", entry, err)
	}
	f.noOwner()
}

func TestFutureProbeCannotPostponeBudgetedInputContinuation(t *testing.T) {
	f := newFixture(t)
	f.input("budget-first")
	f.input("budget-second")
	f.sweep()
	r := f.runner("budgeted")
	r.CommandBudget, r.StepBudget = 1, 1
	r.Queue = &finishHook{Store: f.queue, hook: func() {
		if err := f.queue.Signal(f.ctx, scheduling.Signal{Session: sid, Source: "future-probe", Revision: 1,
			NotBeforeUnixMilli: f.clock.Load() + time.Hour.Milliseconds()}); err != nil {
			t.Fatal(err)
		}
	}}
	f.run(r)
	f.run(f.runner("continuation")) // must be due now, not behind that probe
	entry, _, err := f.mail.Lookup(f.ctx, sid, "budget-second")
	if err != nil || entry.Result == nil {
		t.Fatalf("continuation lost behind probe: %+v %v", entry, err)
	}
	f.noOwner()
}

func TestDormantExecutionSurvivesWorkerReplacement(t *testing.T) {
	f := newFixture(t)
	f.input("worker-recovery")
	f.sweep()
	f.run(f.runner("owner-A"))
	select {
	case <-f.inv.started:
	case <-f.ctx.Done():
		t.Fatal(f.ctx.Err())
	}
	f.worker.Close() // execution lease remains; backend effect is not cancelled
	f.clock.Add(1001)
	f.newWorker("worker-B")
	f.discovery.Executions = f.worker
	f.sweep()
	f.run(f.runner("owner-B"))
	f.noOwner()
	close(f.inv.release)
	f.terminalOutcome()
	f.sweep()
	f.run(f.runner("owner-C"))
	if f.inv.calls.Load() != 1 {
		t.Fatalf("worker replacement repeated model call: %d", f.inv.calls.Load())
	}
	work, err := runtime.Inspect(f.ctx, f.k.Projections, sid)
	if err != nil || work.Ready || work.Pending {
		t.Fatalf("unsettled=%+v %v", work, err)
	}
}

func TestCancelledActivationContextDoesNotCancelAcceptedEffect(t *testing.T) {
	f := newFixture(t)
	f.input("detached")
	f.sweep()
	ctx, cancel := context.WithCancel(f.ctx)
	worked, err := f.runner("owner-A").RunOne(ctx)
	if err != nil || !worked {
		t.Fatalf("worked=%v error=%v", worked, err)
	}
	cancel()
	f.noOwner()
	close(f.inv.release)
	f.terminalOutcome()
	f.sweep()
	f.run(f.runner("owner-B"))
	if f.inv.calls.Load() != 1 {
		t.Fatal("detachment cancelled or repeated execution")
	}
}

func TestServiceDrivesWithoutKeepingWaitingSessionOwned(t *testing.T) {
	f := newFixture(t)
	f.input("service")
	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	service := &activation.Service{Discovery: f.discovery, Runner: f.runner("service-worker"),
		SweepInterval: 2 * time.Millisecond, IdleInterval: time.Millisecond}
	done := make(chan error, 1)
	go func() { done <- service.Run(ctx) }()
	select {
	case <-f.inv.started:
	case err := <-done:
		t.Fatalf("service returned before dispatch: %v", err)
	case <-f.ctx.Done():
		t.Fatal(f.ctx.Err())
	}
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		_, held, err := f.k.Store.LeaseOf(f.ctx, sid)
		if err != nil {
			t.Fatal(err)
		}
		if !held {
			break
		}
		select {
		case <-f.ctx.Done():
			t.Fatal(f.ctx.Err())
		case <-ticker.C:
		}
	}
	close(f.inv.release)
	for {
		surface, err := turn.ReadSurface(f.ctx, f.k.Projections, sid)
		if err != nil {
			t.Fatal(err)
		}
		completed := false
		for _, view := range surface.Turns {
			completed = view.Status == turn.TurnCompleted
		}
		if completed {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("service returned before settlement: %v", err)
		case <-f.ctx.Done():
			t.Fatal(f.ctx.Err())
		case <-ticker.C:
		}
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("service shutdown: %v", err)
		}
	case <-f.ctx.Done():
		t.Fatal(f.ctx.Err())
	}
	f.noOwner()
}

func TestDurableStopCancelsExecutionAfterSessionWasDormant(t *testing.T) {
	f := newFixture(t)
	f.input("domain-stop")
	f.sweep()
	f.run(f.runner("owner-A"))
	select {
	case <-f.inv.started:
	case <-f.ctx.Done():
		t.Fatal(f.ctx.Err())
	}
	h, err := f.owners.Acquire(f.ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	conversation, err := runtime.New(runtime.Config{Writer: h.Writer(), Driver: f.x.Driver, Turns: f.k.Turns, Chatlog: f.k.Chatlog, Preset: f.preset})
	if err != nil {
		t.Fatal(err)
	}
	if _, stopped, err := conversation.Stop(f.ctx, "user stopped"); err != nil || !stopped {
		t.Fatalf("stopped=%v error=%v", stopped, err)
	}
	if err := h.Close(f.ctx); err != nil {
		t.Fatal(err)
	}
	if err := f.queue.Signal(f.ctx, scheduling.Signal{Session: sid, Source: "explicit-stop", Revision: 1}); err != nil {
		t.Fatal(err)
	}
	f.run(f.runner("owner-B"))
	f.terminalOutcome()
	f.sweep()
	f.run(f.runner("owner-C"))
	page, err := f.worker.ListOutstanding(f.ctx, effect.AssignmentKey{}, 10)
	if err != nil || len(page.Items) != 0 {
		t.Fatalf("cancelled effect was not safely acknowledged: %+v %v", page, err)
	}
	if f.inv.calls.Load() != 1 {
		t.Fatal("cancellation recovery repeated an effect")
	}
	f.noOwner()
}

func TestStoppedRunRecoversCancellationOfOrphanExecution(t *testing.T) {
	f := newFixture(t)
	f.input("orphan-stop")
	f.sweep()
	f.run(f.runner("owner-A"))
	select {
	case <-f.inv.started:
	case <-f.ctx.Done():
		t.Fatal(f.ctx.Err())
	}
	f.worker.Close()
	h, err := f.owners.Acquire(f.ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	conversation, err := runtime.New(runtime.Config{Writer: h.Writer(), Driver: f.x.Driver, Turns: f.k.Turns, Chatlog: f.k.Chatlog, Preset: f.preset})
	if err != nil {
		t.Fatal(err)
	}
	if _, stopped, err := conversation.Stop(f.ctx, "cancel after worker failure"); err != nil || !stopped {
		t.Fatalf("stopped=%v error=%v", stopped, err)
	}
	if err := h.Close(f.ctx); err != nil {
		t.Fatal(err)
	}
	f.clock.Add(1001)
	f.newWorker("replacement-cancellation-worker")
	f.discovery.Executions = f.worker
	f.sweep()
	f.run(f.runner("owner-B"))
	f.terminalOutcome()
	f.sweep()
	f.run(f.runner("owner-C"))
	page, err := f.worker.ListOutstanding(f.ctx, effect.AssignmentKey{}, 10)
	if err != nil || len(page.Items) != 0 {
		t.Fatalf("orphan cancellation stuck: %+v %v", page, err)
	}
	if f.inv.calls.Load() != 1 {
		t.Fatal("cancel recovery started another physical execution")
	}
	f.noOwner()
}

type lostDispatchResponse struct {
	*executor.Worker
	once atomic.Bool
}

func (p *lostDispatchResponse) Dispatch(ctx context.Context, a effect.Assignment) error {
	if err := p.Worker.Dispatch(ctx, a); err != nil {
		return err
	}
	if p.once.CompareAndSwap(false, true) {
		return effect.ErrDispatchUnknown
	}
	return nil
}

func (f *fixture) usePort(port effect.ExecutionPort) {
	f.x.Executor = port
	f.x.Driver.Loops.Executor = port
	f.x.Compactions.Executor = port
}

func TestAcceptedDispatchWithLostResponseIsNotRepeated(t *testing.T) {
	f := newFixture(t)
	f.usePort(&lostDispatchResponse{Worker: f.worker})
	f.input("uncertain-dispatch")
	f.sweep()
	worked, err := f.runner("owner-A").RunOne(f.ctx)
	if !worked || !errors.Is(err, effect.ErrDispatchUnknown) {
		t.Fatalf("worked=%v error=%v", worked, err)
	}
	f.noOwner()
	select {
	case <-f.inv.started:
	case <-f.ctx.Done():
		t.Fatal(f.ctx.Err())
	}
	close(f.inv.release)
	f.terminalOutcome()
	f.sweep()
	f.run(f.runner("owner-B"))
	if f.inv.calls.Load() != 1 {
		t.Fatalf("unknown acceptance redispatched: %d", f.inv.calls.Load())
	}
}

type acceptedTemporaryFailure struct{ *executor.Worker }

func (p *acceptedTemporaryFailure) Dispatch(ctx context.Context, assignment effect.Assignment) error {
	if err := p.Worker.Dispatch(ctx, assignment); err != nil {
		return err
	}
	return effect.ErrDispatchRetryable // acceptance was durable; RPC cleanup failed
}

func TestTemporaryFailureAfterAcceptancePreservesSameIntent(t *testing.T) {
	f := newFixture(t)
	f.usePort(&acceptedTemporaryFailure{Worker: f.worker})
	f.input("accepted-temporary-failure")
	f.sweep()
	worked, err := f.runner("owner-A").RunOne(f.ctx)
	if !worked || !errors.Is(err, effect.ErrDispatchRetryable) {
		t.Fatalf("worked=%v error=%v", worked, err)
	}
	f.noOwner()
	work, err := runtime.Inspect(f.ctx, f.k.Projections, sid)
	if err != nil || !work.Pending {
		t.Fatalf("temporary refusal discarded accepted intent: %+v %v", work, err)
	}
	close(f.inv.release)
	f.terminalOutcome()
	f.sweep()
	f.run(f.runner("owner-B"))
	if f.inv.calls.Load() != 1 {
		t.Fatalf("accepted intent was replaced: %d calls", f.inv.calls.Load())
	}
}

type failedAcknowledgement struct {
	*executor.Worker
	once atomic.Bool
}

func (p *failedAcknowledgement) Acknowledge(ctx context.Context, key effect.AssignmentKey) error {
	if p.once.CompareAndSwap(false, true) {
		return errors.New("ACK transport failed")
	}
	return p.Worker.Acknowledge(ctx, key)
}

func TestSettlementSurvivesFailedAcknowledgement(t *testing.T) {
	f := newFixture(t)
	f.usePort(&failedAcknowledgement{Worker: f.worker})
	f.input("ack-window")
	f.sweep()
	f.run(f.runner("owner-A"))
	close(f.inv.release)
	f.terminalOutcome()
	f.sweep()
	f.run(f.runner("owner-B"))
	work, err := runtime.Inspect(f.ctx, f.k.Projections, sid)
	if err != nil || work.Pending || work.Ready {
		t.Fatalf("semantic settlement=%+v %v", work, err)
	}
	page, err := f.worker.ListOutstanding(f.ctx, effect.AssignmentKey{}, 10)
	if err != nil || len(page.Items) != 1 {
		t.Fatalf("missing ACK obligation: %+v %v", page, err)
	}
	// No new notification or revision is necessary: Finish retained the
	// outstanding cleanup obligation even though the Run already ended.
	f.clock.Add(1001)
	f.run(f.runner("owner-C"))
	page, err = f.worker.ListOutstanding(f.ctx, effect.AssignmentKey{}, 10)
	if err != nil || len(page.Items) != 0 {
		t.Fatalf("ACK not repaired: %+v %v", page, err)
	}
	if f.inv.calls.Load() != 1 {
		t.Fatal("ACK repair repeated the external effect")
	}
}
