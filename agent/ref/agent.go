package ref

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/felinics/twilight/agent/decision"
	"github.com/felinics/twilight/agent/es"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/loop"
	"github.com/felinics/twilight/agent/turn"
)

// Profile, PublicTool and DigestProfile live in the turn module now
// (TRN-PRF); these aliases keep the host package readable until it is
// rewritten by role.
type (
	Profile          = turn.Profile
	PublicTool       = turn.PublicTool
	ProjectionSource = decision.ProjectionSource
)

// DigestProfile is turn.DigestProfile.
func DigestProfile(p *Profile) (es.Digest, error) { return turn.DigestProfile(p) }

// Agent is one registrable execution configuration: the durable Profile plus
// the live effect capabilities that resolve it. Implement it directly for
// custom catalogs, or build the common shape with NewAgent.
type Agent interface {
	Profile() Profile
	ResolveModel(run.ModelRef) (loop.ModelInvoker, error)
	ResolveTool(run.ToolRef) (loop.ExecutableTool, error)
}

type agentConfig struct {
	tools        []loop.ExecutableTool
	systemPrompt string
	streaming    bool
	planner      turn.PlannerRef
	policy       turn.PolicyRef
	workspace    turn.WorkspaceRef
}

type AgentOption func(*agentConfig)

// WithTool adds one executable tool; its frozen definition and response
// policy enter the Profile.
func WithTool(t loop.ExecutableTool) AgentOption {
	return func(c *agentConfig) { c.tools = append(c.tools, t) }
}

func WithSystemPrompt(s string) AgentOption {
	return func(c *agentConfig) { c.systemPrompt = s }
}

func WithStreaming(on bool) AgentOption {
	return func(c *agentConfig) { c.streaming = on }
}

// WithPlanner selects the decision component; the default is
// decision.PlannerContextV1.
func WithPlanner(ref turn.PlannerRef) AgentOption {
	return func(c *agentConfig) { c.planner = ref }
}

// WithPolicy selects the execution policy by ref; the default is
// decision.PolicyDefaultV1. The policy value itself comes from the
// PolicyCatalog at registration.
func WithPolicy(ref turn.PolicyRef) AgentOption {
	return func(c *agentConfig) { c.policy = ref }
}

// WithWorkspace records the execution environment identity in the Profile.
func WithWorkspace(ref turn.WorkspaceRef) AgentOption {
	return func(c *agentConfig) { c.workspace = ref }
}

// NewAgent builds the common one-model Agent: the Profile is assembled from
// the model ref, the tools' frozen definitions and the default decision refs.
func NewAgent(model run.ModelRef, invoker loop.ModelInvoker, opts ...AgentOption) (Agent, error) {
	if model == "" || invoker == nil {
		return nil, errors.New("ref: agent requires a model ref and an invoker")
	}
	cfg := agentConfig{planner: decision.PlannerContextV1, policy: decision.PolicyDefaultV1}
	for _, opt := range opts {
		opt(&cfg)
	}
	a := &builtAgent{
		profile: Profile{SchemaVersion: 1, Model: model, Streaming: cfg.streaming, SystemPrompt: cfg.systemPrompt,
			Planner: cfg.planner, Policy: cfg.policy, Workspace: cfg.workspace},
		invoker: invoker,
		tools:   make(map[run.ToolRef]loop.ExecutableTool, len(cfg.tools)),
	}
	for _, t := range cfg.tools {
		if _, dup := a.tools[t.Ref()]; dup {
			return nil, fmt.Errorf("ref: duplicate tool %q", t.Ref())
		}
		def, err := run.FreezeToolDefinition(t.Definition())
		if err != nil {
			return nil, err
		}
		a.profile.Tools = append(a.profile.Tools, PublicTool{Ref: t.Ref(), Definition: def, Policy: t.ResponsePolicy()})
		a.tools[t.Ref()] = t
	}
	if err := turn.ValidateProfile(&a.profile); err != nil {
		return nil, err
	}
	return a, nil
}

type builtAgent struct {
	profile Profile
	invoker loop.ModelInvoker
	tools   map[run.ToolRef]loop.ExecutableTool
}

func (a *builtAgent) Profile() Profile { return a.profile }

func (a *builtAgent) ResolveModel(run.ModelRef) (loop.ModelInvoker, error) { return a.invoker, nil }

func (a *builtAgent) ResolveTool(r run.ToolRef) (loop.ExecutableTool, error) {
	t, ok := a.tools[r]
	if !ok {
		return nil, fmt.Errorf("ref: unknown tool %q", r)
	}
	return t, nil
}

// ErrProfileUnavailable reports that the persisted profile cannot be resolved
// by this process (REF-BND-2).
var ErrProfileUnavailable = errors.New("ref: profile_unavailable")

// ErrAlreadyDriving is how a RunDriver reports that another local driver
// already drives the Run: the commit (if any) landed and the running driver
// carries it forward. The host turns it into a successful Result with
// ResumeAlreadyDriving, not an error.
var ErrAlreadyDriving = errors.New("ref: already_driving")

// ResumeAlreadyDriving extends the turn disposition vocabulary for hosts: the
// inputs (if any) are committed and another local driver of the same Run
// carries them forward. The Coordinator itself never produces it.
const ResumeAlreadyDriving turn.ResumeDisposition = "already_driving"

type DriveRequest struct {
	Ref   turn.TurnRef
	RunID run.RunID
}

// RunDriver drives one Run to its next quiescent point; the reference driver
// wraps loop.Run (REF-DRV-1). A second local driver of the same Run reports
// ErrAlreadyDriving instead of driving.
type RunDriver interface {
	Drive(context.Context, DriveRequest) error
}

// ProfileRegistry resolves a persisted ProfileRef to a live driver.
type ProfileRegistry interface {
	Resolve(turn.ProfileRef) (RunDriver, error)
}

// Agents is the in-process ProfileRegistry (REF-BND-2). Register resolves the
// Profile's decision components through the catalogs, builds one long-lived
// Loop per registration, and every drive of a profile shares that Loop's
// already-driving guard.
type Agents struct {
	runtime     run.Runtime
	projections ProjectionSource
	sink        loop.EventSink
	decisions   decision.Catalogs

	mu   sync.RWMutex
	byID map[turn.ProfileID]registeredAgent
}

type registeredAgent struct {
	agent  Agent
	driver RunDriver
}

// NewAgents builds the registry; decisions resolve PlannerRef and PolicyRef
// at registration (DEC-CAT-2).
func NewAgents(runtime run.Runtime, projections ProjectionSource, sink loop.EventSink, decisions decision.Catalogs) *Agents {
	return &Agents{runtime: runtime, projections: projections, sink: sink, decisions: decisions, byID: map[turn.ProfileID]registeredAgent{}}
}

// Register validates the Profile, resolves its decision components, builds
// the driver and returns the ref the Session records.
func (r *Agents) Register(id turn.ProfileID, agent Agent) (turn.ProfileRef, error) {
	if id == "" || agent == nil {
		return turn.ProfileRef{}, errors.New("ref: register requires an id and an agent")
	}
	p := agent.Profile()
	if err := turn.ValidateProfile(&p); err != nil {
		return turn.ProfileRef{}, err
	}
	digest, err := turn.DigestProfile(&p)
	if err != nil {
		return turn.ProfileRef{}, err
	}
	planner, policy, err := r.decisions.Resolve(p, r.projections)
	if err != nil {
		return turn.ProfileRef{}, err
	}
	exec, err := loop.NewLocalExecutor(agent, agent, r.runtime, r.sink, p.Streaming)
	if err != nil {
		return turn.ProfileRef{}, err
	}
	l, err := loop.New(exec, planner, policy)
	if err != nil {
		return turn.ProfileRef{}, err
	}
	r.mu.Lock()
	r.byID[id] = registeredAgent{agent: agent, driver: loopDriver{loop: l, runtime: r.runtime, sink: r.sink}}
	r.mu.Unlock()
	return turn.ProfileRef{ID: id, Digest: digest}, nil
}

// Resolve returns the registration's driver when the ref's digest matches the
// agent's current Profile (REF-BND-2).
func (r *Agents) Resolve(ref turn.ProfileRef) (RunDriver, error) {
	reg, err := r.lookup(ref)
	if err != nil {
		return nil, err
	}
	return reg.driver, nil
}

// Agent returns the registered agent behind a ref, digest-checked like
// Resolve; hosts use it for profile-model calls outside any Run (REF-CKP-1).
func (r *Agents) Agent(ref turn.ProfileRef) (Agent, error) {
	reg, err := r.lookup(ref)
	if err != nil {
		return nil, err
	}
	return reg.agent, nil
}

func (r *Agents) lookup(ref turn.ProfileRef) (registeredAgent, error) {
	r.mu.RLock()
	reg, ok := r.byID[ref.ID]
	r.mu.RUnlock()
	if !ok {
		return registeredAgent{}, fmt.Errorf("ref: unknown profile %s", ref.ID)
	}
	p := reg.agent.Profile()
	digest, err := turn.DigestProfile(&p)
	if err != nil {
		return registeredAgent{}, err
	}
	if digest != ref.Digest {
		return registeredAgent{}, fmt.Errorf("ref: profile %s digest mismatch", ref.ID)
	}
	return reg, nil
}

// loopDriver is REF-DRV-1: Drive is loop.Run. A concurrent local driver of
// the same Run is reported as ErrAlreadyDriving, not as a failure.
type loopDriver struct {
	loop    *loop.Loop
	runtime run.Runtime
	sink    loop.EventSink
}

func (d loopDriver) Drive(ctx context.Context, req DriveRequest) error {
	_, err := d.loop.Run(ctx, d.runtime, req.Ref.SessionID, req.RunID, d.sink)
	if errors.Is(err, loop.ErrRunAlreadyRunning) {
		return fmt.Errorf("%w: %v", ErrAlreadyDriving, err)
	}
	return err
}
