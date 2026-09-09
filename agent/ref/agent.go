package ref

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/memohai/twilight/agent/es"
	"github.com/memohai/twilight/agent/run"
	"github.com/memohai/twilight/agent/run/loop"
	"github.com/memohai/twilight/agent/session"
	"github.com/memohai/twilight/agent/turn"
	"github.com/memohai/twilight/sdk"
)

type PublicTool struct {
	Ref        run.ToolRef        `json:"ref"`
	Definition run.ToolDefinition `json:"definition"`
	Policy     run.ResponsePolicy `json:"policy"`
}

// Profile is the public configuration of an Agent (REF-BND-1). Credentials
// and clients stay in process; the Session records turn.ProfileRef{ID, Digest}.
type Profile struct {
	SchemaVersion uint16       `json:"schemaVersion"`
	Model         run.ModelRef `json:"model"`
	Tools         []PublicTool `json:"tools,omitempty"`
	Streaming     bool         `json:"streaming,omitempty"`
	// SystemPrompt tunes the conversation. It is outside the profile digest,
	// so editing it never orphans a resumable Turn.
	SystemPrompt string `json:"systemPrompt,omitempty"`
}

// DigestProfile covers the fields that change replay correctness:
// SchemaVersion, Model, Tools and Streaming. SystemPrompt is excluded.
func DigestProfile(p *Profile) (es.Digest, error) {
	body := struct {
		SchemaVersion uint16       `json:"schemaVersion"`
		Model         run.ModelRef `json:"model"`
		Tools         []PublicTool `json:"tools,omitempty"`
		Streaming     bool         `json:"streaming,omitempty"`
	}{p.SchemaVersion, p.Model, p.Tools, p.Streaming}
	raw, err := es.EncodeTypedPayload(1, "twilight/ref/profile", body)
	if err != nil {
		return "", err
	}
	return es.DigestBytes(raw), nil
}

// ToolSpecs derives the frozen ToolSpecs and provider definitions of p, in
// order (REF-PLN-4).
func (p *Profile) ToolSpecs() ([]run.ToolSpec, []sdk.ToolDefinition, error) {
	specs := make([]run.ToolSpec, 0, len(p.Tools))
	defs := make([]sdk.ToolDefinition, 0, len(p.Tools))
	for _, t := range p.Tools {
		d, err := run.ProtocolV1().DigestToolDefinition(t.Definition)
		if err != nil {
			return nil, nil, err
		}
		specs = append(specs, run.ToolSpec{Ref: t.Ref, Name: t.Definition.Name, DefinitionDigest: d, Policy: t.Policy})
		defs = append(defs, t.Definition.SDK())
	}
	return specs, defs, nil
}

// Agent is one registrable execution configuration: the durable Profile plus
// the live capabilities that resolve it. Implement it directly for custom
// catalogs, or build the common shape with NewAgent.
type Agent interface {
	Profile() Profile
	ResolveModel(run.ModelRef) (loop.ModelInvoker, error)
	ResolveTool(run.ToolRef) (loop.ExecutableTool, error)
}

// PolicyProvider is optional: an Agent that tunes the Loop's execution policy.
type PolicyProvider interface {
	Policy() loop.ExecutionPolicy
}

type agentConfig struct {
	tools        []loop.ExecutableTool
	systemPrompt string
	streaming    bool
	policy       loop.ExecutionPolicy
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

func WithPolicy(p loop.ExecutionPolicy) AgentOption {
	return func(c *agentConfig) { c.policy = p }
}

// NewAgent builds the common one-model Agent: the Profile is assembled from
// the model ref and the tools' frozen definitions.
func NewAgent(model run.ModelRef, invoker loop.ModelInvoker, opts ...AgentOption) (Agent, error) {
	if model == "" || invoker == nil {
		return nil, errors.New("ref: agent requires a model ref and an invoker")
	}
	var cfg agentConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	a := &builtAgent{
		profile: Profile{SchemaVersion: 1, Model: model, Streaming: cfg.streaming, SystemPrompt: cfg.systemPrompt},
		invoker: invoker,
		tools:   make(map[run.ToolRef]loop.ExecutableTool, len(cfg.tools)),
		policy:  cfg.policy,
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
	return a, nil
}

type builtAgent struct {
	profile Profile
	invoker loop.ModelInvoker
	tools   map[run.ToolRef]loop.ExecutableTool
	policy  loop.ExecutionPolicy
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

func (a *builtAgent) Policy() loop.ExecutionPolicy { return a.policy }

// Agents is the in-process turn.ProfileRegistry (REF-BND-2). Register builds
// one long-lived Loop per registration, so every drive of a profile shares
// the already-driving guard: a second local driver of a running Run reports
// turn.ErrAlreadyDriving instead of racing the first.
type Agents struct {
	runtime     run.Runtime
	projections ProjectionSource
	sink        loop.EventSink

	mu   sync.RWMutex
	byID map[turn.ProfileID]registeredAgent
}

type registeredAgent struct {
	agent  Agent
	driver turn.RunDriver
}

// ProjectionSource is what the planner reads context from.
type ProjectionSource interface {
	Load(ctx context.Context, sid session.SessionID, id extensionProjectionID, v extensionProjectionVersion) (any, session.Head, error)
}

func NewAgents(runtime run.Runtime, projections ProjectionSource, sink loop.EventSink) *Agents {
	return &Agents{runtime: runtime, projections: projections, sink: sink, byID: map[turn.ProfileID]registeredAgent{}}
}

// Register stores an agent, builds its driver and returns the ref the Session
// records.
func (r *Agents) Register(id turn.ProfileID, agent Agent) (turn.ProfileRef, error) {
	if id == "" || agent == nil {
		return turn.ProfileRef{}, errors.New("ref: register requires an id and an agent")
	}
	p := agent.Profile()
	if p.Model == "" || p.SchemaVersion == 0 {
		return turn.ProfileRef{}, errors.New("ref: profile requires model and schemaVersion")
	}
	digest, err := DigestProfile(&p)
	if err != nil {
		return turn.ProfileRef{}, err
	}
	var policy loop.ExecutionPolicy
	if pp, ok := agent.(PolicyProvider); ok {
		policy = pp.Policy()
	}
	planner := &ContextPlanner{Projections: r.projections, Profile: p}
	l, err := loop.New(agent, agent, planner, policy, p.Streaming)
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
func (r *Agents) Resolve(ref turn.ProfileRef) (turn.RunDriver, error) {
	r.mu.RLock()
	reg, ok := r.byID[ref.ID]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("ref: unknown profile %s", ref.ID)
	}
	p := reg.agent.Profile()
	digest, err := DigestProfile(&p)
	if err != nil {
		return nil, err
	}
	if digest != ref.Digest {
		return nil, fmt.Errorf("ref: profile %s digest mismatch", ref.ID)
	}
	return reg.driver, nil
}

// loopDriver is TRN-DRV-1: Drive is loop.Run. A concurrent local driver of
// the same Run is reported as turn.ErrAlreadyDriving, not as a failure.
type loopDriver struct {
	loop    *loop.Loop
	runtime run.Runtime
	sink    loop.EventSink
}

func (d loopDriver) Drive(ctx context.Context, req turn.DriveRequest) error {
	_, err := d.loop.Run(ctx, d.runtime, req.Ref.SessionID, req.RunID, d.sink)
	if errors.Is(err, loop.ErrRunAlreadyRunning) {
		return fmt.Errorf("%w: %v", turn.ErrAlreadyDriving, err)
	}
	return err
}
