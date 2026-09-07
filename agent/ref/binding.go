// Package ref is the Memory reference assembly
// (docs/design/agent-reference-assembly.md): ExecutionBinding, the context
// Planner, the Session-scoped input router and the wiring of every layer into
// one in-process agent.
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

// PlannerID names the reference planner (REF 2).
const PlannerID = "twilight/turn/planner/context-v1"

type PublicTool struct {
	Ref        run.ToolRef        `json:"ref"`
	Definition run.ToolDefinition `json:"definition"`
	Policy     run.ResponsePolicy `json:"policy"`
}

// BindingPublic is the digest-covered public configuration of a binding
// (REF-BND-1). Credentials and clients stay in process.
type BindingPublic struct {
	SchemaVersion uint16       `json:"schemaVersion"`
	Model         run.ModelRef `json:"model"`
	Tools         []PublicTool `json:"tools,omitempty"`
	Streaming     bool         `json:"streaming,omitempty"`
	PlannerID     string       `json:"plannerId"`
	SystemPrompt  string       `json:"systemPrompt,omitempty"`
}

func DigestBinding(pub *BindingPublic) (es.Digest, error) {
	raw, err := es.EncodeTypedPayload(1, "twilight/turn/binding", pub)
	if err != nil {
		return "", err
	}
	return es.DigestBytes(raw), nil
}

// ToolSpecs derives the frozen ToolSpecs and provider definitions of pub, in
// order (REF-PLN-4).
func (pub *BindingPublic) ToolSpecs() ([]run.ToolSpec, []sdk.ToolDefinition, error) {
	specs := make([]run.ToolSpec, 0, len(pub.Tools))
	defs := make([]sdk.ToolDefinition, 0, len(pub.Tools))
	for _, t := range pub.Tools {
		d, err := run.ProtocolV1().DigestToolDefinition(t.Definition)
		if err != nil {
			return nil, nil, err
		}
		specs = append(specs, run.ToolSpec{Ref: t.Ref, Name: t.Definition.Name, DefinitionDigest: d, Policy: t.Policy})
		defs = append(defs, t.Definition.SDK())
	}
	return specs, defs, nil
}

// Binding is one resolvable execution binding: public config plus the
// in-process model and tool catalogs.
type Binding struct {
	Public BindingPublic
	Models loop.ModelCatalog
	Tools  loop.ToolCatalog
	Policy loop.ExecutionPolicy
}

// Bindings is the in-process ExecutionBindingRegistry (REF-BND-2).
type Bindings struct {
	runtime     run.Runtime
	projections ProjectionSource
	sink        loop.EventSink

	mu   sync.RWMutex
	byID map[turn.ExecutionBindingID]Binding
}

// ProjectionSource is what the planner reads context from.
type ProjectionSource interface {
	Load(ctx context.Context, sid session.SessionID, id extensionProjectionID, v extensionProjectionVersion) (any, session.Head, error)
}

func NewBindings(runtime run.Runtime, projections ProjectionSource, sink loop.EventSink) *Bindings {
	return &Bindings{runtime: runtime, projections: projections, sink: sink, byID: map[turn.ExecutionBindingID]Binding{}}
}

// Register stores a binding and returns the ref the Session records.
func (b *Bindings) Register(id turn.ExecutionBindingID, binding Binding) (turn.ExecutionBindingRef, error) {
	if id == "" || binding.Models == nil || binding.Tools == nil || binding.Public.Model == "" {
		return turn.ExecutionBindingRef{}, errors.New("ref: binding requires id, model, model catalog and tool catalog")
	}
	if binding.Public.SchemaVersion == 0 {
		binding.Public.SchemaVersion = 1
	}
	if binding.Public.PlannerID == "" {
		binding.Public.PlannerID = PlannerID
	}
	digest, err := DigestBinding(&binding.Public)
	if err != nil {
		return turn.ExecutionBindingRef{}, err
	}
	b.mu.Lock()
	b.byID[id] = binding
	b.mu.Unlock()
	return turn.ExecutionBindingRef{ID: id, Digest: digest}, nil
}

// Resolve returns a RunDriver when the ref's digest matches the registered
// public configuration (REF-BND-2).
func (b *Bindings) Resolve(ref turn.ExecutionBindingRef) (turn.RunDriver, error) {
	b.mu.RLock()
	binding, ok := b.byID[ref.ID]
	b.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("ref: unknown binding %s", ref.ID)
	}
	digest, err := DigestBinding(&binding.Public)
	if err != nil {
		return nil, err
	}
	if digest != ref.Digest {
		return nil, fmt.Errorf("ref: binding %s digest mismatch", ref.ID)
	}
	planner := &ContextPlanner{Projections: b.projections, Public: binding.Public}
	l, err := loop.New(binding.Models, binding.Tools, planner, binding.Policy, binding.Public.Streaming)
	if err != nil {
		return nil, err
	}
	return loopDriver{loop: l, runtime: b.runtime, sink: b.sink}, nil
}

// loopDriver is TRN-DRV-1: Drive is loop.Run.
type loopDriver struct {
	loop    *loop.Loop
	runtime run.Runtime
	sink    loop.EventSink
}

func (d loopDriver) Drive(ctx context.Context, req turn.DriveRequest) error {
	_, err := d.loop.Run(ctx, d.runtime, req.Ref.SessionID, req.RunID, d.sink)
	return err
}
