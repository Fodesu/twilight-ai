package decision

import (
	"fmt"

	"github.com/felinics/twilight/agent/run/loop"
	"github.com/felinics/twilight/agent/turn"
)

// PlannerFactory builds the RequestPlanner of one PlannerRef for one Profile.
// The factory is pure configuration: the planner it returns reads state only
// through the ProjectionSource (DEC-CAT-1).
type PlannerFactory func(turn.Profile, ProjectionSource) loop.RequestPlanner

// PlannerCatalog resolves PlannerRefs on the authority side (DEC-CAT-1).
type PlannerCatalog struct {
	factories map[turn.PlannerRef]PlannerFactory
}

// NewPlannerCatalog builds a catalog; empty refs and nil factories are
// rejected, duplicates conflict.
func NewPlannerCatalog(entries map[turn.PlannerRef]PlannerFactory) (*PlannerCatalog, error) {
	c := &PlannerCatalog{factories: make(map[turn.PlannerRef]PlannerFactory, len(entries))}
	for ref, f := range entries {
		if err := c.Register(ref, f); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// Register adds one planner; the ref must be new.
func (c *PlannerCatalog) Register(ref turn.PlannerRef, f PlannerFactory) error {
	if ref == "" || f == nil {
		return fmt.Errorf("decision: planner registration requires a ref and a factory")
	}
	if _, dup := c.factories[ref]; dup {
		return fmt.Errorf("decision: planner %q registered twice", ref)
	}
	c.factories[ref] = f
	return nil
}

// Resolve returns the planner of profile.Planner or ErrUnknownPlanner.
func (c *PlannerCatalog) Resolve(profile turn.Profile, projections ProjectionSource) (loop.RequestPlanner, error) {
	f, ok := c.factories[profile.Planner]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownPlanner, profile.Planner)
	}
	return f(profile, projections), nil
}

// PolicyDefaultV1 names the default execution policy: parallel tool
// execution, no bound on local workers, a malformed model result fails the
// Run (DEC-POL-1). Its value is the zero loop.ExecutionPolicy, named so the
// Profile can refer to it.
const PolicyDefaultV1 turn.PolicyRef = "twilight/decision/policy/default-v1"

// DefaultPolicy is the value behind PolicyDefaultV1.
var DefaultPolicy = loop.ExecutionPolicy{}

// PolicyCatalog resolves PolicyRefs on the authority side (DEC-CAT-1).
type PolicyCatalog struct {
	policies map[turn.PolicyRef]loop.ExecutionPolicy
}

// NewPolicyCatalog builds a catalog; empty refs are rejected, duplicates
// conflict.
func NewPolicyCatalog(entries map[turn.PolicyRef]loop.ExecutionPolicy) (*PolicyCatalog, error) {
	c := &PolicyCatalog{policies: make(map[turn.PolicyRef]loop.ExecutionPolicy, len(entries))}
	for ref, p := range entries {
		if err := c.Register(ref, p); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// Register adds one policy; the ref must be new.
func (c *PolicyCatalog) Register(ref turn.PolicyRef, p loop.ExecutionPolicy) error {
	if ref == "" {
		return fmt.Errorf("decision: policy registration requires a ref")
	}
	if _, dup := c.policies[ref]; dup {
		return fmt.Errorf("decision: policy %q registered twice", ref)
	}
	c.policies[ref] = p
	return nil
}

// Resolve returns the policy of ref or ErrUnknownPolicy.
func (c *PolicyCatalog) Resolve(ref turn.PolicyRef) (loop.ExecutionPolicy, error) {
	p, ok := c.policies[ref]
	if !ok {
		return loop.ExecutionPolicy{}, fmt.Errorf("%w: %q", ErrUnknownPolicy, ref)
	}
	return p, nil
}

// Catalogs bundles the decision catalogs an authority resolves Profiles with.
type Catalogs struct {
	Planners *PlannerCatalog
	Policies *PolicyCatalog
}

// DefaultCatalogs holds the first-party decision components: the context
// planner and the default policy.
func DefaultCatalogs() Catalogs {
	planners, _ := NewPlannerCatalog(map[turn.PlannerRef]PlannerFactory{PlannerContextV1: NewContextPlanner})
	policies, _ := NewPolicyCatalog(map[turn.PolicyRef]loop.ExecutionPolicy{PolicyDefaultV1: DefaultPolicy})
	return Catalogs{Planners: planners, Policies: policies}
}

// Resolve resolves both components of a Profile (DEC-CAT-2).
func (c Catalogs) Resolve(profile turn.Profile, projections ProjectionSource) (loop.RequestPlanner, loop.ExecutionPolicy, error) {
	if c.Planners == nil || c.Policies == nil {
		return nil, loop.ExecutionPolicy{}, fmt.Errorf("decision: catalogs are incomplete")
	}
	planner, err := c.Planners.Resolve(profile, projections)
	if err != nil {
		return nil, loop.ExecutionPolicy{}, err
	}
	policy, err := c.Policies.Resolve(profile.Policy)
	if err != nil {
		return nil, loop.ExecutionPolicy{}, err
	}
	return planner, policy, nil
}
