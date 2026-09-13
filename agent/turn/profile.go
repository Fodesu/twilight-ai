package turn

import (
	"errors"

	"github.com/felinics/twilight/agent/es"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/sdk"
)

type (
	// PlannerRef names the decision component that assembles model requests
	// for a Turn (DEC-PLN). It is part of the Profile digest.
	PlannerRef string
	// PolicyRef names the execution policy the Loop runs under (DEC-POL). It
	// is part of the Profile digest.
	PolicyRef string
	// WorkspaceRef names the execution environment a Turn's effects run in.
	// The core does not interpret it; it is recorded so a takeover knows the
	// environment the Turn was started in.
	WorkspaceRef = run.WorkspaceRef
)

// PublicTool is one tool of a Profile: its ref, frozen definition and
// response policy. ToolSpecs and the provider-facing tool list both derive
// from it.
type PublicTool struct {
	Ref        run.ToolRef        `json:"ref"`
	Definition run.ToolDefinition `json:"definition"`
	Policy     run.ResponsePolicy `json:"policy"`
}

// Profile is the decision identity a Turn is started under (TRN-SCP-6): every
// input to the decision layer that must be the same when another process
// resumes the Turn. The Session records ProfileRef{ID, Digest}; credentials,
// clients and tool implementations never enter it.
type Profile struct {
	SchemaVersion uint16       `json:"schemaVersion"`
	Model         run.ModelRef `json:"model"`
	Tools         []PublicTool `json:"tools,omitempty"`
	Streaming     bool         `json:"streaming,omitempty"`
	// Planner and Policy are decision components resolved on the authority
	// side; Workspace is the environment identity handed to effect execution.
	Planner   PlannerRef   `json:"planner"`
	Policy    PolicyRef    `json:"policy"`
	Workspace WorkspaceRef `json:"workspace,omitempty"`
	// SystemPrompt tunes the conversation. It is outside the profile digest,
	// so editing it never orphans a resumable Turn.
	SystemPrompt string `json:"systemPrompt,omitempty"`
}

// ProfileDigestDomain is the digest domain of DigestProfile.
const ProfileDigestDomain = "twilight/turn/profile"

// DigestProfile covers the fields that change what the decision layer does
// for a Turn: SchemaVersion, Model, Tools, Streaming, Planner, Policy and
// Workspace. SystemPrompt is excluded (TRN-PRF-1).
func DigestProfile(p *Profile) (es.Digest, error) {
	body := struct {
		SchemaVersion uint16       `json:"schemaVersion"`
		Model         run.ModelRef `json:"model"`
		Tools         []PublicTool `json:"tools,omitempty"`
		Streaming     bool         `json:"streaming,omitempty"`
		Planner       PlannerRef   `json:"planner"`
		Policy        PolicyRef    `json:"policy"`
		Workspace     WorkspaceRef `json:"workspace,omitempty"`
	}{p.SchemaVersion, p.Model, p.Tools, p.Streaming, p.Planner, p.Policy, p.Workspace}
	raw, err := es.EncodeTypedPayload(1, ProfileDigestDomain, body)
	if err != nil {
		return "", err
	}
	return es.DigestBytes(raw), nil
}

// ValidateProfile checks the identity fields a registry must refuse to record
// without (TRN-PRF-2): schema version, model, planner and policy. Workspace
// may be empty.
func ValidateProfile(p *Profile) error {
	switch {
	case p.SchemaVersion == 0:
		return errors.New("turn: profile requires schemaVersion")
	case p.Model == "":
		return errors.New("turn: profile requires a model")
	case p.Planner == "":
		return errors.New("turn: profile requires a planner ref")
	case p.Policy == "":
		return errors.New("turn: profile requires a policy ref")
	}
	return nil
}

// ToolSpecs derives the frozen ToolSpecs and provider definitions of p, in
// order (DEC-PLN-4).
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
