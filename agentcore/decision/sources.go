// Package decision is the decision seam of the agent core
// (docs/design/agent-decision.md): the catalog that resolves an AgentPreset's
// PromptBuilderRef to a PromptBuilder, the Sources a builder reads from, and
// the v1 input content codec. It holds no builder of its own: how a model
// request is assembled from Session state is a strategy of the agent built on
// the core (agent/prompt is the first-party one), named by a ref the
// AgentPreset digest covers, so the process that takes a Turn over resolves
// the same function from the same catalog.
package decision

import (
	"fmt"

	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/session/chatlog"
	"github.com/felinics/twilight/agentcore/session/extension"
)

// ProjectionSource is what a prompt builder reads state from: the owner
// process serves it from the Session Writer (writer.Projections), an observer
// from the Store.
type ProjectionSource = extension.ProjectionReader

// Sources are the two read ports of a prompt builder (DEC-PMT-1): the
// structural projections and the content resolver that materializes the
// frozen bodies the projections name by digest. Folding is pure; reading a
// body is I/O and happens only here.
type Sources struct {
	Projections ProjectionSource
	Content     chatlog.ContentResolver
}

func v1InputText(content run.CanonicalJSON) (string, error) {
	var body struct {
		Text string `json:"text"`
	}
	if err := content.Decode(&body); err != nil {
		return "", fmt.Errorf("decision: input payload: %w", err)
	}
	return body.Text, nil
}

// InputContent is the v1 user body shape (DEC-INP-1): the same canonical
// JSON is the chatlog Input content and the Run AgentInput payload. The
// constructor lives in chatlog, which owns the Input.Content wire shape.
func InputContent(text string) run.CanonicalJSON {
	return chatlog.TextContent(text)
}

// InputText is the v1 inverse of InputContent: the user text of an input
// payload (DEC-INP-1). Hosts use it to render transcripts.
func InputText(content run.CanonicalJSON) (string, error) { return v1InputText(content) }
