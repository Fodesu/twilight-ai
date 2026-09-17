// Package spawn defines the subagent protocol primitives (SPN): the tool
// a model calls to delegate a task, the argument and result shapes, the
// deterministic child Session identity, and the provenance record that makes
// a spawn call recoverable after a crash.
//
// A subagent is an ordinary agent Session started by a ToolCall. The ToolCall
// is the invocation identity, the child Session is the durable history, the
// child's Turn and Run are its execution, and the Host drives it. Nothing new
// enters the Run fact ontology: the parent sees one tool call that completes
// with the child's reply.
//
// The binding from the call to the child is derived, not stored: the child's
// SessionID is a digest of (parent Session, parent Run, CallID), and the
// child segment's creation metadata records the provenance and the full
// arguments. That record is what a takeover needs to continue the same
// invocation after a crash; when the effect runs through a durable Worker,
// the Worker persists it as ExecutionRef{twilight/session, child} (RUN-EXE-9).
//
// This package is the protocol core. The orchestration that creates, drives
// and settles child Sessions lives in the Host (Ports.Spawn).
package spawn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/felinics/twilight/agent/es"
	"github.com/felinics/twilight/agent/jsonstable"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/effect"
	"github.com/felinics/twilight/agent/run/loop"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/turn"
	"github.com/felinics/twilight/sdk"
)

// DefaultTool is the ToolRef the model calls to start a subagent.
const DefaultTool run.ToolRef = "agent_spawn"

// DefaultDepth bounds how deep subagents may nest.
const DefaultDepth = 3

// Provider is the ExecutionRef provider of the subagent Backend.
const Provider = "twilight/session"

// MetadataKey is the metadata key the provenance lives under in the child
// segment's creation record.
const MetadataKey = "twilight/spawn"

// Mode selects where the child's history starts.
type Mode string

const (
	// Empty starts the child from an empty Session.
	Empty Mode = "spawn"
	// Fork starts the child from the parent's history before the Turn that
	// made the call (AUTH-FRK-2): the conversation so far, without the Turn
	// that is still executing.
	Fork Mode = "fork"
)

// Arguments is the shape of the spawn tool's arguments.
type Arguments struct {
	Task   string `json:"task"`
	Mode   Mode   `json:"mode,omitempty"`
	Preset string `json:"preset,omitempty"`
}

// Result is the spawn tool's output: the child and its settled Turn.
type Result struct {
	ChildSession session.SessionID `json:"childSession"`
	TurnID       turn.TurnID       `json:"turnId"`
	Status       turn.TurnStatus   `json:"status"`
	Reply        string            `json:"reply"`
}

// Provenance is the child segment's creation metadata under MetadataKey: who
// spawned it, with what, and how deep it sits.
type Provenance struct {
	ParentSession session.SessionID `json:"parentSession"`
	ParentRun     run.RunID         `json:"parentRun"`
	CallID        run.CallID        `json:"callId"`
	Depth         int               `json:"depth"`
	Arguments     Arguments         `json:"arguments"`
}

// ChildID derives the child Session of one spawn call. The identity is a
// function of the invocation alone, so a takeover recomputes it from the
// Executing call it finds (RUN-CMT-7).
func ChildID(parent session.SessionID, runID run.RunID, callID run.CallID) session.SessionID {
	raw, err := es.EncodeTypedPayload(1, "twilight/spawn/child", struct {
		Parent session.SessionID `json:"parent"`
		Run    run.RunID         `json:"run"`
		Call   run.CallID        `json:"call"`
	}{parent, runID, callID})
	if err != nil {
		panic(err) // three strings always encode
	}
	return session.SessionID("spawn-" + string(es.DigestBytes(raw))[len("sha256:"):])
}

// DecodeArguments decodes and validates the tool arguments.
func DecodeArguments(args run.CanonicalJSON) (Arguments, error) {
	var a Arguments
	if err := args.Decode(&a); err != nil {
		return a, err
	}
	if a.Task == "" {
		return a, errors.New("task is required")
	}
	switch a.Mode {
	case "":
		a.Mode = Empty
	case Empty, Fork:
	default:
		return a, fmt.Errorf("unknown mode %q", a.Mode)
	}
	return a, nil
}

// Metadata builds the segment creation metadata that records prov.
func Metadata(prov Provenance) (jsonstable.Value, error) {
	return jsonstable.FromValue(map[string]Provenance{MetadataKey: prov})
}

// ProvenanceFromHeader decodes the spawn record from a segment's creation
// metadata; ok is false when the segment carries none.
func ProvenanceFromHeader(header session.SegmentHeader) (prov Provenance, ok bool, err error) {
	if header.Metadata.IsZero() {
		return Provenance{}, false, nil
	}
	var meta map[string]json.RawMessage
	if err := header.Metadata.Decode(&meta); err != nil {
		return Provenance{}, false, nil
	}
	raw, ok := meta[MetadataKey]
	if !ok {
		return Provenance{}, false, nil
	}
	if err := json.Unmarshal(raw, &prov); err != nil {
		return Provenance{}, false, fmt.Errorf("spawn: provenance: %w", err)
	}
	return prov, true, nil
}

// CheckDefinition verifies the assignment's recorded definition digest and
// response policy against the tool's canonical definition (SPN-1). It
// returns a nil failure when they match.
func CheckDefinition(schema run.Schema, tool loop.ExecutableTool, assigned *effect.ToolAssignment) (*run.ToolFailure, error) {
	def, err := run.FreezeToolDefinition(tool.Definition())
	if err != nil {
		return &run.ToolFailure{Class: run.FailureDefinitionMismatch, Message: err.Error()}, nil
	}
	digest, err := schema.Canonical.DigestToolDefinition(def)
	if err != nil {
		return &run.ToolFailure{Class: run.FailureDefinitionMismatch, Message: err.Error()}, nil
	}
	switch {
	case digest != assigned.DefinitionDigest:
		return &run.ToolFailure{Class: run.FailureDefinitionMismatch, Message: "tool definition digest mismatch"}, nil
	case assigned.Policy != tool.ResponsePolicy():
		return &run.ToolFailure{Class: run.FailureDefinitionMismatch, Message: "response policy mismatch"}, nil
	}
	return nil, nil
}

// DepthExceeded reports whether a Session at depth has reached the nesting
// limit and may no longer spawn (SPN-3).
func DepthExceeded(depth, limit int) bool {
	return depth >= limit
}

// ArgumentsConflict reports whether a recorded child was created for
// different arguments than the call carries now (SPN-2, RUN-EXE-3).
func ArgumentsConflict(prov Provenance, args Arguments) bool {
	return prov.Arguments != args
}

// Tool is the model-facing definition of the spawn tool for preset
// catalogs. Its Execute never runs: the Host intercepts the tool's
// Assignments (Ports.Spawn). A catalog need not hold it.
func Tool(ref run.ToolRef) loop.ExecutableTool { return tool{ref: ref} }

type tool struct{ ref run.ToolRef }

func (t tool) Ref() run.ToolRef { return t.ref }

func (t tool) Definition() sdk.ToolDefinition {
	return sdk.ToolDefinition{
		Name:        string(t.ref),
		Description: "Delegate a task to a subagent that runs in its own session and returns its final reply. mode \"fork\" starts it from this conversation's history before the current turn; \"spawn\" (default) starts it empty.",
		Parameters: json.RawMessage(`{"type":"object","properties":{` +
			`"task":{"type":"string","description":"What the subagent should do."},` +
			`"mode":{"type":"string","enum":["spawn","fork"]},` +
			`"preset":{"type":"string","description":"Optional named preset the subagent runs under."}},` +
			`"required":["task"],"additionalProperties":false}`),
	}
}

func (tool) ResponsePolicy() run.ResponsePolicy { return run.DirectExecution }

func (tool) ValidateArguments(args run.CanonicalJSON) error {
	_, err := DecodeArguments(args)
	return err
}

func (t tool) Execute(context.Context, loop.ToolExecutionRequest) loop.ToolExecutionOutcome {
	return loop.ToolExecutionFailed{Failure: run.ToolFailure{Class: run.FailureExecution,
		Message: fmt.Sprintf("%s executes on the authority through the spawn effect (app.Config.Spawn)", t.ref)}}
}
