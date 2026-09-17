package run

import (
	"fmt"

	"github.com/felinics/twilight/agent/es"
)

// SchemaVersion1 is the current pre-release wire schema. Its canonical
// encoding and Evolve folding semantics may still change before publication;
// a stream written by an earlier pre-release binary is not guaranteed to fold
// (the ModelStepRecovered transition moved from Prepared to Open under this
// number). Once a schema is published, its encoding and folding semantics are
// frozen: any later change to Evolve bumps the SchemaVersion (RUN-CMT-8).
const SchemaVersion1 uint16 = 1

// Schema is everything that is frozen at a Run's creation, as four separate
// contracts bound to one version number (RUN-CMT-7). Each contract can change
// independently in a later version; the aggregate exists so a caller selects
// a version once (SchemaFor at the Run header, the envelope or the event
// boundary) and does not thread the number through digest, Decide or Evolve.
// The zero Schema is unusable: callers that may hold one check Valid.
type Schema struct {
	Version uint16
	// Machine is the state machine: Decide and Evolve (RUN-MCH).
	Machine Machine
	// Wire names and decodes the fact and command variants (RUN-WIR-2).
	Wire WireSchema
	// Canonical is the digest rules for the bodies facts name (RUN-WIR-4).
	Canonical Canonical
	// Snapshot encodes a MachineState for durable storage and comparison.
	Snapshot SnapshotCodec
}

// Valid reports whether s is a bound schema.
func (s Schema) Valid() bool {
	return s.Version != 0 && s.Machine != nil && s.Wire != nil && s.Canonical != nil && s.Snapshot != nil
}

// Machine is the pure Run state machine of one schema version.
type Machine interface {
	// Decide produces the facts a command yields against a state, or the
	// rejection (RUN-MCH-1).
	Decide(MachineState, AgentCommand) ([]Fact, error)
	// Evolve folds one fact (RUN-MCH-3).
	Evolve(MachineState, Fact) (MachineState, error)
	// CreateGroup returns the RunCreated and InputAccepted facts that
	// establish a Run (RUN-NEW-1). It is pure: the owning module places
	// these facts in its creation commit.
	CreateGroup(NewRun, []AgentInput) ([]Fact, error)
}

// WireSchema names and (de)codes the sealed fact and command variants of one
// schema version. Encoding is canonical JSON of the variant under the typed
// payload envelope (schema version, type discriminator, body).
type WireSchema interface {
	// DecodeFact restores the sealed fact variant named by typ.
	DecodeFact(typ string, raw []byte) (Fact, error)
	// DecodeCommand restores the sealed command variant named by typ.
	DecodeCommand(typ string, raw []byte) (AgentCommand, error)
	// EncodeFact renders the typed payload bytes of a fact; typ must be the
	// fact's own discriminator.
	EncodeFact(typ string, fact Fact) ([]byte, error)
	// Envelope is the sanctioned envelope constructor (RUN-WIR-3).
	Envelope(run RunID, id CommandID, cmd AgentCommand) (CommandEnvelope, error)
}

// Canonical is the digest rules of one schema version for every body a fact
// names or a derived identity covers.
type Canonical interface {
	DigestRequest(ModelRequest) (Digest, error)
	DigestToolDefinition(ToolDefinition) (Digest, error)
	DigestToolSpec(ToolSpec) (Digest, error)
	DigestToolSpecs([]ToolSpec) (Digest, error)
	DigestModelStepBinding(model ModelRef, requestDigest, toolsDigest Digest) (Digest, error)
	DigestToolResponseDecision(ResponseKind, ResponseDecision, string) (Digest, error)
	DigestToolResponsePayload(CanonicalJSON) (Digest, error)
	// DigestModelResult names a frozen model result (ModelStepCompleted.ResultDigest).
	DigestModelResult(ModelResult) (Digest, error)
	// DigestToolOutput names one tool output (ToolCallCompleted.OutputDigest).
	DigestToolOutput(CanonicalJSON) (Digest, error)
}

// SnapshotCodec renders a MachineState to and from its canonical persisted
// bytes. The bytes are canonical: StatesEquivalent, the InitialStateDigest
// preimage and durable snapshot storage all use them.
type SnapshotCodec interface {
	Encode(*MachineState) ([]byte, error)
	Decode([]byte) (MachineState, error)
}

// CommandEnvelope carries one command with its protocol identity. Commands
// are not persisted: ID is the CommitID of the commit the command produces,
// and replay is told from conflict by the store's command index (RUN-WIR-2,
// EXT-WRT-2). The envelope names no store: which store a command reaches is
// the bound RunStore's.
type CommandEnvelope struct {
	SchemaVersion uint16       `json:"schemaVersion"`
	Type          string       `json:"type"`
	RunID         RunID        `json:"runId"`
	ID            CommandID    `json:"id"`
	Command       AgentCommand `json:"command"`
}

// encodeEnvelopeBody is the digest input for a command: schema version, type
// discriminator and canonical command bytes.
func encodeEnvelopeBody(schemaVersion uint16, typ string, body any) ([]byte, error) {
	return es.EncodeTypedPayload(schemaVersion, typ, body)
}

var schemaV1 = Schema{Version: SchemaVersion1, Machine: machineV1{}, Wire: wireV1{}, Canonical: canonicalV1{}, Snapshot: snapshotV1{}}

// SchemaV1 is the SchemaVersion1 binding. New Runs are created with it; every
// later operation on a Run binds through SchemaFor(header.SchemaVersion) or
// RuntimeSnapshot.Schema() (RUN-CMT-7). There are no package-level functions
// that implicitly select a version.
func SchemaV1() Schema { return schemaV1 }

// SchemaFor binds the schema of a persisted version.
func SchemaFor(schemaVersion uint16) (Schema, error) {
	switch schemaVersion {
	case SchemaVersion1:
		return schemaV1, nil
	default:
		return Schema{}, unsupportedSchemaVersion(schemaVersion)
	}
}

func unsupportedSchemaVersion(schemaVersion uint16) error {
	return fmt.Errorf("agent: unsupported schema version %d", schemaVersion)
}

// --- v1 machine -------------------------------------------------------------------

type machineV1 struct{}

func (machineV1) Decide(s MachineState, c AgentCommand) ([]Fact, error) { //nolint:gocritic // hugeParam: the machine is a pure value interpreter.
	return decideV1(s, c)
}

func (machineV1) Evolve(s MachineState, f Fact) (MachineState, error) { //nolint:gocritic // hugeParam: the machine is a pure value interpreter.
	return evolveV1(s, f)
}

func (machineV1) CreateGroup(run NewRun, inputs []AgentInput) ([]Fact, error) {
	if run.SchemaVersion != SchemaVersion1 {
		return nil, fmt.Errorf("agent: create group: run schema %d does not match schema %d", run.SchemaVersion, SchemaVersion1)
	}
	return buildCreateGroupV1(run, inputs)
}

// --- v1 wire ------------------------------------------------------------------------

type wireV1 struct{}

func (wireV1) DecodeFact(typ string, raw []byte) (Fact, error) { return decodeFactVariant(typ, raw) }

func (wireV1) DecodeCommand(typ string, raw []byte) (AgentCommand, error) {
	return decodeCommandVariant(typ, raw)
}

func (wireV1) EncodeFact(typ string, fact Fact) ([]byte, error) {
	if typ == "" || typ != factType(fact) {
		return nil, fmt.Errorf("agent: encode: type %q does not match fact variant", typ)
	}
	return encodeEnvelopeBody(SchemaVersion1, typ, fact)
}

func (wireV1) Envelope(run RunID, id CommandID, cmd AgentCommand) (CommandEnvelope, error) {
	typ := commandType(cmd)
	if typ == "" {
		return CommandEnvelope{}, fmt.Errorf("agent: envelope: unknown command variant %T", cmd)
	}
	return CommandEnvelope{SchemaVersion: SchemaVersion1, Type: typ, RunID: run, ID: id, Command: cmd}, nil
}

// --- v1 snapshot --------------------------------------------------------------------

type snapshotV1 struct{}

func (snapshotV1) Encode(s *MachineState) ([]byte, error)  { return encodeMachineStateV1(s) }
func (snapshotV1) Decode(raw []byte) (MachineState, error) { return decodeMachineStateV1(raw) }
