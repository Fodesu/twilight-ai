package schema

import (
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/canonical"
	"github.com/felinics/twilight/agentcore/run/frozen"
	"github.com/felinics/twilight/agentcore/run/wire"
)

// Schema is everything that is frozen at a Run's creation, as six separate
// contracts bound to one version number (RUN-CMT-8). Each contract can change
// independently in a later version; the aggregate exists so a caller selects
// a version once (For at the Run header, the envelope or the event
// boundary) and does not thread the number through digest, Decide or Evolve.
// The zero Schema is unusable: callers that may hold one check Valid.
type Schema struct {
	Version uint16
	// Machine is the state machine: Decide and Evolve (RUN-MCH).
	Machine run.Machine
	// Wire names and decodes the fact and command variants (RUN-WIR-2).
	Wire wire.Codec
	// Canonical is the digest rules for the bodies facts name (RUN-WIR-4).
	Canonical run.Canonical
	// Snapshot encodes a MachineState for durable storage and comparison.
	Snapshot wire.SnapshotCodec
	// Identity derives every RunID-scoped identity: step, call, response and
	// command ids (RUN-WIR-3). Identity derivation is persisted semantics --
	// a fact carries the ids it derived -- so it is versioned with the rest.
	Identity run.Identity
	// Bodies encodes the frozen bodies facts name by digest (RUN-WIR-4) as
	// the versioned envelopes their digests are computed from.
	Bodies frozen.Codec
}

// Valid reports whether s is a bound schema.
func (s Schema) Valid() bool {
	return s.Version != 0 && s.Machine != nil && s.Wire != nil && s.Canonical != nil && s.Snapshot != nil && s.Identity != nil && s.Bodies != nil
}

var v1 = Schema{
	Version:   run.SchemaVersion1,
	Machine:   run.MachineV1{Canonical: canonical.V1{}, Identity: canonical.IdentityV1{}},
	Wire:      wire.V1{},
	Canonical: canonical.V1{},
	Snapshot:  wire.SnapshotV1{},
	Identity:  canonical.IdentityV1{},
	Bodies:    frozen.V1{},
}

// V1 is the SchemaVersion1 binding. A Run is created under the Schema
// of the Session segment it lands on (RUN-NEW-1); every later operation on
// it binds through For(the version of its facts) or
// runtime.Snapshot.Schema() (RUN-CMT-8). There are no package-level
// functions that implicitly select a version.
func V1() Schema { return v1 }

// For binds the schema of a persisted version.
func For(schemaVersion uint16) (Schema, error) {
	switch schemaVersion {
	case run.SchemaVersion1:
		return v1, nil
	default:
		return Schema{}, run.UnsupportedSchemaVersion(schemaVersion)
	}
}
