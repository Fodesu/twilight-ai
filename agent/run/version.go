package run

import (
	"fmt"
)

// SchemaVersion1 is the current pre-release wire schema. Its canonical
// encoding and Evolve folding semantics may still change before publication;
// a stream written by an earlier pre-release binary is not guaranteed to fold
// (the ModelStepRecovered transition moved from Prepared to Open under this
// number). Once a schema is published, its encoding and folding semantics are
// frozen: any later change to Evolve bumps the SchemaVersion (RUN-CMT-8).
const SchemaVersion1 uint16 = 1

// UnsupportedSchemaVersion is the error every version table returns for a
// version it has no binding for.
func UnsupportedSchemaVersion(schemaVersion uint16) error {
	return fmt.Errorf("agent: unsupported schema version %d", schemaVersion)
}
