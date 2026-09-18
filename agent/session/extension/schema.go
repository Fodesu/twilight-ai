package extension

import (
	"encoding/json"
	"fmt"

	"github.com/felinics/twilight/agent/jsonstable"
	"github.com/felinics/twilight/agent/session"
)

// SchemaVersion is the application schema a Session segment is written
// under (EXT-SCH-1): the version every module's event codec, projection and
// Run protocol are interpreted with on that segment. It is also the `v`
// each payload carries (EXT-REG-2). A segment declares exactly one Schema in
// its creation metadata and never changes it; a logical Session moves to a
// newer Schema only through an explicit migration that publishes a new
// segment (SES-ADV-1). The kernel reads none of this (SES-VER-1).
type SchemaVersion uint16

// SchemaVersion1 is the first application schema.
const SchemaVersion1 SchemaVersion = 1

// SchemaKey is the first-level key of the segment metadata object holding
// the segment's SchemaVersion as an integer.
const SchemaKey = "twilight/schema"

// DeclareSchema returns meta with the segment's Schema declared. An absent
// or null meta becomes an object; a meta already declaring the same Schema
// is returned unchanged; a meta declaring another Schema or one that is not
// an object is refused.
func DeclareSchema(meta jsonstable.Value, v SchemaVersion) (jsonstable.Value, error) {
	if v == 0 {
		return jsonstable.Value{}, &Error{Code: ErrSchema, Detail: "schema version 0 is not a schema"}
	}
	m := map[string]json.RawMessage{}
	if !meta.IsZero() && string(meta.Bytes()) != "null" {
		if err := json.Unmarshal(meta.Bytes(), &m); err != nil {
			return jsonstable.Value{}, &Error{Code: ErrSchema, Detail: "segment metadata is not an object"}
		}
	}
	if raw, ok := m[SchemaKey]; ok {
		declared, err := parseSchema(raw)
		if err != nil {
			return jsonstable.Value{}, err
		}
		if declared != v {
			return jsonstable.Value{}, &Error{Code: ErrSchema, Detail: fmt.Sprintf("segment metadata declares schema %d, not %d", declared, v)}
		}
		return meta, nil
	}
	m[SchemaKey] = json.RawMessage(fmt.Sprintf("%d", v))
	return jsonstable.FromValue(m)
}

// SchemaMetadata is the creation metadata of a segment declaring only its
// Schema.
func SchemaMetadata(v SchemaVersion) jsonstable.Value {
	meta, err := DeclareSchema(jsonstable.Value{}, v)
	if err != nil {
		panic(err)
	}
	return meta
}

// SchemaOf reads the Schema a segment header declares. A header declaring
// none, or a malformed declaration, is an ErrSchema error: the Writer never
// guesses a Schema for a segment (EXT-SCH-1).
func SchemaOf(h session.SegmentHeader) (SchemaVersion, error) {
	if h.Metadata.IsZero() {
		return 0, &Error{Code: ErrSchema, Detail: "segment declares no schema"}
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(h.Metadata.Bytes(), &m); err != nil {
		return 0, &Error{Code: ErrSchema, Detail: "segment metadata is not an object"}
	}
	raw, ok := m[SchemaKey]
	if !ok {
		return 0, &Error{Code: ErrSchema, Detail: "segment declares no schema"}
	}
	return parseSchema(raw)
}

func parseSchema(raw json.RawMessage) (SchemaVersion, error) {
	var n uint64
	if err := json.Unmarshal(raw, &n); err != nil || n == 0 || n > 0xFFFF {
		return 0, &Error{Code: ErrSchema, Detail: fmt.Sprintf("%s is not a schema version: %s", SchemaKey, string(raw))}
	}
	return SchemaVersion(n), nil
}
