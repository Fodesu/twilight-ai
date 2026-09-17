package writer

import (
	"fmt"

	"github.com/felinics/twilight/agent/artifact"
	"github.com/felinics/twilight/agent/jsonstable"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/extension"
)

// bindingRef is one artifact reference an event declares: the BindingID, the
// declaration it must satisfy and where in the group it sits, for the
// commit's detail string. encode extracts them; the admission stage judges
// them.
type bindingRef struct {
	id    artifact.BindingID
	decl  *extension.BindingReferenceDefinition
	where string
}

func bindingIDs(refs []bindingRef) []artifact.BindingID {
	if len(refs) == 0 {
		return nil
	}
	out := make([]artifact.BindingID, len(refs))
	for i, r := range refs {
		out[i] = r.id
	}
	return out
}

// encode is the pure stage of the pipeline: it validates and encodes every
// event against the Registry, checks each batch's stream attribution against
// the event type's StreamPolicy, extracts the artifact references the events
// declare and returns the proposal batches. It touches no store.
func encode(registry *extension.Registry, group *SemanticGroup) ([]session.StreamBatch, []bindingRef, string, error) {
	batches := make([]session.StreamBatch, len(group.Batches))
	var refs []bindingRef
	for bi, tb := range group.Batches {
		events := make([]session.Event, len(tb.Events))
		for i, te := range tb.Events {
			where := fmt.Sprintf("batch %d event %d", bi, i)
			_, def, ok := registry.LookupEvent(te.Type)
			if !ok {
				return nil, nil, fmt.Sprintf("%s: unknown type %s", where, te.Type), nil
			}
			payload, _, err := registry.Encode(te.Type, te.Value)
			if err != nil {
				return nil, nil, fmt.Sprintf("%s: %v", where, err), nil
			}
			if verdict := checkStreamAffinity(tb.Stream, def.Stream, payload); verdict != "" {
				return nil, nil, fmt.Sprintf("%s: %s", where, verdict), nil
			}
			for d := range def.Bindings {
				decl := &def.Bindings[d]
				ids, err := decl.Extractor.BindingIDs(te.Value)
				if err != nil {
					return nil, nil, fmt.Sprintf("%s: binding extraction: %v", where, err), nil
				}
				if uint32(len(ids)) < decl.Cardinality.Min || (decl.Cardinality.Max != nil && uint32(len(ids)) > *decl.Cardinality.Max) {
					return nil, nil, fmt.Sprintf("%s: binding cardinality violated", where), nil
				}
				for _, id := range ids {
					refs = append(refs, bindingRef{id: id, decl: decl, where: where})
				}
			}
			events[i] = session.Event{Type: te.Type, RecordedAtUnixMilli: te.RecordedAtUnixMilli, Payload: payload}
		}
		batches[bi] = session.StreamBatch{Stream: tb.Stream, Events: events}
	}
	return batches, refs, "", nil
}

// checkStreamAffinity verifies a batch's stream attribution against the
// event type's declared StreamPolicy. It returns a human verdict for the
// commit's detail string; policies themselves are validated at BuildRegistry.
func checkStreamAffinity(stream session.StreamRef, pol extension.StreamPolicy, payload jsonstable.Value) string {
	switch pol.Kind {
	case session.StreamKindSession:
		if stream.Kind != session.StreamKindSession {
			return fmt.Sprintf("event is session-scoped but the batch is %s", stream.Kind)
		}
	case session.StreamKindRun:
		if stream.Kind != session.StreamKindRun {
			return fmt.Sprintf("event is run-scoped but the batch is %s", stream.Kind)
		}
		decoded, err := payload.Any()
		if err != nil {
			return fmt.Sprintf("payload is not decodable for the stream binding: %v", err)
		}
		fields, ok := decoded.(map[string]any)
		if !ok {
			return "payload is not an object"
		}
		id, ok := fields[pol.IDField].(string)
		if !ok || id == "" {
			return fmt.Sprintf("payload lacks the stream binding field %q", pol.IDField)
		}
		if id != stream.ID {
			return fmt.Sprintf("payload %s %q does not match the batch stream %q", pol.IDField, id, stream.ID)
		}
	default:
		return "event type declares no stream policy"
	}
	return ""
}
