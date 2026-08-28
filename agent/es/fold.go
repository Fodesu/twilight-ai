package es

import "fmt"

// FoldStandardRecords validates standard Record aggregate digests, then folds
// their events in record order. Domains with extra record metadata should
// validate that metadata themselves and use FoldRecords with RecordView.
func FoldStandardRecords[S any, E any](
	initial S,
	expectedStream StreamID,
	records []Record[E],
	supported SchemaSupported,
	inspect EventInspector[E],
	evolve func(schemaVersion uint16, state S, event E) (S, error),
) (S, Revision, error) {
	views := make([]RecordView[E], len(records))
	for i := range records {
		if err := ValidateRecord(&records[i], supported, inspect); err != nil {
			return initial, 0, err
		}
		views[i] = RecordView[E]{
			SchemaVersion: records[i].SchemaVersion,
			StreamID:      records[i].StreamID,
			Revision:      records[i].Revision,
			Events:        records[i].Events,
		}
	}
	return FoldRecords(initial, expectedStream, views, supported, inspect, evolve)
}

// FoldRecords validates a contiguous record sequence and folds each event in
// record order. It is purely mechanical: it never decides new events and does
// no IO. Domains provide an initial state, schema-aware event validation, and
// their own versioned evolve function.
func FoldRecords[S any, E any](
	initial S,
	expectedStream StreamID,
	records []RecordView[E],
	supported SchemaSupported,
	inspect EventInspector[E],
	evolve func(schemaVersion uint16, state S, event E) (S, error),
) (S, Revision, error) {
	state := initial
	if expectedStream == "" {
		return initial, 0, fmt.Errorf("es: fold: empty expected stream")
	}
	if evolve == nil {
		return initial, 0, fmt.Errorf("es: fold: nil evolve function")
	}
	var revision Revision
	for i, record := range records {
		if err := ValidateRecordView(record, supported, inspect); err != nil {
			return initial, 0, err
		}
		if record.StreamID != expectedStream {
			return initial, 0, fmt.Errorf("es: fold: record %d stream %q does not match expected stream %q", i, record.StreamID, expectedStream)
		}
		if record.Revision != revision+1 {
			return initial, 0, fmt.Errorf("es: fold: gap at revision %d (expected %d)", record.Revision, revision+1)
		}
		for _, event := range record.Events {
			var err error
			state, err = evolve(record.SchemaVersion, state, event)
			if err != nil {
				return initial, 0, err
			}
		}
		revision = record.Revision
	}
	return state, revision, nil
}
