package es

import (
	"errors"
	"testing"
)

type testEvent struct {
	Schema uint16
	Stream StreamID
	Rev    Revision
	Pos    Index
	Value  int
}

func testInspector(event testEvent) (EventMetadata, error) {
	if event.Value < 0 {
		return EventMetadata{}, errors.New("negative payload")
	}
	return EventMetadata{
		SchemaVersion: event.Schema,
		StreamID:      event.Stream,
		Revision:      event.Rev,
		Index:         event.Pos,
	}, nil
}

func v1(version uint16) bool { return version == 1 }

func validRecord() Record[testEvent] {
	record := Record[testEvent]{
		SchemaVersion: 1,
		StreamID:      "stream-1",
		Revision:      1,
		Events: []testEvent{
			{Schema: 1, Stream: "stream-1", Rev: 1, Pos: 0, Value: 2},
			{Schema: 1, Stream: "stream-1", Rev: 1, Pos: 1, Value: 3},
		},
	}
	digest, err := DigestRecord(&record)
	if err != nil {
		panic(err)
	}
	record.RecordDigest = digest
	return record
}

func TestStandardEventAndRecord(t *testing.T) {
	event, err := BuildEvent(1, "stream-1", 1, 0, "added", "cause-1", map[string]string{"x": "y"})
	if err != nil {
		t.Fatal(err)
	}
	record := Record[Event[map[string]string]]{
		SchemaVersion: 1,
		StreamID:      "stream-1",
		Revision:      1,
		Events:        []Event[map[string]string]{event},
	}
	record.RecordDigest, err = DigestRecord(&record)
	if err != nil {
		t.Fatal(err)
	}
	inspector := StandardEventInspector[map[string]string](v1)
	if err := ValidateRecord(&record, v1, inspector); err != nil {
		t.Fatal(err)
	}
	state, revision, err := FoldStandardRecords(0, "stream-1", []Record[Event[map[string]string]]{record}, v1, inspector,
		func(_ uint16, state int, _ Event[map[string]string]) (int, error) { return state + 1, nil })
	if err != nil || state != 1 || revision != 1 {
		t.Fatalf("standard fold = state %d revision %d err %v", state, revision, err)
	}
	record.Events[0].Payload["x"] = "tampered"
	if err := ValidateRecord(&record, v1, StandardEventInspector[map[string]string](v1)); err == nil {
		t.Fatal("tampered payload accepted")
	}
}

func TestValidateRecord(t *testing.T) {
	record := validRecord()
	if err := ValidateRecord(&record, v1, testInspector); err != nil {
		t.Fatal(err)
	}

	record.Events[1].Pos = 2
	if err := ValidateRecord(&record, v1, testInspector); err == nil {
		t.Fatal("index gap accepted")
	}

	record = validRecord()
	record.RecordDigest = "sha256:tampered"
	if err := ValidateRecord(&record, v1, testInspector); err == nil {
		t.Fatal("tampered aggregate digest accepted")
	}
}

func TestFoldRecords(t *testing.T) {
	first := validRecord()
	second := Record[testEvent]{
		SchemaVersion: 1,
		StreamID:      "stream-1",
		Revision:      2,
		Events: []testEvent{
			{Schema: 1, Stream: "stream-1", Rev: 2, Pos: 0, Value: 5},
		},
	}
	views := []RecordView[testEvent]{
		{SchemaVersion: first.SchemaVersion, StreamID: first.StreamID, Revision: first.Revision, Events: first.Events},
		{SchemaVersion: second.SchemaVersion, StreamID: second.StreamID, Revision: second.Revision, Events: second.Events},
	}
	state, revision, err := FoldRecords(0, "stream-1", views, v1, testInspector,
		func(_ uint16, state int, event testEvent) (int, error) { return state + event.Value, nil })
	if err != nil {
		t.Fatal(err)
	}
	if state != 10 || revision != 2 {
		t.Fatalf("fold = state %d revision %d, want 10/2", state, revision)
	}

	views[1].Revision = 3
	if _, _, err := FoldRecords(0, "stream-1", views, v1, testInspector,
		func(_ uint16, state int, event testEvent) (int, error) { return state + event.Value, nil }); err == nil {
		t.Fatal("revision gap accepted")
	}
}

func TestCanonicalDigest(t *testing.T) {
	left, err := DigestCanonical(map[string]any{"b": 2, "a": 1})
	if err != nil {
		t.Fatal(err)
	}
	right, err := DigestCanonical(map[string]any{"a": 1, "b": 2})
	if err != nil {
		t.Fatal(err)
	}
	if left != right {
		t.Fatalf("canonical digests differ: %s != %s", left, right)
	}
	if _, err := Canonicalize([]byte(`{"a":1,"a":2}`)); err == nil {
		t.Fatal("duplicate JSON object key accepted")
	}
}
