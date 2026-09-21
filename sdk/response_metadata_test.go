package sdk

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// A reply without metadata must not invent any: a missing Unix timestamp is
// the zero time, a zero ResponseMetadata is omitted from the result's JSON,
// and both boundary paths hand out the same representation.
func TestResponseMetadataAbsentStaysAbsent(t *testing.T) {
	if got := TimestampFromUnix(0); !got.IsZero() {
		t.Fatalf("TimestampFromUnix(0) = %v, want the zero time", got)
	}
	if got := TimestampFromUnix(1700000000); got.Location() != time.UTC || got.Unix() != 1700000000 {
		t.Fatalf("TimestampFromUnix(1700000000) = %v, want the same instant in UTC", got)
	}

	local := time.Unix(1700000000, 0).In(time.FixedZone("X", 8*3600))
	cases := []struct {
		name         string
		response     ResponseMetadata
		wantJSONKey  bool
		wantLocation *time.Location
	}{
		{"zero metadata is omitted", ResponseMetadata{}, false, nil},
		{"id alone is kept", ResponseMetadata{ID: "r1"}, true, nil},
		{"timestamp is normalized to UTC", ResponseMetadata{ID: "r1", Timestamp: local}, true, time.UTC},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			provider := boundaryProvider{
				generate: func(Request) (ModelResult, error) { return ModelResult{Response: tc.response}, nil },
				stream: func(Request) (<-chan StreamPart, error) {
					ch := make(chan StreamPart, 2)
					ch <- &FinishStepPart{Response: tc.response}
					close(ch)
					return ch, nil
				},
			}
			model := &Model{ID: "m", Provider: provider}
			generated, err := model.Generate(context.Background(), Request{Model: "m"})
			if err != nil {
				t.Fatal(err)
			}
			stream, err := model.Stream(context.Background(), Request{Model: "m"})
			if err != nil {
				t.Fatal(err)
			}
			for range stream.Parts {
			}
			streamed, err := stream.Result()
			if err != nil {
				t.Fatal(err)
			}
			g, _ := json.Marshal(generated)
			s, _ := json.Marshal(*streamed)
			if string(g) != string(s) {
				t.Fatalf("paths disagree:\n generate: %s\n stream:   %s", g, s)
			}
			if got := strings.Contains(string(g), `"response"`); got != tc.wantJSONKey {
				t.Fatalf("response key present = %v, want %v: %s", got, tc.wantJSONKey, g)
			}
			if tc.wantLocation != nil && generated.Response.Timestamp.Location() != tc.wantLocation {
				t.Fatalf("timestamp location = %v, want %v", generated.Response.Timestamp.Location(), tc.wantLocation)
			}
		})
	}
}
