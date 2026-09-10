package chatlog

import (
	"fmt"
	"testing"

	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/extension"
)

// benchEvents builds n assistant rows, the cheapest event that grows both the
// maps and EntryOrder of each projection.
func benchEvents(n int) []extension.DecodedEvent {
	out := make([]extension.DecodedEvent, n)
	for i := range out {
		out[i] = extension.DecodedEvent{
			Event: session.SessionEvent{Seq: session.Seq(i)},
			Value: AssistantPayload{Assistant: Assistant{ID: AssistantID(fmt.Sprint(i)), Digest: "sha256:x"}},
		}
	}
	return out
}

func benchFold(b *testing.B, def extension.ProjectionDefinition, n int) {
	events := benchEvents(n)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		state, err := def.Initial()
		if err != nil {
			b.Fatal(err)
		}
		for _, e := range events {
			if state, err = def.Apply(state, e); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// Both folds copy the whole derived state once per event, so folding an N-row
// log costs O(N^2). These benchmarks are the baseline that shows it: each time
// N doubles, elapsed time grows several-fold rather than twofold. The cost is
// paid by OpenWriter's rebuild and ProjectionReader.Load, which both fold the
// entire log, so it grows with session length.
func BenchmarkSurfaceFold(b *testing.B) {
	for _, n := range []int{400, 800, 1600, 3200} {
		b.Run(fmt.Sprint(n), func(b *testing.B) { benchFold(b, SurfaceProjection, n) })
	}
}

func BenchmarkContextFold(b *testing.B) {
	for _, n := range []int{400, 800, 1600, 3200} {
		b.Run(fmt.Sprint(n), func(b *testing.B) { benchFold(b, ContextProjection, n) })
	}
}
