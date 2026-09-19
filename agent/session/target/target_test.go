package target_test

import (
	"context"
	"errors"
	"testing"

	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/loop"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/extension"
	"github.com/felinics/twilight/agent/session/target"
)

var (
	ws1 = run.TargetRef{Kind: "workspace", ID: "ws-1"}
	ws2 = run.TargetRef{Kind: "workspace", ID: "ws-2"}
)

// The projection keeps the latest bound target; a payload of another shape
// is a fold error.
func TestProjectionKeepsLatestBound(t *testing.T) {
	state, err := target.Projection.Initial()
	if err != nil {
		t.Fatal(err)
	}
	if cur := state.(target.Current); cur.Target != nil {
		t.Fatalf("initial = %+v, want unbound", cur)
	}
	for _, ref := range []run.TargetRef{ws1, ws2} {
		if state, err = target.Projection.Apply(state, extension.DecodedEvent{Value: target.BoundPayload{Target: ref}}); err != nil {
			t.Fatal(err)
		}
	}
	if cur := state.(target.Current); cur.Target == nil || *cur.Target != ws2 {
		t.Fatalf("current = %+v, want %+v", cur, ws2)
	}
	if _, err := target.Projection.Apply(state, extension.DecodedEvent{Value: "bound"}); err == nil {
		t.Fatal("a foreign payload folded")
	}
}

// The bound wire under Schema 1 and its payload check.
func TestBoundWire(t *testing.T) {
	reg, err := extension.BuildRegistry(session.ProtocolVersion1, target.Module)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := reg.Encode(target.TypeBound, target.BoundPayload{Target: ws1}, extension.SchemaVersion1)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := wire.String(), `{"target":{"id":"ws-1","kind":"workspace"},"v":1}`; got != want {
		t.Fatalf("bound wire = %s, want %s", got, want)
	}
	if _, err := reg.Encode(target.TypeBound, target.BoundPayload{Target: run.TargetRef{Kind: "workspace"}}, extension.SchemaVersion1); err == nil {
		t.Fatal("bound without an id encoded")
	}
}

type fakeReader struct {
	state any
	err   error
}

func (r fakeReader) Load(context.Context, session.SessionID, extension.ProjectionID, extension.ProjectionVersion) (any, session.Head, error) {
	return r.state, session.Head{}, r.err
}

func TestResolver(t *testing.T) {
	boom := errors.New("boom")
	tests := []struct {
		name     string
		kind     loop.AssignmentKind
		state    any
		err      error
		required bool
		want     *run.TargetRef
		wantErr  error
		anyErr   bool
	}{
		{name: "model effect has no target", kind: loop.AssignmentModel, state: target.Current{Target: &ws1}},
		{name: "tool effect resolves the bound target", kind: loop.AssignmentTool, state: target.Current{Target: &ws1}, want: &ws1},
		{name: "unbound gives no target", kind: loop.AssignmentTool, state: target.Current{}},
		{name: "unbound is refused when required", kind: loop.AssignmentTool, state: target.Current{}, required: true, wantErr: target.ErrUnbound},
		{name: "read failure", kind: loop.AssignmentTool, err: boom, wantErr: boom},
		{name: "foreign state", kind: loop.AssignmentTool, state: "bound", anyErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := &target.Resolver{Projections: fakeReader{state: tc.state, err: tc.err}, Required: tc.required}
			got, err := r.ResolveTarget(context.Background(), loop.EffectContext{Session: "s-1", Kind: tc.kind})
			switch {
			case tc.anyErr:
				if err == nil {
					t.Fatal("want an error")
				}
			case !errors.Is(err, tc.wantErr):
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			if (got == nil) != (tc.want == nil) || (got != nil && *got != *tc.want) {
				t.Fatalf("target = %v, want %v", got, tc.want)
			}
		})
	}
}
