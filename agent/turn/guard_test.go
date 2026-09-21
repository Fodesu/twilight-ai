package turn

import (
	"errors"
	"strings"
	"testing"

	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/extension"
)

// guardView is a writer.View whose only answer is the surface projection.
type guardView struct {
	state any
	err   error
}

func (guardView) Head() session.Head                                     { return session.Head{} }
func (guardView) Epoch() session.Epoch                                   { return 0 }
func (guardView) Schema() extension.PayloadVersion                       { return 1 }
func (guardView) Header() session.SegmentHeader                          { return session.SegmentHeader{} }
func (guardView) Committed(session.CommitID) bool                        { return false }
func (guardView) StreamHead(session.StreamRef) (session.StreamSeq, bool) { return 0, false }
func (guardView) LookupCommit(session.CommitID) (session.Commit, bool, error) {
	return session.Commit{}, false, nil
}
func (v guardView) Projection(extension.ProjectionID, extension.ProjectionVersion) (any, error) {
	return v.state, v.err
}

func surfaceWith(status TurnStatus) TurnSurface {
	return TurnSurface{Order: []TurnID{"t1"}, Turns: map[TurnID]TurnView{"t1": {TurnID: "t1", Status: status}}}
}

func TestRequireNoActiveTurn(t *testing.T) {
	boom := errors.New("projection unavailable")
	cases := map[string]struct {
		view    guardView
		wantErr error  // errors.Is
		mention string // substring of the error
	}{
		"no turns":          {view: guardView{state: TurnSurface{}}},
		"completed turn":    {view: guardView{state: surfaceWith(TurnCompleted)}},
		"active turn":       {view: guardView{state: surfaceWith(TurnActive)}, wantErr: ErrConflict, mention: "t1"},
		"projection failed": {view: guardView{err: boom}, wantErr: boom},
		"foreign state":     {view: guardView{state: "nope"}, mention: "string"},
	}
	for name, tc := range cases {
		err := RequireNoActiveTurn(tc.view)
		if tc.wantErr == nil && tc.mention == "" {
			if err != nil {
				t.Errorf("%s: err = %v, want nil", name, err)
			}
			continue
		}
		if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
			t.Errorf("%s: err = %v, want %v", name, err, tc.wantErr)
		}
		if tc.mention != "" && (err == nil || !strings.Contains(err.Error(), tc.mention)) {
			t.Errorf("%s: err = %v, want one mentioning %q", name, err, tc.mention)
		}
	}
}
